//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::worker::handler::WriteContext;
use curvine_client::block::BlockWriterRemote;
use curvine_client::file::FsContext;
use curvine_common::error::FsError;
use curvine_common::state::WorkerAddress;
use curvine_common::FsResult;
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::ErrorMonitor;
use orpc::sys::DataSlice;
use std::sync::Arc;

pub struct ClientPipelineForwarder {
    fs_context: Arc<FsContext>,
    queue_size: usize,
}

impl ClientPipelineForwarder {
    pub fn new(fs_context: Arc<FsContext>, queue_size: usize) -> Self {
        Self {
            fs_context,
            queue_size,
        }
    }

    pub async fn connect(&self, ctx: &WriteContext) -> FsResult<PipelineStream> {
        let next_worker = ctx
            .pipeline_stream
            .first()
            .ok_or_else(|| FsError::common("No pipeline stream worker specified"))?;
        let remaining: Vec<WorkerAddress> = ctx.pipeline_stream[1..].to_vec();

        let writer = BlockWriterRemote::new(
            &self.fs_context,
            ctx.block.clone(),
            next_worker.clone(),
            ctx.off,
        )
        .await?;

        Ok(PipelineStream::new(writer, remaining, self.queue_size))
    }
}

enum PipelineTask {
    Complete((bool, CallSender<i8>)),
}

struct PipelineChannel {
    worker_id: u32,
    data_sender: AsyncSender<DataSlice>,
    task_sender: AsyncSender<PipelineTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
}

impl PipelineChannel {
    fn new(writer: BlockWriterRemote, queue_size: usize) -> Self {
        let worker_id = writer.worker_address().worker_id;
        let (data_sender, data_receiver) = AsyncChannel::new(queue_size).split();
        let (task_sender, task_receiver) = AsyncChannel::new(2).split();
        let err_monitor = Arc::new(ErrorMonitor::new());
        let monitor = err_monitor.clone();

        tokio::spawn(async move {
            let res = pipeline_writer_task(writer, data_receiver, task_receiver).await;
            if let Err(e) = res {
                monitor.set_error(e);
            }
        });

        Self {
            worker_id,
            data_sender,
            task_sender,
            err_monitor,
        }
    }

    fn wrap_error(&self, err: FsError) -> FsError {
        FsError::pipeline_error(self.worker_id, err.to_string())
    }

    fn check_error(&self, err: FsError) -> FsError {
        self.wrap_error(self.err_monitor.take_error().unwrap_or(err))
    }

    async fn write(&mut self, data: DataSlice) -> FsResult<()> {
        self.err_monitor
            .check_error()
            .map_err(|e| self.wrap_error(e))?;
        self.data_sender
            .send(data)
            .await
            .map_err(|e| self.check_error(e.into()))?;
        self.err_monitor
            .check_error()
            .map_err(|e| self.wrap_error(e))?;
        Ok(())
    }

    async fn complete(&mut self, commit: bool) -> FsResult<()> {
        let (tx, rx) = CallChannel::channel();
        self.task_sender
            .send(PipelineTask::Complete((commit, tx)))
            .await
            .map_err(|e| self.check_error(e.into()))?;
        rx.receive().await.map_err(|e| self.check_error(e.into()))?;
        self.err_monitor
            .check_error()
            .map_err(|e| self.wrap_error(e))?;
        Ok(())
    }
}

pub struct PipelineStream {
    writer: Option<PipelineChannel>,
    remaining_pipeline_stream: Vec<WorkerAddress>,
}

impl PipelineStream {
    fn new(
        writer: BlockWriterRemote,
        remaining_pipeline_stream: Vec<WorkerAddress>,
        queue_size: usize,
    ) -> Self {
        Self {
            writer: Some(PipelineChannel::new(writer, queue_size)),
            remaining_pipeline_stream,
        }
    }

    pub fn established_count(&self) -> i32 {
        self.remaining_pipeline_stream.len() as i32 + 1
    }

    pub async fn write(&mut self, data: DataSlice) -> FsResult<()> {
        match self.writer.as_mut() {
            Some(writer) => writer.write(data).await,
            None => Ok(()),
        }
    }

    pub async fn complete(&mut self, commit: bool) -> FsResult<()> {
        match self.writer.as_mut() {
            Some(writer) => writer.complete(commit).await,
            None => Ok(()),
        }
    }
}

impl Drop for PipelineStream {
    fn drop(&mut self) {
        if let Some(mut writer) = self.writer.take() {
            tokio::spawn(async move {
                let _ = writer.complete(false).await;
            });
        }
    }
}

async fn pipeline_writer_task(
    mut writer: BlockWriterRemote,
    mut data_receiver: AsyncReceiver<DataSlice>,
    mut task_receiver: AsyncReceiver<PipelineTask>,
) -> FsResult<()> {
    loop {
        let task = tokio::select! {
            biased;

            ctrl = task_receiver.recv_check() => {
                ctrl.map(SelectTask::Control)?
            }

            data = data_receiver.recv_check() => {
                data.map(SelectTask::Data)?
            }
        };

        match task {
            SelectTask::Data(chunk) => {
                writer.write(chunk).await?;
            }

            SelectTask::Control(PipelineTask::Complete((commit, tx))) => {
                while let Some(chunk) = data_receiver.try_recv()? {
                    writer.write(chunk).await?;
                }
                if commit {
                    writer.complete().await?;
                } else {
                    writer.cancel().await?;
                }
                tx.send(1)?;
                return Ok(());
            }
        }
    }
}

enum SelectTask {
    Control(PipelineTask),
    Data(DataSlice),
}
