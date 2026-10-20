// Copyright 2025 OPPO.
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

use crate::file::FsWriterBase;
use curvine_error::FsResult;
use curvine_error::{ErrorKind, FsError};
use curvine_fs_api::Path;
use curvine_io::DataSlice;
use curvine_io::IOError;
use curvine_model::{FileAllocOpts, FileBlocks, FileStatus, SetAttrOpts};
use curvine_runtime::runtime::RpcRuntime;
use curvine_runtime::sync::channel::{
    AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender,
};
use curvine_runtime::sync::ErrorMonitor;
use log::{debug, error};
use std::sync::Arc;
use std::time::Duration;

// Control task type
enum WriterTask {
    Flush(CallSender<i8>),
    Complete((bool, Option<SetAttrOpts>, CallSender<i8>)),
    Cancel(CallSender<FsResult<()>>),
    Seek((i64, CallSender<i8>)),
    Resize((FileAllocOpts, CallSender<FileBlocks>)),
}

enum SelectTask {
    Control(WriterTask),
    Data(DataSlice),
}

#[cfg(test)]
async fn recv_task(
    chunk_receiver: &mut AsyncReceiver<DataSlice>,
    task_receiver: &mut AsyncReceiver<WriterTask>,
) -> Option<SelectTask> {
    recv_task_until(chunk_receiver, task_receiver, None)
        .await
        .flatten()
}

/// Waits for the next data chunk or control task, giving up after `idle`.
///
/// The outer `None` means a channel closed and the writer must stop. The inner
/// `Option` reports whether work arrived: `Some(None)` is an idle turn, which is
/// exactly when an underlying connection needs a heartbeat and can never be
/// reached by a plain blocking wait. `idle = None` keeps the wait unbounded, so a
/// client without a heartbeat configured pays nothing extra.
async fn recv_task_until(
    chunk_receiver: &mut AsyncReceiver<DataSlice>,
    task_receiver: &mut AsyncReceiver<WriterTask>,
    idle: Option<Duration>,
) -> Option<Option<SelectTask>> {
    let wait = async {
        tokio::select! {
            biased;

            // Control tasks take priority. Flush/complete/seek/resize drain the
            // data queue explicitly, while cancel intentionally does not;
            // prioritizing data here would keep writing queued chunks after
            // cancellation arrived.
            task = task_receiver.recv() => task.map(SelectTask::Control),
            chunk = chunk_receiver.recv() => chunk.map(SelectTask::Data),
        }
    };

    match idle {
        None => Some(wait.await),
        Some(idle) => match tokio::time::timeout(idle, wait).await {
            Ok(next) => Some(next),
            Err(_) => Some(None),
        },
    }
}

struct BufferChannel {
    chunk_sender: AsyncSender<DataSlice>,
    task_sender: AsyncSender<WriterTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
}

impl BufferChannel {
    fn check_error(&self, e: impl Into<FsError>) -> FsError {
        self.err_monitor.take_error().unwrap_or(e.into())
    }

    async fn write(&mut self, data: DataSlice) -> FsResult<()> {
        self.chunk_sender
            .send(data)
            .await
            .map_err(|e| self.check_error(e))
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.complete_with_attr(None).await
    }

    async fn complete_with_attr(&mut self, opts: Option<SetAttrOpts>) -> FsResult<()> {
        let fun = async {
            let (tx, rx) = CallChannel::channel();
            self.task_sender
                .send(WriterTask::Complete((false, opts, tx)))
                .await?;
            rx.receive().await?;
            Ok::<(), IOError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    async fn flush(&mut self) -> FsResult<()> {
        let fun = async {
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(WriterTask::Flush(tx)).await?;
            rx.receive().await?;
            Ok::<(), IOError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    async fn cancel(&mut self) -> FsResult<()> {
        let fun = async {
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(WriterTask::Cancel(tx)).await?;
            rx.receive().await??;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        let fun = async {
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(WriterTask::Seek((pos, tx))).await?;
            rx.receive().await?;
            Ok::<(), IOError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    async fn resize(&mut self, opts: FileAllocOpts) -> FsResult<FileBlocks> {
        let fun = async {
            let (tx, rx) = CallChannel::channel();
            self.task_sender
                .send(WriterTask::Resize((opts, tx)))
                .await?;
            let file_blocks = rx.receive().await?;
            Ok::<FileBlocks, IOError>(file_blocks)
        };
        fun.await.map_err(|e| self.check_error(e))
    }
}

// Reader with buffer.
pub struct FsWriterBuffer {
    path: Path,
    file_blocks: FileBlocks,
    writer: BufferChannel,
    pos: i64,
}

impl FsWriterBuffer {
    pub fn new(writer: FsWriterBase, chunk_num: usize) -> Self {
        let err_monitor = Arc::new(ErrorMonitor::new());
        let path = writer.path().clone();
        let file_blocks = writer.file_blocks();
        let pos = writer.pos();

        let (chunk_sender, chunk_receiver) = AsyncChannel::new(chunk_num).split();
        let (task_sender, task_receiver) = AsyncChannel::new(2).split();
        let monitor = err_monitor.clone();

        let rt = writer.fs_context().clone_runtime();
        rt.spawn(async move {
            let res = Self::write_future(chunk_receiver, task_receiver, writer).await;
            match res {
                Ok(_) => {}
                Err(e) => {
                    if matches!(e.kind(), ErrorKind::FileNotFound) {
                        debug!("buffer writer error: {:?}", e);
                    } else {
                        error!("buffer writer error: {:?}", e);
                    }

                    monitor.set_error(e);
                }
            }
        });

        let writer = BufferChannel {
            chunk_sender,
            task_sender,
            err_monitor,
        };

        Self {
            path,
            file_blocks,
            writer,
            pos,
        }
    }

    pub fn path_str(&self) -> &str {
        self.path.path()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn status(&self) -> &FileStatus {
        &self.file_blocks.status
    }

    pub fn file_blocks(&self) -> &FileBlocks {
        &self.file_blocks
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    pub async fn write(&mut self, data: DataSlice) -> FsResult<()> {
        let len = data.len();
        if len == 0 {
            return Ok(());
        }
        self.writer.write(data).await?;
        self.pos += len as i64;
        Ok(())
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        self.writer.complete().await
    }

    pub async fn complete_with_attr(&mut self, opts: Option<SetAttrOpts>) -> FsResult<()> {
        self.writer.complete_with_attr(opts).await
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        self.writer.flush().await
    }

    pub async fn cancel(&mut self) -> FsResult<()> {
        self.writer.cancel().await
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        self.writer.seek(pos).await?;
        self.pos = pos;
        Ok(())
    }

    pub async fn resize(&mut self, opts: FileAllocOpts) -> FsResult<()> {
        let file_blocks = self.writer.resize(opts).await?;
        self.pos = self.pos.min(file_blocks.len);
        self.file_blocks = file_blocks;
        Ok(())
    }

    async fn write_future(
        mut chunk_receiver: AsyncReceiver<DataSlice>,
        mut task_receiver: AsyncReceiver<WriterTask>,
        mut writer: FsWriterBase,
    ) -> FsResult<()> {
        loop {
            // The queue can be written and controlled to complete any future.
            //
            // The wait is bounded by the writer's idle heartbeat period: an
            // unbounded wait would only ever observe work, so the one situation
            // that needs a heartbeat - a connection silent long enough for the
            // peer to reap it - would be unreachable. On an idle timeout the
            // loop wakes, refreshes the connections, and goes back to waiting.
            let pending = recv_task_until(
                &mut chunk_receiver,
                &mut task_receiver,
                writer.keepalive_interval(),
            );
            let Some(select_task) = pending.await else {
                return Ok(());
            };
            let Some(select_task) = select_task else {
                // No work arrived within the idle window; keep the underlying
                // write connections alive and resume waiting.
                let _ = writer.keepalive().await;
                continue;
            };

            match select_task {
                SelectTask::Control(task) => match task {
                    WriterTask::Flush(cx) => {
                        while let Some(chunk) = chunk_receiver.try_recv()? {
                            writer.write(chunk).await?;
                        }
                        writer.flush().await?;
                        cx.send(1)?;
                    }

                    WriterTask::Complete((_, opts, tx)) => {
                        while let Some(chunk) = chunk_receiver.try_recv()? {
                            writer.write(chunk).await?;
                        }
                        writer.complete_with_attr(opts).await?;
                        tx.send(1)?;
                        return Ok(());
                    }

                    WriterTask::Cancel(tx) => {
                        // Do not drain chunk_receiver: queued chunks are
                        // uncommitted data and cancellation intentionally drops
                        // them. FsWriterBase::cancel attempts every active block
                        // writer before returning its first error.
                        let res = writer.cancel().await;
                        tx.send(res)?;
                        return Ok(());
                    }

                    WriterTask::Seek((pos, tx)) => {
                        // Process all buffered data first
                        while let Some(chunk) = chunk_receiver.try_recv()? {
                            writer.write(chunk).await?
                        }

                        // Execute seek operation
                        writer.seek(pos).await?;

                        if let Err(e) = tx.send(1) {
                            return Err(e.into());
                        }
                    }

                    WriterTask::Resize((opts, tx)) => {
                        while let Some(chunk) = chunk_receiver.try_recv()? {
                            writer.write(chunk).await?
                        }

                        writer.resize(opts).await?;

                        if let Err(e) = tx.send(writer.file_blocks()) {
                            return Err(e.into());
                        }
                    }
                },

                SelectTask::Data(chunk) => {
                    writer.write(chunk).await?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{recv_task, recv_task_until, SelectTask, WriterTask};
    use bytes::Bytes;
    use curvine_error::FsResult;
    use curvine_io::DataSlice;
    use curvine_runtime::sync::channel::{AsyncChannel, CallChannel};
    use std::time::Duration;

    /// The case that makes an idle heartbeat reachable at all: with nothing
    /// queued the bounded wait must give up and report an idle turn instead of
    /// blocking forever, otherwise a silent connection is never refreshed.
    #[tokio::test]
    async fn idle_wait_reports_idle_instead_of_blocking() {
        let (_chunk_tx, mut chunk_rx) = AsyncChannel::<DataSlice>::new(2).split();
        let (_task_tx, mut task_rx) = AsyncChannel::<WriterTask>::new(2).split();

        let idle = tokio::time::timeout(
            Duration::from_secs(5),
            recv_task_until(&mut chunk_rx, &mut task_rx, Some(Duration::from_millis(20))),
        )
        .await
        .expect("bounded wait returned");

        // Some(None) = a live channel that had no work for the whole window.
        assert!(matches!(idle, Some(None)), "expected an idle turn");
    }

    /// Work still arrives promptly when it exists: the idle bound must not delay
    /// the normal path.
    #[tokio::test]
    async fn bounded_wait_still_returns_work_immediately() {
        let (chunk_tx, mut chunk_rx) = AsyncChannel::<DataSlice>::new(2).split();
        let (_task_tx, mut task_rx) = AsyncChannel::<WriterTask>::new(2).split();
        chunk_tx
            .send(DataSlice::Bytes(Bytes::from_static(b"chunk")))
            .await
            .unwrap();

        let next = recv_task_until(&mut chunk_rx, &mut task_rx, Some(Duration::from_secs(30)))
            .await
            .expect("channel open");
        assert!(matches!(next, Some(SelectTask::Data(_))));
    }

    /// An idle turn must not swallow a chunk that lands right after the wait gave
    /// up: the next wait has to observe it. This is the cancel-safety property the
    /// whole mechanism rests on - if dropping the timeout future lost a message,
    /// enabling the heartbeat would silently corrupt every quiet write.
    #[tokio::test]
    async fn chunk_arriving_after_an_idle_turn_is_not_lost() {
        let (chunk_tx, mut chunk_rx) = AsyncChannel::<DataSlice>::new(2).split();
        let (_task_tx, mut task_rx) = AsyncChannel::<WriterTask>::new(2).split();

        let first = recv_task_until(&mut chunk_rx, &mut task_rx, Some(Duration::from_millis(20)))
            .await
            .expect("channel open");
        assert!(first.is_none(), "expected an idle turn");

        chunk_tx
            .send(DataSlice::Bytes(Bytes::from_static(b"late")))
            .await
            .unwrap();

        let second = recv_task_until(&mut chunk_rx, &mut task_rx, Some(Duration::from_secs(5)))
            .await
            .expect("channel open");
        assert!(
            matches!(second, Some(SelectTask::Data(_))),
            "the chunk sent after the idle turn must still be delivered"
        );
    }

    #[tokio::test]
    async fn cancel_control_preempts_queued_data() {
        let (chunk_tx, mut chunk_rx) = AsyncChannel::new(2).split();
        let (task_tx, mut task_rx) = AsyncChannel::new(2).split();
        chunk_tx
            .send(DataSlice::Bytes(Bytes::from_static(b"queued")))
            .await
            .unwrap();
        let (cancel_tx, _cancel_rx) = CallChannel::channel::<FsResult<()>>();
        task_tx.send(WriterTask::Cancel(cancel_tx)).await.unwrap();

        assert!(matches!(
            recv_task(&mut chunk_rx, &mut task_rx).await,
            Some(SelectTask::Control(WriterTask::Cancel(_)))
        ));
    }
}
