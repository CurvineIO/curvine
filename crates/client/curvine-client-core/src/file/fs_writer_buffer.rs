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
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;

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

async fn recv_task(
    chunk_receiver: &mut AsyncReceiver<DataSlice>,
    task_receiver: &mut AsyncReceiver<WriterTask>,
) -> Option<SelectTask> {
    tokio::select! {
        biased;

        // Control tasks take priority. Flush/complete/seek/resize drain the data
        // queue explicitly, while cancel intentionally does not; prioritizing
        // data here would keep writing queued chunks after cancellation arrived.
        task = task_receiver.recv() => task.map(SelectTask::Control),
        chunk = chunk_receiver.recv() => chunk.map(SelectTask::Data),
    }
}

struct BufferChannel {
    path: String,
    chunk_sender: AsyncSender<DataSlice>,
    task_sender: AsyncSender<WriterTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    /// Resolves when the background writer task exits (normally or with an
    /// error), so pending handshakes can wake up instead of waiting for their
    /// full timeout.
    exited: watch::Receiver<bool>,
    /// Upper bound for one request/response handshake with the background
    /// task. Without it a wedged-but-alive task would leave callers pending
    /// forever (see https://github.com/CurvineIO/curvine/issues/1908).
    timeout: Duration,
}

impl BufferChannel {
    fn check_error(&self, e: impl Into<FsError>) -> FsError {
        self.err_monitor.take_error().unwrap_or(e.into())
    }

    /// Run one request/response handshake with a deadline, so a background
    /// task that is alive but no longer responding cannot block the caller
    /// forever. When the background task has already exited, the call fails
    /// immediately instead of waiting for the deadline.
    async fn guard<F, T, E>(&self, fut: F) -> FsResult<T>
    where
        F: Future<Output = Result<T, E>>,
        E: Into<FsError>,
    {
        let mut exited = self.exited.clone();
        // Failed fast when the task already exited before this call.
        if *exited.borrow_and_update() {
            return Err(self.task_exited_error());
        }

        let res = time::timeout(self.timeout, async {
            tokio::select! {
                biased;
                _ = exited.changed() => Err(self.task_exited_error()),
                res = fut => res.map_err(|e| e.into()),
            }
        })
        .await;

        match res {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(self.check_error(e)),
            Err(_) => {
                let e = FsError::common(format!(
                    "write buffer task not responding after {:?}, path={}",
                    self.timeout, self.path
                ));
                error!("{}", e);
                Err(self.check_error(e))
            }
        }
    }

    fn task_exited_error(&self) -> FsError {
        FsError::common(format!(
            "write buffer background task has exited, path={}",
            self.path
        ))
    }

    async fn write(&mut self, data: DataSlice) -> FsResult<()> {
        self.guard(self.chunk_sender.send(data)).await
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
        self.guard(fun).await
    }

    async fn flush(&mut self) -> FsResult<()> {
        let fun = async {
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(WriterTask::Flush(tx)).await?;
            rx.receive().await?;
            Ok::<(), IOError>(())
        };
        self.guard(fun).await
    }

    async fn cancel(&mut self) -> FsResult<()> {
        let fun = async {
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(WriterTask::Cancel(tx)).await?;
            rx.receive().await??;
            Ok::<(), FsError>(())
        };
        self.guard(fun).await
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        let fun = async {
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(WriterTask::Seek((pos, tx))).await?;
            rx.receive().await?;
            Ok::<(), IOError>(())
        };
        self.guard(fun).await
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
        self.guard(fun).await
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
        let timeout =
            Duration::from_millis(writer.fs_context().conf.client.write_buffer_timeout_ms);

        let (chunk_sender, chunk_receiver) = AsyncChannel::new(chunk_num).split();
        let (task_sender, task_receiver) = AsyncChannel::new(2).split();
        let (exited_tx, exited_rx) = watch::channel(false);
        let exited_tx2 = exited_tx.clone();
        let monitor = err_monitor.clone();

        let rt = writer.fs_context().clone_runtime();
        let handle: JoinHandle<()> = rt.spawn(async move {
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
            // Wake up every in-flight handshake so callers fail fast instead
            // of waiting for their timeout.
            let _ = exited_tx2.send(true);
        });

        // The background task must survive the writer being polled; keeping
        // the handle here (instead of dropping it) also lets a watchdog turn
        // an unexpected task panic into a visible error for pending callers.
        let watchdog_path = path.path().to_string();
        let watchdog_monitor = err_monitor.clone();
        rt.spawn(async move {
            if let Err(e) = handle.await {
                if e.is_panic() {
                    let err = FsError::common(format!(
                        "write buffer background task panicked, path={}: {}",
                        watchdog_path, e
                    ));
                    error!("{}", err);
                    watchdog_monitor.set_error(err);
                }
            }
            let _ = exited_tx.send(true);
        });

        let writer = BufferChannel {
            path: path.path().to_string(),
            chunk_sender,
            task_sender,
            err_monitor,
            exited: exited_rx,
            timeout,
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
            let select_task = match recv_task(&mut chunk_receiver, &mut task_receiver).await {
                Some(task) => task,
                None => return Ok(()),
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
    use super::{recv_task, BufferChannel, SelectTask, WriterTask};
    use bytes::Bytes;
    use curvine_error::FsResult;
    use curvine_io::DataSlice;
    use curvine_runtime::sync::channel::{AsyncChannel, CallChannel};
    use curvine_runtime::sync::ErrorMonitor;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;
    use tokio::time;

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

    // Build a BufferChannel whose background side is fully controlled by the
    // test: nothing answers the handshakes unless the test does.
    fn test_channel(timeout_ms: u64) -> (BufferChannel, watch::Sender<bool>) {
        let (chunk_sender, chunk_receiver) = AsyncChannel::new(2).split();
        let (task_sender, task_receiver) = AsyncChannel::new(2).split();
        let (exited_tx, exited_rx) = watch::channel(false);
        std::mem::forget((chunk_receiver, task_receiver));

        let channel = BufferChannel {
            path: "/gh-1908-test".to_string(),
            chunk_sender,
            task_sender,
            err_monitor: Arc::new(ErrorMonitor::new()),
            exited: exited_rx,
            timeout: Duration::from_millis(timeout_ms),
        };
        (channel, exited_tx)
    }

    // Regression test for #1908: the background task is alive but stops
    // polling, so the handshake can never complete. flush()/complete() must
    // return an error within the configured deadline instead of hanging.
    #[tokio::test]
    async fn handshakes_time_out_when_background_task_stops_responding() {
        let (mut channel, _exited_tx) = test_channel(200);

        let res = time::timeout(Duration::from_secs(5), channel.flush()).await;
        assert!(res.is_ok(), "flush() hung instead of returning an error");
        let err = res.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("not responding"),
            "unexpected error: {}",
            err
        );

        let (mut channel, _exited_tx) = test_channel(200);
        let res = time::timeout(Duration::from_secs(5), channel.complete()).await;
        assert!(res.is_ok(), "complete() hung instead of returning an error");
        let err = res.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("not responding"),
            "unexpected error: {}",
            err
        );
    }

    // When the background task exits, an in-flight request must be woken up
    // immediately with an error, without waiting for its timeout.
    #[tokio::test]
    async fn exited_background_task_wakes_pending_requests() {
        let (mut channel, exited_tx) = test_channel(60 * 1000);

        let handle = tokio::spawn(async move { channel.flush().await });
        time::sleep(Duration::from_millis(100)).await;
        // Simulate the watchdog noticing that the background task exited.
        let _ = exited_tx.send(true);

        let res = time::timeout(Duration::from_secs(5), handle).await;
        assert!(res.is_ok(), "pending flush() was not woken up on task exit");
        let err = res.unwrap().unwrap().unwrap_err();
        assert!(
            err.to_string().contains("exited"),
            "unexpected error: {}",
            err
        );
    }

    // A handshake issued after the background task already exited must fail
    // fast instead of waiting for the deadline.
    #[tokio::test]
    async fn handshake_fails_fast_when_task_already_exited() {
        let (mut channel, exited_tx) = test_channel(60 * 1000);
        let _ = exited_tx.send(true);

        let res = time::timeout(Duration::from_secs(5), channel.flush()).await;
        assert!(res.is_ok(), "flush() hung on an already-exited task");
        let err = res.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("exited"),
            "unexpected error: {}",
            err
        );
    }

    // After a timeout has been observed, the error is recorded and later
    // calls keep seeing it via check_error() semantics.
    #[tokio::test]
    async fn timeout_error_is_visible_to_later_calls() {
        let (mut channel, _exited_tx) = test_channel(200);

        let err = channel.flush().await.unwrap_err();
        assert!(err.to_string().contains("not responding"));

        // The next call fails fast: the channel is unusable now.
        let res = time::timeout(Duration::from_secs(5), channel.flush()).await;
        assert!(res.is_ok());
        assert!(res.unwrap().is_err());
    }
}
