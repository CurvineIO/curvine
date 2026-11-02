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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    /// Set once a handshake has timed out: the background task is not
    /// responding, so every later call fails immediately instead of queuing
    /// another request and waiting out a fresh timeout. The writer is
    /// considered abandoned at that point (see
    /// https://github.com/CurvineIO/curvine/issues/1908).
    abandoned: AtomicBool,
    /// Handle for the background write task. Kept so the latch can abort the
    /// task when it fires: a task parked with no timer never wakes to see the
    /// closed channels, and an abandoned writer would otherwise hold its
    /// `FsWriterBase` (worker connections, block lease) and queued chunks for
    /// the process lifetime. The master lease reclaims the unfinished file as
    /// it does after a client crash.
    task_handle: tokio::task::JoinHandle<()>,
    /// Upper bound for one request/response handshake with the background
    /// task. Without it a wedged-but-alive task would leave callers pending
    /// forever (see https://github.com/CurvineIO/curvine/issues/1908). Task
    /// exit does not need special handling: when the task returns or panics,
    /// the channel receivers and `CallChannel` senders are dropped, so any
    /// pending `send`/`receive` fails right away.
    timeout: Duration,
}

impl BufferChannel {
    fn check_error(&self, e: impl Into<FsError>) -> FsError {
        self.err_monitor.take_error().unwrap_or(e.into())
    }

    /// Run one request/response handshake with a deadline, so a background
    /// task that is alive but no longer responding cannot block the caller
    /// forever. The first timeout latches the channel as abandoned (and
    /// records the error in `err_monitor`), so later calls fail immediately
    /// instead of waiting another full timeout.
    async fn guard<F, T, E>(&self, fut: F) -> FsResult<T>
    where
        F: Future<Output = Result<T, E>>,
        E: Into<FsError>,
    {
        if self.abandoned.load(Ordering::Acquire) {
            return Err(self.check_error(FsError::common(format!(
                "write buffer task not responding, writer abandoned, path={}",
                self.path
            ))));
        }

        match time::timeout(self.timeout, fut).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(self.check_error(e)),
            Err(_) => {
                // The deadline fired while the handshake was in flight, so
                // the task may still be working on the queued request: the
                // file's final state is unknown from the caller's side. The
                // error says so instead of claiming the write failed.
                let e = FsError::common(format!(
                    "write buffer task not responding after {:?}, path={}: \
                     the write may still complete in the background, outcome unknown",
                    self.timeout, self.path
                ));
                error!("{}", e);
                // Latch: this writer is unusable now, fail every later call
                // immediately instead of waiting out another timeout. The
                // latched error also flows through err_monitor/check_error()
                // so later callers observe the original timeout cause.
                let latch = e.to_string();
                self.abandoned.store(true, Ordering::Release);
                self.err_monitor.set_error(e);
                // Stop the background task: a parked task never wakes to see
                // the closed channels, so aborting releases its `FsWriterBase`
                // (worker connections, block lease) and queued chunks instead
                // of leaking them for the process lifetime. The master lease
                // reclaims the unfinished file as it does after a client crash.
                self.task_handle.abort();
                Err(FsError::common(latch))
            }
        }
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
        let monitor = err_monitor.clone();

        let rt = writer.fs_context().clone_runtime();
        let task_handle: tokio::task::JoinHandle<()> = rt.spawn(async move {
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
            path: path.path().to_string(),
            chunk_sender,
            task_sender,
            err_monitor,
            abandoned: AtomicBool::new(false),
            task_handle,
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
    use std::time::{Duration, Instant};
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
    fn test_channel(timeout_ms: u64) -> BufferChannel {
        let (chunk_sender, chunk_receiver) = AsyncChannel::new(2).split();
        let (task_sender, task_receiver) = AsyncChannel::new(2).split();
        // Keep the receivers alive without owning them: this mirrors a
        // background task that stays alive but stops polling.
        std::mem::forget((chunk_receiver, task_receiver));

        BufferChannel {
            path: "/gh-1908-test".to_string(),
            chunk_sender,
            task_sender,
            err_monitor: Arc::new(ErrorMonitor::new()),
            abandoned: std::sync::atomic::AtomicBool::new(false),
            task_handle: tokio::spawn(async {}),
            timeout: Duration::from_millis(timeout_ms),
        }
    }

    // Regression test for #1908: the background task is alive but stops
    // polling, so the handshake can never complete. flush()/complete() must
    // return an error within the configured deadline instead of hanging.
    #[tokio::test]
    async fn handshakes_time_out_when_background_task_stops_responding() {
        let mut channel = test_channel(200);

        let start = Instant::now();
        let res = time::timeout(Duration::from_secs(5), channel.flush()).await;
        assert!(res.is_ok(), "flush() hung instead of returning an error");
        let err = res.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("not responding"),
            "unexpected error: {}",
            err
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "flush() waited {:?}, expected to stop at the 200ms deadline",
            start.elapsed()
        );

        let mut channel = test_channel(200);
        let res = time::timeout(Duration::from_secs(5), channel.complete()).await;
        assert!(res.is_ok(), "complete() hung instead of returning an error");
        let err = res.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("not responding"),
            "unexpected error: {}",
            err
        );
    }

    // A responder that mirrors the real Complete arm: it replies, then exits
    // right away. The reply must win: a successful complete must not be
    // misread as "task exited" or time out.
    #[tokio::test]
    async fn complete_succeeds_when_responder_replies_then_exits() {
        let (chunk_sender, chunk_receiver) = AsyncChannel::new(2).split();
        let (task_sender, task_receiver) = AsyncChannel::new(2).split();

        // Responder: drain tasks, answer each one, exit after the first.
        tokio::spawn(async move {
            // Hold the chunk receiver so the channels stay open.
            let mut _chunk_receiver = chunk_receiver;
            let mut task_receiver = task_receiver;
            if let Some(task) = task_receiver.recv().await {
                match task {
                    WriterTask::Flush(tx) => {
                        let _ = tx.send(1);
                    }
                    WriterTask::Complete((_, _, tx)) => {
                        let _ = tx.send(1);
                    }
                    _ => {}
                }
            }
            // Drop everything immediately after the reply: the task exits.
        });

        let mut channel = BufferChannel {
            path: "/gh-1908-reply-then-exit".to_string(),
            chunk_sender,
            task_sender,
            err_monitor: Arc::new(ErrorMonitor::new()),
            abandoned: std::sync::atomic::AtomicBool::new(false),
            task_handle: tokio::spawn(async {}),
            timeout: Duration::from_secs(10),
        };

        let res = time::timeout(Duration::from_secs(5), channel.flush()).await;
        assert!(res.is_ok(), "flush() hung");
        res.unwrap()
            .expect("flush() must succeed when the responder replies then exits");
    }

    // The background task exits (receivers dropped) while a flush is in
    // flight: the pending receive must fail right away via the dropped
    // CallChannel sender, without waiting for the timeout.
    #[tokio::test]
    async fn pending_flush_fails_when_responder_exits_without_replying() {
        let (chunk_sender, chunk_receiver) = AsyncChannel::new(2).split();
        let (task_sender, task_receiver) = AsyncChannel::new(2).split();

        // Responder exits immediately without answering anything.
        drop((chunk_receiver, task_receiver));

        let mut channel = BufferChannel {
            path: "/gh-1908-exit-no-reply".to_string(),
            chunk_sender,
            task_sender,
            err_monitor: Arc::new(ErrorMonitor::new()),
            abandoned: std::sync::atomic::AtomicBool::new(false),
            task_handle: tokio::spawn(async {}),
            timeout: Duration::from_secs(60),
        };

        let start = Instant::now();
        let res = time::timeout(Duration::from_secs(5), channel.flush()).await;
        assert!(res.is_ok(), "flush() hung after responder exit");
        assert!(
            res.unwrap().is_err(),
            "flush() must fail when the responder exited without replying"
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "flush() waited {:?}, expected a fast failure on dropped channels",
            start.elapsed()
        );
    }

    // When the deadline fires before the reply, the error must say the
    // outcome is unknown: the queued handshake is already handed to the task
    // and can still succeed in the background, so the caller must not read
    // the error as "the write failed".
    #[tokio::test]
    async fn timeout_error_reports_unknown_outcome_when_reply_is_late() {
        let (chunk_sender, chunk_receiver) = AsyncChannel::new(2).split();
        let (task_sender, task_receiver) = AsyncChannel::new(2).split();

        // Responder: wait for the flush, then reply well after the deadline.
        tokio::spawn(async move {
            let mut _chunk_receiver = chunk_receiver;
            let mut task_receiver = task_receiver;
            if let Some(WriterTask::Flush(tx)) = task_receiver.recv().await {
                time::sleep(Duration::from_millis(400)).await;
                let _ = tx.send(1);
            }
        });

        let mut channel = BufferChannel {
            path: "/gh-1908-late-reply".to_string(),
            chunk_sender,
            task_sender,
            err_monitor: Arc::new(ErrorMonitor::new()),
            abandoned: std::sync::atomic::AtomicBool::new(false),
            task_handle: tokio::spawn(async {}),
            timeout: Duration::from_millis(200),
        };

        let err = channel.flush().await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("outcome unknown"),
            "error must state the outcome is unknown: {}",
            msg
        );
    }

    // When the latch fires, the background task must be aborted so an
    // abandoned writer does not hold its FsWriterBase and queued chunks for
    // the process lifetime.
    #[tokio::test]
    async fn latch_aborts_the_background_task() {
        let (chunk_sender, chunk_receiver) = AsyncChannel::new(2).split();
        let (task_sender, task_receiver) = AsyncChannel::new(2).split();

        // Responder: a parked task with no timer - the #1908 shape. It never
        // polls and never notices the channels closing.
        let handle = tokio::spawn(async move {
            let _chunk_receiver = chunk_receiver;
            let _task_receiver = task_receiver;
            loop {
                time::sleep(Duration::from_secs(3600)).await;
            }
        });

        let mut channel = BufferChannel {
            path: "/gh-1908-abort".to_string(),
            chunk_sender,
            task_sender,
            err_monitor: Arc::new(ErrorMonitor::new()),
            abandoned: std::sync::atomic::AtomicBool::new(false),
            task_handle: handle,
            timeout: Duration::from_millis(200),
        };

        let err = channel.flush().await.unwrap_err();
        assert!(err.to_string().contains("not responding"));

        // The latch aborted the parked task: it finishes (cancelled) shortly.
        let res = time::timeout(Duration::from_secs(5), channel.task_handle).await;
        assert!(res.is_ok(), "aborted task did not finish");
        assert!(
            res.unwrap().is_err(),
            "task should have been cancelled by the latch abort"
        );
    }

    // After the first timeout the channel is latched as abandoned: later
    // calls fail immediately instead of waiting another full timeout.
    #[tokio::test]
    async fn timeout_latches_abandoned_and_later_calls_fail_fast() {
        let mut channel = test_channel(200);

        let err = channel.flush().await.unwrap_err();
        assert!(err.to_string().contains("not responding"));

        let start = Instant::now();
        let res = time::timeout(Duration::from_secs(5), channel.flush()).await;
        assert!(res.is_ok(), "latched flush() hung");
        let err = res.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("not responding"),
            "unexpected error: {}",
            err
        );
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "latched flush() waited {:?}, expected an immediate failure",
            start.elapsed()
        );
    }

    // End-to-end note: a full FsWriterBase-backed complete requires a live
    // master/worker (the task's complete RPC enters the 60/120s connection
    // retry path without one), so the reply-wins-over-exit regression is
    // covered by the controlled in-process responder above instead of an
    // environment-dependent server test.
}
