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

use crate::fuse_metrics::{mono_now, ActiveGuard, FuseMetrics, IO_TYPE_WRITE};
use crate::raw::fuse_abi::fuse_write_out;
use crate::session::FuseResponse;
use bytes::Bytes;
use curvine_client::unified::UnifiedWriter;
use curvine_common::conf::FuseConf;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::{FileAllocOpts, FileStatus, SetAttrOpts};
use curvine_common::FsResult;
use log::{error, warn};
use orpc::common::LocalTime;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::{AtomicCounter, AtomicLong, ErrorMonitor};
use orpc::sys::DataSlice;
use std::sync::Arc;

enum WriteTask {
    Write(i64, Bytes, Option<FuseResponse>),
    Flush(CallSender<FsResult<()>>, Option<FuseResponse>),
    Complete(
        CallSender<FsResult<()>>,
        Option<FuseResponse>,
        Option<SetAttrOpts>,
    ),
    Resize(CallSender<FsResult<()>>, FileAllocOpts),
}

struct QueuedWriteTask {
    task: WriteTask,
    queue_guard: Option<ActiveGuard>,
}

pub struct FuseWriter {
    path: Path,
    sender: AsyncSender<QueuedWriteTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    status: FileStatus,
    is_ufs: bool,
    len: Arc<AtomicLong>,
    mtime: Arc<AtomicLong>,
    write_ver: AtomicCounter,
    /// Serializes write/resize enqueue with dirty-read snapshot flush.
    ///
    /// On a bounded stream channel, `send_queued_task` may wait for capacity.
    /// Holding this gate across that wait ensures a dirty-read Flush cannot be
    /// queued ahead of a Write that has already started enqueueing, without
    /// requiring a global zero-inflight instant (which can starve under
    /// continuous concurrent writers).
    enqueue_gate: tokio::sync::Mutex<()>,
    metrics_enabled: bool,
}

#[inline]
fn mark_dequeued(queue_guard: &mut Option<ActiveGuard>) {
    drop(queue_guard.take());
}

impl FuseWriter {
    pub fn new(conf: &FuseConf, rt: Arc<Runtime>, writer: UnifiedWriter) -> Self {
        let is_ufs = !writer.path().is_cv();
        let path = writer.path().clone();
        let err_monitor = Arc::new(ErrorMonitor::new());
        let (sender, receiver) = AsyncChannel::new(conf.stream_channel_size).split();

        let status = writer.status().clone();
        let monitor = err_monitor.clone();
        let len = Arc::new(AtomicLong::new(status.len));
        let mtime = Arc::new(AtomicLong::new(status.mtime));
        let write_ver = AtomicCounter::new(0);
        let path_type = writer.path_type();
        let metrics_enabled = conf.metrics_enabled;

        let len1 = len.clone();
        let mtime1 = mtime.clone();
        rt.spawn(async move {
            let res =
                Self::writer_future(writer, receiver, len1, mtime1, path_type, metrics_enabled)
                    .await;
            match res {
                Ok(_) => (),

                Err(e) => {
                    error!("fuse writer error: {}", e);
                    monitor.set_error(e);
                }
            }
        });

        Self {
            path,
            sender,
            err_monitor,
            status,
            is_ufs,
            len,
            mtime,
            write_ver,
            enqueue_gate: tokio::sync::Mutex::new(()),
            metrics_enabled,
        }
    }

    pub fn write_ver(&self) -> u64 {
        self.write_ver.get()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    pub fn is_ufs(&self) -> bool {
        self.is_ufs
    }

    fn check_error(&self, e: FsError) -> FsError {
        self.err_monitor.take_error().unwrap_or(e)
    }

    fn checked_write_end(off: i64, len: usize) -> FsResult<i64> {
        let len = i64::try_from(len).map_err(|_| FsError::file_too_large(i64::MAX))?;
        off.checked_add(len)
            .ok_or_else(|| FsError::file_too_large(i64::MAX))
    }

    async fn send_queued_task(&self, task: WriteTask) -> Result<(), FsError> {
        if self.sender.is_bounded() {
            // Reserve before creating the guard to avoid cancellation leaks.
            let permit = self.sender.reserve().await?;
            let queue_guard = FuseMetrics::stream_write_queue_guard(self.metrics_enabled);
            permit.send(QueuedWriteTask { task, queue_guard });
            return Ok(());
        }

        let queue_guard = FuseMetrics::stream_write_queue_guard(self.metrics_enabled);
        self.sender
            .send(QueuedWriteTask { task, queue_guard })
            .await?;
        Ok(())
    }

    pub async fn write(&self, off: i64, data: Bytes, reply: Option<FuseResponse>) -> FsResult<()> {
        Self::checked_write_end(off, data.len())?;

        // Bump write_ver only after the Write is queued. Hold enqueue_gate across
        // the send so dirty-read flush cannot overtake a mid-reserve Write.
        let _gate = self.enqueue_gate.lock().await;
        let result = self
            .send_queued_task(WriteTask::Write(off, data, reply))
            .await
            .map_err(|e| self.check_error(e));
        if result.is_ok() {
            self.write_ver.incr();
        }
        result
    }

    pub async fn flush(&self, reply: Option<FuseResponse>) -> FsResult<()> {
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            self.send_queued_task(WriteTask::Flush(rx, reply)).await?;
            // Propagate backend flush failures even on reply=None paths.
            tx.receive().await??;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    /// Flush a dirty-read snapshot covering every write queued at capture time.
    ///
    /// Acquires `enqueue_gate` so the Flush is ordered after in-flight enqueues
    /// without waiting for a global zero-inflight instant. Continuous writers may
    /// keep advancing `write_ver` after this returns; the caller pins `read_ver`
    /// to the returned snapshot and republishes on a later read if needed.
    ///
    /// Returns `Ok(None)` when `read_ver` already matches the gated `write_ver`.
    pub async fn publish_dirty_read_snapshot(&self, read_ver: u64) -> FsResult<Option<u64>> {
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            let target_ver;
            {
                let _gate = self.enqueue_gate.lock().await;
                target_ver = self.write_ver.get();
                if target_ver == read_ver {
                    return Ok(None);
                }
                self.send_queued_task(WriteTask::Flush(rx, None)).await?;
            }
            tx.receive().await??;
            Ok(Some(target_ver))
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    pub async fn complete(&self, reply: Option<FuseResponse>) -> FsResult<()> {
        self.complete_with_attr(reply, None).await
    }

    pub async fn complete_with_attr(
        &self,
        reply: Option<FuseResponse>,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<()> {
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            self.send_queued_task(WriteTask::Complete(rx, reply, set_attr_opts))
                .await?;
            // Double `?`: the outer unwraps the channel receive, the inner
            // propagates the real backend complete result.
            tx.receive().await??;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    pub async fn resize(&self, opts: FileAllocOpts) -> FsResult<()> {
        let len = opts.len;
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            {
                let _gate = self.enqueue_gate.lock().await;
                let send = self.send_queued_task(WriteTask::Resize(rx, opts)).await;
                if send.is_ok() {
                    self.write_ver.incr();
                }
                send?;
            }
            // Double `?`: unwrap the channel receive, then propagate the real
            // backend resize result.
            tx.receive().await??;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))?;
        self.len.set(len);
        Ok(())
    }

    pub fn len(&self) -> i64 {
        self.len.get()
    }

    pub fn mtime(&self) -> i64 {
        self.mtime.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    async fn writer_future<W: Writer>(
        mut writer: W,
        mut req_receiver: AsyncReceiver<QueuedWriteTask>,
        file_len: Arc<AtomicLong>,
        file_mtime: Arc<AtomicLong>,
        path_type: &'static str,
        metrics_enabled: bool,
    ) -> FsResult<()> {
        let mut completed = false;
        // Sticky once backend data may be durable or visible; later writes do not clear it.
        let mut preserve_on_exit = false;
        let worker_result = Self::run_writer_tasks(
            &mut writer,
            &mut req_receiver,
            &file_len,
            &file_mtime,
            path_type,
            metrics_enabled,
            &mut completed,
            &mut preserve_on_exit,
        )
        .await;

        if completed {
            return worker_result;
        }

        // Abort only before any durability boundary may have published the data.
        let cleanup_result = if preserve_on_exit {
            writer.complete_with_attr(None).await
        } else {
            writer.cancel().await
        };
        match (worker_result, cleanup_result) {
            (Err(worker_error), Err(cleanup_error)) => {
                // Cleanup is best effort and must not hide the error that caused
                // the worker to exit.
                error!(
                    "failed to clean up writer after worker error: {}",
                    cleanup_error
                );
                Err(worker_error)
            }
            (Err(worker_error), Ok(())) => Err(worker_error),
            (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_writer_tasks<W: Writer>(
        writer: &mut W,
        req_receiver: &mut AsyncReceiver<QueuedWriteTask>,
        file_len: &AtomicLong,
        file_mtime: &AtomicLong,
        path_type: &'static str,
        metrics_enabled: bool,
        completed: &mut bool,
        preserve_on_exit: &mut bool,
    ) -> FsResult<()> {
        while let Some(mut queued) = req_receiver.recv().await {
            // Dequeue point: backlog ends before backend work starts.
            mark_dequeued(&mut queued.queue_guard);
            match queued.task {
                WriteTask::Write(off, data, reply) => {
                    // New writes invalidate prior complete state before backend IO starts.
                    *completed = false;
                    let len = data.len();
                    let write_end = Self::checked_write_end(off, len)?;
                    let io_start = if metrics_enabled {
                        Some(mono_now())
                    } else {
                        None
                    };
                    let inflight = FuseMetrics::stream_io_guard(metrics_enabled, IO_TYPE_WRITE);
                    let res: FsResult<fuse_write_out> = writer
                        .fuse_write(off, DataSlice::Bytes(data))
                        .await
                        .map(|_| fuse_write_out {
                            size: len as u32,
                            padding: 0,
                        });
                    drop(inflight);

                    if let Some(start) = io_start {
                        let ok = res.is_ok();
                        // Both transferred bytes and request size are the input
                        // data len (a successful write transfers all of it).
                        FuseMetrics::get().record_stream_io(
                            IO_TYPE_WRITE,
                            path_type,
                            ok,
                            len as u64,
                            len as u64,
                            start.elapsed().as_micros() as u64,
                        );
                    }

                    if res.is_ok() {
                        let cur_len = file_len.get();
                        file_len.set(cur_len.max(write_end));
                        file_mtime.set(LocalTime::mills() as i64);
                    }

                    if let Some(reply) = reply {
                        if let Err(e) = reply.send_rep(res).await {
                            // Reply enqueue failure must not terminate the long-lived writer worker.
                            warn!("failed to send FUSE write reply: {}", e);
                        }
                    } else {
                        res?;
                    }
                }

                WriteTask::Flush(tx, reply) => {
                    // Set this before the await because a lost/failed response
                    // cannot prove that the master did not publish the flush.
                    *preserve_on_exit = true;
                    let res = writer.flush().await;
                    // Deliver backend result to tx before the kernel reply.
                    crate::fs::deliver_stream_result(res, tx, reply).await?;
                }

                WriteTask::Complete(tx, reply, opts) => {
                    *preserve_on_exit = true;
                    let res = writer.complete_with_attr(opts).await;
                    *completed = res.is_ok();
                    crate::fs::deliver_stream_result(res, tx, reply).await?;
                }

                WriteTask::Resize(tx, opts) => {
                    *completed = false;
                    *preserve_on_exit = true;
                    // Propagate resize failure via tx instead of killing the worker.
                    let res = writer.resize(opts).await;
                    crate::fs::deliver_stream_result(res, tx, None).await?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{mark_dequeued, FuseWriter, QueuedWriteTask, WriteTask};
    use crate::fuse_metrics::ActiveGuard;
    use bytes::{Bytes, BytesMut};
    use curvine_common::error::FsError;
    use curvine_common::fs::{Path, Writer};
    use curvine_common::state::FileStatus;
    use curvine_common::FsResult;
    use curvine_metrics::Metrics as m;
    use orpc::sync::channel::{AsyncChannel, CallChannel};
    use orpc::sync::AtomicLong;
    use orpc::sys::DataSlice;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct TrackingWriter {
        path: Path,
        status: FileStatus,
        pos: i64,
        chunk: BytesMut,
        cancel_count: Arc<AtomicUsize>,
        complete_count: Arc<AtomicUsize>,
        fail_write: bool,
        fail_cancel: bool,
    }

    impl TrackingWriter {
        fn new(cancel_count: Arc<AtomicUsize>, complete_count: Arc<AtomicUsize>) -> Self {
            Self {
                path: Path::from_str("/tmp/fuse-writer-lifecycle").unwrap(),
                status: FileStatus::default(),
                pos: 0,
                chunk: BytesMut::new(),
                cancel_count,
                complete_count,
                fail_write: false,
                fail_cancel: false,
            }
        }
    }

    impl Writer for TrackingWriter {
        fn status(&self) -> &FileStatus {
            &self.status
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn pos(&self) -> i64 {
            self.pos
        }

        fn pos_mut(&mut self) -> &mut i64 {
            &mut self.pos
        }

        fn chunk_mut(&mut self) -> &mut BytesMut {
            &mut self.chunk
        }

        fn chunk_size(&self) -> usize {
            4096
        }

        async fn write_chunk(&mut self, chunk: DataSlice) -> FsResult<i64> {
            if self.fail_write {
                Err(FsError::common("injected backend write failure"))
            } else {
                Ok(chunk.len() as i64)
            }
        }

        async fn flush(&mut self) -> FsResult<()> {
            Ok(())
        }

        async fn complete(&mut self) -> FsResult<()> {
            self.complete_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn cancel(&mut self) -> FsResult<()> {
            self.cancel_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_cancel {
                Err(FsError::common("injected cancellation failure"))
            } else {
                Ok(())
            }
        }

        async fn seek(&mut self, pos: i64) -> FsResult<()> {
            self.pos = pos;
            Ok(())
        }
    }

    #[test]
    fn checked_write_end_accepts_maximum_endpoint() {
        assert_eq!(
            FuseWriter::checked_write_end(i64::MAX, 0).unwrap(),
            i64::MAX
        );
        assert_eq!(
            FuseWriter::checked_write_end(i64::MAX - 1, 1).unwrap(),
            i64::MAX
        );
    }

    #[test]
    fn checked_write_end_rejects_overflowing_endpoint() {
        for (off, len) in [(i64::MAX, 1_usize), (i64::MAX - 1, 2_usize)] {
            let err = FuseWriter::checked_write_end(off, len)
                .expect_err("overflowing write endpoint must be rejected");
            assert_eq!(crate::fuse_error::errno_of(&err), libc::EFBIG);
            assert!(matches!(err, FsError::InvalidFileSize(_)));
        }
    }

    #[tokio::test]
    async fn abnormal_channel_close_cancels_backend_writer_once() {
        let cancel_count = Arc::new(AtomicUsize::new(0));
        let complete_count = Arc::new(AtomicUsize::new(0));
        let writer = TrackingWriter::new(cancel_count.clone(), complete_count.clone());
        let (sender, receiver) = AsyncChannel::new(1).split();
        drop(sender);

        FuseWriter::writer_future(
            writer,
            receiver,
            Arc::new(AtomicLong::new(0)),
            Arc::new(AtomicLong::new(0)),
            "test",
            false,
        )
        .await
        .unwrap();

        assert_eq!(cancel_count.load(Ordering::SeqCst), 1);
        assert_eq!(complete_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn normal_complete_does_not_cancel_backend_writer() {
        let cancel_count = Arc::new(AtomicUsize::new(0));
        let complete_count = Arc::new(AtomicUsize::new(0));
        let writer = TrackingWriter::new(cancel_count.clone(), complete_count.clone());
        let (sender, receiver) = AsyncChannel::new(1).split();
        let (result_tx, result_rx) = CallChannel::channel::<FsResult<()>>();
        sender
            .send(QueuedWriteTask {
                task: WriteTask::Complete(result_tx, None, None),
                queue_guard: None,
            })
            .await
            .unwrap();
        drop(sender);

        FuseWriter::writer_future(
            writer,
            receiver,
            Arc::new(AtomicLong::new(0)),
            Arc::new(AtomicLong::new(0)),
            "test",
            false,
        )
        .await
        .unwrap();

        assert!(result_rx.receive().await.unwrap().is_ok());
        assert_eq!(complete_count.load(Ordering::SeqCst), 1);
        assert_eq!(cancel_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn successful_flush_is_finalized_instead_of_cancelled_on_channel_close() {
        let cancel_count = Arc::new(AtomicUsize::new(0));
        let complete_count = Arc::new(AtomicUsize::new(0));
        let writer = TrackingWriter::new(cancel_count.clone(), complete_count.clone());
        let (sender, receiver) = AsyncChannel::new(1).split();
        let (result_tx, result_rx) = CallChannel::channel::<FsResult<()>>();
        sender
            .send(QueuedWriteTask {
                task: WriteTask::Flush(result_tx, None),
                queue_guard: None,
            })
            .await
            .unwrap();
        drop(sender);

        FuseWriter::writer_future(
            writer,
            receiver,
            Arc::new(AtomicLong::new(0)),
            Arc::new(AtomicLong::new(0)),
            "test",
            false,
        )
        .await
        .unwrap();

        assert!(result_rx.receive().await.unwrap().is_ok());
        assert_eq!(complete_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            cancel_count.load(Ordering::SeqCst),
            0,
            "cancelling here could delete data published by the flush"
        );
    }

    #[tokio::test]
    async fn write_after_flush_keeps_the_durable_cleanup_boundary() {
        let cancel_count = Arc::new(AtomicUsize::new(0));
        let complete_count = Arc::new(AtomicUsize::new(0));
        let writer = TrackingWriter::new(cancel_count.clone(), complete_count.clone());
        let (sender, receiver) = AsyncChannel::new(2).split();
        let (result_tx, result_rx) = CallChannel::channel::<FsResult<()>>();
        sender
            .send(QueuedWriteTask {
                task: WriteTask::Flush(result_tx, None),
                queue_guard: None,
            })
            .await
            .unwrap();
        sender
            .send(QueuedWriteTask {
                task: WriteTask::Write(0, Bytes::from_static(b"later"), None),
                queue_guard: None,
            })
            .await
            .unwrap();
        drop(sender);

        FuseWriter::writer_future(
            writer,
            receiver,
            Arc::new(AtomicLong::new(0)),
            Arc::new(AtomicLong::new(0)),
            "test",
            false,
        )
        .await
        .unwrap();

        assert!(result_rx.receive().await.unwrap().is_ok());
        assert_eq!(complete_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            cancel_count.load(Ordering::SeqCst),
            0,
            "a later write must not make an earlier durable block abortable"
        );
    }

    #[tokio::test]
    async fn cancel_failure_does_not_mask_worker_error() {
        let cancel_count = Arc::new(AtomicUsize::new(0));
        let complete_count = Arc::new(AtomicUsize::new(0));
        let mut writer = TrackingWriter::new(cancel_count.clone(), complete_count);
        writer.fail_write = true;
        writer.fail_cancel = true;
        let (sender, receiver) = AsyncChannel::new(1).split();
        sender
            .send(QueuedWriteTask {
                task: WriteTask::Write(0, Bytes::from_static(b"data"), None),
                queue_guard: None,
            })
            .await
            .unwrap();
        drop(sender);

        let error = FuseWriter::writer_future(
            writer,
            receiver,
            Arc::new(AtomicLong::new(0)),
            Arc::new(AtomicLong::new(0)),
            "test",
            false,
        )
        .await
        .expect_err("the backend write failure remains visible");

        assert!(error.to_string().contains("injected backend write failure"));
        assert!(!error.to_string().contains("injected cancellation failure"));
        assert_eq!(cancel_count.load(Ordering::SeqCst), 1);
    }

    fn queued_task(gauge: &curvine_metrics::Gauge) -> QueuedWriteTask {
        let (rx, _tx) = CallChannel::channel::<FsResult<()>>();
        QueuedWriteTask {
            task: WriteTask::Flush(rx, None),
            queue_guard: Some(ActiveGuard::new(gauge.clone())),
        }
    }

    #[tokio::test]
    async fn queue_depth_drops_at_dequeue() {
        let g = m::new_gauge("test_swqd_dequeue", "test").unwrap();
        let (tx, mut rx) = AsyncChannel::<QueuedWriteTask>::new(16).split();

        tx.send(queued_task(&g)).await.unwrap();
        assert_eq!(g.get(), 1, "queue depth +1 while task is in the channel");

        let mut queued = rx.recv().await.expect("a task");
        // writer_future's first line after recv.
        mark_dequeued(&mut queued.queue_guard);
        assert_eq!(g.get(), 0, "dequeue decrements before any backend work");
        drop(queued);
        assert_eq!(g.get(), 0, "no double dec");
    }

    #[tokio::test]
    async fn queue_depth_unreceived_task_drop_balances() {
        let g = m::new_gauge("test_swqd_unrecv", "test").unwrap();
        let (tx, rx) = AsyncChannel::<QueuedWriteTask>::new(16).split();

        tx.send(queued_task(&g)).await.unwrap();
        tx.send(queued_task(&g)).await.unwrap();
        assert_eq!(g.get(), 2, "two tasks enqueued, not yet received");

        drop(rx);
        drop(tx);
        assert_eq!(g.get(), 0, "un-received task drop balances queue depth");
    }

    #[tokio::test]
    async fn queue_depth_bounded_full_does_not_inflate() {
        let g = m::new_gauge("test_swqd_bounded_full", "test").unwrap();
        let (tx, _rx) = AsyncChannel::<QueuedWriteTask>::new(1).split();
        debug_assert!(tx.is_bounded());

        let permit = tx.try_reserve().unwrap().expect("one permit");
        permit.send(QueuedWriteTask {
            task: {
                let (rx, _tx) = CallChannel::channel::<FsResult<()>>();
                WriteTask::Flush(rx, None)
            },
            queue_guard: None,
        });

        assert!(
            tx.try_reserve().unwrap().is_none(),
            "full bounded channel yields no permit"
        );
        assert_eq!(
            g.get(),
            0,
            "no permit -> no queue guard built -> depth not inflated"
        );
    }

    #[tokio::test]
    async fn queue_depth_disabled_carries_no_guard() {
        let (rx, _tx) = CallChannel::channel::<FsResult<()>>();
        let mut queued = QueuedWriteTask {
            task: WriteTask::Flush(rx, None),
            queue_guard: None,
        };
        mark_dequeued(&mut queued.queue_guard);
        assert!(queued.queue_guard.is_none());
    }

    // --- real FuseWriter task-body integration ---

    mod task_body_integration {
        use super::super::FuseWriter;
        use crate::fs::operator::Write;
        use crate::fuse_metrics::{
            ActiveGuard, FuseMetrics, FuseReqCtx, FuseReqKind, FuseReqLabels, IO_TYPE_WRITE,
            STAGE_STREAM_IO,
        };
        use crate::raw::fuse_abi::{fuse_in_header, fuse_write_in};
        use crate::session::{FuseResponse, FuseTask};
        use bytes::Bytes;
        use curvine_client::unified::UnifiedWriter;
        use curvine_common::conf::FuseConf;
        use curvine_common::error::FsError;
        use curvine_common::fs::local::LocalWriter;
        use curvine_metrics::Metrics as m;
        use orpc::runtime::{AsyncRuntime, RpcRuntime};
        use orpc::sync::channel::AsyncChannel;
        use std::sync::Arc;

        fn metrics_reply(rt: &AsyncRuntime) -> FuseResponse {
            FuseMetrics::ensure_init().unwrap();
            let (tx, mut rx) = AsyncChannel::<FuseTask>::new(16).split();
            rt.spawn(async move { while rx.recv().await.is_some() {} });
            let gauge = m::new_gauge(
                format!("fw_it_active_{}", std::process::id()),
                "test".to_string(),
            )
            .unwrap();
            let labels = FuseReqLabels::new("Write", FuseReqKind::Stream, 64);
            let ctx = FuseReqCtx {
                labels,
                active: Some(ActiveGuard::new(gauge)),
            };
            FuseResponse::new_reply(1, tx, false, Some(ctx))
        }

        fn closed_reply(unique: u64) -> FuseResponse {
            let (tx, rx) = AsyncChannel::<FuseTask>::new(1).split();
            drop(rx);
            FuseResponse::new_reply(unique, tx, false, None)
        }

        #[test]
        fn reply_send_error_does_not_kill_writer_worker() {
            let rt = AsyncRuntime::single();
            rt.block_on(async {
                let path_buf = std::env::temp_dir().join(format!(
                    "fw_reply_failure_{}_{:?}",
                    std::process::id(),
                    std::thread::current().id()
                ));
                let path = curvine_common::fs::Path::from_str(path_buf.to_str().unwrap()).unwrap();

                let conf = FuseConf {
                    metrics_enabled: false,
                    ..Default::default()
                };
                let writer = UnifiedWriter::Local(LocalWriter::new(&path, 4096).unwrap());
                let rt2 = Arc::new(AsyncRuntime::single());
                let fuse_writer = FuseWriter::new(&conf, rt2.clone(), writer);
                std::mem::forget(rt2);

                // Closed write reply must not prevent the same worker from handling flush.
                fuse_writer
                    .write(0, Bytes::from_static(b"first"), Some(closed_reply(1)))
                    .await
                    .unwrap();
                fuse_writer
                    .flush(None)
                    .await
                    .expect("writer survives a write reply-send failure");

                // Exercise the shared flush/complete helper after a closed kernel reply.
                fuse_writer
                    .flush(Some(closed_reply(2)))
                    .await
                    .expect("flush caller receives the backend result");
                fuse_writer
                    .write(5, Bytes::from_static(b"second"), None)
                    .await
                    .unwrap();
                fuse_writer
                    .complete(None)
                    .await
                    .expect("writer survives a flush reply-send failure");

                assert_eq!(std::fs::read(&path_buf).unwrap(), b"firstsecond");
                let _ = std::fs::remove_file(&path_buf);
            });
        }

        #[test]
        fn overflowing_write_endpoint_is_rejected_before_enqueue() {
            let rt = AsyncRuntime::single();
            rt.block_on(async {
                let path_buf = std::env::temp_dir().join(format!(
                    "fw_overflow_reject_{}_{:?}",
                    std::process::id(),
                    std::thread::current().id()
                ));
                let path = curvine_common::fs::Path::from_str(path_buf.to_str().unwrap()).unwrap();

                let conf = FuseConf {
                    metrics_enabled: false,
                    ..Default::default()
                };
                let writer = UnifiedWriter::Local(LocalWriter::new(&path, 4096).unwrap());
                let rt2 = Arc::new(AsyncRuntime::single());
                let fuse_writer = FuseWriter::new(&conf, rt2.clone(), writer);
                std::mem::forget(rt2);

                assert_eq!(fuse_writer.write_ver(), 0);
                assert_eq!(fuse_writer.len(), 0);

                let err = fuse_writer
                    .write(i64::MAX, Bytes::from_static(b"x"), None)
                    .await
                    .expect_err("overflowing write endpoint must be rejected");
                assert_eq!(crate::fuse_error::errno_of(&err), libc::EFBIG);
                assert!(matches!(err, FsError::InvalidFileSize(_)));

                assert_eq!(fuse_writer.write_ver(), 0);
                assert_eq!(
                    fuse_writer.len(),
                    0,
                    "rejected writes must not change cached file length"
                );

                fuse_writer.complete(None).await.unwrap();
                let _ = std::fs::remove_file(&path_buf);
            });
        }

        #[test]
        fn local_writer_task_body_observes_io_with_local_path_type() {
            let rt = AsyncRuntime::single();
            rt.block_on(async {
                FuseMetrics::ensure_init().unwrap();
                let mx = FuseMetrics::get();
                let dur_before = mx
                    .io_duration_us
                    .with_label_values(&[IO_TYPE_WRITE, "local", "success"])
                    .get_sample_count();
                let req_before = mx
                    .io_requests_total
                    .with_label_values(&[IO_TYPE_WRITE, "local", "success"])
                    .get();
                let bytes_before = mx
                    .io_bytes_total
                    .with_label_values(&[IO_TYPE_WRITE, "local", "success"])
                    .get();
                let size_before = mx
                    .io_size_bytes
                    .with_label_values(&[IO_TYPE_WRITE, "local"])
                    .get_sample_count();
                let stage_before = mx
                    .stage_duration_us
                    .with_label_values(&[STAGE_STREAM_IO, "stream", "success"])
                    .get_sample_count();

                let path_buf = std::env::temp_dir().join(format!(
                    "fw_it_write_{}_{:?}",
                    std::process::id(),
                    std::thread::current().id()
                ));
                let path = curvine_common::fs::Path::from_str(path_buf.to_str().unwrap()).unwrap();

                let conf = FuseConf::default();
                let writer = UnifiedWriter::Local(LocalWriter::new(&path, 4096).unwrap());
                assert_eq!(writer.path_type(), "local");
                let rt2 = Arc::new(AsyncRuntime::single());
                let fuse_writer = FuseWriter::new(&conf, rt2.clone(), writer);
                // Avoid dropping the last runtime Arc inside an async context.
                std::mem::forget(rt2);

                // Write 2048 bytes (a non-zero, sub-4K write — still a real backend IO).
                let reply = metrics_reply(&rt);
                let header = fuse_in_header::default();
                let arg = fuse_write_in {
                    size: 2048,
                    ..Default::default()
                };
                let op = Write {
                    header: &header,
                    arg: &arg,
                    data: vec![3u8; 2048].into(),
                };
                fuse_writer
                    .write(op.arg.offset as i64, op.data.clone(), Some(reply))
                    .await
                    .unwrap();

                for _ in 0..50 {
                    if mx
                        .io_duration_us
                        .with_label_values(&[IO_TYPE_WRITE, "local", "success"])
                        .get_sample_count()
                        > dur_before
                    {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }

                assert_eq!(
                    mx.io_duration_us
                        .with_label_values(&[IO_TYPE_WRITE, "local", "success"])
                        .get_sample_count(),
                    dur_before + 1,
                    "write task body observed io_duration_us{{write,local,success}} once"
                );
                assert_eq!(
                    mx.io_requests_total
                        .with_label_values(&[IO_TYPE_WRITE, "local", "success"])
                        .get(),
                    req_before + 1,
                    "io_requests_total{{write,local,success}} +1"
                );
                assert_eq!(
                    mx.io_bytes_total
                        .with_label_values(&[IO_TYPE_WRITE, "local", "success"])
                        .get(),
                    bytes_before + 2048,
                    "io_bytes_total uses input length (2048)"
                );
                assert_eq!(
                    mx.io_size_bytes
                        .with_label_values(&[IO_TYPE_WRITE, "local"])
                        .get_sample_count(),
                    size_before + 1,
                    "io_size_bytes observed once"
                );
                assert!(
                    mx.stage_duration_us
                        .with_label_values(&[STAGE_STREAM_IO, "stream", "success"])
                        .get_sample_count()
                        > stage_before,
                    "write backend call also emits stage=stream_io,kind=stream"
                );

                let _ = std::fs::remove_file(&path_buf);
            });
        }

        /// Continuous writers on a capacity-1 stream channel must not starve
        /// dirty-read snapshot publication (former global zero-inflight wait).
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn dirty_read_snapshot_completes_under_continuous_bounded_writers() {
            use std::sync::atomic::{AtomicBool, Ordering};
            use std::time::Duration;

            let path_buf = std::env::temp_dir().join(format!(
                "fw_dirty_read_stress_{}_{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let path = curvine_common::fs::Path::from_str(path_buf.to_str().unwrap()).unwrap();

            let conf = FuseConf {
                stream_channel_size: 1,
                metrics_enabled: false,
                ..Default::default()
            };
            let writer = UnifiedWriter::Local(LocalWriter::new(&path, 4096).unwrap());
            let rt2 = Arc::new(AsyncRuntime::single());
            let fuse_writer = Arc::new(FuseWriter::new(&conf, rt2.clone(), writer));
            std::mem::forget(rt2);

            let stop = Arc::new(AtomicBool::new(false));
            let mut writer_tasks = Vec::new();
            for i in 0..4 {
                let w = fuse_writer.clone();
                let stop = stop.clone();
                writer_tasks.push(tokio::spawn(async move {
                    let mut off = (i as i64) * 1_000_000;
                    while !stop.load(Ordering::Relaxed) {
                        // Ignore individual write errors once complete() races shutdown.
                        let _ = w.write(off, Bytes::from_static(b"x"), None).await;
                        off += 1;
                    }
                }));
            }

            // Let writers fill the bounded queue and contend on enqueue_gate.
            tokio::time::sleep(Duration::from_millis(50)).await;

            let published = tokio::time::timeout(
                Duration::from_secs(5),
                fuse_writer.publish_dirty_read_snapshot(0),
            )
            .await
            .expect("dirty-read snapshot must complete under continuous bounded writers")
            .expect("dirty-read snapshot flush must succeed");
            let ver = published.expect("expected a snapshot while writers are active");
            assert!(ver > 0, "expected a non-zero snapshot, got {ver}");

            // Re-check must also complete under load (may flush a newer snapshot).
            tokio::time::timeout(
                Duration::from_secs(5),
                fuse_writer.publish_dirty_read_snapshot(ver),
            )
            .await
            .expect("dirty-read re-publish must complete under continuous writers")
            .expect("dirty-read re-publish flush must succeed");

            stop.store(true, Ordering::Relaxed);
            for task in writer_tasks {
                let _ = task.await;
            }
            let _ = fuse_writer.complete(None).await;
            let _ = std::fs::remove_file(&path_buf);
        }
    }
}
