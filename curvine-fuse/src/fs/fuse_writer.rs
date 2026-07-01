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

use crate::fs::operator::Write;
use crate::fuse_metrics::{mono_now, ActiveGuard, FuseMetrics, IO_TYPE_WRITE};
use crate::raw::fuse_abi::fuse_write_out;
use crate::session::FuseResponse;
use curvine_client::unified::UnifiedWriter;
use curvine_common::conf::FuseConf;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::{FileAllocOpts, FileStatus};
use curvine_common::FsResult;
use log::error;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::{AtomicCounter, ErrorMonitor};
use orpc::sys::DataSlice;
use std::sync::{Arc, Mutex};
use tokio_util::bytes::Bytes;

enum WriteTask {
    Write(i64, Bytes, FuseResponse),
    Flush(CallSender<i8>, Option<FuseResponse>),
    Complete(CallSender<i8>, Option<FuseResponse>),
    Resize(CallSender<i8>, FileAllocOpts),
}

/// A `WriteTask` plus the `stream_write_queue_depth` guard that rides with it
/// through the writer channel (Phase 2b). The guard is created at the enqueue
/// boundary (reserve-first on a bounded channel, so a producer parked in
/// `reserve().await` is not counted) and dropped the moment `writer_future`
/// dequeues — or, if the task is never received (writer task exit / channel drop),
/// when the task is dropped. `None` when metrics are disabled. This is the same
/// task-embedded-guard pattern the reply channel uses (`FuseTask::RequestReply`).
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
    len: Arc<Mutex<i64>>,
    write_ver: AtomicCounter,
    completed: bool,
    /// Phase 2b kill-switch flag: decides whether `send_queued_task` creates a
    /// `stream_write_queue_depth` guard. (The `path_type` label is captured as a
    /// local and moved into the writer task, not stored here.)
    metrics_enabled: bool,
}

/// Drop a dequeued task's `stream_write_queue_depth` guard, decrementing the gauge
/// at the dequeue point (the first line after `recv()`), so the gauge reflects
/// only channel backlog and excludes the subsequent backend work. A named helper
/// so the "drop on dequeue, not on completion" rule is explicit and hard to
/// misplace; mirrors `fuse_sender::mark_dequeued`. `None` (disabled) is a no-op.
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
        let len = Arc::new(Mutex::new(status.len));
        let write_ver = AtomicCounter::new(0);
        // Phase 2b: backend kind + metrics gate, captured at open and moved into
        // the writer task for the per-IO observe (same as FuseReader).
        let path_type = writer.path_type();
        let metrics_enabled = conf.metrics_enabled;

        let len1 = len.clone();
        rt.spawn(async move {
            let res = Self::writer_future(writer, receiver, len1, path_type, metrics_enabled).await;
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
            write_ver,
            completed: false,
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

    pub fn is_completed(&self) -> bool {
        self.completed
    }

    fn check_error(&self, e: FsError) -> FsError {
        self.err_monitor.take_error().unwrap_or(e)
    }

    /// Enqueue a `WriteTask` into the writer channel, carrying the
    /// `stream_write_queue_depth` guard (Phase 2b). Reserve-first on a bounded
    /// channel so a producer parked in `reserve().await` (channel full) is NOT
    /// counted as backlog — the guard is created only AFTER a permit is in hand
    /// (bounded) or just before the synchronous unbounded send. No manual
    /// inc/dec/rollback: the guard rides the task and the writer drops it at the
    /// dequeue point (or it is dropped with an un-received task on teardown).
    ///
    /// IMPORTANT: this helper only handles the queue guard + channel send. It must
    /// NOT touch `write_ver` — the producers below keep `write_ver.incr()` at its
    /// existing position (a read-after-write consistency dependency, not a metrics
    /// concern), see `write`/`resize`.
    async fn send_queued_task(&self, task: WriteTask) -> Result<(), FsError> {
        if self.sender.is_bounded() {
            // Reserve a permit first WITHOUT a guard; a cancelled reserve leaves the
            // gauge untouched. Once the permit is in hand there is no await, so no
            // cancellation window between creating the guard and enqueuing.
            let permit = self.sender.reserve().await?;
            let queue_guard = FuseMetrics::stream_write_queue_guard(self.metrics_enabled);
            permit.send(QueuedWriteTask { task, queue_guard });
            return Ok(());
        }

        // Unbounded: synchronous send, so no cancellation window between building
        // the guard task and enqueuing. A send error drops the task (and its
        // guard, decrementing the gauge).
        let queue_guard = FuseMetrics::stream_write_queue_guard(self.metrics_enabled);
        self.sender
            .send(QueuedWriteTask { task, queue_guard })
            .await?;
        Ok(())
    }

    pub async fn write(&mut self, op: Write<'_>, reply: FuseResponse) -> FsResult<()> {
        // `write_ver.incr()` stays BEFORE the enqueue (unchanged): the read path
        // (`FileHandle::read`) compares write_ver to decide a dirty-read flush+reopen,
        // so its timing is a consistency invariant, not something to reorder for
        // metrics.
        self.write_ver.incr();
        self.send_queued_task(WriteTask::Write(op.arg.offset as i64, op.data, reply))
            .await
            .map_err(|e| self.check_error(e))
    }

    pub async fn flush(&mut self, reply: Option<FuseResponse>) -> FsResult<()> {
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            self.send_queued_task(WriteTask::Flush(rx, reply)).await?;
            tx.receive().await?;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    pub async fn complete(&mut self, reply: Option<FuseResponse>) -> FsResult<()> {
        self.completed = true;
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            self.send_queued_task(WriteTask::Complete(rx, reply))
                .await?;
            tx.receive().await?;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    pub async fn resize(&mut self, opts: FileAllocOpts) -> FsResult<()> {
        let len = opts.len;
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            self.send_queued_task(WriteTask::Resize(rx, opts)).await?;
            tx.receive().await?;
            Ok::<(), FsError>(())
        };
        // `write_ver.incr()` stays at its existing position (after building `fun`,
        // before awaiting it) — unchanged consistency timing.
        self.write_ver.incr();
        fun.await.map_err(|e| self.check_error(e))?;
        let mut lock = self.len.lock().unwrap();
        *lock = len;
        Ok(())
    }

    pub fn len(&self) -> i64 {
        *self.len.lock().unwrap()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    async fn writer_future(
        mut writer: UnifiedWriter,
        mut req_receiver: AsyncReceiver<QueuedWriteTask>,
        file_len: Arc<Mutex<i64>>,
        path_type: &'static str,
        metrics_enabled: bool,
    ) -> FsResult<()> {
        let mut complete = false;
        while let Some(mut queued) = req_receiver.recv().await {
            // Dequeue point: drop the queue guard FIRST (before any backend work),
            // so `stream_write_queue_depth` counts only channel backlog. An
            // un-received task on teardown drops the guard via its own Drop.
            mark_dequeued(&mut queued.queue_guard);
            match queued.task {
                WriteTask::Write(off, data, reply) => {
                    let len = data.len();
                    // Phase 2b: observe the write backend IO (io_type=write). The
                    // inflight guard wraps ONLY the `fuse_write` call; dropped
                    // before the reply enqueue. zero-length writes never reach here
                    // (FileHandle::write direct-replies), so every sample is a real
                    // write of >0 bytes.
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
                        let mut lock = file_len.lock().unwrap();
                        *lock = lock.max(off + len as i64);
                    }

                    reply.send_rep(res).await?;
                }

                WriteTask::Flush(tx, reply) => {
                    let res = writer.flush().await;
                    if let Some(reply) = reply {
                        reply.send_rep(res).await?;
                    }
                    tx.send(1)?;
                }

                WriteTask::Complete(tx, reply) => {
                    let res = if !complete {
                        let res = writer.complete().await;
                        if res.is_ok() {
                            complete = true;
                        }
                        res
                    } else {
                        Ok(())
                    };

                    if let Some(reply) = reply {
                        reply.send_rep(res).await?;
                    }
                    tx.send(1)?;
                }

                WriteTask::Resize(tx, opts) => {
                    writer.resize(opts).await?;
                    tx.send(1)?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{mark_dequeued, QueuedWriteTask, WriteTask};
    use crate::fuse_metrics::ActiveGuard;
    use orpc::common::Metrics as m;
    use orpc::sync::channel::{AsyncChannel, CallChannel};

    // Build a QueuedWriteTask carrying a queue guard backed by `gauge`. The wrapped
    // WriteTask is a Flush (it only needs a CallChannel sender, no FuseResponse), so
    // these tests exercise the queue-depth guard lifecycle without a backend.
    fn queued_task(gauge: &orpc::common::Gauge) -> QueuedWriteTask {
        let (rx, _tx) = CallChannel::channel::<i8>();
        QueuedWriteTask {
            task: WriteTask::Flush(rx, None),
            queue_guard: Some(ActiveGuard::new(gauge.clone())),
        }
    }

    // (a) Dequeue: the writer's first line after recv() drops the queue guard, so
    // depth returns to baseline BEFORE any backend work. Injected isolated gauge so
    // it is parallel-safe (not the process-global stream_write_queue_depth).
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
        // The remaining backend processing (here: just dropping the task) does not
        // touch the gauge again.
        drop(queued);
        assert_eq!(g.get(), 0, "no double dec");
    }

    // (b) A task enqueued but NEVER received (writer task exit / channel drop) still
    // balances: the guard rides the task and drops with it. Covers R3 P1#3 (worker
    // exit) + channel drop teardown.
    #[tokio::test]
    async fn queue_depth_unreceived_task_drop_balances() {
        let g = m::new_gauge("test_swqd_unrecv", "test").unwrap();
        let (tx, rx) = AsyncChannel::<QueuedWriteTask>::new(16).split();

        tx.send(queued_task(&g)).await.unwrap();
        tx.send(queued_task(&g)).await.unwrap();
        assert_eq!(g.get(), 2, "two tasks enqueued, not yet received");

        // Drop both ends without recv: the queued tasks (and their guards) drop.
        drop(rx);
        drop(tx);
        assert_eq!(g.get(), 0, "un-received task drop balances queue depth");
    }

    // (c) Bounded-full reserve-first does NOT inflate depth: the guard is created
    // only AFTER a permit is acquired, so a full channel (no permit) means no guard.
    // Protocol-level stand-in (mirrors the reply-queue test): asserts the
    // reserve-first invariant directly against a full bounded channel.
    #[tokio::test]
    async fn queue_depth_bounded_full_does_not_inflate() {
        let g = m::new_gauge("test_swqd_bounded_full", "test").unwrap();
        let (tx, _rx) = AsyncChannel::<QueuedWriteTask>::new(1).split();
        debug_assert!(tx.is_bounded());

        // Fill the single slot WITHOUT a guard (simulating the first task already
        // committed and received-pending).
        let permit = tx.try_reserve().unwrap().expect("one permit");
        permit.send(QueuedWriteTask {
            task: {
                let (rx, _tx) = CallChannel::channel::<i8>();
                WriteTask::Flush(rx, None)
            },
            queue_guard: None,
        });

        // Channel full: a producer would park in reserve().await. Reserve-first
        // means no guard is created until a permit is in hand, so depth is untouched.
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

    // disabled mode: send_queued_task builds queue_guard=None, so mark_dequeued is a
    // no-op and the task carries nothing.
    #[tokio::test]
    async fn queue_depth_disabled_carries_no_guard() {
        let (rx, _tx) = CallChannel::channel::<i8>();
        let mut queued = QueuedWriteTask {
            task: WriteTask::Flush(rx, None),
            queue_guard: None,
        };
        // No guard to drop; must not panic.
        mark_dequeued(&mut queued.queue_guard);
        assert!(queued.queue_guard.is_none());
    }

    // --- Phase 2b: real FuseWriter task-body integration (codex review P1-2) ---

    mod task_body_integration {
        use super::super::FuseWriter;
        use crate::fs::operator::Write;
        use crate::fuse_metrics::{
            ActiveGuard, FuseMetrics, FuseReqCtx, FuseReqKind, FuseReqLabels, IO_TYPE_WRITE,
            STAGE_STREAM_IO,
        };
        use crate::raw::fuse_abi::{fuse_in_header, fuse_write_in};
        use crate::session::{FuseResponse, FuseTask};
        use curvine_client::unified::UnifiedWriter;
        use curvine_common::conf::FuseConf;
        use curvine_common::fs::local::LocalWriter;
        use orpc::common::Metrics as m;
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

        // A real FuseWriter over UnifiedWriter::Local drives the write task body via
        // the public `write()`, proving the production wiring: path_type="local"
        // flows into the task body, io_*{write,local} + stage=stream_io are observed
        // with the input data length for both bytes and size.
        //
        // Parallel-safety: (io_type=write, path_type=local) children are touched ONLY
        // by this test, so exact deltas are safe; the shared stage child uses a lower
        // bound. Mirrors the FuseReader integration test.
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
                let mut fuse_writer = FuseWriter::new(&conf, rt2.clone(), writer);
                // Leak our Arc so this runtime is never the-last-Arc-dropped inside
                // the outer async block (dropping a tokio runtime from an async
                // context panics).
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
                fuse_writer.write(op, reply).await.unwrap();

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
                // NOTE: we deliberately do NOT assert `stream_io_inflight{write}` here.
                // It is a process-global gauge shared with other tests, so a
                // point-in-time read (even against a baseline) races a concurrent test
                // holding a write guard — neither `==0` nor `==before` is reliable under
                // the default parallel harness. The guard's inc/dec balance and its
                // "wraps only the backend call, dropped before reply" scope are pinned
                // deterministically by the isolated `stream_io_guard_gate_and_balance`
                // unit test instead.

                let _ = std::fs::remove_file(&path_buf);
            });
        }
    }
}
