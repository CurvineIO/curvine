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

use crate::fs::operator::Read;
use crate::fuse_metrics::{mono_now, FuseMetrics, IO_TYPE_READ};
use crate::session::FuseResponse;
use curvine_client::unified::UnifiedReader;
use curvine_common::conf::FuseConf;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Reader};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use log::error;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::ErrorMonitor;
use std::sync::Arc;

enum ReadTask {
    Read(i64, usize, FuseResponse),
    Complete(CallSender<FsResult<()>>, Option<FuseResponse>),
}

pub struct FuseReader {
    path: Path,
    len: i64,
    sender: AsyncSender<ReadTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    status: FileStatus,
}

impl FuseReader {
    pub fn new(conf: &FuseConf, rt: Arc<Runtime>, reader: UnifiedReader) -> Self {
        let path = reader.path().clone();
        let len = reader.len();
        let err_monitor = Arc::new(ErrorMonitor::new());
        let (sender, receiver) = AsyncChannel::new(conf.stream_channel_size).split();
        let status = reader.status().clone();
        // Phase 2b: capture the backend kind and the metrics gate at open time and
        // move them into the read task, so the per-IO observe in `read_future` has
        // the `path_type` label without re-resolving it on the hot path.
        let path_type = reader.path_type();
        let metrics_enabled = conf.metrics_enabled;

        let monitor = err_monitor.clone();
        rt.spawn(async move {
            let res = Self::read_future(reader, receiver, path_type, metrics_enabled).await;
            match res {
                Ok(_) => (),

                Err(e) => {
                    error!("fuse reader error: {}", e);
                    monitor.set_error(e);
                }
            }
        });

        Self {
            path,
            len,
            sender,
            err_monitor,
            status,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    fn check_error(&self, e: FsError) -> FsError {
        self.err_monitor.take_error().unwrap_or(e)
    }

    pub async fn read(&self, op: Read<'_>, reply: FuseResponse) -> FsResult<()> {
        let res = self
            .sender
            .send(ReadTask::Read(
                op.arg.offset as i64,
                op.arg.size as usize,
                reply,
            ))
            .await
            .map_err(|e| self.check_error(e.into()));
        res
    }

    pub async fn complete(&self, reply: Option<FuseResponse>) -> FsResult<()> {
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            self.sender.send(ReadTask::Complete(rx, reply)).await?;
            // Double `?`: unwrap the channel receive, then propagate the real
            // backend complete result (issue #1118).
            tx.receive().await??;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    async fn read_future(
        mut reader: UnifiedReader,
        mut req_receiver: AsyncReceiver<ReadTask>,
        path_type: &'static str,
        metrics_enabled: bool,
    ) -> FsResult<()> {
        while let Some(task) = req_receiver.recv().await {
            match task {
                ReadTask::Read(off, len, reply) => {
                    // Phase 2b: observe the read backend IO (io_type=read). The
                    // inflight guard wraps ONLY the `fuse_read` call (backend
                    // concurrency), and is dropped before the reply enqueue so the
                    // gauge excludes reply-channel time. Then record duration /
                    // requests / size / bytes via the shared helper.
                    let io_start = if metrics_enabled {
                        Some(mono_now())
                    } else {
                        None
                    };
                    let inflight = FuseMetrics::stream_io_guard(metrics_enabled, IO_TYPE_READ);
                    let data = reader.fuse_read(off, len).await;
                    drop(inflight);

                    if let Some(start) = io_start {
                        let ok = data.is_ok();
                        // Actual bytes read (short reads count actual); requested
                        // size is `len` (the request-size distribution).
                        let bytes = data
                            .as_ref()
                            .map(|v| v.iter().map(|s| s.len() as u64).sum())
                            .unwrap_or(0);
                        FuseMetrics::get().record_stream_io(
                            IO_TYPE_READ,
                            path_type,
                            ok,
                            bytes,
                            len as u64,
                            start.elapsed().as_micros() as u64,
                        );
                    }

                    reply.send_data(data.map_err(|x| x.into())).await?;
                }

                ReadTask::Complete(tx, reply) => {
                    // Complete/release IO accounting lives at the send_stream layer
                    // (stream_lifecycle_*), not here — the Complete arm cannot tell
                    // flush from fsync from release, and a reader+writer double
                    // observe would double-count. So no io_* observe here.
                    let res = reader.complete().await;
                    // Deliver the real backend result to the caller (tx) first,
                    // then the kernel reply (issue #1118).
                    crate::fs::deliver_stream_result(res, tx, reply).await?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fuse_metrics::{
        ActiveGuard, FuseMetrics, FuseReqCtx, FuseReqKind, FuseReqLabels, IO_TYPE_READ,
        STAGE_STREAM_IO,
    };
    use crate::raw::fuse_abi::{fuse_in_header, fuse_read_in};
    use crate::session::{FuseResponse, FuseTask};
    use curvine_client::unified::UnifiedReader;
    use curvine_common::fs::local::LocalReader;
    use orpc::common::Metrics as m;
    use orpc::runtime::AsyncRuntime;
    use orpc::sync::channel::AsyncChannel;
    use std::io::Write as _;

    // Build a metrics-enabled FuseResponse over a real channel, and spawn a drainer
    // so the worker's `send_data`/`send_rep` enqueue completes (the reply task is
    // discarded — we only care that the worker's task body ran and observed IO).
    fn metrics_reply(rt: &AsyncRuntime) -> FuseResponse {
        FuseMetrics::ensure_init().unwrap();
        let (tx, mut rx) = AsyncChannel::<FuseTask>::new(16).split();
        rt.spawn(async move { while rx.recv().await.is_some() {} });
        let gauge = m::new_gauge(
            format!("fr_it_active_{}", std::process::id()),
            "test".to_string(),
        )
        .unwrap();
        let labels = FuseReqLabels::new("Read", FuseReqKind::Stream, 64);
        let ctx = FuseReqCtx {
            labels,
            active: Some(ActiveGuard::new(gauge)),
        };
        FuseResponse::new_reply(1, tx, false, Some(ctx))
    }

    fn read_op_arg(offset: u64, size: u32) -> fuse_read_in {
        fuse_read_in {
            fh: 0,
            offset,
            size,
            read_flags: 0,
            lock_owner: 0,
            flags: 0,
            padding: 0,
        }
    }

    // A real FuseReader over UnifiedReader::Local drives the read task body through
    // the public `read()`, proving the production wiring the helper test cannot:
    // path_type="local" flows from `path_type()` into the task body, io_* and
    // stage=stream_io are observed with ACTUAL bytes read and REQUESTED size.
    //
    // Parallel-safety: the (io_type=read, path_type=local) children are touched ONLY
    // by this test (send_stream tests use TestFileSystem which ENOSYS before the task
    // body; the helper test uses a synthetic path_type), so exact deltas are safe.
    // The shared stage=stream_io child uses a lower bound.
    #[test]
    fn local_reader_task_body_observes_io_with_local_path_type() {
        let rt = AsyncRuntime::single();
        rt.block_on(async {
            FuseMetrics::ensure_init().unwrap();
            let mx = FuseMetrics::get();
            let dur_before = mx
                .io_duration_us
                .with_label_values(&[IO_TYPE_READ, "local", "success"])
                .get_sample_count();
            let req_before = mx
                .io_requests_total
                .with_label_values(&[IO_TYPE_READ, "local", "success"])
                .get();
            let bytes_before = mx
                .io_bytes_total
                .with_label_values(&[IO_TYPE_READ, "local", "success"])
                .get();
            let size_before = mx
                .io_size_bytes
                .with_label_values(&[IO_TYPE_READ, "local"])
                .get_sample_count();
            let stage_before = mx
                .stage_duration_us
                .with_label_values(&[STAGE_STREAM_IO, "stream", "success"])
                .get_sample_count();

            // A temp file with 4096 bytes of content to read back.
            let path_buf = std::env::temp_dir().join(format!(
                "fr_it_read_{}_{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            {
                let mut f = std::fs::File::create(&path_buf).unwrap();
                f.write_all(&vec![7u8; 4096]).unwrap();
            }
            let path = curvine_common::fs::Path::from_str(path_buf.to_str().unwrap()).unwrap();

            let conf = FuseConf::default(); // metrics_enabled=true, unbounded channel
            let reader = UnifiedReader::Local(LocalReader::new(&path, 4096).unwrap());
            assert_eq!(reader.path_type(), "local");
            let rt2 = Arc::new(AsyncRuntime::single());
            let fuse_reader = FuseReader::new(&conf, rt2.clone(), reader);
            // Leak our Arc so this runtime is never the-last-Arc-dropped inside the
            // outer async block (dropping a tokio runtime from an async context panics).
            std::mem::forget(rt2);

            // Request 8192 bytes; the file only has 4096 (a short read).
            let reply = metrics_reply(&rt);
            let header = fuse_in_header::default();
            let arg = read_op_arg(0, 8192);
            let op = Read {
                header: &header,
                arg: &arg,
            };
            fuse_reader.read(op, reply).await.unwrap();

            // The read task runs on its own spawned task; poll the metric until the
            // observe lands (bounded wait so a wiring regression fails, not hangs).
            let deadline = 50;
            for _ in 0..deadline {
                if mx
                    .io_duration_us
                    .with_label_values(&[IO_TYPE_READ, "local", "success"])
                    .get_sample_count()
                    > dur_before
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }

            assert_eq!(
                mx.io_duration_us
                    .with_label_values(&[IO_TYPE_READ, "local", "success"])
                    .get_sample_count(),
                dur_before + 1,
                "read task body observed io_duration_us{{read,local,success}} exactly once"
            );
            assert_eq!(
                mx.io_requests_total
                    .with_label_values(&[IO_TYPE_READ, "local", "success"])
                    .get(),
                req_before + 1,
                "io_requests_total{{read,local,success}} +1"
            );
            assert_eq!(
                mx.io_bytes_total
                    .with_label_values(&[IO_TYPE_READ, "local", "success"])
                    .get(),
                bytes_before + 4096,
                "io_bytes_total uses ACTUAL bytes read (4096, the short read), not requested 8192"
            );
            assert_eq!(
                mx.io_size_bytes
                    .with_label_values(&[IO_TYPE_READ, "local"])
                    .get_sample_count(),
                size_before + 1,
                "io_size_bytes observed once (the requested-size distribution)"
            );
            assert!(
                mx.stage_duration_us
                    .with_label_values(&[STAGE_STREAM_IO, "stream", "success"])
                    .get_sample_count()
                    > stage_before,
                "read backend call also emits stage=stream_io,kind=stream"
            );
            // NOTE: we deliberately do NOT assert `stream_io_inflight{read}` here.
            // It is a process-global gauge shared with other tests, so a point-in-time
            // read (even against a baseline) races a concurrent test holding a read
            // guard — neither `==0` nor `==before` is reliable under the default
            // parallel harness. The guard's inc/dec balance and its "wraps only the
            // backend call, dropped before reply" scope are pinned deterministically by
            // the isolated `stream_io_guard_gate_and_balance` unit test instead.

            let _ = std::fs::remove_file(&path_buf);
        });
    }
}
