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

use crate::fs::FileSystem;
use crate::fuse_metrics::{
    mono_now, ActiveGuard, FuseMetrics, FuseReqStatus, WriteOutcome, NOTIFY_SUCCESS,
    NOTIFY_WRITE_FAILED,
};
use crate::session::{FuseTask, ResponseData};
use crate::FuseResult;
use log::{info, warn};
use orpc::common::Gauge;
use orpc::io::IOResult;
use orpc::runtime::Runtime;
use orpc::sync::channel::AsyncReceiver;
use orpc::sys::pipe::{AsyncFd, Pipe2, PipeFd};
use orpc::{err_box, sys, try_option_ref};
use std::sync::Arc;

/// Small responses use writev; splice only pays off for larger payloads.
const SPLICE_THRESHOLD: usize = 8192;

/// FuseSender reads data from queue and writes to fuse fd.
/// 1. For metadata requests, write response directly
/// 2. For read/write data requests, process then write response
pub struct FuseSender<T> {
    pub fs: Arc<T>,
    rt: Arc<Runtime>,
    kernel_fd: Arc<AsyncFd>,
    receiver: AsyncReceiver<FuseTask>,
    pipe2: Option<Pipe2>,
    debug: bool,
    progress: Option<Gauge>,
}

impl<T: FileSystem> FuseSender<T> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        fs: Arc<T>,
        rt: Arc<Runtime>,
        kernel_fd: Arc<AsyncFd>,
        receiver: AsyncReceiver<FuseTask>,
        buf_size: usize,
        debug: bool,
        enable_splice: bool,
        mnt: &str,
        idx: usize,
        metrics_enabled: bool,
    ) -> IOResult<Self> {
        let pipe2 = if enable_splice {
            Some(Pipe2::new(PipeFd::new(buf_size, false, false)?)?)
        } else {
            None
        };
        let progress = if metrics_enabled {
            let g = FuseMetrics::get().sender_progress_gauge(mnt, idx);
            FuseMetrics::record_sender_progress(&g);
            Some(g)
        } else {
            None
        };
        let fuse_rx = Self {
            fs,
            rt,
            kernel_fd,
            receiver,
            pipe2,
            debug,
            progress,
        };

        Ok(fuse_rx)
    }

    pub fn rt(&self) -> &Runtime {
        &self.rt
    }

    pub async fn start(mut self) -> FuseResult<()> {
        while let Some(task) = self.receiver.recv().await {
            match task {
                FuseTask::RequestReply {
                    data,
                    labels,
                    active,
                    status,
                    errno,
                    unsupported_reason,
                    queue_guard,
                } => {
                    mark_dequeued(queue_guard);
                    let id = data.header.unique;
                    let response_bytes = data.len();

                    let write_start = mono_now();
                    let send_result = self.send(data).await;
                    let write_us = write_start.elapsed().as_micros() as u64;
                    let total_us = labels.elapsed_us();

                    let write = match &send_result {
                        Ok(()) => WriteOutcome::Success,
                        Err(e) => {
                            let os_errno = e.raw_error().raw_os_error();
                            if os_errno != Some(libc::ENOENT) {
                                warn!("error send unique {}: {}", id, e);
                            }
                            WriteOutcome::Failed { errno: os_errno }
                        }
                    };

                    // Delivery failure turns request_status into Error without changing op_status.
                    let op_status = status;
                    let request_status = match write {
                        WriteOutcome::Success => op_status,
                        WriteOutcome::Failed { .. } => FuseReqStatus::Error,
                    };

                    FuseMetrics::get().record_request_finish(
                        labels.opcode,
                        labels.kind,
                        op_status,
                        request_status,
                        errno,
                        unsupported_reason,
                        response_bytes,
                        write,
                        write_us,
                        total_us,
                    );

                    drop(active);

                    // Record sender liveness on a successful delivery. A stalled sender
                    // stops advancing this timestamp while siblings keep refreshing,
                    // localizing the stall at scrape time.
                    if matches!(write, WriteOutcome::Success) {
                        if let Some(g) = &self.progress {
                            FuseMetrics::record_sender_progress(g);
                        }
                    }
                }

                FuseTask::NotifyReply {
                    data,
                    code,
                    queue_guard,
                } => {
                    // Same dequeue-point dec as the request path.
                    mark_dequeued(queue_guard);
                    let id = data.header.unique;
                    let metrics = FuseMetrics::get();
                    match self.send(data).await {
                        Ok(()) => {
                            metrics.record_notify_result(code, NOTIFY_SUCCESS);
                            // Same liveness signal as the request path.
                            if let Some(g) = &self.progress {
                                FuseMetrics::record_sender_progress(g);
                            }
                        }
                        Err(e) => {
                            if e.raw_error().raw_os_error() != Some(libc::ENOENT) {
                                warn!("error send notify {}: {}", id, e);
                            }
                            metrics.record_notify_result(code, NOTIFY_WRITE_FAILED);
                        }
                    }
                }

                FuseTask::Reply(reply) => {
                    let id = reply.header.unique;
                    if let Err(e) = self.send(reply).await {
                        if e.raw_error().raw_os_error() != Some(libc::ENOENT) {
                            warn!("error send unique {}: {}", id, e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn send(&mut self, rep: ResponseData) -> IOResult<()> {
        if self.debug {
            info!("reply {:?}", rep.header);
        }

        let len = rep.header.len as usize;
        if self.pipe2.is_some() && len >= SPLICE_THRESHOLD {
            self.splice(rep).await
        } else {
            self.write(rep).await
        }
    }

    // Non-splice reply path. The fuse device has no send-buffer watermark, so writev
    // never returns EAGAIN like the SPLICE_F_NONBLOCK transfer does — it fails with a
    // real errno. Hence enable_splice=false works and splice_retry is NOT applied here.
    pub async fn write(&mut self, rep: ResponseData) -> IOResult<()> {
        let (len, iovec) = rep.as_iovec()?;
        let written = self
            .kernel_fd
            .async_write(|fd| sys::writev(fd.fd(), &iovec))
            .await?;
        if written as usize != len {
            return err_box!("short writev: wrote {} of {}", written, len);
        }
        Ok(())
    }

    async fn splice(&self, rep: ResponseData) -> IOResult<()> {
        let pipe2 = try_option_ref!(self.pipe2);

        let (len, iovec) = rep.as_iovec()?;
        if let Err(e) = pipe2.write_iov(len, &iovec).await {
            Self::drain_pipe(pipe2);
            return Err(e);
        }

        if let Err(e) = pipe2.read_io(&self.kernel_fd, len).await {
            Self::drain_pipe(pipe2);
            return Err(e);
        }

        Ok(())
    }

    /// Drain residual bytes after a failed transfer, else stale bytes at the FIFO
    /// head poison every subsequent response. EINTR is retried; the loop stops on
    /// EAGAIN/EWOULDBLOCK (empty), EOF, or any other error.
    fn drain_pipe(pipe2: &Pipe2) {
        let fd = pipe2.read_raw_fd();
        let mut buf = [0u8; 8192];
        loop {
            match sys::read(fd, &mut buf) {
                Ok(n) if n > 0 => continue,
                Ok(_) => break, // EOF
                Err(e) => {
                    if e.raw_error().raw_os_error() == Some(libc::EINTR) {
                        continue; // interrupted; retry
                    }
                    // EAGAIN/EWOULDBLOCK: pipe is empty; any other error: stop.
                    break;
                }
            }
        }
    }
}

#[inline]
fn mark_dequeued(queue_guard: Option<ActiveGuard>) {
    drop(queue_guard);
}

#[cfg(test)]
mod tests {
    use super::mark_dequeued;
    use crate::fuse_metrics::ActiveGuard;
    use orpc::common::Metrics as m;

    // `mark_dequeued` decrements at the dequeue point.
    #[test]
    fn mark_dequeued_drops_guard_at_dequeue() {
        let g = m::new_gauge("test_mark_dequeued_gauge", "test").unwrap();
        let guard = ActiveGuard::new(g.clone());
        assert_eq!(g.get(), 1, "guard rides the task: +1 in the channel");
        mark_dequeued(Some(guard));
        assert_eq!(g.get(), 0, "dequeue decrements before any splice work");
    }

    // disabled mode carries `None`; mark_dequeued is a no-op.
    #[test]
    fn mark_dequeued_none_is_noop() {
        mark_dequeued(None);
    }
}
