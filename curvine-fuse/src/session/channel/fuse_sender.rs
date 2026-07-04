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
use orpc::io::IOResult;
use orpc::runtime::Runtime;
use orpc::sync::channel::AsyncReceiver;
use orpc::sys::pipe::{AsyncFd, Pipe2, PipeFd};
use orpc::{err_box, sys};
use std::sync::Arc;

/// FuseSender
/// Reads data from queue and writes to fuse fd.
/// 1. For metadata requests, write response directly
/// 2. For read/write data requests, process then write response
pub struct FuseSender<T> {
    pub fs: Arc<T>,
    rt: Arc<Runtime>,
    kernel_fd: Arc<AsyncFd>,
    receiver: AsyncReceiver<FuseTask>,
    pipe2: Pipe2,
    debug: bool,
}

impl<T: FileSystem> FuseSender<T> {
    pub(crate) fn new(
        fs: Arc<T>,
        rt: Arc<Runtime>,
        kernel_fd: Arc<AsyncFd>,
        receiver: AsyncReceiver<FuseTask>,
        buf_size: usize,
        debug: bool,
    ) -> IOResult<Self> {
        let pipe2 = Pipe2::new(PipeFd::new(buf_size, false, false)?)?;
        let fuse_rx = Self {
            fs,
            rt,
            kernel_fd,
            receiver,
            pipe2,
            debug,
        };

        Ok(fuse_rx)
    }

    pub fn rt(&self) -> &Runtime {
        &self.rt
    }

    pub async fn start(mut self) -> FuseResult<()> {
        while let Some(task) = self.receiver.recv().await {
            match task {
                // A replied request with metrics context. This is the E2E finish
                // point: after the kernel-fd write, the request metrics are
                // recorded and the `active` guard is dropped (releasing the
                // in-flight count). The match arm only does the splice + a single
                // helper call; all `with_label_values` lives in the helper so it
                // is unit-testable without a kernel fd.
                FuseTask::RequestReply {
                    data,
                    labels,
                    active,
                    status,
                    errno,
                    unsupported_reason,
                    queue_guard,
                } => {
                    // reply_queue_depth dec at the DEQUEUE point: the task has left
                    // the channel, so the subsequent splice() is NOT counted as
                    // queue backlog. (The E2E `active` guard, dropped after the
                    // write below, is a separate gauge.)
                    mark_dequeued(queue_guard);
                    let id = data.header.unique;
                    let response_bytes = data.len();

                    let write_start = mono_now();
                    let send_result = self.send(data).await;
                    let write_us = write_start.elapsed().as_micros() as u64;
                    // Taken AFTER the send so the E2E duration includes the write
                    // (even a failed splice).
                    let total_us = labels.elapsed_us();

                    let write = match &send_result {
                        Ok(()) => WriteOutcome::Success,
                        Err(e) => {
                            let os_errno = e.raw_error().raw_os_error();
                            // Keep the existing diagnostic log alongside the metric.
                            if os_errno != Some(libc::ENOENT) {
                                warn!("error send unique {}: {}", id, e);
                            }
                            WriteOutcome::Failed { errno: os_errno }
                        }
                    };

                    // `status` from the task is the FS-operation result
                    // (`op_status`). The kernel-observed `request_status` is the
                    // same, except a delivery (kernel-fd write) failure makes it
                    // Error even when the op succeeded — see the design's
                    // "operation vs request status".
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

                    // Finish point: drop the in-flight guard after the write.
                    drop(active);
                }

                // A kernel notification: same splice, no request guard/finish.
                // Records notify_total at its two sender-side points: success
                // after the write, write_failed on splice error (enqueue_failed
                // is recorded earlier in send_notify).
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
                        Ok(()) => metrics.record_notify_result(code, NOTIFY_SUCCESS),
                        Err(e) => {
                            if e.raw_error().raw_os_error() != Some(libc::ENOENT) {
                                warn!("error send notify {}: {}", id, e);
                            }
                            metrics.record_notify_result(code, NOTIFY_WRITE_FAILED);
                        }
                    }
                }

                // Legacy fast path (metrics disabled): byte-identical to before.
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

    // Send response data to fuse.
    pub async fn send(&mut self, rep: ResponseData) -> IOResult<()> {
        if self.debug {
            info!("reply {:?}", rep.header);
        }
        self.splice(rep).await
    }

    pub async fn write(&mut self, rep: ResponseData) -> IOResult<()> {
        let (_, iovec) = rep.as_iovec()?;
        self.kernel_fd
            .async_write(|fd| sys::writev(fd.fd(), &iovec))
            .await?;
        Ok(())
    }

    async fn splice(&mut self, rep: ResponseData) -> IOResult<()> {
        let (len, iovec) = rep.as_iovec()?;
        let write_len = self.pipe2.write_iov(&iovec).await?;
        if write_len != len {
            return err_box!("io return value error, res: {}, expect: {}", write_len, len);
        }

        let read_len = self.pipe2.read_io(&self.kernel_fd, len).await?;
        if read_len != len {
            err_box!("io return value error, res: {}, expect: {}", read_len, len)
        } else {
            Ok(())
        }
    }
}

/// Drop a dequeued task's `reply_queue_depth` guard, decrementing the gauge at
/// the dequeue point (immediately after `recv()`), so the gauge reflects only
/// channel backlog and excludes the subsequent `splice()`. A named helper so the
/// "drop on dequeue, not on completion" rule is explicit at both call sites and
/// hard to misplace. `None` (metrics disabled) is a no-op.
#[inline]
fn mark_dequeued(queue_guard: Option<ActiveGuard>) {
    drop(queue_guard);
}

#[cfg(test)]
mod tests {
    use super::mark_dequeued;
    use crate::fuse_metrics::ActiveGuard;
    use orpc::common::Metrics as m;

    // `mark_dequeued` decrements at the dequeue point. Uses an INJECTED isolated
    // gauge (not the process-global `reply_queue_depth`) so it is parallel-safe
    // and asserts the exact "guard held -> +1, mark_dequeued -> back to 0"
    // behaviour the sender relies on (dec before the splice, not after).
    #[test]
    fn mark_dequeued_drops_guard_at_dequeue() {
        let g = m::new_gauge("test_mark_dequeued_gauge", "test").unwrap();
        let guard = ActiveGuard::new(g.clone());
        assert_eq!(g.get(), 1, "guard rides the task: +1 in the channel");
        // Sender's first line after recv(): drop the queue guard.
        mark_dequeued(Some(guard));
        assert_eq!(g.get(), 0, "dequeue decrements before any splice work");
    }

    // disabled mode carries `None`; mark_dequeued is a no-op.
    #[test]
    fn mark_dequeued_none_is_noop() {
        mark_dequeued(None);
    }
}
