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

use crate::fuse_metrics::{FuseReqCtx, FuseReqStatus, FuseRespMetrics};
use crate::raw::fuse_abi::{
    fuse_notify_inval_entry_out, fuse_notify_inval_inode_out, fuse_out_header,
};
use crate::session::{FuseNotifyCode, FuseTask};
use crate::{FuseError, FuseResult, FuseUtils};
use crate::{FUSE_NOTIFY_UNIQUE, FUSE_OUT_HEADER_LEN, FUSE_SUCCESS};
use log::{info, warn};
use orpc::io::IOResult;
use orpc::sync::channel::AsyncSender;
use orpc::sys::DataSlice;
use orpc::ternary;
use parking_lot::Mutex;
use std::fmt::Debug;
use std::io::IoSlice;
use std::sync::Arc;
use std::vec;
use tokio_util::bytes::BytesMut;

pub struct ResponseData {
    pub header: fuse_out_header,
    pub data: Vec<DataSlice>,
}

impl ResponseData {
    pub fn new(header: fuse_out_header, data: Vec<DataSlice>) -> Self {
        Self { header, data }
    }

    pub fn len(&self) -> u32 {
        self.header.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_iovec(&self) -> IOResult<(usize, Vec<IoSlice<'_>>)> {
        let mut iovec: Vec<IoSlice<'_>> = Vec::with_capacity(self.data.len() + 1);

        // write header
        let header_bytes = FuseUtils::struct_as_bytes(&self.header);
        iovec.push(IoSlice::new(header_bytes));

        // write data
        for data in &self.data {
            iovec.push(IoSlice::new(data.as_slice()));
        }
        Ok((self.header.len as usize, iovec))
    }

    fn create(unique: u64, error: i32, data: Vec<DataSlice>) -> Self {
        let data_len = data.iter().map(|x| x.len()).sum::<usize>();
        let error = ternary!(unique == FUSE_NOTIFY_UNIQUE, error, -error);

        // The fuse error code is the negative number of the os error code.
        let header = fuse_out_header {
            len: (FUSE_OUT_HEADER_LEN + data_len) as u32,
            error,
            unique,
        };

        Self::new(header, data)
    }
}

// Send fuse response to the mount point.
//
// `metrics` is the per-request metrics slot (Phase 1a): `Some` when metrics are
// enabled, `None` for the disabled fast path. It is `Arc<Mutex<…>>` so that
// cloning a `FuseResponse` (used by `dispatch_meta`, which borrows `&self`)
// shares the *same* slot — the active guard is taken exactly once regardless of
// clones. The lock is uncontended (written once on the reply path, read once in
// the sender) and is never held across an `.await`.
#[derive(Clone)]
pub struct FuseResponse {
    pub(crate) unique: u64,
    pub(crate) sender: AsyncSender<FuseTask>,
    pub(crate) debug: bool,
    pub(crate) metrics: Option<Arc<Mutex<FuseRespMetrics>>>,
}

impl FuseResponse {
    /// Build a reply handle. `ctx = Some(..)` enables metrics (the reply path
    /// produces `RequestReply`/`NotifyReply` and finishes in the sender);
    /// `ctx = None` is the disabled fast path (produces the legacy `Reply`).
    pub(crate) fn new_reply(
        unique: u64,
        sender: AsyncSender<FuseTask>,
        debug: bool,
        ctx: Option<FuseReqCtx>,
    ) -> Self {
        let metrics = ctx.map(|c| Arc::new(Mutex::new(FuseRespMetrics::new(c))));
        Self {
            unique,
            sender,
            debug,
            metrics,
        }
    }

    pub fn unique(&self) -> u64 {
        self.unique
    }

    fn rep_log(&self, e: &FuseError) {
        if self.debug || !matches!(e.errno, libc::ENOENT | libc::ENODATA | libc::ENOSYS) {
            warn!("send_rep unique {}: {}", self.unique, e);
        }
    }

    /// Classify a *non-Ok* reply into a `FuseReqStatus` from the explicit
    /// source tag — **never from errno alone** (a backend `ENOSYS`/`EINTR` with
    /// no tag stays `Error`). `Unsupported`/`Interrupted` are reached only when
    /// the caller passes the matching tag (set at the wildcard / `Notimplemented`
    /// / SETLKW-interrupt sites). Phase 1a-1 wires this as control flow; the
    /// status-labelled metrics read the stashed value in Phase 1a-2.
    fn err_status(unsupported_reason: Option<&'static str>, interrupted: bool) -> FuseReqStatus {
        if unsupported_reason.is_some() {
            FuseReqStatus::Unsupported
        } else if interrupted {
            FuseReqStatus::Interrupted
        } else {
            FuseReqStatus::Error
        }
    }

    /// Build the reply task and enqueue it. The single finish entry point for a
    /// replied request:
    ///
    /// - metrics enabled: stash status/errno, `take()` the active guard out of
    ///   the shared slot (move-only, exactly once), mark `finished`, then send a
    ///   `RequestReply`. All slot access is scoped *before* the `.await` so the
    ///   `parking_lot` guard is never held across the await. **If the enqueue
    ///   fails**, the request never reaches the sender, so we re-lock and correct
    ///   `request_status` to `Error` (the `Pending → FinishedEarly(enqueue err)`
    ///   transition) while leaving `op_status` as the FS-operation result.
    /// - metrics disabled: send the legacy `Reply`.
    ///
    /// `status`/`errno` are computed by the caller (which still holds the typed
    /// result); they cannot be derived from the enqueue outcome because a
    /// successful enqueue of an error frame returns `Ok(())`.
    async fn finish_request(
        &self,
        data: ResponseData,
        status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
    ) -> IOResult<()> {
        let slot = match &self.metrics {
            None => return self.sender.send(FuseTask::Reply(data)).await,
            Some(slot) => slot,
        };

        // Extract everything BEFORE the send: the active guard is move-only and
        // `send` consumes the task (it is not returned on error), so the
        // guard/labels must leave the slot here.
        let (labels, active) = {
            let mut m = slot.lock();
            if m.finished {
                // Double reply on an already-finished context: a logic bug.
                // Never double-count/double-drop — log and no-op in all builds
                // (this is testable, unlike a debug_assert that would panic the
                // release-path test).
                warn!(
                    "double reply on an already-finished FuseResponse (unique {})",
                    self.unique
                );
                return Ok(());
            }
            m.op_status = Some(status);
            m.request_status = Some(status);
            m.errno = errno;
            m.unsupported_reason = unsupported_reason;
            m.finished = true;
            let active = m
                .active
                .take()
                .unwrap_or_else(crate::fuse_metrics::ActiveGuard::noop);
            (m.labels, active)
        }; // lock dropped here, before the .await below.

        let send_result = self
            .sender
            .send(FuseTask::RequestReply {
                data,
                labels,
                active,
                status,
                errno,
                unsupported_reason,
            })
            .await;

        if send_result.is_err() {
            // Enqueue failed: the request never reaches the sender's finish
            // point. The FS-operation status (`op_status`) is preserved, but the
            // delivered/result status becomes `Error`. (The guard was moved into
            // the consumed task and dropped with it.) Phase 1a-2 reads this to
            // emit `reply_enqueue_errors_total` + `request_duration_us{status=error}`.
            let mut m = slot.lock();
            m.request_status = Some(FuseReqStatus::Error);
        }
        send_result
    }

    /// Finish a no-reply request (`Forget` / `BatchForget`). Inspects the
    /// operation result (so a failing forget is not a phantom success), drops
    /// the active guard, and sends no task. Non-async: nothing reaches the
    /// sender. Phase 1a-1 runs the state machine but emits no real metric.
    ///
    /// No-reply errors are always classified `Error` (via `err_status` with no
    /// interrupt tag): `Forget`/`BatchForget` are never interrupted, and the
    /// `trait_default` unsupported reason is out of scope for Phase 1a-1. If
    /// Phase 1a-2 needs no-reply `unsupported`, add a source-tag param here.
    fn finish_no_reply(&self, res: FuseResult<()>) {
        if let Some(slot) = &self.metrics {
            let mut m = slot.lock();
            if m.finished {
                return;
            }
            // No-reply errors are always `Error`: Forget/BatchForget are never
            // interrupted, and the `trait_default` unsupported reason is out of
            // scope for Phase 1a-1. Classified explicitly (not via the slot's
            // `unsupported_reason`) so the code matches the doc comment and does
            // not depend on prior slot state. If Phase 1a-2 needs no-reply
            // `unsupported`, add a source-tag parameter here.
            let status = match &res {
                Ok(_) => FuseReqStatus::Success,
                Err(_) => FuseReqStatus::Error,
            };
            m.op_status = Some(status);
            m.request_status = Some(status);
            m.finished = true;
            // Drop the guard explicitly (no task carries it on the no-reply path).
            let _ = m.active.take();
        }
    }

    /// Finish a request that errored **before** any reply could be produced —
    /// e.g. a structural `parse_operator()` failure after the context was
    /// created. Drops the active guard and marks the slot `finished` so the
    /// `active_requests` count cannot leak, but enqueues **no** task and emits
    /// no `requests_total` (the request never dispatched).
    ///
    /// `reason` is the `&'static str` parse-failure reason
    /// (`short_read`/`invalid_header`/`length_mismatch`/`other`) — stashed now,
    /// read by `decode_errors_total{phase="parse",reason}` in Phase 1a-2. A
    /// reason rather than an errno is carried because structural parse failures
    /// have no stable OS errno; `errno` is kept too for diagnostics.
    pub(crate) fn finish_early(&self, errno: i32, reason: &'static str) {
        if let Some(slot) = &self.metrics {
            let mut m = slot.lock();
            if m.finished {
                return;
            }
            m.op_status = Some(FuseReqStatus::Error);
            m.request_status = Some(FuseReqStatus::Error);
            m.errno = errno;
            m.parse_reason = Some(reason);
            m.finished = true;
            let _ = m.active.take();
        }
    }

    pub async fn send_rep<T: Debug, E: Into<FuseError> + Debug>(
        &self,
        res: Result<T, E>,
    ) -> IOResult<()> {
        self.send_rep_tagged(res, None, false).await
    }

    /// Like `send_rep`, but lets the caller attach an explicit status source
    /// tag for the error case: `unsupported_reason` (a known unsupported path —
    /// `unknown_opcode`/`unimplemented_opcode`/`trait_default`) or `interrupted`
    /// (the SETLKW interrupt-notify path). Status is **never** inferred from
    /// errno; an untagged error is always `Error`. (Phase 1a-1 stashes the tag
    /// as control flow; `unsupported_total`/`interrupted_total` read it in 1a-2.)
    pub async fn send_rep_tagged<T: Debug, E: Into<FuseError> + Debug>(
        &self,
        res: Result<T, E>,
        unsupported_reason: Option<&'static str>,
        interrupted: bool,
    ) -> IOResult<()> {
        let (data, status, errno) = match res {
            Ok(v) => {
                if self.debug {
                    info!("send_rep unique {}, res: {:?}", self.unique, v);
                }

                let data = if size_of::<T>() == 0 {
                    vec![]
                } else {
                    vec![DataSlice::buffer(FuseUtils::struct_as_buf(&v))]
                };
                (
                    ResponseData::create(self.unique, FUSE_SUCCESS, data),
                    FuseReqStatus::Success,
                    0,
                )
            }

            Err(e) => {
                let e = e.into();
                self.rep_log(&e);
                let errno = e.errno;
                let status = Self::err_status(unsupported_reason, interrupted);
                (
                    ResponseData::create(self.unique, errno, vec![]),
                    status,
                    errno,
                )
            }
        };

        self.finish_request(data, status, errno, unsupported_reason)
            .await
    }

    pub async fn send_notify(&self, code: FuseNotifyCode, data: Vec<DataSlice>) -> IOResult<()> {
        if self.debug {
            info!("send_notify code {:?}", code);
        }

        let data = ResponseData::create(FUSE_NOTIFY_UNIQUE, code.into(), data);
        // Notifications are not request replies: they never touch the request
        // metrics slot. When metrics are disabled, fall back to the legacy Reply
        // so the disabled path is byte-identical.
        if self.metrics.is_some() {
            self.sender
                .send(FuseTask::NotifyReply {
                    data,
                    code: code.as_str(),
                })
                .await
        } else {
            self.sender.send(FuseTask::Reply(data)).await
        }
    }

    // `send_buf` / `send_data` always classify an error as `Error` and have no
    // source-tag variant: every current `unsupported`/`interrupted` source goes
    // through `send_rep_tagged` (dispatch wildcard, SETLKW interrupt). If a
    // future buffer/data-returning op needs to be tagged unsupported/interrupted,
    // add `send_buf_tagged` / `send_data_tagged` (or route through a shared
    // helper) rather than inferring status from errno.
    pub async fn send_buf(&self, res: FuseResult<BytesMut>) -> IOResult<()> {
        let (data, status, errno) = match res {
            Ok(v) => {
                if self.debug {
                    info!("send_buf unique {}, data len: {}", self.unique, v.len());
                }
                (
                    ResponseData::create(self.unique, FUSE_SUCCESS, vec![DataSlice::Buffer(v)]),
                    FuseReqStatus::Success,
                    0,
                )
            }

            Err(e) => {
                self.rep_log(&e);
                let errno = e.errno;
                (
                    ResponseData::create(self.unique, errno, vec![]),
                    FuseReqStatus::Error,
                    errno,
                )
            }
        };

        self.finish_request(data, status, errno, None).await
    }

    pub async fn send_data(&self, res: FuseResult<Vec<DataSlice>>) -> IOResult<()> {
        let (data, status, errno) = match res {
            Ok(v) => {
                if self.debug {
                    let len = v.iter().map(|x| x.len()).sum::<usize>();
                    info!("send_data unique {}, data len: {}", self.unique, len);
                }
                (
                    ResponseData::create(self.unique, FUSE_SUCCESS, v),
                    FuseReqStatus::Success,
                    0,
                )
            }

            Err(e) => {
                self.rep_log(&e);
                let errno = e.errno;
                (
                    ResponseData::create(self.unique, errno, vec![]),
                    FuseReqStatus::Error,
                    errno,
                )
            }
        };

        self.finish_request(data, status, errno, None).await
    }

    pub fn send_none(&self, res: FuseResult<()>) -> IOResult<()> {
        // No reply is sent to the kernel for Forget/BatchForget, but the request
        // context must still be finished (guard dropped, result classified).
        self.finish_no_reply(res);
        Ok(())
    }

    // notify kernel cache invalidation
    pub async fn send_inode_out(&self, ino: u64, off: i64, len: i64) -> IOResult<()> {
        let arg = fuse_notify_inval_inode_out { ino, off, len };
        let data = vec![DataSlice::buffer(FuseUtils::struct_as_buf(&arg))];
        self.send_notify(FuseNotifyCode::FUSE_NOTIFY_INVAL_INODE, data)
            .await
    }

    pub async fn send_rep_then_inval_inode<T: Debug, E: Into<FuseError> + Debug>(
        &self,
        res: Result<T, E>,
        ino: u64,
        off: i64,
        len: i64,
    ) -> IOResult<()> {
        self.send_rep(res).await?;
        self.send_inode_out(ino, off, len).await
    }

    pub async fn send_rep_then_inval_entry<E: Into<FuseError> + Debug>(
        &self,
        res: Result<(), E>,
        parent: u64,
        name: &str,
    ) -> IOResult<()> {
        self.send_rep(res).await?;
        self.send_entry_out(parent, name).await
    }

    pub async fn send_entry_out(&self, parent: u64, name: &str) -> IOResult<()> {
        let arg = fuse_notify_inval_entry_out {
            parent,
            namelen: name.len() as u32,
            flags: 0,
        };

        let mut name_buf = BytesMut::with_capacity(name.len() + 1);
        name_buf.extend_from_slice(name.as_bytes());
        name_buf.extend_from_slice(b"\0");

        let data = vec![
            DataSlice::buffer(FuseUtils::struct_as_buf(&arg)),
            DataSlice::buffer(name_buf),
        ];
        self.send_notify(FuseNotifyCode::FUSE_NOTIFY_INVAL_ENTRY, data)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fuse_metrics::{ActiveGuard, FuseReqKind, FuseReqLabels};
    use orpc::common::{Gauge, Metrics as m};
    use orpc::sync::channel::{AsyncChannel, AsyncReceiver};

    // Build a FuseResponse whose active guard is backed by `gauge`, so tests can
    // assert "guard dropped exactly once" as a concrete `gauge.get()` count.
    fn reply_with_gauge(unique: u64, gauge: &Gauge) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        let (tx, rx) = AsyncChannel::new(16).split();
        let labels = FuseReqLabels::new("Lookup", FuseReqKind::Metadata, 64);
        let ctx = FuseReqCtx {
            labels,
            active: Some(ActiveGuard::new(gauge.clone())), // inc to 1 now
        };
        (FuseResponse::new_reply(unique, tx, false, Some(ctx)), rx)
    }

    fn disabled_reply(unique: u64) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        let (tx, rx) = AsyncChannel::new(16).split();
        (FuseResponse::new_reply(unique, tx, false, None), rx)
    }

    // T1: a normal metadata reply produces a RequestReply, finishes the slot
    // exactly once, and the active guard is NOT dropped until the task is
    // (i.e. the count is still 1 while the task is in flight, 0 after).
    #[tokio::test]
    async fn t1_request_reply_finishes_once_and_holds_guard_until_task_drops() {
        let g = m::new_gauge("t1_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge(1, &g);
        assert_eq!(g.get(), 1, "guard live after ctx creation");

        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();

        // The slot is finished, and the guard was moved onto the task (still 1).
        {
            let slot = reply.metrics.as_ref().unwrap().lock();
            assert!(slot.finished, "slot marked finished after reply");
            assert!(
                slot.active.is_none(),
                "guard taken out of slot exactly once"
            );
        }
        assert_eq!(g.get(), 1, "guard rides on the task, not yet dropped");

        let task = rx.try_recv().unwrap().expect("a task was enqueued");
        assert!(
            matches!(task, FuseTask::RequestReply { .. }),
            "produced RequestReply"
        );
        drop(task); // sender finish: dropping the task drops the guard
        assert_eq!(g.get(), 0, "guard dropped exactly once at task drop");
    }

    // T13: a real second reply on an already-finished slot is a no-op — no
    // second task enqueued, the guard is not double-taken or double-dropped.
    // (This actually calls send_rep twice, unlike the earlier modelled version.)
    #[tokio::test]
    async fn t13_real_double_reply_is_noop() {
        let g = m::new_gauge("t13_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge(2, &g);

        // First reply: takes the guard, finishes, enqueues a RequestReply.
        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();
        let t1 = rx.try_recv().unwrap().expect("first task");
        assert!(matches!(t1, FuseTask::RequestReply { .. }));
        assert_eq!(g.get(), 1, "guard rides on the first task");

        // Second reply: slot already finished → no-op, no second task.
        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();
        assert!(rx.try_recv().unwrap().is_none(), "no second task enqueued");

        drop(t1);
        assert_eq!(g.get(), 0, "exactly one guard, dropped once");
    }

    // T6: Forget/BatchForget — finish_no_reply inspects the result, drops the
    // guard, and enqueues NO task. Run for both Ok and Err.
    #[tokio::test]
    async fn t6_no_reply_finishes_without_task_for_ok_and_err() {
        // Ok case
        let g_ok = m::new_gauge("t6_ok_active", "test").unwrap();
        let (reply_ok, mut rx_ok) = reply_with_gauge(3, &g_ok);
        reply_ok.send_none(Ok(())).unwrap();
        assert_eq!(g_ok.get(), 0, "no-reply drops the guard");
        assert!(
            rx_ok.try_recv().unwrap().is_none(),
            "no task enqueued on no-reply"
        );
        {
            let slot = reply_ok.metrics.as_ref().unwrap().lock();
            assert!(slot.finished);
            assert_eq!(slot.op_status, Some(FuseReqStatus::Success));
        }

        // Err case — must classify as Error, not a phantom success.
        let g_err = m::new_gauge("t6_err_active", "test").unwrap();
        let (reply_err, mut rx_err) = reply_with_gauge(4, &g_err);
        reply_err.send_none(Err(FuseError::from("boom"))).unwrap();
        assert_eq!(g_err.get(), 0);
        assert!(rx_err.try_recv().unwrap().is_none());
        {
            let slot = reply_err.metrics.as_ref().unwrap().lock();
            assert!(slot.finished);
            assert_eq!(slot.op_status, Some(FuseReqStatus::Error));
        }
    }

    // T7: send_rep_then_inval_inode — the request reply finishes once (a
    // RequestReply), the trailing notification is a NotifyReply that does NOT
    // re-finish the request slot.
    #[tokio::test]
    async fn t7_rep_then_inval_splits_request_and_notify() {
        let g = m::new_gauge("t7_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge(5, &g);

        reply
            .send_rep_then_inval_inode::<(), FuseError>(Ok(()), 1, 0, 0)
            .await
            .unwrap();

        let first = rx.try_recv().unwrap().expect("request reply");
        assert!(
            matches!(first, FuseTask::RequestReply { .. }),
            "first = RequestReply"
        );
        let second = rx.try_recv().unwrap().expect("trailing notify");
        assert!(
            matches!(second, FuseTask::NotifyReply { .. }),
            "second = NotifyReply"
        );

        // Request slot finished exactly once; notify did not touch it again.
        {
            let slot = reply.metrics.as_ref().unwrap().lock();
            assert!(slot.finished);
            assert!(slot.active.is_none());
        }
        drop(first);
        assert_eq!(
            g.get(),
            0,
            "request guard dropped once; notify carried none"
        );
    }

    // T8: parse-after-ctx early finish — `finish_early` (the API the receiver
    // calls when `parse_operator()` fails after the ctx exists) drops the guard,
    // marks finished, and enqueues NO task. No requests_total would be emitted.
    #[tokio::test]
    async fn t8_finish_early_drops_guard_no_task() {
        let g = m::new_gauge("t8_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge(6, &g);
        reply.finish_early(libc::EINVAL, "other");
        assert_eq!(g.get(), 0, "guard dropped on early finish (no leak)");
        assert!(rx.try_recv().unwrap().is_none(), "no request task enqueued");
        {
            let slot = reply.metrics.as_ref().unwrap().lock();
            assert!(slot.finished);
            assert_eq!(slot.errno, libc::EINVAL, "errno stashed for decode_errors");
            assert_eq!(
                slot.parse_reason,
                Some("other"),
                "parse reason stashed for 1a-2"
            );
            assert_eq!(slot.op_status, Some(FuseReqStatus::Error));
        }
    }

    // T11: metrics disabled — produces the legacy Reply, constructs no metrics
    // slot, and notifications also fall back to Reply.
    #[tokio::test]
    async fn t11_disabled_uses_legacy_reply() {
        let (reply, mut rx) = disabled_reply(7);
        assert!(reply.metrics.is_none(), "no metrics slot when disabled");

        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();
        let task = rx.try_recv().unwrap().expect("a task");
        assert!(
            matches!(task, FuseTask::Reply(_)),
            "disabled path = legacy Reply"
        );

        reply
            .send_notify(FuseNotifyCode::FUSE_NOTIFY_INVAL_INODE, vec![])
            .await
            .unwrap();
        let n = rx.try_recv().unwrap().expect("a notify task");
        assert!(
            matches!(n, FuseTask::Reply(_)),
            "disabled notify = legacy Reply"
        );
    }

    // Clone shares the single slot: finishing via a clone marks the original.
    #[tokio::test]
    async fn t13_clone_shares_one_slot() {
        let g = m::new_gauge("t13_clone_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge(8, &g);
        let clone = reply.clone();

        // Finish via the clone.
        clone.send_rep::<(), FuseError>(Ok(())).await.unwrap();

        // The original sees finished=true and the guard gone — shared slot.
        {
            let slot = reply.metrics.as_ref().unwrap().lock();
            assert!(slot.finished, "clone and original share one slot");
            assert!(slot.active.is_none());
        }
        let task = rx.try_recv().unwrap().expect("one task");
        drop(task);
        assert_eq!(g.get(), 0, "single guard, dropped once");
    }

    // #3: reply enqueue failure splits op_status (FS result) from request_status
    // (delivery). FS op succeeds but the channel is closed → request_status=Error
    // while op_status stays Success; guard dropped exactly once (with the
    // consumed task).
    #[tokio::test]
    async fn enqueue_failure_sets_request_status_error_keeps_op_status() {
        let g = m::new_gauge("enq_fail_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge(9, &g);
        drop(rx); // close the channel so send() fails

        let send_result = reply.send_rep::<(), FuseError>(Ok(())).await;
        assert!(
            send_result.is_err(),
            "enqueue must fail on a closed channel"
        );

        let slot = reply.metrics.as_ref().unwrap().lock();
        assert!(slot.finished);
        assert_eq!(
            slot.op_status,
            Some(FuseReqStatus::Success),
            "FS op succeeded"
        );
        assert_eq!(
            slot.request_status,
            Some(FuseReqStatus::Error),
            "delivery failed → request_status=Error"
        );
        drop(slot);
        assert_eq!(g.get(), 0, "guard dropped once (with the consumed task)");
    }

    // #4/#5: status is classified from the explicit source tag, never errno.
    #[tokio::test]
    async fn status_classification_from_source_tag_not_errno() {
        // backend ENOSYS with no tag → Error (not laundered into Unsupported).
        let g1 = m::new_gauge("tag_backend_enosys", "test").unwrap();
        let (r1, mut rx1) = reply_with_gauge(10, &g1);
        let err: FuseResult<()> = Err(FuseError::new(libc::ENOSYS, "backend".into()));
        r1.send_rep_tagged(err, None, false).await.unwrap();
        let _ = rx1.try_recv();
        assert_eq!(
            r1.metrics.as_ref().unwrap().lock().op_status,
            Some(FuseReqStatus::Error),
            "untagged ENOSYS is Error"
        );

        // tagged unimplemented_opcode → Unsupported.
        let g2 = m::new_gauge("tag_unimpl", "test").unwrap();
        let (r2, mut rx2) = reply_with_gauge(11, &g2);
        let err: FuseResult<()> = Err(FuseError::new(libc::ENOSYS, "unimpl".into()));
        r2.send_rep_tagged(err, Some("unimplemented_opcode"), false)
            .await
            .unwrap();
        let _ = rx2.try_recv();
        assert_eq!(
            r2.metrics.as_ref().unwrap().lock().op_status,
            Some(FuseReqStatus::Unsupported),
            "tagged path is Unsupported"
        );

        // ordinary EINTR with no interrupt tag → Error (not Interrupted).
        let g3 = m::new_gauge("tag_plain_eintr", "test").unwrap();
        let (r3, mut rx3) = reply_with_gauge(12, &g3);
        let err: FuseResult<()> = Err(FuseError::new(libc::EINTR, "plain".into()));
        r3.send_rep_tagged(err, None, false).await.unwrap();
        let _ = rx3.try_recv();
        assert_eq!(
            r3.metrics.as_ref().unwrap().lock().op_status,
            Some(FuseReqStatus::Error),
            "untagged EINTR is Error"
        );

        // interrupt source tag → Interrupted.
        let g4 = m::new_gauge("tag_interrupt", "test").unwrap();
        let (r4, mut rx4) = reply_with_gauge(13, &g4);
        let err: FuseResult<()> = Err(FuseError::new(libc::EINTR, "setlkw".into()));
        r4.send_rep_tagged(err, None, true).await.unwrap();
        let _ = rx4.try_recv();
        assert_eq!(
            r4.metrics.as_ref().unwrap().lock().op_status,
            Some(FuseReqStatus::Interrupted),
            "interrupt-tagged path is Interrupted"
        );
    }
}
