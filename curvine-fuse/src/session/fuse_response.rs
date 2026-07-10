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

use crate::fuse_metrics::{
    FuseMetrics, FuseReqCtx, FuseReqStatus, FuseRespMetrics, ENQUEUE_REASON_CHANNEL_CLOSED,
    NOTIFY_ENQUEUE_FAILED, REPLY_TYPE_NO_REPLY,
};
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

    /// The stashed FS-operation status, read back after the reply path has run
    /// (Phase 2a `operation_duration_us`). `None` when metrics are disabled (no
    /// slot) or when nothing finished the request yet. The send helpers stash
    /// `op_status` synchronously before enqueueing, so by the time `dispatch_meta`
    /// finishes its match this reflects the FS result, not the enqueue outcome.
    pub(crate) fn metrics_op_status(&self) -> Option<FuseReqStatus> {
        self.metrics.as_ref().and_then(|m| m.lock().op_status)
    }

    fn rep_log(&self, e: &FuseError) {
        if self.debug
            || !matches!(
                e.errno,
                libc::ENOENT | libc::ENODATA | libc::ENOSYS | libc::ENOTEMPTY
            )
        {
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
        // The two source tags are mutually exclusive — no current call site
        // passes both, and the classification below would silently drop the
        // interrupt signal if they were combined. Catch a future mis-wiring.
        debug_assert!(
            unsupported_reason.is_none() || !interrupted,
            "send_rep_tagged: unsupported_reason and interrupted are mutually exclusive"
        );
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

        // **Cancellation safety on bounded channels.** A bounded `send().await`
        // can suspend (channel full); if the holding task is cancelled mid-await,
        // the task — and the `ActiveGuard` moved into it — is dropped, decrementing
        // `active_requests` but emitting NO terminal metric. To avoid that
        // "silent finish", on bounded channels we first `reserve()` a permit
        // WITHOUT touching the slot. The only suspendable point is the reserve;
        // if cancelled there, the slot is still `finished=false` and the guard is
        // still in the slot, so the request is simply dropped (guard decremented
        // by its own Drop) with no half-finished state. Once the permit is in
        // hand, we commit the slot and `permit.send(...)` synchronously (no await,
        // cannot be cancelled). Unbounded `send` is already synchronous, so it
        // keeps the simple commit-then-send fast path.
        if self.sender.is_bounded() {
            let permit = match self.sender.reserve().await {
                Ok(p) => p,
                Err(e) => {
                    // Channel closed before we could reserve. The slot is still
                    // pending here, so commit the early-finish terminal now (guard
                    // is taken and dropped inside), then surface the real error.
                    self.finish_enqueue_failure(slot, status, errno, unsupported_reason);
                    return Err(e);
                }
            };
            // Permit acquired: from here on there is no await, so no cancellation
            // window. Commit the slot, then send synchronously.
            let task = match self.commit_reply_task(slot, data, status, errno, unsupported_reason) {
                Some(task) => task,
                None => return Ok(()), // double reply: warned/asserted in commit.
            };
            permit.send(task);
            return Ok(());
        }

        // Unbounded fast path: `AsyncSender::send` resolves synchronously on its
        // `Unbounded` branch (no `.await` suspension point before the value is
        // enqueued), so there is no cancellation window between commit and
        // enqueue. NOTE: this relies on that `Unbounded` behaviour — if
        // `AsyncSender::send` ever gains a pre-enqueue await for unbounded
        // channels, this path must move to the `reserve()`-style handling above.
        let task = match self.commit_reply_task(slot, data, status, errno, unsupported_reason) {
            Some(task) => task,
            None => return Ok(()),
        };
        let send_result = self.sender.send(task).await;
        if send_result.is_err() {
            self.record_enqueue_failure_metrics(slot, status, errno, unsupported_reason);
        }
        send_result
    }

    /// Commit the metrics slot for a replied request and build its task: stash
    /// status/errno, take the move-only `ActiveGuard` out of the slot exactly
    /// once, mark `finished`. Returns `None` on a double reply (already finished)
    /// — a logic bug surfaced via `debug_assert!`/`warn!`, never double-counting.
    /// All slot access is scoped before any `.await` by the caller.
    fn commit_reply_task(
        &self,
        slot: &Arc<Mutex<FuseRespMetrics>>,
        data: ResponseData,
        status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
    ) -> Option<FuseTask> {
        let (labels, active) = {
            let mut m = slot.lock();
            if m.finished {
                // Double reply on an already-finished context: a logic bug (e.g.
                // a `finish_early` followed by a `send_rep` on the same request).
                // Surfaced loudly in debug; in release we never double-count or
                // double-drop — we warn and no-op so a stray second reply cannot
                // corrupt the gauges.
                debug_assert!(
                    !m.finished,
                    "double reply on an already-finished FuseResponse (unique {})",
                    self.unique
                );
                warn!(
                    "double reply on an already-finished FuseResponse (unique {})",
                    self.unique
                );
                return None;
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
        };
        // reply_queue_depth guard: created here, at the enqueue boundary. This
        // function is reached only after the bounded `reserve()` succeeded (or on
        // the unbounded synchronous path), so a producer still parked in
        // `reserve().await` has NOT created a guard and is not counted as backlog.
        // The guard rides on the task and is dropped by the sender at dequeue (or
        // on task drop if the task is never received).
        let queue_guard = FuseMetrics::reply_queue_guard();
        Some(FuseTask::RequestReply {
            data,
            labels,
            active,
            status,
            errno,
            unsupported_reason,
            queue_guard,
        })
    }

    /// Enqueue-failure terminal for the path where the slot is **still pending**
    /// (bounded `reserve()` returned a closed-channel error). Commits the slot
    /// (taking and dropping the guard) and records the enqueue-failure metrics.
    /// The caller surfaces the original channel error.
    fn finish_enqueue_failure(
        &self,
        slot: &Arc<Mutex<FuseRespMetrics>>,
        status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
    ) {
        {
            let mut m = slot.lock();
            if m.finished {
                return;
            }
            m.op_status = Some(status);
            m.request_status = Some(FuseReqStatus::Error);
            m.errno = errno;
            m.unsupported_reason = unsupported_reason;
            m.finished = true;
            // No task carries the guard on this path; drop it explicitly.
            let _ = m.active.take();
        }
        self.record_enqueue_failure_metrics(slot, status, errno, unsupported_reason);
    }

    /// Record the metrics for a reply-enqueue failure (the request never reaches
    /// the sender). Shared by the unbounded post-send-failure path and the
    /// bounded reserve-failure path: enqueue error + duration{error} (NOT
    /// `requests_total` — excluded from QPS), plus the op-level terminal counter
    /// if the FS op itself failed (with the real FS errno/reason).
    fn record_enqueue_failure_metrics(
        &self,
        slot: &Arc<Mutex<FuseRespMetrics>>,
        status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
    ) {
        let labels = {
            let mut m = slot.lock();
            m.request_status = Some(FuseReqStatus::Error);
            m.labels
        };
        let metrics = FuseMetrics::get();
        metrics.record_reply_enqueue_error(labels.opcode, ENQUEUE_REASON_CHANNEL_CLOSED);
        metrics.record_request_duration(
            labels.opcode,
            labels.kind,
            FuseReqStatus::Error,
            labels.elapsed_us(),
        );
        // op_status side: if the FS op itself failed, record its terminal counter
        // (with the real FS errno/reason) — symmetric with the sender write-failure
        // path. Otherwise this records nothing. Without it, an op failure that
        // races a closed channel would vanish behind the channel error.
        metrics.record_op_terminal(
            labels.opcode,
            labels.kind,
            status,
            errno,
            unsupported_reason,
        );
    }

    /// Finish a no-reply request (`Forget` / `BatchForget`). Inspects the
    /// operation result (so a failing forget is not a phantom success), drops
    /// the active guard, and sends no task. Non-async: nothing reaches the
    /// sender.
    ///
    /// No-reply errors are always classified `Error` (no interrupt/unsupported
    /// tag): `Forget`/`BatchForget` are never interrupted, and no-reply
    /// `unsupported` (`trait_default`) is out of scope here. If a later phase
    /// needs it, add a source-tag parameter.
    fn finish_no_reply(&self, res: FuseResult<()>) {
        if let Some(slot) = &self.metrics {
            // Classified explicitly (not via the slot's `unsupported_reason`) so
            // the code matches the doc comment and does not depend on prior slot
            // state.
            let status = match &res {
                Ok(_) => FuseReqStatus::Success,
                Err(_) => FuseReqStatus::Error,
            };
            let labels = {
                let mut m = slot.lock();
                if m.finished {
                    return;
                }
                m.op_status = Some(status);
                m.request_status = Some(status);
                m.finished = true;
                // Drop the guard explicitly (no task carries it on the no-reply path).
                let _ = m.active.take();
                m.labels
            }; // lock dropped before recording metrics.

            // No-reply requests count toward QPS with reply_type=no_reply, and
            // toward the E2E duration, but emit NO response_* (no reply pipeline)
            // and NO errors_total. (`FuseError` does carry an errno, but this
            // phase deliberately does not attribute no-reply failures by errno —
            // forget failures are rare and the errno's diagnostic value is low;
            // decision 2 / R14.)
            let metrics = FuseMetrics::get();
            metrics.record_request_total(labels.opcode, labels.kind, REPLY_TYPE_NO_REPLY, status);
            metrics.record_request_duration(
                labels.opcode,
                labels.kind,
                status,
                labels.elapsed_us(),
            );
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
            {
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
            } // lock dropped before recording the metric.

            // A structural parse failure after the ctx existed: record the
            // decode error, but NOT `requests_total` (the request never
            // dispatched). Phase 1a-2 only ever passes reason="other" (the
            // `finish_early` call sites); finer reasons need decoder changes.
            FuseMetrics::get().record_parse_error(reason);
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
    /// `unknown_opcode`/`unimplemented_opcode`; `trait_default` reserved for a
    /// later phase) or `interrupted` (the SETLKW interrupt-notify path). Status
    /// is **never** inferred from errno; an untagged error is always `Error`.
    /// The sender reads the tag to emit `unsupported_total`/`interrupted_total`.
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
        // so the disabled path is byte-identical AND emits no notify metric.
        if self.metrics.is_some() {
            self.send_notify_metrics(code, data).await
        } else {
            self.sender.send(FuseTask::Reply(data)).await
        }
    }

    /// Metrics-enabled `send_notify`: enqueue a `NotifyReply` carrying a
    /// `reply_queue_depth` guard, using the same reserve-first discipline as the
    /// request reply path so a producer blocked on a full bounded channel is not
    /// counted as backlog. The guard is created only *after* the enqueue boundary
    /// is committed (bounded: permit acquired; unbounded: just before the
    /// synchronous send), and rides on the task to the sender's dequeue point.
    ///
    /// `enqueue_failed` is recorded at every point the notify fails to enter the
    /// channel — the bounded `reserve()` error path (NEW: previously only the
    /// `send()` error was counted) and the unbounded `send()` error path. A
    /// *cancelled* bounded `reserve().await` is not a failure: no guard was
    /// created and nothing is recorded.
    async fn send_notify_metrics(&self, code: FuseNotifyCode, data: ResponseData) -> IOResult<()> {
        let code_str = code.as_str();

        // Bounded: reserve first (no guard yet — a cancelled reserve leaves the
        // gauge untouched), then build the guard-carrying task and `permit.send`
        // synchronously (no await, no cancellation window).
        if self.sender.is_bounded() {
            let permit = match self.sender.reserve().await {
                Ok(p) => p,
                Err(e) => {
                    // Channel closed before we could reserve: the notify never
                    // reaches the sender. (A cancelled reserve unwinds here without
                    // returning Err, so it records nothing.)
                    FuseMetrics::get().record_notify_result(code_str, NOTIFY_ENQUEUE_FAILED);
                    return Err(e);
                }
            };
            permit.send(FuseTask::NotifyReply {
                data,
                code: code_str,
                queue_guard: FuseMetrics::reply_queue_guard(),
            });
            return Ok(());
        }

        // Unbounded: synchronous send, so there is no cancellation window between
        // building the guard task and enqueuing. A send error drops the task (and
        // its guard) and records enqueue_failed.
        let send_result = self
            .sender
            .send(FuseTask::NotifyReply {
                data,
                code: code_str,
                queue_guard: FuseMetrics::reply_queue_guard(),
            })
            .await;
        if send_result.is_err() {
            FuseMetrics::get().record_notify_result(code_str, NOTIFY_ENQUEUE_FAILED);
        }
        send_result
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
    use crate::fuse_metrics::{
        ActiveGuard, FuseMetrics, FuseReqKind, FuseReqLabels, DECODE_PHASE_PARSE,
        ENQUEUE_REASON_CHANNEL_CLOSED, NOTIFY_ENQUEUE_FAILED, REPLY_TYPE_NO_REPLY,
        REPLY_TYPE_REPLIED,
    };
    use orpc::common::{Gauge, Metrics as m};
    use orpc::sync::channel::{AsyncChannel, AsyncReceiver};

    // The finish paths (`finish_no_reply` / `finish_early` / enqueue-failure)
    // now read `FuseMetrics::get()`, which panics if the process-global registry
    // was never initialized. `ensure_init` is idempotent, so every test that
    // exercises a real finish path calls this first.
    fn init_metrics() {
        FuseMetrics::ensure_init().expect("init FuseMetrics for tests");
    }

    // Build a FuseResponse whose active guard is backed by `gauge`, so tests can
    // assert "guard dropped exactly once" as a concrete `gauge.get()` count.
    fn reply_with_gauge(unique: u64, gauge: &Gauge) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        reply_with_gauge_opcode(unique, gauge, "Lookup")
    }

    // Like `reply_with_gauge` but with a caller-chosen opcode label. Value-
    // assertion tests use a UNIQUE opcode each so their counter children never
    // collide with another (parallel) test's deltas on the shared registry.
    fn reply_with_gauge_opcode(
        unique: u64,
        gauge: &Gauge,
        opcode: &'static str,
    ) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        reply_with_gauge_opcode_kind(unique, gauge, opcode, FuseReqKind::Metadata)
    }

    // Like `reply_with_gauge_opcode` but also lets the test choose the kind, so
    // stream-path tests can assert `kind="stream"` labels.
    fn reply_with_gauge_opcode_kind(
        unique: u64,
        gauge: &Gauge,
        opcode: &'static str,
        kind: FuseReqKind,
    ) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        // Metrics-enabled reply path now resolves `reply_queue_guard()` via
        // `get()` (strict), so the singleton must be initialized for any enabled
        // fixture. Idempotent.
        init_metrics();
        let (tx, rx) = AsyncChannel::new(16).split();
        let labels = FuseReqLabels::new(opcode, kind, 64);
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
    // Release-only: a double reply trips `debug_assert!(!finished)` in debug
    // builds (see `double_reply_panics_in_debug`); the release behaviour is the
    // safe warn+no-op asserted here.
    #[tokio::test]
    #[cfg(not(debug_assertions))]
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

    // Debug counterpart: a double reply is a logic bug and must trip the
    // debug_assert. (Release turns this into a safe warn+no-op — see T13.)
    #[tokio::test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "double reply")]
    async fn double_reply_panics_in_debug() {
        let g = m::new_gauge("dbl_reply_dbg_active", "test").unwrap();
        let (reply, _rx) = reply_with_gauge(2, &g);
        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();
        // Second reply on the finished slot trips debug_assert!(!finished).
        let _ = reply.send_rep::<(), FuseError>(Ok(())).await;
    }

    // T6: Forget/BatchForget — finish_no_reply inspects the result, drops the
    // guard, and enqueues NO task. Run for both Ok and Err.
    #[tokio::test]
    async fn t6_no_reply_finishes_without_task_for_ok_and_err() {
        init_metrics();
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
        init_metrics();
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
    // slot, and notifications also fall back to Reply AND emit no notify metric
    // (R8d: the disabled production path records nothing).
    #[tokio::test]
    async fn t11_disabled_uses_legacy_reply() {
        init_metrics();
        let code = FuseNotifyCode::FUSE_NOTIFY_INVAL_INODE.as_str();
        let notify_before = FuseMetrics::get()
            .notify_total
            .with_label_values(&[code, "success"])
            .get();

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

        // The disabled notify went out as a legacy Reply, so notify_total is
        // untouched (and the sender's Reply arm never records notify metrics).
        assert_eq!(
            FuseMetrics::get()
                .notify_total
                .with_label_values(&[code, "success"])
                .get(),
            notify_before,
            "disabled notify must not increment notify_total"
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
        init_metrics();
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

    // The process-global registry accumulates across tests, so value assertions
    // read a child's counter/histogram before and after and check the delta.
    fn requests_total(opcode: &str, kind: &str, reply_type: &str, status: &str) -> i64 {
        FuseMetrics::get()
            .requests_total
            .with_label_values(&[opcode, kind, reply_type, status])
            .get()
    }
    fn request_duration_count(opcode: &str, kind: &str, status: &str) -> u64 {
        FuseMetrics::get()
            .request_duration_us
            .with_label_values(&[opcode, kind, status])
            .get_sample_count()
    }
    fn errors_total(opcode: &str, kind: &str, errno: &str) -> i64 {
        FuseMetrics::get()
            .errors_total
            .with_label_values(&[opcode, kind, errno])
            .get()
    }
    fn unsupported_total(opcode: &str, reason: &str) -> i64 {
        FuseMetrics::get()
            .unsupported_total
            .with_label_values(&[opcode, reason])
            .get()
    }
    fn interrupted_total(opcode: &str) -> i64 {
        FuseMetrics::get()
            .interrupted_total
            .with_label_values(&[opcode])
            .get()
    }

    // B2 / test 4: enqueue failure records `reply_enqueue_errors_total` +
    // `request_duration_us{status=error}` exactly once, and does NOT count
    // toward `requests_total` (QPS) or `errors_total` (no OS errno).
    #[tokio::test]
    async fn enqueue_failure_emits_enqueue_error_and_duration_not_requests_total() {
        init_metrics();
        const OP: &str = "EnqFailTest";
        let metrics = FuseMetrics::get();
        let dur_before = request_duration_count(OP, "metadata", "error");

        let g = m::new_gauge("enq_emit_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(20, &g, OP);
        drop(rx); // close channel so enqueue fails
        assert!(reply.send_rep::<(), FuseError>(Ok(())).await.is_err());

        assert_eq!(
            metrics
                .reply_enqueue_errors_total
                .with_label_values(&[OP, ENQUEUE_REASON_CHANNEL_CLOSED])
                .get(),
            1,
            "one reply_enqueue_errors_total channel_closed"
        );
        assert_eq!(
            request_duration_count(OP, "metadata", "error"),
            dur_before + 1,
            "request_duration_us error observed once"
        );
        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_REPLIED, "error"),
            0,
            "enqueue failure must NOT count toward requests_total"
        );
        assert_eq!(g.get(), 0, "guard dropped once with the consumed task");
    }

    // P1#1: enqueue failure layered on a FAILED op must still record the
    // op-level terminal counter with the real FS errno — the channel error must
    // not swallow the operation failure (symmetric with the sender write-failure
    // path's op/request status split).
    #[tokio::test]
    async fn enqueue_failure_on_failed_op_still_records_errors_total() {
        init_metrics();
        const OP: &str = "EnqFailOpErr";
        let before = errors_total(OP, "metadata", "EIO");

        let g = m::new_gauge("enq_op_err_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(30, &g, OP);
        drop(rx); // close channel so enqueue fails
        let err: FuseResult<()> = Err(FuseError::new(libc::EIO, "backend".into()));
        assert!(reply.send_rep(err).await.is_err());

        assert_eq!(
            errors_total(OP, "metadata", "EIO"),
            before + 1,
            "failed op + enqueue failure still records errors_total with FS errno"
        );
        // enqueue error recorded too; QPS still excluded.
        assert_eq!(
            FuseMetrics::get()
                .reply_enqueue_errors_total
                .with_label_values(&[OP, ENQUEUE_REASON_CHANNEL_CLOSED])
                .get(),
            1
        );
        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_REPLIED, "error"),
            0
        );
        assert_eq!(g.get(), 0);
    }

    // P1#1: enqueue failure layered on a tagged-unsupported op still records
    // unsupported_total{reason}.
    #[tokio::test]
    async fn enqueue_failure_on_unsupported_op_still_records_unsupported_total() {
        init_metrics();
        const OP: &str = "EnqFailUnsup";
        let before = unsupported_total(OP, "unimplemented_opcode");

        let g = m::new_gauge("enq_unsup_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(31, &g, OP);
        drop(rx);
        let err: FuseResult<()> = Err(FuseError::new(libc::ENOSYS, "unimpl".into()));
        assert!(reply
            .send_rep_tagged(err, Some("unimplemented_opcode"), false)
            .await
            .is_err());

        assert_eq!(
            unsupported_total(OP, "unimplemented_opcode"),
            before + 1,
            "unsupported op + enqueue failure still records unsupported_total"
        );
        assert_eq!(g.get(), 0);
    }

    // P1#1: enqueue failure layered on an interrupted op still records
    // interrupted_total.
    #[tokio::test]
    async fn enqueue_failure_on_interrupted_op_still_records_interrupted_total() {
        init_metrics();
        const OP: &str = "EnqFailIntr";
        let before = interrupted_total(OP);

        let g = m::new_gauge("enq_intr_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(32, &g, OP);
        drop(rx);
        let err: FuseResult<()> = Err(FuseError::new(libc::EINTR, "setlkw".into()));
        assert!(reply.send_rep_tagged(err, None, true).await.is_err());

        assert_eq!(
            interrupted_total(OP),
            before + 1,
            "interrupted op + enqueue failure still records interrupted_total"
        );
        assert_eq!(g.get(), 0);
    }

    // P1#2 (round-2): a stream worker (FuseReader::read_future /
    // FuseWriter::writer_future) holds the `FuseResponse` and replies via
    // `send_data`/`send_rep` from *inside* the task. If the reply channel is
    // closed by then (sender shutdown), the worker's `send_*().await` returns Err
    // and the worker exits via `?` — but the finish must still happen: the active
    // guard must drop (no leak) and the enqueue error + duration{error} recorded.
    // This exercises the worker-internal finish path without a real kernel fd.
    #[tokio::test]
    async fn stream_worker_send_data_enqueue_failure_finishes_without_leak() {
        init_metrics();
        const OP: &str = "StreamWorkerRead";
        let g = m::new_gauge("stream_worker_read_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(40, &g, OP);
        assert_eq!(g.get(), 1, "guard live while the worker holds the reply");
        drop(rx); // sender gone: the worker's reply enqueue will fail.

        // The reader worker replies with data; enqueue fails on the closed channel.
        let data: FuseResult<Vec<DataSlice>> = Ok(vec![]);
        assert!(reply.send_data(data).await.is_err());

        assert_eq!(
            g.get(),
            0,
            "active guard dropped on worker enqueue failure (no leak)"
        );
        assert_eq!(
            FuseMetrics::get()
                .reply_enqueue_errors_total
                .with_label_values(&[OP, ENQUEUE_REASON_CHANNEL_CLOSED])
                .get(),
            1,
            "worker enqueue failure records reply_enqueue_errors_total"
        );
        // The fixture's labels carry kind=metadata; the worker enqueue-failure
        // finish records request_duration_us{error} for whatever kind the ctx
        // holds. (The reader/writer kind is exercised end-to-end, not here.)
        assert!(
            request_duration_count(OP, "metadata", "error") >= 1,
            "worker enqueue failure records request_duration_us error"
        );
        // The op itself succeeded (data was Ok), so no op-level errors_total.
        assert_eq!(errors_total(OP, "metadata", "OTHER"), 0);
    }

    // P1#2 companion: the writer worker's `send_rep` path on a closed channel.
    #[tokio::test]
    async fn stream_worker_send_rep_enqueue_failure_finishes_without_leak() {
        init_metrics();
        const OP: &str = "StreamWorkerWrite";
        let g = m::new_gauge("stream_worker_write_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(41, &g, OP);
        drop(rx);

        assert!(reply.send_rep::<(), FuseError>(Ok(())).await.is_err());

        assert_eq!(g.get(), 0, "active guard dropped on writer enqueue failure");
        assert_eq!(
            FuseMetrics::get()
                .reply_enqueue_errors_total
                .with_label_values(&[OP, ENQUEUE_REASON_CHANNEL_CLOSED])
                .get(),
            1
        );
    }

    // P2#2 (round-3): the worker enqueue-failure finish records under the real
    // stream kind label — verifies `request_duration_us{kind="stream",error}`.
    #[tokio::test]
    async fn stream_worker_enqueue_failure_records_stream_kind_duration() {
        init_metrics();
        const OP: &str = "StreamWorkerKind";
        let before = request_duration_count(OP, "stream", "error");

        let g = m::new_gauge("stream_worker_kind_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode_kind(42, &g, OP, FuseReqKind::Stream);
        drop(rx);
        let data: FuseResult<Vec<DataSlice>> = Ok(vec![]);
        assert!(reply.send_data(data).await.is_err());

        assert_eq!(g.get(), 0, "stream worker guard dropped, no leak");
        assert_eq!(
            request_duration_count(OP, "stream", "error"),
            before + 1,
            "stream-kind worker enqueue failure records request_duration_us kind=stream error"
        );
    }

    // Build a reply backed by an explicitly-bounded channel of the given
    // capacity, so bounded-path tests don't depend on the default helper's
    // internal `AsyncChannel::new(16)`.
    fn bounded_reply_with_capacity(
        unique: u64,
        gauge: &Gauge,
        opcode: &'static str,
        cap: usize,
    ) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        init_metrics();
        let (tx, rx) = AsyncChannel::new(cap).split();
        debug_assert!(tx.is_bounded(), "cap>0 must yield a bounded channel");
        let labels = FuseReqLabels::new(opcode, FuseReqKind::Metadata, 64);
        let ctx = FuseReqCtx {
            labels,
            active: Some(ActiveGuard::new(gauge.clone())),
        };
        (FuseResponse::new_reply(unique, tx, false, Some(ctx)), rx)
    }

    // Build a bounded size-1 reply slot whose only buffer position is already
    // filled, so the next `send` must suspend on `reserve().await`.
    fn full_bounded_reply(
        unique: u64,
        gauge: &Gauge,
        opcode: &'static str,
    ) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        init_metrics();
        let (tx, rx) = AsyncChannel::new(1).split();
        // Fill the single slot so a subsequent reserve()/send() blocks.
        tx.try_reserve()
            .unwrap()
            .expect("one permit available")
            .send(FuseTask::Reply(ResponseData::create(unique, 0, vec![])));
        let labels = FuseReqLabels::new(opcode, FuseReqKind::Metadata, 64);
        let ctx = FuseReqCtx {
            labels,
            active: Some(ActiveGuard::new(gauge.clone())),
        };
        (FuseResponse::new_reply(unique, tx, false, Some(ctx)), rx)
    }

    // P1 (round-3): on a bounded channel, if the task is cancelled while the
    // reply is suspended on `reserve().await` (channel full), the request must
    // NOT enter a "silent finished" state — the slot stays `finished=false` and
    // the guard is released by passive Drop, so a retry/cleanup is still possible
    // and no half-finished state corrupts the gauges.
    #[tokio::test]
    async fn bounded_reserve_cancellation_leaves_slot_unfinished() {
        init_metrics();
        const OP: &str = "BoundedCancel";
        let g = m::new_gauge("bounded_cancel_active", "test").unwrap();
        let (reply, _rx) = full_bounded_reply(99, &g, OP);
        assert_eq!(g.get(), 1, "guard live before the reply");

        // The slot we will inspect after cancellation.
        let slot = reply.metrics.as_ref().unwrap().clone();

        // Spawn the reply; it suspends on reserve() because the channel is full.
        let handle = tokio::spawn(async move {
            let _ = reply.send_rep::<(), FuseError>(Ok(())).await;
        });
        // Give it a moment to reach the suspended reserve().
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Cancel the suspended task: its future (and the `FuseResponse`) drops.
        handle.abort();
        let _ = handle.await;

        // The critical invariant: NO silent finish. The slot was never committed,
        // so the guard is still IN the slot (not moved onto a task, not dropped),
        // and a real terminal path (retry / teardown cleanup) can still run.
        {
            let m = slot.lock();
            assert!(
                !m.finished,
                "cancellation during reserve() must NOT mark the slot finished"
            );
            assert!(
                m.active.is_some(),
                "guard stays in the unfinished slot, available for a real terminal path"
            );
        }
        assert_eq!(g.get(), 1, "guard still held by the unfinished slot");

        // Dropping the last slot reference releases the guard by Drop — so even
        // the abandoned request does not leak `active_requests`.
        drop(slot);
        assert_eq!(
            g.get(),
            0,
            "guard released once the slot is finally dropped"
        );
    }

    // P1 (round-3): bounded channel, reserve succeeds (slot free) -> the reply
    // finishes normally (RequestReply enqueued, slot finished, guard rides task).
    #[tokio::test]
    async fn bounded_reserve_success_finishes_normally() {
        init_metrics();
        const OP: &str = "BoundedOk";
        let g = m::new_gauge("bounded_ok_active", "test").unwrap();
        // Explicitly bounded with a free slot, so reserve() succeeds immediately.
        let (reply, mut rx) = bounded_reply_with_capacity(100, &g, OP, 4);

        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();

        let task = rx.try_recv().unwrap().expect("a task was enqueued");
        assert!(matches!(task, FuseTask::RequestReply { .. }));
        {
            let slot = reply.metrics.as_ref().unwrap().lock();
            assert!(slot.finished, "reserve-success path finishes the slot");
        }
        assert_eq!(g.get(), 1, "guard rides on the task");
        drop(task);
        assert_eq!(g.get(), 0, "guard dropped once at task drop");
    }

    // B3 / test 6: no-reply forget emits requests_total{reply_type=no_reply} +
    // duration for both Ok and Err, and never errors_total.
    #[tokio::test]
    async fn no_reply_emits_requests_total_no_reply_for_ok_and_err() {
        init_metrics();
        const OP: &str = "NoReplyTest";
        let err_errors_before = errors_total(OP, "metadata", "OTHER");

        let g_ok = m::new_gauge("nr_emit_ok", "test").unwrap();
        let (reply_ok, _rx_ok) = reply_with_gauge_opcode(21, &g_ok, OP);
        reply_ok.send_none(Ok(())).unwrap();

        let g_err = m::new_gauge("nr_emit_err", "test").unwrap();
        let (reply_err, _rx_err) = reply_with_gauge_opcode(22, &g_err, OP);
        reply_err.send_none(Err(FuseError::from("boom"))).unwrap();

        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_NO_REPLY, "success"),
            1,
            "Ok forget increments requests_total no_reply success"
        );
        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_NO_REPLY, "error"),
            1,
            "Err forget increments requests_total no_reply error"
        );
        assert_eq!(
            errors_total(OP, "metadata", "OTHER"),
            err_errors_before,
            "no-reply error must NOT emit errors_total"
        );
    }

    // B4 / test 8: parse-after-ctx early finish emits decode_errors_total
    // {phase=parse,reason=other} once and NO requests_total.
    #[tokio::test]
    async fn finish_early_emits_decode_error_not_requests_total() {
        init_metrics();
        const OP: &str = "FinishEarlyTest";
        let metrics = FuseMetrics::get();
        // `decode_errors_total` is opcode-free (phase,reason), so other parallel
        // tests could also bump {parse,other}; assert a delta, not an absolute.
        let decode_before = metrics
            .decode_errors_total
            .with_label_values(&[DECODE_PHASE_PARSE, "other"])
            .get();

        let g = m::new_gauge("fe_emit_active", "test").unwrap();
        let (reply, _rx) = reply_with_gauge_opcode(23, &g, OP);
        reply.finish_early(libc::EINVAL, "other");

        assert!(
            metrics
                .decode_errors_total
                .with_label_values(&[DECODE_PHASE_PARSE, "other"])
                .get()
                > decode_before,
            "decode_errors_total parse other incremented at least once"
        );
        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_REPLIED, "error"),
            0,
            "parse-after-ctx must NOT emit requests_total"
        );
        assert_eq!(g.get(), 0, "guard dropped on early finish");
    }

    // --- Phase 2a: reply_queue_depth task-embedded guard ---
    //
    // The production `reply_queue_guard()` increments the *process-global*
    // `reply_queue_depth` gauge, which every parallel test in this binary shares.
    // So these tests assert the **structural** invariant that is deterministic
    // under parallelism — "is the guard present on the right task variant?" — and
    // leave the numeric inc/dec balance to `ActiveGuard`'s own isolated-gauge unit
    // test (`active_guard_inc_dec_balances` in fuse_metrics). The guard's effect
    // on the gauge is the same `ActiveGuard` machinery either way.

    // B1: `metrics_op_status()` reads back the stashed op_status that the
    // `operation_duration_us` timer uses after the dispatch_meta match. It is the
    // FS-operation result (here: an untagged error → Error), NOT the enqueue
    // outcome, and is None when there is no metrics slot (disabled) or before any
    // finish.
    #[tokio::test]
    async fn metrics_op_status_reads_stashed_fs_result() {
        init_metrics();
        let g = m::new_gauge("op_status_active", "test").unwrap();

        // Before any finish: nothing stashed yet.
        let (reply, mut rx) = reply_with_gauge_opcode(60, &g, "OpStatusOp");
        assert_eq!(
            reply.metrics_op_status(),
            None,
            "no op_status before the reply path runs"
        );

        // A failed FS op stashes Error (read by operation_duration as status=error).
        let err: FuseResult<()> = Err(FuseError::new(libc::EIO, "backend".into()));
        reply.send_rep(err).await.unwrap();
        let _ = rx.try_recv();
        assert_eq!(
            reply.metrics_op_status(),
            Some(FuseReqStatus::Error),
            "metrics_op_status reflects the FS op result after the reply path"
        );

        // Disabled reply has no slot, so the accessor is None.
        let (disabled, _rx2) = disabled_reply(61);
        assert_eq!(disabled.metrics_op_status(), None, "disabled has no slot");
    }

    // B2 test 3(a): a metrics-enabled reply produces a RequestReply that CARRIES a
    // queue guard (so the sender's `mark_dequeued` has something to drop at the
    // dequeue point).
    #[tokio::test]
    async fn request_reply_carries_queue_guard() {
        init_metrics();
        let g = m::new_gauge("rq_req_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge_opcode(50, &g, "RqReqOp");
        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();

        let task = rx.try_recv().unwrap().expect("a task");
        match task {
            FuseTask::RequestReply { queue_guard, .. } => {
                assert!(
                    queue_guard.is_some(),
                    "metrics-enabled RequestReply carries a reply_queue_depth guard"
                );
            }
            other => panic!("expected RequestReply, got {}", as_variant(&other)),
        }
    }

    // B2 test 3(d): the disabled legacy `Reply` carries no queue guard at all —
    // the variant has no guard field, so disabled mode cannot touch
    // reply_queue_depth. (Structural, not a gauge read.)
    #[tokio::test]
    async fn disabled_reply_carries_no_queue_guard() {
        init_metrics();
        let (reply, mut rx) = disabled_reply(52);
        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();
        assert!(
            matches!(rx.try_recv().unwrap().unwrap(), FuseTask::Reply(_)),
            "disabled path produces the legacy Reply (no queue guard field)"
        );
    }

    // B2 test (notify): a metrics-enabled notify produces a NotifyReply carrying a
    // queue guard, same discipline as the request path.
    #[tokio::test]
    async fn notify_reply_carries_queue_guard() {
        init_metrics();
        let g = m::new_gauge("rq_notify_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge_opcode(53, &g, "RqNotifyOp");
        reply
            .send_notify(FuseNotifyCode::FUSE_NOTIFY_INVAL_INODE, vec![])
            .await
            .unwrap();

        let task = rx.try_recv().unwrap().expect("a notify task");
        match task {
            FuseTask::NotifyReply { queue_guard, .. } => {
                assert!(queue_guard.is_some(), "NotifyReply carries a queue guard");
            }
            other => panic!("expected NotifyReply, got {}", as_variant(&other)),
        }
    }

    // B2 test 18(a): on a bounded channel, a `reserve()` that fails with a closed
    // channel records `notify_total{status=enqueue_failed}` — the NEW reserve-path
    // failure point (previously only the send() error was counted). `notify_total`
    // is keyed by a code unique to this test, so the delta is parallel-safe.
    #[tokio::test]
    async fn notify_bounded_reserve_closed_records_enqueue_failed() {
        init_metrics();
        // FUSE_NOTIFY_INVAL_ENTRY is the code this test owns for its delta; no
        // other test enqueue_failed's this code.
        let code = FuseNotifyCode::FUSE_NOTIFY_INVAL_ENTRY.as_str();
        let before = FuseMetrics::get()
            .notify_total
            .with_label_values(&[code, NOTIFY_ENQUEUE_FAILED])
            .get();

        // Bounded channel, then close it by dropping the receiver so reserve()
        // returns a closed-channel error.
        let g = m::new_gauge("rq_notify_closed_active", "test").unwrap();
        let (tx, rx) = AsyncChannel::new(1).split();
        debug_assert!(tx.is_bounded());
        let labels = FuseReqLabels::new("RqNotifyClosed", FuseReqKind::Metadata, 64);
        let ctx = FuseReqCtx {
            labels,
            active: Some(ActiveGuard::new(g.clone())),
        };
        let reply = FuseResponse::new_reply(54, tx, false, Some(ctx));
        drop(rx);

        assert!(reply
            .send_notify(FuseNotifyCode::FUSE_NOTIFY_INVAL_ENTRY, vec![])
            .await
            .is_err());

        assert_eq!(
            FuseMetrics::get()
                .notify_total
                .with_label_values(&[code, NOTIFY_ENQUEUE_FAILED])
                .get(),
            before + 1,
            "bounded reserve-closed records notify enqueue_failed"
        );
    }

    // Small helper so panic messages name the unexpected variant without deriving
    // Debug on FuseTask (which carries non-Debug fields).
    fn as_variant(task: &FuseTask) -> &'static str {
        match task {
            FuseTask::RequestReply { .. } => "RequestReply",
            FuseTask::NotifyReply { .. } => "NotifyReply",
            FuseTask::Reply(_) => "Reply",
        }
    }

    // --- Phase 2a: reply_queue_depth REAL queue lifecycle (review P1#4) ---
    //
    // These drive an actual channel with locally-gauged guards (NOT the global
    // `reply_queue_depth`, which parallel tests share and would make absolute/delta
    // asserts flaky). The queue guard is just an `ActiveGuard`; its real-queue
    // behaviour is fully determined by (a) where it is created (the enqueue
    // boundary) and (b) where it is dropped (sender dequeue, or task drop). We
    // build the `RequestReply` task with both an `active` guard and a `queue`
    // guard on distinct local gauges so the two scopes can be asserted apart.

    // Build a RequestReply task whose active/queue guards are backed by the two
    // given gauges, so a test can watch each scope independently.
    fn request_reply_task(unique: u64, active_g: &Gauge, queue_g: &Gauge) -> FuseTask {
        let labels = FuseReqLabels::new("QDepthOp", FuseReqKind::Metadata, 64);
        FuseTask::RequestReply {
            data: ResponseData::create(unique, 0, vec![]),
            labels,
            active: ActiveGuard::new(active_g.clone()),
            status: FuseReqStatus::Success,
            errno: 0,
            unsupported_reason: None,
            queue_guard: Some(ActiveGuard::new(queue_g.clone())),
        }
    }

    // P1#4 (a): recv dequeues -> the queue guard drops at the dequeue point, so
    // queue depth returns to baseline BEFORE any splice; the active guard is a
    // SEPARATE scope and is still held (active stays 1 while queue is back to 0).
    #[tokio::test]
    async fn reply_queue_depth_drops_at_dequeue_active_still_held() {
        let active_g = m::new_gauge("qd_dequeue_active", "test").unwrap();
        let queue_g = m::new_gauge("qd_dequeue_queue", "test").unwrap();
        let (tx, mut rx) = AsyncChannel::<FuseTask>::new(16).split();

        tx.send(request_reply_task(1, &active_g, &queue_g))
            .await
            .unwrap();
        // Enqueued and not yet received: both scopes are live.
        assert_eq!(queue_g.get(), 1, "queue depth +1 while task is in channel");
        assert_eq!(active_g.get(), 1, "active +1 from ctx creation");

        // Sender dequeues: take the task, drop ONLY the queue guard (== sender's
        // `mark_dequeued`). The active guard rides on the task until sender finish.
        let task = rx.try_recv().unwrap().expect("task");
        match task {
            FuseTask::RequestReply {
                queue_guard,
                active,
                ..
            } => {
                drop(queue_guard); // mark_dequeued
                assert_eq!(queue_g.get(), 0, "queue depth back to 0 at dequeue");
                assert_eq!(
                    active_g.get(),
                    1,
                    "active still held after dequeue (separate scope, splice not done)"
                );
                drop(active); // sender finish
                assert_eq!(active_g.get(), 0, "active released at sender finish");
            }
            other => panic!("expected RequestReply, got {}", as_variant(&other)),
        }
    }

    // P1#4 (b): a task enqueued but NEVER received (sender/channel dropped) still
    // balances — the queue guard rides the task and drops with it, no leak.
    #[tokio::test]
    async fn reply_queue_depth_unreceived_task_drop_balances() {
        let active_g = m::new_gauge("qd_unrecv_active", "test").unwrap();
        let queue_g = m::new_gauge("qd_unrecv_queue", "test").unwrap();
        let (tx, rx) = AsyncChannel::<FuseTask>::new(16).split();

        tx.send(request_reply_task(2, &active_g, &queue_g))
            .await
            .unwrap();
        assert_eq!(queue_g.get(), 1, "enqueued: +1");

        // Drop both ends without recv: the queued task (and both its guards) is
        // dropped with the channel.
        drop(rx);
        drop(tx);
        assert_eq!(
            queue_g.get(),
            0,
            "un-received task drop balances queue depth"
        );
        assert_eq!(active_g.get(), 0, "and the active guard too");
    }

    // P1#4 (c): bounded-full reserve-first does NOT inflate queue depth. The real
    // `commit_reply_task` creates the queue guard only AFTER a permit is acquired
    // (see `finish_request`), so a producer parked in `reserve().await` on a full
    // channel holds no guard.
    //
    // SCOPE (review R2 P2#6): this is a PROTOCOL-LEVEL STAND-IN, not a real
    // blocked-producer test. It asserts the reserve-first invariant directly
    // against a full bounded channel (`try_reserve()` yields no permit -> no guard
    // built -> local gauge stays 0); it does NOT drive `FuseResponse::finish_request`
    // through a pending `reserve().await` and observe the guard's absence while the
    // send future is parked. The cancellation half of that production path —
    // `reserve()` cancelled mid-await leaves the slot unfinished — IS covered by
    // `bounded_reserve_cancellation_leaves_slot_unfinished`. Driving a genuinely
    // parked producer + observing queue depth would need the shared global gauge
    // (parallel-unsafe) or a custom waker harness; deferred as not worth the flake.
    #[tokio::test]
    async fn reply_queue_depth_bounded_full_does_not_inflate() {
        let queue_g = m::new_gauge("qd_bounded_full_queue", "test").unwrap();
        let (tx, _rx) = AsyncChannel::<FuseTask>::new(1).split();
        debug_assert!(tx.is_bounded());

        // Fill the single slot.
        let permit = tx.try_reserve().unwrap().expect("one permit");
        permit.send(FuseTask::Reply(ResponseData::create(0, 0, vec![])));

        // Channel now full: a producer would block in `reserve().await`. Reserve-
        // first means the queue guard is created only after a permit is in hand,
        // so right now NO guard exists and queue depth is untouched.
        assert!(
            tx.try_reserve().unwrap().is_none(),
            "full bounded channel yields no permit"
        );
        assert_eq!(
            queue_g.get(),
            0,
            "no permit -> no queue guard built -> depth not inflated"
        );
    }
}
