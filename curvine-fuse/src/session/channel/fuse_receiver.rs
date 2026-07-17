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

use crate::fs::operator::FuseOperator;
use crate::fs::FileSystem;
use crate::fuse_error::splice_errno_label;
use crate::fuse_metrics::{
    dispatch_io_type, lifecycle_io_type, mono_now, ActiveGuard, FuseMetrics, FuseReqCtx,
    FuseReqKind, FuseReqLabels, FuseReqStatus, RECEIVE_ACTION_CONTINUE, RECEIVE_ACTION_EXIT,
};
use crate::raw::fuse_abi::fuse_out_header;
use crate::session::{FuseOpCode, FuseRequest, FuseResponse, FuseTask};
use crate::{err_fuse, FuseResult, FUSE_IN_HEADER_LEN};
use bytes::BytesMut;
use libc::{EAGAIN, EINTR, ENODEV, ENOENT};
use log::{debug, error, info, warn};
use orpc::io::IOResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::AsyncSender;
use orpc::sync::FastDashMap;
use orpc::sys::pipe::{AsyncFd, Pipe2, PipeFd};
use orpc::{err_box, sys, try_option_ref};
use std::sync::Arc;
use tokio::sync::{watch, Notify};

/// FuseReceiver provides the following functionality:
/// 1. Receive data from fuse fd using splice
/// 2. For metadata requests (mkdir, ls), spawn a task to execute
/// 3. For file read/write requests, send task to queue
pub struct FuseReceiver<T> {
    kernel_fd: Arc<AsyncFd>,
    fs: Arc<T>,
    rt: Arc<Runtime>,
    sender: AsyncSender<FuseTask>,
    pipe2: Option<Pipe2>,
    buf: BytesMut,
    fuse_len: usize,
    debug: bool,
    audit_logging_enabled: bool,
    metrics_enabled: bool,
    pending_requests: Arc<FastDashMap<u64, Arc<Notify>>>,
}

impl<T: FileSystem> FuseReceiver<T> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        fs: Arc<T>,
        rt: Arc<Runtime>,
        kernel_fd: Arc<AsyncFd>,
        sender: AsyncSender<FuseTask>,
        buf_size: usize,
        debug: bool,
        audit_logging_enabled: bool,
        metrics_enabled: bool,
        pending_requests: Arc<FastDashMap<u64, Arc<Notify>>>,
        enable_splice: bool,
    ) -> IOResult<Self> {
        let pipe2 = if enable_splice {
            Some(Pipe2::new(PipeFd::new(buf_size, false, false)?)?)
        } else {
            None
        };
        let buf = BytesMut::zeroed(buf_size);

        let client = Self {
            kernel_fd,
            fs,
            rt,
            sender,
            pipe2,
            buf,
            fuse_len: buf_size,
            debug,
            audit_logging_enabled,
            metrics_enabled,
            pending_requests,
        };

        Ok(client)
    }

    // Read a data from fuse.
    pub async fn receive(&mut self) -> IOResult<BytesMut> {
        if self.pipe2.is_some() {
            self.splice().await
        } else {
            self.read().await
        }
    }

    // Use libc::read to read data directly into the buffer (no splice).
    pub async fn read(&mut self) -> IOResult<BytesMut> {
        self.buf.reserve(self.fuse_len);
        unsafe {
            self.buf.set_len(self.fuse_len);
        }

        let len = self
            .kernel_fd
            .async_read(|fd| sys::read(fd.fd(), &mut self.buf))
            .await?;
        let len = len as usize;
        if len < FUSE_IN_HEADER_LEN {
            return err_box!("short read on fuse device");
        }

        Ok(self.buf.split_to(len))
    }

    pub async fn splice(&mut self) -> IOResult<BytesMut> {
        let pipe2 = try_option_ref!(self.pipe2);

        let write_len = pipe2.write_io(&self.kernel_fd, None, self.fuse_len).await?;

        self.buf.reserve(write_len);
        unsafe {
            self.buf.set_len(write_len);
        }

        let read_len = pipe2.read_buf(&mut self.buf[..write_len]).await?;
        if write_len != read_len {
            return err_box!(
                "splice read and write lengths are inconsistent, write len {}, read len {}",
                write_len,
                read_len
            );
        }
        if read_len < FUSE_IN_HEADER_LEN {
            return err_box!("short read on fuse device");
        };

        Ok(self.buf.split_to(read_len))
    }

    /// Build a reply handle for `unique`. When `labels` is `Some`, a metrics
    /// context is created (incrementing the `active_requests` gauge via the
    /// `ActiveGuard`) so the reply finishes in the sender; when `None`, the
    /// legacy disabled path — `FuseMetrics::get()` is never touched, so a
    /// disabled or uninitialized-metrics process cannot panic here.
    pub(crate) fn new_reply(&self, unique: u64, labels: Option<FuseReqLabels>) -> FuseResponse {
        let ctx = labels.map(|labels| {
            let gauge = FuseMetrics::get()
                .active_requests
                .with_label_values(&[labels.kind.as_str()]);
            FuseReqCtx {
                labels,
                active: Some(ActiveGuard::new(gauge)),
            }
        });
        FuseResponse::new_reply(unique, self.sender.clone(), self.debug, ctx)
    }

    /// Derive the copyable metrics labels for a decoded request.
    fn req_labels(req: &FuseRequest) -> FuseReqLabels {
        let kind = if req.is_stream() {
            FuseReqKind::Stream
        } else {
            FuseReqKind::Metadata
        };
        let request_bytes = req.get_header().map(|h| h.len).unwrap_or(0);
        FuseReqLabels::new(req.opcode().as_str(), kind, request_bytes)
    }

    /// The kill switch (`metrics_enabled`) gate: `Some(labels)` enables the
    /// metrics path for this request, `None` selects the legacy zero-cost path.
    /// When disabled, no `FuseReqLabels`/ctx/gauge is constructed at all.
    fn maybe_req_labels(&self, req: &FuseRequest) -> Option<FuseReqLabels> {
        if self.metrics_enabled {
            Some(Self::req_labels(req))
        } else {
            None
        }
    }

    fn audit(&self, req: &FuseRequest) {
        if !self.audit_logging_enabled {
            return;
        }
        let ino = req.get_header().map(|h| h.nodeid).unwrap_or(0);
        info!(
            target: "audit",
            "unique={} ino={} opcode={:?}",
            req.unique(),
            ino,
            req.opcode(),
        );
    }

    pub async fn send_stream(&self, req: FuseRequest) -> FuseResult<()> {
        // Create the metrics context *before* parsing (ctx-before-parse), so a
        // structural parse failure after this point is a real finish-state-machine
        // event, consistent with the metadata path. `None` when metrics disabled.
        let labels = self.maybe_req_labels(&req);
        let rep = self.new_reply(req.unique(), labels);
        // All stream-IO attribution + dispatch logic lives in `send_stream_dispatch`,
        // an associated fn that takes the already-built reply and needs only `&self.fs`
        // — NOT `kernel_fd`/`pipe2`/the runtime. This keeps the metrics logic
        // unit-testable without constructing a real `FuseReceiver` (no fd, no reactor,
        // no `Pipe2`); see the tests module.
        Self::send_stream_dispatch(&self.fs, req, rep).await
    }

    /// The stream dispatch + Phase 2b IO attribution core, factored out of
    /// `send_stream` so it can be driven in tests against a hand-built
    /// `FuseResponse` without a real `FuseReceiver`/`kernel_fd`/`Pipe2`/reactor.
    /// `rep` is the reply handle already built by the caller (`new_reply`).
    ///
    /// The metrics kill switch is derived from a SINGLE source of truth —
    /// `rep.metrics.is_some()` — NOT a separate `metrics_enabled` flag (review
    /// round-5 P2-1): when metrics are disabled, `new_reply` builds the legacy
    /// ctx-less reply (`metrics == None`), and when enabled it builds the ctx-bearing
    /// one. Deriving the gate from `rep` makes it impossible to wire a "record stream
    /// metrics but no request ctx" (or the inverse) split-brain state. Kept private
    /// (the tests module is a child module and reaches it without `pub(crate)`).
    async fn send_stream_dispatch(
        fs: &Arc<T>,
        req: FuseRequest,
        rep: FuseResponse,
    ) -> FuseResult<()> {
        // Single gate source: metrics are on iff the reply carries a metrics ctx.
        let metrics_enabled = rep.metrics.is_some();
        // A structural parse failure after the ctx exists must finish the context
        // early (drop the active guard, mark finished) and emit no reply.
        let operator = match req.parse_operator() {
            Ok(op) => op,
            Err(err) => {
                // Structural parse failure after the ctx exists: no stable errno
                // for the parse reason, so use the catch-all "other".
                rep.finish_early(err.errno(), "other");
                return Err(err);
            }
        };

        // Clone shares the same metrics slot; the clone is the error-path reply
        // so an enqueue/dispatch failure finishes the *original* context once
        // (single logical finish, guard not double-counted) instead of building a
        // fresh context.
        let err_rep = rep.clone();

        // Phase 2b: IO attribution at the send_stream layer, created AFTER parse
        // success and BEFORE the match, so they cover the match arm AND the
        // `if res.is_err()` error reply enqueue below (a pre-dispatch error — e.g.
        // a handle lookup failure — replies there, not inside `fs.<io>`). For a
        // known stream opcode exactly one of these is `Some`:
        //   - read/write  -> `io_dispatch_duration_us{io_type}` RAII timer (dispatch
        //     latency: may include a read-after-write consistency flush/reopen for
        //     read, or a zero-length no-op direct reply for write — NOT pure enqueue).
        //   - flush/fsync/release -> a `StreamLifecycleScope` (attempt counted now,
        //     duration timer + inflight guard held across the whole arm).
        // The read/write backend `io_*` (duration/bytes/size/inflight) is recorded
        // separately in the reader/writer task body (C2); this layer does NOT
        // re-record it. Gated on `metrics_enabled` (disabled => both None, no
        // attempt counted, no clock read).
        //
        // ⚠ INVARIANT (do not break): there must be NO `.await` and NO early
        // return between `parse_operator()` succeeding above and the two scopes
        // below. `stream_lifecycle_scope` counts the attempt and arms the
        // duration timer + inflight guard as one atomic step; inserting a
        // suspension/return in this window could count an attempt whose timer
        // never observes (or vice versa), unbalancing the lifecycle family. The
        // first awaits are the `fs.<io>(op, rep).await` calls inside the match,
        // already covered by the scopes.
        let _dispatch = if metrics_enabled {
            dispatch_io_type(req.opcode()).map(FuseMetrics::io_dispatch_timer)
        } else {
            None
        };
        let _lifecycle = if metrics_enabled {
            lifecycle_io_type(req.opcode()).map(FuseMetrics::stream_lifecycle_scope)
        } else {
            None
        };

        // NOTE: keep this match's stream arms in sync with `FuseRequest::is_stream()`
        // (Read/Write/Flush/Release/Fsync). `send_stream` is only entered when
        // `is_stream()` is true, so the wildcard is unreachable today; it exists
        // as a defensive branch in case the two ever drift.
        let res = match operator {
            FuseOperator::Read(op) => fs.read(op, rep).await,

            FuseOperator::Write(op) => fs.write(op, rep).await,

            FuseOperator::Flush(op) => fs.flush(op, rep).await,

            FuseOperator::Release(op) => fs.release(op, rep).await,

            FuseOperator::FSync(op) => fs.fsync(op, rep).await,

            _ => {
                // Defensive: a stream-gated request whose operator is not a known
                // stream op. Tag it `unimplemented_opcode` so it classifies as
                // Unsupported — the same semantics as the metadata `dispatch_meta`
                // wildcard — rather than a plain backend Error.
                let err: FuseResult<fuse_out_header> = err_fuse!(
                    libc::ENOSYS,
                    "unsupported stream operation {:?}",
                    req.opcode()
                );
                return err_rep
                    .send_rep_tagged(err, Some("unimplemented_opcode"), false)
                    .await
                    .map_err(|x| x.into());
            }
        };

        if res.is_err() {
            err_rep.send_rep(res).await?;
        }
        Ok(())
    }

    pub async fn start(mut self, mut shutdown_rx: watch::Receiver<bool>) -> FuseResult<()> {
        debug!("fuse receiver started");
        loop {
            // Receiver loop wait covers idle wait for the next kernel request
            // plus the splice and header parse below. Observed only on the
            // `receive()` Ok path (splice errors are counted by
            // receive_errors_total instead). framework health -> metrics_enabled:
            // when disabled we don't even read the clock (matches meta_spawn's
            // `Option<Instant>` and the kill-switch "no machinery" goal).
            //
            // The clock is read before the `select!`, so a wake from the
            // shutdown branch (rather than `receive()`) reads an `Instant` that
            // is never observed. Acceptable: shutdown is rare and the read is
            // cheap; scoping the start to only the receive branch would tangle
            // with the `&mut self` borrow inside `select!` for no real gain.
            let wait_start = if self.metrics_enabled {
                Some(mono_now())
            } else {
                None
            };
            tokio::select! {
                res = self.receive() => {
                    match res {
                        Ok(buf) => {
                            // Parse first, THEN observe loop wait — so the
                            // histogram includes the header-parse cost and a
                            // decode failure still records a sample.
                            let parsed = FuseRequest::from_bytes(buf.freeze());
                            if let Some(start) = wait_start {
                                FuseMetrics::get()
                                    .record_receive_loop_wait(start.elapsed().as_micros() as u64);
                            }
                            let req = match parsed {
                                Ok(req) => req,
                                Err(e) => {
                                    // Structural decode failure before any ctx:
                                    // count it, but keep the existing `?` control
                                    // flow (this still terminates the receiver).
                                    if self.metrics_enabled {
                                        FuseMetrics::get().record_decode_error("other");
                                    }
                                    return Err(e.into());
                                }
                            };

                            if self.debug {
                                // Debug logging must NOT parse the operator here:
                                // a parse failure would `?`-return out of the
                                // receiver loop *before* the context is created,
                                // both terminating the receiver and bypassing the
                                // dispatch-path `finish_early` cleanup. The
                                // dispatch path (`dispatch_meta` / `send_stream`)
                                // is the single parse + cleanup site. Log only the
                                // fields available without parsing the body.
                                info!(
                                    "receive unique: {}, code: {:?}",
                                    req.unique(),
                                    req.opcode(),
                                );
                            }
                            if req.is_meta() {
                                self.audit(&req);
                            }

                            if req.is_stream() {
                                if let Err(e) = self.send_stream(req).await {
                                    error!("failed to dispatch stream request: {}", e);
                                }
                            } else {
                                let labels = self.maybe_req_labels(&req);
                                let reply = self.new_reply(req.unique(), labels);
                                let fs = self.fs.clone();
                                let pending_requests = self.pending_requests.clone();
                                // meta_task_inflight guard + meta_spawn stage are
                                // created BEFORE spawn so they cover the runtime
                                // queue wait (submission -> first poll). Both gated
                                // on metrics_enabled (None / no observe when off);
                                // production disabled path must NOT use a noop guard.
                                //
                                // Boundary: `meta_spawn` measures tasks that reach
                                // first poll. If the runtime drops/aborts a task
                                // before its first poll (e.g. shutdown), the guard
                                // still dec's meta_task_inflight on drop, but no
                                // meta_spawn sample is recorded. Out of scope for
                                // 1b-1 (would need spawn-drop instrumentation).
                                let meta_guard = FuseMetrics::meta_task_guard(self.metrics_enabled);
                                let spawn_start =
                                    if self.metrics_enabled { Some(mono_now()) } else { None };
                                self.rt.spawn(async move {
                                    // First poll: record the spawn->first-poll
                                    // scheduling delay (status=success).
                                    if let Some(start) = spawn_start {
                                        FuseMetrics::get()
                                            .record_meta_spawn(start.elapsed().as_micros() as u64);
                                    }
                                    let dispatch_result = Self::dispatch_meta_interrupt(
                                        fs, pending_requests, req, reply,
                                    )
                                    .await;
                                    // Drop the guard the moment dispatch returns
                                    // (incl. error/interrupt paths), BEFORE the
                                    // error log — so meta_task_inflight matches the
                                    // "spawn submission -> dispatch returns" scope
                                    // exactly and excludes log formatting time.
                                    drop(meta_guard);
                                    if let Err(e) = dispatch_result {
                                        error!("failed to dispatch meta request: {}", e);
                                    }
                                });
                            }
                        }

                        Err(e) => {
                            // Splice/receive error before a request is decoded:
                            // count by errno + loop action (framework health ->
                            // metrics_enabled). Control flow is unchanged; the
                            // original error is still propagated on the exit arm.
                            let os_errno = e.raw_error().raw_os_error();
                            if self.metrics_enabled {
                                let (errno_label, action) = receive_error_labels(os_errno);
                                FuseMetrics::get().record_receive_error(errno_label, action);
                            }
                            match os_errno {
                                Some(ENOENT) => continue,
                                Some(EINTR) => continue,
                                Some(EAGAIN) => continue,
                                Some(ENODEV) => break,
                                _ => return Err(e.into()),
                            }
                        }
                    }
                }

                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("receiver observed shutdown broadcast; exiting receive loop");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn dispatch_meta_interrupt(
        fs: Arc<T>,
        pending_requests: Arc<FastDashMap<u64, Arc<Notify>>>,
        req: FuseRequest,
        reply: FuseResponse,
    ) -> FuseResult<()> {
        if !req.is_interrupt() {
            return Self::dispatch_meta(&pending_requests, &fs, &req, &reply).await;
        }

        let notify = Arc::new(Notify::new());
        pending_requests.insert(req.unique(), notify.clone());

        // This branch is reached only for FUSE_SETLKW (`is_interrupt()` is true
        // only there), so the SETLKW metrics live here, wrapping the whole
        // interruptible-request scope (parse + dispatch_meta + lock polling +
        // reply enqueue):
        //
        // - `setlkw_inflight`: SETLKW interruptible-request scopes in flight.
        // - `setlkw_wait_duration_us`: the interruptible-request-duration timer.
        //   It is created here, BEFORE the `select!`, NOT inside `set_lkw()` — so
        //   an interrupt that wins the `select!` before `dispatch_meta`'s future
        //   ever polls into `set_lkw()` (immediate / fast cancellation) still
        //   records a sample on drop; placing it inside `set_lkw()` would miss
        //   exactly that case.
        //
        // CONSEQUENCE (review R2 P1#1): this is NOT pure lock-acquisition time —
        // the scope spans parse, dispatch, AND the reply-channel enqueue, so
        // reply-channel backpressure can inflate it. The metric is deliberately
        // an "interruptible-request duration" wrapper, not a lock-contention gauge
        // (see its help); dashboards must not read it as lock wait. Likewise a
        // malformed SETLKW that fails `parse_operator()` produces a near-zero
        // sample (timer exists before parse). This wrapper framing was the chosen
        // trade-off over special-casing the SETLKW dispatch arm (which would touch
        // the dispatch matrix the design avoids).
        //
        // Both are RAII: their Drop — not the `pending_requests.remove` — does the
        // gauge dec / histogram observe, so every `select!` branch (and any future
        // drop / cancellation) balances the gauge and records a sample exactly
        // once. Gated by `reply.metrics.is_some()` (== metrics_enabled; the
        // singleton is initialized whenever that holds), so disabled builds create
        // neither.
        //
        // **Scope caveat — gauge/histogram only, NOT the map.** RAII here keeps
        // `setlkw_inflight` and the wait histogram correct under cancellation, but
        // it does NOT clean up the `pending_requests` map: the map `remove` runs
        // only in the two `select!` branches below. If the whole future is aborted
        // after the `insert` (runtime shutdown, task drop, a future-added timeout),
        // the map entry leaks and a stray later interrupt could match a waiter that
        // no longer exists. That is pre-existing behaviour, unchanged by Phase 2a;
        // only the gauge/histogram are guaranteed cancellation-safe here.
        let _setlkw_inflight = FuseMetrics::setlkw_inflight_guard(reply.metrics.is_some());
        let _setlkw_wait = FuseMetrics::setlkw_wait_timer(reply.metrics.is_some());

        let res = tokio::select! {
            result = Self::dispatch_meta(&pending_requests, &fs, &req, &reply) => {
                pending_requests.remove(&req.unique());
                result
            }

            _ = notify.notified() => {
                pending_requests.remove(&req.unique());
                let err: FuseResult<()> = err_fuse!(libc::EINTR, "operation interrupted");
                // Source-tagged as interrupted (the SETLKW interrupt-notify path),
                // not inferred from the EINTR errno.
                reply.send_rep_tagged(err, None, true).await.map_err(|x| x.into())
            }
        };

        res
    }

    pub async fn dispatch_meta(
        pending_requests: &FastDashMap<u64, Arc<Notify>>,
        fs: &T,
        req: &FuseRequest,
        reply: &FuseResponse,
    ) -> FuseResult<()> {
        // A structural parse failure happens *after* the ctx was created in the
        // receiver, so it must finish the context early (drop the active guard,
        // mark finished) without emitting a request reply.
        let operator = match req.parse_operator() {
            Ok(op) => op,
            Err(err) => {
                reply.finish_early(err.errno(), "other");
                return Err(err);
            }
        };

        // operation_duration_us: a single timer around the whole match (NOT
        // per-arm — the design's anti-goal is touching the ~30 dispatch arms).
        // Gated by `reply.metrics.is_some()` (the disabled-mode signal — this is
        // a free fn with no `metrics_enabled` field), so disabled mode does not
        // even read the clock. Started after parse success, so a parse failure
        // (handled above via finish_early) emits no operation sample.
        let op_start = reply.metrics.is_some().then(mono_now);

        let res = match operator {
            FuseOperator::Init(op) => reply.send_rep(fs.init(op).await).await,

            FuseOperator::StatFs(op) => reply.send_rep(fs.stat_fs(op).await).await,

            FuseOperator::Access(op) => reply.send_rep(fs.access(op).await).await,

            FuseOperator::Lookup(op) => reply.send_rep(fs.lookup(op).await).await,

            FuseOperator::GetAttr(op) => reply.send_rep(fs.get_attr(op).await).await,

            FuseOperator::SetAttr(op) => reply.send_rep(fs.set_attr(op).await).await,

            FuseOperator::GetXAttr(op) => reply.send_buf(fs.get_xattr(op).await).await,

            FuseOperator::SetXAttr(op) => reply.send_rep(fs.set_xattr(op).await).await,

            FuseOperator::RemoveXAttr(op) => reply.send_rep(fs.remove_xattr(op).await).await,

            FuseOperator::ListXAttr(op) => reply.send_buf(fs.list_xattr(op).await).await,

            FuseOperator::OpenDir(op) => reply.send_rep(fs.open_dir(op).await).await,

            FuseOperator::Mkdir(op) => reply.send_rep(fs.mkdir(op).await).await,

            FuseOperator::FAllocate(op) => reply.send_rep(fs.allocate(op).await).await,

            FuseOperator::ReleaseDir(op) => reply.send_rep(fs.release_dir(op).await).await,

            FuseOperator::ReadDir(op) => {
                let res = fs.read_dir(op).await.map(|x| x.take());
                reply.send_buf(res).await
            }

            FuseOperator::ReadDirPlus(op) => {
                let res = fs.read_dir_plus(op).await.map(|x| x.take());
                reply.send_buf(res).await
            }

            FuseOperator::Forget(op) => reply.send_none(fs.forget(op).await),

            FuseOperator::Open(op) => reply.send_rep(fs.open(op).await).await,

            FuseOperator::MkNod(op) => reply.send_rep(fs.mk_nod(op).await).await,

            FuseOperator::Create(op) => reply.send_rep(fs.create(op).await).await,

            FuseOperator::Unlink(op) => reply.send_rep(fs.unlink(op).await).await,

            FuseOperator::RmDir(op) => reply.send_rep(fs.rm_dir(op).await).await,

            FuseOperator::Link(op) => reply.send_rep(fs.link(op).await).await,

            FuseOperator::BatchForget(op) => reply.send_none(fs.batch_forget(op).await),

            FuseOperator::Rename(op) => reply.send_rep(fs.rename(op).await).await,

            FuseOperator::Interrupt(op) => {
                let res = if let Some(notify) = pending_requests.get(&op.arg.unique) {
                    notify.notify_one();
                    Ok(())
                } else {
                    fs.interrupt(op).await
                };
                reply.send_none(res)
            }

            FuseOperator::Symlink(op) => reply.send_rep(fs.symlink(op).await).await,

            FuseOperator::Readlink(op) => reply.send_buf(fs.readlink(op).await).await,

            FuseOperator::GetLk(op) => reply.send_rep(fs.get_lk(op).await).await,

            FuseOperator::SetLk(op) => reply.send_rep(fs.set_lk(op).await).await,

            FuseOperator::SetLkW(op) => reply.send_rep(fs.set_lkw(op).await).await,

            _ => {
                // A parsed-but-unhandled opcode: source-tagged as Unsupported so
                // it classifies that way (not a backend Error). The reason splits
                // two cases the kernel can produce:
                //   - opcode == NOT_SUPPORTED (the num_enum default, raw opcode
                //     this build has no enum value for) -> `unknown_opcode`
                //     (a kernel/daemon protocol-compatibility signal).
                //   - a known opcode with no dispatch arm (e.g. Rename2) ->
                //     `unimplemented_opcode` (an implementation-gap signal).
                let reason = if req.opcode() == FuseOpCode::NOT_SUPPORTED {
                    "unknown_opcode"
                } else {
                    "unimplemented_opcode"
                };
                let err: FuseResult<fuse_out_header> =
                    err_fuse!(libc::ENOSYS, "unsupported operation {:?}", req.opcode());
                reply.send_rep_tagged(err, Some(reason), false).await
            }
        };

        // operation_duration_us{opcode,kind=metadata,status}: observe once, after
        // the whole match. `status` is the stashed `op_status` (the FS-operation
        // result computed by `finish_status`), NOT the `IOResult` of `res` — a
        // successful enqueue of an error frame returns `Ok(())`, so deriving status
        // from `res` would mislabel almost everything `success`. The send helpers
        // stash `op_status` synchronously before enqueuing, and each match arm
        // awaits its send, so by here the slot's `op_status` is the FS result.
        //
        // `op_start` is `Some` iff metrics are enabled (it was built with
        // `reply.metrics.is_some().then(mono_now)`), so a disabled request never
        // read the clock and skips the observe here too. A missing `op_status`
        // (e.g. a no-reply path that didn't stash, or a defensive gap) is treated
        // as `Error` rather than silently dropped — the only realistic miss is a
        // wiring bug, and `Error` is the safe non-success bucket.
        if let Some(start) = op_start {
            // Every dispatched metadata op should have stashed `op_status` via a
            // send helper / no-reply finish before the match returned. A `None`
            // here means some arm bypassed that — a wiring bug. Surface it loudly
            // in debug (debug_assert) AND leave a release-visible `warn!` so a
            // production occurrence is diagnosable (it would otherwise only show as
            // an unexplained `status=error` latency sample). Release still falls
            // back to `Error` (the safe non-success bucket) rather than dropping
            // the sample (review R1 P2#6 / R2 P2#8).
            let op_status = match reply.metrics_op_status() {
                Some(s) => s,
                None => {
                    debug_assert!(
                        false,
                        "operation_duration_us: op_status not stashed for opcode {} — a \
                         dispatch arm bypassed the send/no-reply finish helpers",
                        req.opcode().as_str()
                    );
                    warn!(
                        "operation_duration_us: op_status missing for opcode {} (unique {}); \
                         recording status=error — likely a dispatch-arm wiring bug",
                        req.opcode().as_str(),
                        req.unique()
                    );
                    FuseReqStatus::Error
                }
            };
            FuseMetrics::get().record_operation(
                req.opcode().as_str(),
                op_status,
                start.elapsed().as_micros() as u64,
            );
        }

        res?;
        Ok(())
    }
}

/// Classify a splice/receive OS errno into `receive_errors_total{errno,action}`
/// labels. A free function (not a method) so this classification — which mirrors
/// the `start()` loop's error match and is otherwise hard to unit-test inline —
/// is deterministically testable without constructing a `FuseReceiver`. The
/// labels track the loop's control flow: ENOENT/EINTR/EAGAIN continue, and
/// everything else (incl. ENODEV and unknown/None) exits.
fn receive_error_labels(os_errno: Option<i32>) -> (&'static str, &'static str) {
    let errno = splice_errno_label(os_errno.unwrap_or(0));
    let action = match os_errno {
        Some(ENOENT) | Some(EINTR) | Some(EAGAIN) => RECEIVE_ACTION_CONTINUE,
        _ => RECEIVE_ACTION_EXIT,
    };
    (errno, action)
}

#[cfg(test)]
mod tests {
    use super::receive_error_labels;
    use crate::fuse_metrics::{RECEIVE_ACTION_CONTINUE, RECEIVE_ACTION_EXIT};
    use libc::{EAGAIN, EINTR, EIO, ENODEV, ENOENT};

    #[test]
    fn receive_error_labels_classify_errno_and_action() {
        // continue arms: lowercase errno, action=continue.
        assert_eq!(
            receive_error_labels(Some(ENOENT)),
            ("enoent", RECEIVE_ACTION_CONTINUE)
        );
        assert_eq!(
            receive_error_labels(Some(EINTR)),
            ("eintr", RECEIVE_ACTION_CONTINUE)
        );
        assert_eq!(
            receive_error_labels(Some(EAGAIN)),
            ("eagain", RECEIVE_ACTION_CONTINUE)
        );
        // ENODEV: graceful break -> exit.
        assert_eq!(
            receive_error_labels(Some(ENODEV)),
            ("enodev", RECEIVE_ACTION_EXIT)
        );
        // unknown errno and missing errno -> other/exit.
        assert_eq!(
            receive_error_labels(Some(EIO)),
            ("other", RECEIVE_ACTION_EXIT)
        );
        assert_eq!(receive_error_labels(None), ("other", RECEIVE_ACTION_EXIT));
    }

    // --- Phase 2a: dispatch_meta integration (operation_duration_us wiring) ---
    //
    // These drive the real `dispatch_meta` link — parse_operator -> match arm ->
    // send helper stashes op_status -> post-match `record_operation` reads it back
    // — to prove the wiring the helper-only tests can't: that the
    // `operation_duration_us{status}` label comes from the stashed `op_status`
    // (the FS-operation result), not from the `IOResult` of the awaited send.
    mod dispatch_meta_integration {
        use crate::fs::TestFileSystem;
        use crate::fuse_metrics::{
            FuseMetrics, FuseReqCtx, FuseReqKind, FuseReqLabels, FuseReqStatus,
        };
        use crate::raw::fuse_abi::{fuse_forget_in, fuse_in_header, fuse_interrupt_in};
        use crate::session::{FuseRequest, FuseResponse, FuseTask};
        use crate::FuseUtils;
        use bytes::{BufMut, BytesMut};
        use curvine_common::conf::FuseConf;
        use orpc::common::Metrics as m;
        use orpc::sync::channel::{AsyncChannel, AsyncReceiver};
        use orpc::sync::FastDashMap;

        // FUSE opcodes used here (avoid pulling the whole abi into scope). Each
        // test uses a DISTINCT opcode so their `operation_duration_us` label
        // children never collide on the shared process-global registry — the tests
        // run in parallel and assert deltas, so a shared opcode would make
        // `==before+1` / `==before` flaky.
        const OP_LOOKUP: u32 = 1;
        const OP_FORGET: u32 = 2;
        const OP_GETATTR: u32 = 3;
        const OP_STATFS: u32 = 17;
        const OP_ACCESS: u32 = 34;
        const OP_INTERRUPT: u32 = 36;

        // Build a raw FUSE request: header (len auto-filled) + payload, then parse.
        fn make_request(opcode: u32, unique: u64, nodeid: u64, payload: &[u8]) -> FuseRequest {
            let header = fuse_in_header {
                len: (size_of::<fuse_in_header>() + payload.len()) as u32,
                opcode,
                unique,
                nodeid,
                uid: 0,
                gid: 0,
                pid: 0,
                padding: 0,
            };
            let mut buf = BytesMut::new();
            buf.put_slice(FuseUtils::struct_as_bytes(&header));
            buf.put_slice(payload);
            FuseRequest::from_bytes(buf.freeze()).expect("parse header")
        }

        fn getattr_request(unique: u64) -> FuseRequest {
            // GetAttr parses only the header (no arg read).
            make_request(OP_GETATTR, unique, 1, &[])
        }

        fn statfs_request(unique: u64) -> FuseRequest {
            // StatFs parses only the header; TestFileSystem.stat_fs returns Ok.
            make_request(OP_STATFS, unique, 1, &[])
        }

        fn lookup_request(unique: u64, name: &str) -> FuseRequest {
            // Lookup reads a null-terminated name os_str after the header.
            let mut payload = Vec::from(name.as_bytes());
            payload.push(0);
            make_request(OP_LOOKUP, unique, 1, &payload)
        }

        fn forget_request(unique: u64) -> FuseRequest {
            let arg = fuse_forget_in { nlookup: 1 };
            make_request(OP_FORGET, unique, 2, FuseUtils::struct_as_bytes(&arg))
        }

        fn interrupt_request(unique: u64, interrupted_unique: u64) -> FuseRequest {
            let arg = fuse_interrupt_in {
                unique: interrupted_unique,
            };
            make_request(OP_INTERRUPT, unique, 0, FuseUtils::struct_as_bytes(&arg))
        }

        // A reply whose metrics slot is live (active guard backed by a throwaway
        // gauge), wired to a real channel so send helpers can enqueue. `opcode`
        // MUST match the request's opcode: the receiver's `new_reply` derives the
        // ctx labels from the parsed request, so the enqueue-failure path
        // (`finish_enqueue_failure`) records `request_duration_us{opcode}` under
        // this label — a mismatch would silently miss the assertion.
        fn metrics_reply(
            unique: u64,
            opcode: &'static str,
        ) -> (FuseResponse, AsyncReceiver<FuseTask>) {
            FuseMetrics::ensure_init().unwrap();
            let (tx, rx) = AsyncChannel::new(16).split();
            let gauge = m::new_gauge(format!("dmi_active_{unique}"), "test".to_string()).unwrap();
            let labels = FuseReqLabels::new(opcode, FuseReqKind::Metadata, 64);
            let ctx = FuseReqCtx {
                labels,
                active: Some(crate::fuse_metrics::ActiveGuard::new(gauge)),
            };
            (FuseResponse::new_reply(unique, tx, false, Some(ctx)), rx)
        }

        fn op_dur_count(opcode: &str, status: &str) -> u64 {
            // Tests read a `before` baseline before building any reply, so init
            // here too (idempotent) — the singleton must exist for `get()`.
            FuseMetrics::ensure_init().unwrap();
            FuseMetrics::get()
                .operation_duration_us
                .with_label_values(&[opcode, "metadata", status])
                .get_sample_count()
        }

        fn request_dur_count(opcode: &str, status: &str) -> u64 {
            FuseMetrics::ensure_init().unwrap();
            FuseMetrics::get()
                .request_duration_us
                .with_label_values(&[opcode, "metadata", status])
                .get_sample_count()
        }

        fn reply_enqueue_err_count(opcode: &str) -> i64 {
            FuseMetrics::ensure_init().unwrap();
            FuseMetrics::get()
                .reply_enqueue_errors_total
                .with_label_values(&[opcode, crate::fuse_metrics::ENQUEUE_REASON_CHANNEL_CLOSED])
                .get()
        }

        fn fs() -> TestFileSystem {
            TestFileSystem::new(FuseConf::default())
        }

        struct InterruptTrackingFileSystem {
            called: Arc<AtomicBool>,
        }

        impl FileSystem for InterruptTrackingFileSystem {
            async fn interrupt(&self, _op: crate::fs::operator::Interrupt<'_>) -> FuseResult<()> {
                self.called.store(true, Ordering::SeqCst);
                Ok(())
            }
        }

        // (1a) GetAttr succeeds (TestFileSystem returns Ok) -> operation sample
        // lands under status=success, NOT polluted by the enqueue IOResult.
        #[tokio::test]
        async fn getattr_success_records_operation_success() {
            let before = op_dur_count("GetAttr", "success");
            let pending = FastDashMap::default();
            let (reply, _rx) = metrics_reply(1001, "GetAttr");
            super::super::FuseReceiver::dispatch_meta(
                &pending,
                &fs(),
                &getattr_request(1001),
                &reply,
            )
            .await
            .unwrap();
            assert_eq!(
                op_dur_count("GetAttr", "success"),
                before + 1,
                "GetAttr Ok -> operation_duration_us{{status=success}}"
            );
        }

        // (1b) Lookup fails with ENOENT (TestFileSystem) -> status=error, derived
        // from the stashed op_status, even though send_buf/send_rep returned Ok(()).
        #[tokio::test]
        async fn lookup_error_records_operation_error() {
            let before = op_dur_count("Lookup", "error");
            let pending = FastDashMap::default();
            let (reply, _rx) = metrics_reply(1002, "Lookup");
            // dispatch_meta surfaces the FS error via `res?`, so it returns Err —
            // but the operation sample must already be recorded as error.
            let _ = super::super::FuseReceiver::dispatch_meta(
                &pending,
                &fs(),
                &lookup_request(1002, "missing"),
                &reply,
            )
            .await;
            assert_eq!(
                op_dur_count("Lookup", "error"),
                before + 1,
                "Lookup ENOENT -> operation_duration_us{{status=error}} (from op_status)"
            );
        }

        // (2) no-reply Forget: TestFileSystem uses the trait-default forget (ENOSYS
        // -> Err), so finish_no_reply classifies status=error. It must STILL record
        // an operation sample (no-reply ops are in operation_duration_us), and emit
        // NO reply task.
        #[tokio::test]
        async fn forget_no_reply_records_operation_and_enqueues_nothing() {
            let before = op_dur_count("Forget", "error");
            let pending = FastDashMap::default();
            let (reply, mut rx) = metrics_reply(1003, "Forget");
            super::super::FuseReceiver::dispatch_meta(
                &pending,
                &fs(),
                &forget_request(1003),
                &reply,
            )
            .await
            .unwrap();
            assert_eq!(
                op_dur_count("Forget", "error"),
                before + 1,
                "no-reply Forget still records an operation sample, status from finish_no_reply"
            );
            assert!(
                rx.try_recv().unwrap().is_none(),
                "Forget is no-reply: no task enqueued"
            );
        }

        #[tokio::test]
        async fn interrupt_notifies_pending_request_and_enqueues_nothing() {
            let interrupted_unique = 2001;
            let pending = FastDashMap::default();
            let notify = std::sync::Arc::new(tokio::sync::Notify::new());
            pending.insert(interrupted_unique, notify.clone());
            let (reply, mut rx) = metrics_reply(1006, "Interrupt");
            let fallback_called = Arc::new(AtomicBool::new(false));
            let fs = InterruptTrackingFileSystem {
                called: fallback_called.clone(),
            };

            super::super::FuseReceiver::dispatch_meta(
                &pending,
                &fs,
                &interrupt_request(1006, interrupted_unique),
                &reply,
            )
            .await
            .unwrap();

            tokio::time::timeout(std::time::Duration::from_secs(1), notify.notified())
                .await
                .expect("pending request is notified");
            assert!(
                rx.try_recv().unwrap().is_none(),
                "Interrupt is no-reply: no task enqueued"
            );
            assert!(
                !fallback_called.load(Ordering::SeqCst),
                "pending request notification is the primary interrupt path"
            );
        }

        #[tokio::test]
        async fn late_interrupt_enqueues_nothing() {
            let pending = FastDashMap::default();
            let (reply, mut rx) = metrics_reply(1007, "Interrupt");
            let fallback_called = Arc::new(AtomicBool::new(false));
            let fs = InterruptTrackingFileSystem {
                called: fallback_called.clone(),
            };

            super::super::FuseReceiver::dispatch_meta(
                &pending,
                &fs,
                &interrupt_request(1007, 2002),
                &reply,
            )
            .await
            .unwrap();

            assert!(
                rx.try_recv().unwrap().is_none(),
                "late Interrupt is no-reply: no task enqueued"
            );
            assert!(
                fallback_called.load(Ordering::SeqCst),
                "late Interrupt invokes the best-effort filesystem fallback"
            );
        }

        // (3) enqueue failure: channel closed before dispatch. The FS op (StatFs)
        // succeeds, so the operation sample is status=success (op_status), even
        // though delivery fails — proving operation status is independent of the
        // request/delivery outcome. Uses StatFs (own opcode) to stay parallel-safe.
        #[tokio::test]
        async fn enqueue_failure_keeps_operation_success() {
            let op_before = op_dur_count("StatFs", "success");
            let req_err_before = request_dur_count("StatFs", "error");
            let enq_err_before = reply_enqueue_err_count("StatFs");

            let pending = FastDashMap::default();
            let (reply, rx) = metrics_reply(1004, "StatFs");
            drop(rx); // close the channel: the reply enqueue will fail.
            let _ = super::super::FuseReceiver::dispatch_meta(
                &pending,
                &fs(),
                &statfs_request(1004),
                &reply,
            )
            .await;

            // operation side: the FS op succeeded, so operation status=success.
            assert_eq!(
                op_dur_count("StatFs", "success"),
                op_before + 1,
                "op succeeded -> operation status=success even when delivery fails"
            );
            // request/delivery side: enqueue failed, so the request is finished
            // early with status=error and a reply_enqueue_errors_total bump. Both
            // sides asserted, proving operation-success vs request-error separation.
            assert_eq!(
                request_dur_count("StatFs", "error"),
                req_err_before + 1,
                "delivery failed -> request_duration_us status=error"
            );
            assert_eq!(
                reply_enqueue_err_count("StatFs"),
                enq_err_before + 1,
                "enqueue failure records reply_enqueue_errors_total reason=channel_closed"
            );
        }

        // (4) parse failure after ctx: an Access request with no arg payload makes
        // parse_operator's get_struct fail -> finish_early, and NO operation
        // sample. Uses Access (own opcode, TestFileSystem doesn't implement it, but
        // we never reach dispatch) so the "no sample" assertion is parallel-safe.
        #[tokio::test]
        async fn parse_failure_records_no_operation_sample() {
            let s_before = op_dur_count("Access", "success");
            let e_before = op_dur_count("Access", "error");
            let pending = FastDashMap::default();
            let (reply, _rx) = metrics_reply(1005, "Access");

            // Access parse needs a fuse_access_in arg via get_struct; with an empty
            // payload that read fails, so parse_operator returns Err -> finish_early
            // (decode_errors_total{phase=parse}), BEFORE the operation timer.
            let truncated = make_request(OP_ACCESS, 1005, 1, &[]);
            let _ = super::super::FuseReceiver::dispatch_meta(&pending, &fs(), &truncated, &reply)
                .await;

            // The truncated Access recorded NO operation sample (parse failed
            // before the post-match `record_operation`).
            assert_eq!(op_dur_count("Access", "success"), s_before);
            assert_eq!(op_dur_count("Access", "error"), e_before);
        }

        // --- R2 P1#5: immediate-interrupt SETLKW records a wait sample ---

        const OP_SETLKW: u32 = 33;

        use crate::fs::operator::SetLkW;
        use crate::fs::FileSystem;
        use crate::raw::fuse_abi::fuse_lk_in;
        use crate::FuseResult;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        // A FileSystem whose `set_lkw` blocks forever (never acquires the lock) and
        // records whether it was polled. Every other method keeps the trait
        // default — only `set_lkw` is overridden — so the `select!` interrupt
        // branch deterministically wins the race against the (never-completing)
        // dispatch branch.
        struct BlockingSetlkwFs {
            polled: Arc<AtomicBool>,
        }
        impl FileSystem for BlockingSetlkwFs {
            fn set_lkw(
                &self,
                _op: SetLkW<'_>,
            ) -> impl std::future::Future<Output = FuseResult<()>> + Send {
                let polled = self.polled.clone();
                async move {
                    polled.store(true, Ordering::SeqCst);
                    // Block forever: the lock is never acquired, so only an
                    // interrupt can end this request.
                    std::future::pending::<()>().await;
                    unreachable!("set_lkw is cancelled by interrupt, never completes")
                }
            }
        }

        fn setlkw_request(unique: u64) -> FuseRequest {
            let arg = fuse_lk_in::default();
            make_request(OP_SETLKW, unique, 1, FuseUtils::struct_as_bytes(&arg))
        }

        fn setlkw_wait_count() -> u64 {
            FuseMetrics::ensure_init().unwrap();
            FuseMetrics::get()
                .setlkw_wait_duration_us
                .get_sample_count()
        }

        // A blocking SETLKW that is interrupted AFTER `set_lkw()` has been polled
        // (it acquired no lock — it blocks forever) still records a wait sample.
        // We drive the real `dispatch_meta_interrupt` race: `set_lkw` blocks
        // forever (polled=true), then we fire the interrupt via the
        // pending_requests Notify, so the notify branch wins and the dispatch
        // (set_lkw) branch is cancelled.
        //
        // SCOPE (review R3 P1#1): this covers "set_lkw entered, then interrupted",
        // NOT "interrupt wins before set_lkw is ever polled" — the assertion
        // `polled == true` makes that explicit. The complementary case where the
        // timer's outer placement matters even though `set_lkw()` is NEVER reached
        // is covered by `malformed_setlkw_records_wait_sample_without_set_lkw`
        // below (parse failure → set_lkw never called → wait sample still +1).
        #[tokio::test]
        async fn interrupted_blocking_setlkw_records_wait_sample() {
            FuseMetrics::ensure_init().unwrap();
            let wait_before = setlkw_wait_count();

            let polled = Arc::new(AtomicBool::new(false));
            let fs = Arc::new(BlockingSetlkwFs {
                polled: polled.clone(),
            });
            let pending: Arc<FastDashMap<u64, Arc<tokio::sync::Notify>>> =
                Arc::new(FastDashMap::default());
            let (reply, mut rx) = metrics_reply(2001, "SetLkW");

            let pending2 = pending.clone();
            let handle = tokio::spawn(async move {
                super::super::FuseReceiver::dispatch_meta_interrupt(
                    fs,
                    pending,
                    setlkw_request(2001),
                    reply,
                )
                .await
            });

            // Wait until `set_lkw()` has ACTUALLY been polled (it sets `polled`
            // then blocks forever), THEN fire the interrupt. Waiting on the
            // `pending_requests` entry would be racy: the entry is inserted BEFORE
            // the `select!`, so seeing it does NOT prove the dispatch branch (and
            // thus `set_lkw()`) has been polled — under some schedules notify could
            // win before `set_lkw()` is ever entered, breaking the `polled==true`
            // assertion. Gating on `polled` makes this test deterministically cover
            // "set_lkw entered, then interrupted". A timeout prevents a dead wait if
            // that invariant ever regresses.
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if polled.load(Ordering::SeqCst) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("set_lkw() must be polled within the timeout");
            // set_lkw is now polled and blocked; fire the interrupt so the notify
            // branch wins (the pending_requests entry exists — it was inserted
            // before set_lkw was ever polled).
            pending2
                .get(&2001)
                .expect("pending_requests entry exists once set_lkw is polled")
                .notify_one();

            let res = handle.await.unwrap();

            // The interrupt branch builds an EINTR *reply frame* and enqueues it;
            // `send_rep_tagged` returns Ok on a successful enqueue (an error frame
            // is still a successful reply), so the function returns Ok — the EINTR
            // is delivered as a reply, NOT propagated as a Rust Err.
            assert!(res.is_ok(), "interrupt reply enqueued successfully");
            // The enqueued reply is a RequestReply tagged Interrupted (source tag,
            // not inferred from errno).
            match rx.try_recv().unwrap().expect("an interrupt reply task") {
                FuseTask::RequestReply { status, .. } => {
                    assert_eq!(
                        status,
                        FuseReqStatus::Interrupted,
                        "interrupt-notify reply is status=Interrupted"
                    );
                }
                FuseTask::NotifyReply { .. } => panic!("expected RequestReply, got NotifyReply"),
                FuseTask::Reply(_) => panic!("expected RequestReply, got legacy Reply"),
            }
            // set_lkw was entered but never acquired the lock (it blocks forever).
            assert!(
                polled.load(Ordering::SeqCst),
                "set_lkw was polled (the dispatch branch started)"
            );
            // The core P1#1 invariant: a wait sample is recorded even though the
            // lock poll loop never completed.
            assert!(
                setlkw_wait_count() > wait_before,
                "interrupted SETLKW still records a setlkw_wait_duration_us sample"
            );
            // pending_requests entry was removed by the interrupt branch.
            assert!(
                pending2.get(&2001).is_none(),
                "interrupt branch removed the pending_requests entry"
            );
        }

        // R3 P1#1: the case that actually justifies hoisting the timer OUT of
        // `set_lkw()`. A malformed SETLKW (empty payload) fails `parse_operator()`
        // inside `dispatch_meta`, so `set_lkw()` is NEVER called — yet because the
        // timer is created in `dispatch_meta_interrupt` BEFORE the `select!`, a
        // `setlkw_wait_duration_us` sample is still recorded on drop (the wrapper
        // semantics: parse-failure produces a near-zero sample). Had the timer
        // stayed inside `set_lkw()`, this path would record nothing.
        #[tokio::test]
        async fn malformed_setlkw_records_wait_sample_without_set_lkw() {
            FuseMetrics::ensure_init().unwrap();
            let wait_before = setlkw_wait_count();

            let polled = Arc::new(AtomicBool::new(false));
            let fs = Arc::new(BlockingSetlkwFs {
                polled: polled.clone(),
            });
            let pending: Arc<FastDashMap<u64, Arc<tokio::sync::Notify>>> =
                Arc::new(FastDashMap::default());
            let pending2 = pending.clone();
            let (reply, _rx) = metrics_reply(2002, "SetLkW");

            // Malformed SETLKW: opcode is FUSE_SETLKW (so is_interrupt() routes it
            // through dispatch_meta_interrupt and creates the timer), but the empty
            // payload makes parse_operator's get_struct::<fuse_lk_in> fail.
            let malformed = make_request(OP_SETLKW, 2002, 1, &[]);
            let res =
                super::super::FuseReceiver::dispatch_meta_interrupt(fs, pending, malformed, reply)
                    .await;

            // parse failure went through finish_early / Err (not mistaken success).
            assert!(res.is_err(), "malformed SETLKW returns the parse Err");
            // set_lkw was NEVER called (parse failed before dispatch reached it).
            assert!(
                !polled.load(Ordering::SeqCst),
                "set_lkw must NOT be called for a malformed SETLKW (parse failed first)"
            );
            // Yet a wait sample is still recorded — this is the timer-hoist value.
            assert!(
                setlkw_wait_count() > wait_before,
                "malformed SETLKW still records a setlkw_wait_duration_us sample \
                 (timer hoisted before parse)"
            );
            // The dispatch branch also cleaned up the pending_requests entry.
            assert!(
                pending2.get(&2002).is_none(),
                "malformed SETLKW dispatch branch removed its pending_requests entry"
            );
        }
    }

    // --- Phase 2b: send_stream IO attribution integration ---
    //
    // These drive the real stream dispatch core `FuseReceiver::send_stream_dispatch`
    // (extracted from `send_stream` so it needs only `&fs`, a `FuseRequest`, and a
    // pre-built `FuseResponse` — NOT a `kernel_fd`/`Pipe2`/reactor; the metrics gate is
    // derived from `rep.metrics.is_some()`, see `dispatch_one`). `TestFileSystem` returns
    // ENOSYS for every stream op WITHOUT sending a reply, so each op exercises the
    // pre-dispatch-error path: `fs.<io>` returns Err and the error reply is enqueued
    // by the `if res.is_err()` arm — INSIDE the dispatch/lifecycle RAII scope, exactly
    // the boundary we want to assert.
    mod send_stream_integration {
        use crate::fs::TestFileSystem;
        use crate::fuse_metrics::{
            ActiveGuard, FuseMetrics, IO_TYPE_FLUSH, IO_TYPE_FSYNC, IO_TYPE_READ, IO_TYPE_RELEASE,
            IO_TYPE_WRITE, PATH_TYPE_UNKNOWN,
        };
        use crate::raw::fuse_abi::{
            fuse_flush_in, fuse_fsync_in, fuse_in_header, fuse_read_in, fuse_release_in,
            fuse_write_in,
        };
        use crate::session::{FuseRequest, FuseResponse};
        use crate::FuseUtils;
        use bytes::{BufMut, BytesMut};
        use orpc::runtime::{AsyncRuntime, RpcRuntime};
        use orpc::sync::channel::AsyncChannel;
        use std::sync::Arc;

        const OP_READ: u32 = 15;
        const OP_WRITE: u32 = 16;
        const OP_RELEASE: u32 = 18;
        const OP_FSYNC: u32 = 20;
        const OP_FLUSH: u32 = 25;

        fn make_request(opcode: u32, unique: u64, payload: &[u8]) -> FuseRequest {
            let header = fuse_in_header {
                len: (size_of::<fuse_in_header>() + payload.len()) as u32,
                opcode,
                unique,
                nodeid: 1,
                uid: 0,
                gid: 0,
                pid: 0,
                padding: 0,
            };
            let mut buf = BytesMut::new();
            buf.put_slice(FuseUtils::struct_as_bytes(&header));
            buf.put_slice(payload);
            FuseRequest::from_bytes(buf.freeze()).expect("parse stream request")
        }

        fn flush_request(unique: u64) -> FuseRequest {
            make_request(
                OP_FLUSH,
                unique,
                FuseUtils::struct_as_bytes(&fuse_flush_in::default()),
            )
        }
        fn fsync_request(unique: u64) -> FuseRequest {
            make_request(
                OP_FSYNC,
                unique,
                FuseUtils::struct_as_bytes(&fuse_fsync_in::default()),
            )
        }
        fn release_request(unique: u64) -> FuseRequest {
            make_request(
                OP_RELEASE,
                unique,
                FuseUtils::struct_as_bytes(&fuse_release_in::default()),
            )
        }
        fn read_request(unique: u64) -> FuseRequest {
            make_request(
                OP_READ,
                unique,
                FuseUtils::struct_as_bytes(&fuse_read_in::default()),
            )
        }
        fn write_request(unique: u64) -> FuseRequest {
            // size=0 (no write payload): the request parses, and TestFileSystem's
            // fs.write returns ENOSYS at dispatch (pre-task), so this drives the
            // io_dispatch path without needing a real writer task.
            make_request(
                OP_WRITE,
                unique,
                FuseUtils::struct_as_bytes(&fuse_write_in::default()),
            )
        }

        // Drive ONE stream op through the REAL `send_stream_dispatch` core and run it
        // to completion. `op` is the FUSE opcode; `unique`/`payload` build the
        // request. Returns the dispatch result.
        //
        // FD-FREE by construction (review round-2/3/4 P1): earlier rounds built a
        // real `FuseReceiver` here just to reach `send_stream`, which dragged in a
        // `kernel_fd` `AsyncFd` + an internal `Pipe2` whose Drop deregisters from the
        // tokio reactor AFTER closing its fds — aborting the process with `IO Safety
        // violation` under the default parallel harness, and which leak workarounds
        // failed to tame across two rounds. The real fix (codex's repeated
        // recommendation): `send_stream`'s metrics/dispatch logic was extracted into
        // `FuseReceiver::send_stream_dispatch`, which takes a pre-built `FuseResponse`
        // (the metrics gate is derived from `rep.metrics.is_some()`) and only needs
        // `&fs` — NO fd, NO Pipe2, NO reactor. So this helper builds just a
        // `FuseResponse` over an in-memory reply channel
        // and a single-thread runtime to `block_on` the async fn (which spawns
        // nothing and touches no fd). Nothing here registers with a reactor, so there
        // is no fd/runtime lifecycle hazard at all.
        //
        // The reply channel is drained by a spawned task so the error-reply enqueue
        // inside the dispatch (the path the lifecycle/dispatch RAII scope covers)
        // never blocks regardless of how many ops a body drives (review round-4 P2-1).
        //
        // Parallel-safety of the ASSERTIONS: these feed the process-global registry
        // under FIXED io_type labels shared by every send_stream test, so the tests
        // assert only POSITIVE LOWER BOUNDS on each op's own label ("incremented by
        // at least 1"), never exact deltas. The deterministic negatives are covered
        // by the structural unit tests in `fuse_metrics`.
        // `with_metrics_ctx` decides whether the built `FuseResponse` carries a metrics
        // ctx — which, since `send_stream_dispatch` derives its gate from
        // `rep.metrics.is_some()`, IS the metrics on/off switch for the dispatch. It is
        // NOT a separate flag passed to the dispatch (there is none).
        fn dispatch_one(with_metrics_ctx: bool, req: FuseRequest) {
            use crate::fuse_metrics::{FuseReqCtx, FuseReqKind, FuseReqLabels};
            FuseMetrics::ensure_init().unwrap();
            let rt = AsyncRuntime::single();
            rt.block_on(async {
                let fs = Arc::new(TestFileSystem::new(
                    curvine_common::conf::FuseConf::default(),
                ));
                // Reply channel with a drainer so any enqueued error reply is consumed
                // (never blocks), independent of how many ops the caller drives.
                let (tx, mut rx) = AsyncChannel::new(64).split();
                let drainer = tokio::spawn(async move { while rx.recv().await.is_some() {} });

                // Build the reply handle exactly as `send_stream`'s caller would.
                // with-ctx => an active guard backed by a throwaway gauge; without =>
                // None (the legacy zero-cost path). The presence of the ctx IS the
                // metrics gate — `send_stream_dispatch` derives it from
                // `rep.metrics.is_some()`, so there is no separate flag to pass.
                let opcode = req.opcode().as_str();
                let ctx = if with_metrics_ctx {
                    let gauge = orpc::common::Metrics::new_gauge(
                        format!("ss_dispatch_active_{}", req.unique()),
                        "test".to_string(),
                    )
                    .unwrap();
                    Some(FuseReqCtx {
                        labels: FuseReqLabels::new(opcode, FuseReqKind::Stream, 64),
                        active: Some(ActiveGuard::new(gauge)),
                    })
                } else {
                    None
                };
                let rep = FuseResponse::new_reply(req.unique(), tx, false, ctx);

                let _ = super::super::FuseReceiver::<TestFileSystem>::send_stream_dispatch(
                    &fs, req, rep,
                )
                .await;

                // Drop the senders' side and let the drainer finish — no fd, no
                // reactor registration, so nothing here can leak or abort.
                drainer.abort();
            });
        }

        fn lifecycle_attempts(io_type: &str) -> i64 {
            FuseMetrics::get()
                .stream_lifecycle_requests_total
                .with_label_values(&[io_type, PATH_TYPE_UNKNOWN])
                .get()
        }
        fn lifecycle_dur(io_type: &str) -> u64 {
            FuseMetrics::get()
                .stream_lifecycle_duration_us
                .with_label_values(&[io_type, PATH_TYPE_UNKNOWN])
                .get_sample_count()
        }
        fn dispatch_dur(io_type: &str) -> u64 {
            FuseMetrics::get()
                .io_dispatch_duration_us
                .with_label_values(&[io_type])
                .get_sample_count()
        }

        // E21/E26: a flush whose backend errors (pre-dispatch ENOSYS) STILL records a
        // lifecycle attempt (counted before the match) AND a duration sample (the RAII
        // timer observes on drop, covering the error reply enqueue that happens inside
        // the scope), under {io_type=flush,path_type=unknown}. Lower-bound asserts.
        #[test]
        fn flush_records_lifecycle_attempt_and_duration() {
            FuseMetrics::ensure_init().unwrap();
            let attempts_before = lifecycle_attempts(IO_TYPE_FLUSH);
            let dur_before = lifecycle_dur(IO_TYPE_FLUSH);

            dispatch_one(true, flush_request(7001));

            assert!(
                lifecycle_attempts(IO_TYPE_FLUSH) > attempts_before,
                "flush counts a lifecycle attempt (counted before the match)"
            );
            assert!(
                lifecycle_dur(IO_TYPE_FLUSH) > dur_before,
                "flush lifecycle duration observed (RAII timer covers the error reply)"
            );
        }

        // fsync lands under io_type=fsync (NOT flush): at send_stream the operator
        // distinguishes FSYNC from FLUSH — the very ambiguity the writer-task-body
        // Flush arm cannot resolve, which is why lifecycle attribution lives here.
        #[test]
        fn fsync_records_lifecycle_fsync() {
            FuseMetrics::ensure_init().unwrap();
            let fsync_before = lifecycle_attempts(IO_TYPE_FSYNC);

            dispatch_one(true, fsync_request(7002));

            assert!(
                lifecycle_attempts(IO_TYPE_FSYNC) > fsync_before,
                "fsync lands under io_type=fsync"
            );
        }

        // E15 (release attributed at send_stream): one FUSE Release opcode increments
        // stream_lifecycle_requests_total{release}. Doing it here (one operator arm =
        // one increment) is what avoids the reader+writer double-count the task-body
        // approach would have. Lower-bound assert (parallel-safe).
        #[test]
        fn release_records_lifecycle_release() {
            FuseMetrics::ensure_init().unwrap();
            let before = lifecycle_attempts(IO_TYPE_RELEASE);

            dispatch_one(true, release_request(7003));

            assert!(
                lifecycle_attempts(IO_TYPE_RELEASE) > before,
                "release counted at send_stream"
            );
        }

        // E20/E25: read/write at send_stream record io_dispatch_duration_us{io_type}.
        // The backend io_* (duration/bytes/size) is recorded in the task body, not
        // here, so a pre-dispatch ENOSYS read/write records dispatch only — which is
        // exactly what this asserts (the dispatch timer fired). Lower-bound asserts.
        #[test]
        fn read_write_record_dispatch() {
            FuseMetrics::ensure_init().unwrap();
            let read_before = dispatch_dur(IO_TYPE_READ);
            let write_before = dispatch_dur(IO_TYPE_WRITE);

            dispatch_one(true, read_request(7004));
            dispatch_one(true, write_request(7005));

            assert!(
                dispatch_dur(IO_TYPE_READ) > read_before,
                "read records io_dispatch_duration_us{{read}}"
            );
            assert!(
                dispatch_dur(IO_TYPE_WRITE) > write_before,
                "write records io_dispatch_duration_us{{write}}"
            );
        }

        // P2-2 (review round-2): a direct disabled-path integration test for
        // `send_stream`. The send_stream gate (`self.metrics_enabled`) is a DIFFERENT
        // source from the reader/writer task body's gate (`FuseConf.metrics_enabled`),
        // so this guards the send_stream wiring specifically.
        //
        // What this is (review round-3 P2-2): a SMOKE test, not a no-emission proof.
        // - It proves: one op of EACH stream family runs end-to-end through the real
        //   disabled `send_stream` and completes (no panic, no hang). It is a guard
        //   against the disabled send_stream path being wired so badly it cannot even
        //   run all five families.
        // - It does NOT prove "disabled emits nothing": the metrics singleton is
        //   already initialized in the test binary, so a regression that wrongly built
        //   the dispatch timer / lifecycle scope on the disabled path would NOT panic
        //   here — it would just (wrongly) emit. The deterministic "disabled => guard
        //   is None, emits nothing" guarantee is pinned by the isolated helper `*_gate`
        //   unit tests instead. An `== before` count assertion is deliberately avoided:
        //   the io_type labels are a fixed set shared with the enabled send_stream
        //   tests, so it would be flaky under the default parallel harness. This split
        //   — smoke for wiring here, isolated unit test for the no-emission guarantee —
        //   is the same discipline the rest of this metrics work follows.
        #[test]
        fn disabled_send_stream_runs_clean_for_all_families() {
            // ENOSYS replies (TestFileSystem); the point is the metrics gate, not the
            // op result. None of these may touch the Phase 2b stream metrics.
            dispatch_one(false, flush_request(7101));
            dispatch_one(false, fsync_request(7102));
            dispatch_one(false, release_request(7103));
            dispatch_one(false, read_request(7104));
            dispatch_one(false, write_request(7105));
            // Reaching here (no panic) proves the disabled send_stream path drove all
            // five op families to completion with `metrics_enabled=false`.
        }

        // P2-2 (review round-5): a malformed stream request whose `parse_operator()`
        // fails AFTER the ctx was built. This is the ctx-before-parse corner case the
        // round-4 seam refactor must preserve: the parse failure must finish the ctx
        // early (drop the active guard, record the parse decode error) and must NOT
        // enter the dispatch/lifecycle RAII scope (no io_dispatch / lifecycle sample).
        // Pins the seam's most fragile behaviour — that nobody inserts an early
        // return / await between ctx creation and the scope in a way that leaks the
        // guard or emits a stray dispatch sample on the parse-failure path.
        #[test]
        fn malformed_stream_request_finishes_early_without_dispatch_or_lifecycle() {
            use crate::fuse_metrics::{FuseReqCtx, FuseReqKind, FuseReqLabels, DECODE_PHASE_PARSE};
            FuseMetrics::ensure_init().unwrap();
            let mx = FuseMetrics::get();

            // A FUSE_READ header with NO `fuse_read_in` payload: `parse_operator()`'s
            // `get_struct::<fuse_read_in>` fails, so dispatch is never reached.
            let malformed = make_request(OP_READ, 7201, &[]);

            // Baseline: the opcode-free parse decode counter. The active guard uses a
            // LOCAL gauge so we can assert it drops deterministically.
            let decode_before = mx
                .decode_errors_total
                .with_label_values(&[DECODE_PHASE_PARSE, "other"])
                .get();

            let rt = AsyncRuntime::single();
            rt.block_on(async {
                let fs = Arc::new(TestFileSystem::new(
                    curvine_common::conf::FuseConf::default(),
                ));
                let (tx, mut rx) = AsyncChannel::new(16).split();
                let drainer = tokio::spawn(async move { while rx.recv().await.is_some() {} });

                let active_g = orpc::common::Metrics::new_gauge(
                    "ss_malformed_active_7201".to_string(),
                    "test".to_string(),
                )
                .unwrap();
                let ctx = FuseReqCtx {
                    labels: FuseReqLabels::new("Read", FuseReqKind::Stream, 64),
                    active: Some(ActiveGuard::new(active_g.clone())),
                };
                assert_eq!(active_g.get(), 1, "active guard live before dispatch");
                let rep = FuseResponse::new_reply(7201, tx, false, Some(ctx));

                let res = super::super::FuseReceiver::<TestFileSystem>::send_stream_dispatch(
                    &fs, malformed, rep,
                )
                .await;

                // Parse failure surfaces as Err (finish_early then return Err).
                assert!(
                    res.is_err(),
                    "malformed stream request returns the parse Err"
                );
                // Active guard released by finish_early — no leak.
                assert_eq!(
                    active_g.get(),
                    0,
                    "parse failure finishes the ctx early and drops the active guard"
                );
                drainer.abort();
            });

            // The "no io_dispatch / no lifecycle sample on parse failure" property is
            // STRUCTURAL, not asserted on the shared dispatch gauge (which concurrent
            // tests bump, making any count check flaky): the dispatch/lifecycle RAII
            // scope is created only AFTER `parse_operator()` succeeds, so an Err parse
            // returns via `finish_early` before the scope exists. The `res.is_err()` +
            // active-guard-dropped assertions above prove we took exactly that path.
            //
            // The parse decode error was recorded (phase=parse). decode_errors_total is
            // opcode-free/shared, so assert it moved by AT LEAST this one.
            assert!(
                mx.decode_errors_total
                    .with_label_values(&[DECODE_PHASE_PARSE, "other"])
                    .get()
                    > decode_before,
                "parse failure records decode_errors_total{{phase=parse}}"
            );
        }
    }
}
