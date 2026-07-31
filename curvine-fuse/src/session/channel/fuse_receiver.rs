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
use libc::{EAGAIN, ECONNABORTED, EINTR, ENODEV, ENOENT};
use log::{debug, error, info, warn};
use orpc::io::IOResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::AsyncSender;
use orpc::sync::FastDashMap;
use orpc::sys::pipe::{AsyncFd, Pipe2, PipeFd};
use orpc::{err_box, sys, try_option_ref};
use std::sync::Arc;
use tokio::sync::{watch, Notify};

/// Removes an interruptible-request registration when its dispatch future ends.
/// `Drop` covers cancellation paths (e.g. task abort on shutdown) where neither
/// the completion nor interrupt branch runs.
struct PendingRequestGuard {
    pending_requests: Arc<FastDashMap<u64, Arc<Notify>>>,
    unique: u64,
    notify: Arc<Notify>,
    registered: bool,
}

impl PendingRequestGuard {
    fn register(
        pending_requests: Arc<FastDashMap<u64, Arc<Notify>>>,
        unique: u64,
        notify: Arc<Notify>,
    ) -> Self {
        pending_requests.insert(unique, notify.clone());
        Self {
            pending_requests,
            unique,
            notify,
            registered: true,
        }
    }

    fn remove(&mut self) {
        if !self.registered {
            return;
        }

        // Only remove our own registration: an older future's Drop must not
        // delete a same-unique replacement installed after it.
        let _ = self.pending_requests.remove_if(&self.unique, |_, current| {
            Arc::ptr_eq(current, &self.notify)
        });
        self.registered = false;
    }
}

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        self.remove();
    }
}

/// Reads requests from the fuse fd, spawning a task per metadata request and
/// dispatching read/write requests to the sender queue.
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

    pub async fn receive(&mut self) -> IOResult<BytesMut> {
        if self.pipe2.is_some() {
            self.splice().await
        } else {
            self.read().await
        }
    }

    pub async fn read(&mut self) -> IOResult<BytesMut> {
        Self::prepare_receive_buf(&mut self.buf, self.fuse_len);

        let len = self
            .kernel_fd
            .async_read(|fd| sys::read(fd.fd(), &mut self.buf))
            .await?;
        let len = len as usize;
        if len < FUSE_IN_HEADER_LEN {
            // No OS errno, so the receive loop exits on its `_ => return Err` arm.
            return err_box!(
                "short read on fuse device: read {} bytes, expected at least {} bytes",
                len,
                FUSE_IN_HEADER_LEN
            );
        }

        Ok(self.buf.split_to(len))
    }

    pub async fn splice(&mut self) -> IOResult<BytesMut> {
        let pipe2 = try_option_ref!(self.pipe2);

        let write_len = pipe2.write_io(&self.kernel_fd, None, self.fuse_len).await?;
        if write_len < FUSE_IN_HEADER_LEN {
            // No errno: this tears the receiver down, so there is no next frame to
            // poison. Drain anyway in case a short splice ever becomes recoverable.
            if write_len > 0 {
                Self::drain_pipe(pipe2);
            }
            return err_box!(
                "short splice on fuse device: spliced {} bytes, expected at least {} bytes",
                write_len,
                FUSE_IN_HEADER_LEN
            );
        }

        Self::prepare_receive_buf(&mut self.buf, write_len);

        let read_len = match pipe2.read_buf(&mut self.buf[..write_len]).await {
            Ok(read_len) => read_len,
            Err(err) => {
                // Recoverable (real errno): the loop may `continue` to the next
                // frame, so drain the stale bytes that would otherwise poison it.
                Self::drain_pipe(pipe2);
                return Err(err.into());
            }
        };
        if write_len != read_len {
            Self::drain_pipe(pipe2);
            return err_box!(
                "splice read and write lengths are inconsistent: write len {}, read len {}",
                write_len,
                read_len
            );
        }
        Ok(self.buf.split_to(read_len))
    }

    fn drain_pipe(pipe2: &Pipe2) {
        let fd = pipe2.read_raw_fd();
        let mut buf = [0u8; 8192];
        loop {
            match sys::read(fd, &mut buf) {
                Ok(n) if n > 0 => continue,
                Ok(_) => break,
                Err(err) => {
                    if err.raw_os_error() == Some(EINTR) {
                        continue;
                    }
                    break;
                }
            }
        }
    }

    fn prepare_receive_buf(buf: &mut BytesMut, len: usize) {
        buf.clear();
        buf.reserve(len);
        unsafe {
            buf.set_len(len);
        }
    }

    /// Build a reply handle for `unique`. `Some(labels)` builds a metrics context
    /// (bumping `active_requests`); `None` is the disabled path and never touches
    /// `FuseMetrics::get()`, so an uninitialized-metrics process cannot panic here.
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

    /// `Some(labels)` iff metrics are enabled; `None` builds nothing (the gate).
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
        // Build the ctx before parsing, so a later parse failure is a real
        // finish-state event (matching the metadata path).
        let labels = self.maybe_req_labels(&req);
        let rep = self.new_reply(req.unique(), labels);
        Self::send_stream_dispatch(&self.fs, req, rep).await
    }

    /// Stream dispatch + IO attribution core, factored out of `send_stream` so tests
    /// can drive it with a hand-built `FuseResponse`. The metrics gate derives solely
    /// from `rep.metrics.is_some()`, so a "metrics but no ctx" state is unrepresentable.
    async fn send_stream_dispatch(
        fs: &Arc<T>,
        req: FuseRequest,
        rep: FuseResponse,
    ) -> FuseResult<()> {
        let metrics_enabled = rep.metrics.is_some();
        // Parse failure here is after the ctx exists: finish it early (no reply).
        // No stable errno for a parse error, so tag the catch-all "other".
        let operator = match req.parse_operator() {
            Ok(op) => op,
            Err(err) => {
                rep.finish_early(err.errno(), "other");
                return Err(err);
            }
        };

        // Error-path reply sharing the same metrics slot, so a dispatch/enqueue
        // failure finishes the original ctx once rather than double-counting.
        let err_rep = rep.clone();

        // IO attribution timers, armed before the match so they cover the error-reply enqueue
        // too (one Some per stream opcode). ⚠ INVARIANT: after parse succeeds, no `.await`/
        // early return before these scopes — the lifecycle scope arms its guard atomically.
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

        // Keep the stream arms in sync with `FuseRequest::is_stream()`. The
        // fallback is unreachable today (entered only when `is_stream()`); it
        // guards against the two drifting apart.
        let res = match operator {
            FuseOperator::Read(op) => fs.read(op, rep).await,

            FuseOperator::Write(op) => fs.write(op, rep).await,

            FuseOperator::Flush(op) => fs.flush(op, rep).await,

            FuseOperator::Release(op) => fs.release(op, rep).await,

            FuseOperator::FSync(op) => fs.fsync(op, rep).await,

            // Named variants, NOT a `_` wildcard: a new `FuseOperator` left
            // unwired then fails to compile. Reaching here is a routing bug, so
            // tag `unimplemented_opcode` rather than a backend Error.
            FuseOperator::Notimplemented
            | FuseOperator::Init(_)
            | FuseOperator::StatFs(_)
            | FuseOperator::ReadDir(_)
            | FuseOperator::Lookup(_)
            | FuseOperator::GetAttr(_)
            | FuseOperator::SetAttr(_)
            | FuseOperator::GetXAttr(_)
            | FuseOperator::SetXAttr(_)
            | FuseOperator::RemoveXAttr(_)
            | FuseOperator::OpenDir(_)
            | FuseOperator::Mkdir(_)
            | FuseOperator::FAllocate(_)
            | FuseOperator::ReleaseDir(_)
            | FuseOperator::Access(_)
            | FuseOperator::ReadDirPlus(_)
            | FuseOperator::Forget(_)
            | FuseOperator::Open(_)
            | FuseOperator::MkNod(_)
            | FuseOperator::Create(_)
            | FuseOperator::Unlink(_)
            | FuseOperator::RmDir(_)
            | FuseOperator::Link(_)
            | FuseOperator::BatchForget(_)
            | FuseOperator::Rename(_)
            | FuseOperator::Rename2(_)
            | FuseOperator::Interrupt(_)
            | FuseOperator::ListXAttr(_)
            | FuseOperator::FSyncDir(_)
            | FuseOperator::Destroy(_)
            | FuseOperator::Symlink(_)
            | FuseOperator::Readlink(_)
            | FuseOperator::GetLk(_)
            | FuseOperator::SetLk(_)
            | FuseOperator::SetLkW(_)
            | FuseOperator::Ioctl(_) => {
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
            // Loop-wait timer, observed only on the `receive()` Ok path. Read
            // before the `select!` (a shutdown wake reads an unused `Instant`) to
            // avoid tangling the `&mut self` borrow inside the branch.
            let wait_start = if self.metrics_enabled {
                Some(mono_now())
            } else {
                None
            };
            tokio::select! {
                res = self.receive() => {
                    match res {
                        Ok(buf) => {
                            // Parse before observing loop-wait, so the histogram
                            // includes parse cost and a decode failure still samples.
                            let parsed = FuseRequest::from_bytes(buf.freeze());
                            if let Some(start) = wait_start {
                                FuseMetrics::get()
                                    .record_receive_loop_wait(start.elapsed().as_micros() as u64);
                            }
                            let req = match parsed {
                                Ok(req) => req,
                                Err(e) => {
                                    // Decode failure before any ctx exists: count
                                    // it, then terminate the receiver as before.
                                    if self.metrics_enabled {
                                        FuseMetrics::get().record_decode_error("other");
                                    }
                                    return Err(e.into());
                                }
                            };

                            if self.debug {
                                // Log header fields only: parsing the operator here
                                // could `?`-return early and bypass the dispatch
                                // path's `finish_early` cleanup.
                                info!(
                                    "receive unique: {}, code: {:?}",
                                    req.unique(),
                                    req.opcode(),
                                );
                            }
                            if req.should_audit() {
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
                                // Guard + spawn timer built before spawn so they
                                // cover the runtime queue wait (submit -> first poll).
                                let meta_guard = FuseMetrics::meta_task_guard(self.metrics_enabled);
                                let spawn_start =
                                    if self.metrics_enabled { Some(mono_now()) } else { None };
                                self.rt.spawn(async move {
                                    if let Some(start) = spawn_start {
                                        FuseMetrics::get()
                                            .record_meta_spawn(start.elapsed().as_micros() as u64);
                                    }
                                    let dispatch_result = Self::dispatch_meta_interrupt(
                                        fs, pending_requests, req, reply,
                                    )
                                    .await;
                                    // Drop the guard before the error log, so the
                                    // inflight scope excludes log-formatting time.
                                    drop(meta_guard);
                                    if let Err(e) = dispatch_result {
                                        error!("failed to dispatch meta request: {}", e);
                                    }
                                });
                            }
                        }

                        Err(e) => {
                            // Receive error before any request is decoded: count by
                            // errno + loop action, then dispatch on the errno below.
                            let os_errno = e.raw_error().raw_os_error();
                            if self.metrics_enabled {
                                let (errno_label, action) = receive_error_labels(os_errno);
                                FuseMetrics::get().record_receive_error(errno_label, action);
                            }
                            match os_errno {
                                Some(ENOENT) => continue,
                                Some(EINTR) => continue,
                                Some(EAGAIN) => continue,
                                Some(ENODEV) => {
                                    info!("receiver exiting: fuse device gone (ENODEV)");
                                    break;
                                }
                                Some(ECONNABORTED) => {
                                    info!("receiver exiting: connection aborted (ECONNABORTED)");
                                    break;
                                }
                                _ => return Err(e.into()),
                            }
                        }
                    }
                }

                changed = shutdown_rx.changed() => {
                    match changed {
                        Ok(()) if *shutdown_rx.borrow() => {
                            info!("receiver observed shutdown broadcast; exiting receive loop");
                            break;
                        }
                        Ok(()) => {}
                        Err(_) => {
                            warn!("receiver shutdown channel closed; exiting receive loop");
                            break;
                        }
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
        if !req.is_interruptible_wait() {
            return Self::dispatch_meta(&pending_requests, &fs, &req, &reply).await;
        }

        let notify = Arc::new(Notify::new());
        let mut pending_request =
            PendingRequestGuard::register(pending_requests.clone(), req.unique(), notify.clone());

        // Built before the `select!` so they wrap the WHOLE interruptible scope: an
        // interrupt winning before `set_lkw()` is polled still records. Measures
        // request duration, NOT lock-acquisition time (backpressure inflates it).
        let _setlkw_inflight = FuseMetrics::setlkw_inflight_guard(reply.metrics.is_some());
        let _setlkw_wait = FuseMetrics::setlkw_wait_timer(reply.metrics.is_some());

        let res = tokio::select! {
            result = Self::dispatch_meta(&pending_requests, &fs, &req, &reply) => {
                pending_request.remove();
                result
            }

            _ = notify.notified() => {
                pending_request.remove();
                let err: FuseResult<()> = err_fuse!(libc::EINTR, "operation interrupted");
                // Tagged interrupted by source, not inferred from the EINTR errno.
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
        // Parse failure here is after the ctx exists: finish it early (no reply).
        let operator = match req.parse_operator() {
            Ok(op) => op,
            Err(err) => {
                reply.finish_early(err.errno(), "other");
                return Err(err);
            }
        };

        // One timer around the whole match (not per-arm). Started after parse, so
        // a parse failure records no operation sample.
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

            FuseOperator::Rename2(op) => reply.send_rep(fs.rename2(op).await).await,

            FuseOperator::FSyncDir(op) => reply.send_rep(fs.fsync_dir(op).await).await,

            FuseOperator::Destroy(op) => reply.send_rep(fs.destroy(op).await).await,

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

            FuseOperator::Ioctl(op) => reply.send_buf(fs.ioctl(op).await).await,

            // Named variants, NOT a `_` wildcard, so a new unwired `FuseOperator` fails
            // to compile (caught the RENAME2 half-wiring). Tag: NOT_SUPPORTED ->
            // `unknown_opcode`, else `unimplemented_opcode`.
            FuseOperator::Notimplemented
            | FuseOperator::Read(_)
            | FuseOperator::Write(_)
            | FuseOperator::Flush(_)
            | FuseOperator::Release(_)
            | FuseOperator::FSync(_) => {
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

        // Observe once after the match. `status` is the stashed `op_status` (the FS
        // result), NOT `res`: a successful enqueue of an error frame returns
        // `Ok(())`, which would mislabel almost everything `success`.
        if let Some(start) = op_start {
            // Missing `op_status` means a dispatch arm bypassed the finish helpers
            // (a wiring bug): surface it and fall back to `Error`, don't drop it.
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

/// Classify a receive errno into `receive_errors_total{errno,action}` labels,
/// mirroring the `start()` loop: ENOENT/EINTR/EAGAIN continue, all else exits.
/// A free fn (not a method) so it is testable without a `FuseReceiver`.
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
    use super::{receive_error_labels, FuseReceiver, PendingRequestGuard};
    use crate::fs::TestFileSystem;
    use crate::fuse_metrics::{RECEIVE_ACTION_CONTINUE, RECEIVE_ACTION_EXIT};
    use bytes::BytesMut;
    use libc::{EAGAIN, ECONNABORTED, EINTR, EIO, ENODEV, ENOENT};
    use orpc::sync::FastDashMap;
    use std::sync::Arc;
    use tokio::sync::Notify;

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
        // ENODEV/ECONNABORTED: graceful break -> exit.
        assert_eq!(
            receive_error_labels(Some(ENODEV)),
            ("enodev", RECEIVE_ACTION_EXIT)
        );
        assert_eq!(
            receive_error_labels(Some(ECONNABORTED)),
            ("econnaborted", RECEIVE_ACTION_EXIT)
        );
        // unknown errno and missing errno -> other/exit.
        assert_eq!(
            receive_error_labels(Some(EIO)),
            ("other", RECEIVE_ACTION_EXIT)
        );
        assert_eq!(receive_error_labels(None), ("other", RECEIVE_ACTION_EXIT));
    }

    #[test]
    fn pending_request_guard_does_not_remove_replacement() {
        let pending: Arc<FastDashMap<u64, Arc<Notify>>> = Arc::new(FastDashMap::default());
        let original = Arc::new(Notify::new());
        let guard = PendingRequestGuard::register(pending.clone(), 1, original);

        let replacement = Arc::new(Notify::new());
        pending.insert(1, replacement.clone());
        drop(guard);

        let current = pending.get(&1).expect("replacement remains registered");
        assert!(
            Arc::ptr_eq(current.value(), &replacement),
            "an older guard must not remove a same-unique replacement"
        );
    }

    #[test]
    fn prepare_receive_buf_does_not_return_stale_tail_bytes() {
        let first = b"first-request-with-a-tail";
        let mut buf = BytesMut::from(&first[..]);

        let returned = buf.split_to(5);
        assert_eq!(&returned[..], b"first");
        assert!(!buf.is_empty(), "the first split leaves a stale tail");

        FuseReceiver::<TestFileSystem>::prepare_receive_buf(&mut buf, 8);
        assert_eq!(
            buf.len(),
            8,
            "the next receive gets exactly its read window"
        );

        let read_len = 3;
        buf[..read_len].copy_from_slice(b"new");
        let returned = buf.split_to(read_len);
        assert_eq!(&returned[..], b"new");
        assert_eq!(
            returned.len(),
            read_len,
            "only bytes reported by read are returned"
        );
        assert_eq!(
            buf.len(),
            8 - read_len,
            "the unused read window remains internal to the reusable buffer"
        );
    }

    // Drive the real `dispatch_meta` to prove `operation_duration_us{status}`
    // comes from the stashed `op_status` (the FS result), not the send `IOResult`.
    mod dispatch_meta_integration {
        use crate::fs::TestFileSystem;
        use crate::fuse_metrics::{
            FuseMetrics, FuseReqCtx, FuseReqKind, FuseReqLabels, FuseReqStatus,
        };
        use crate::raw::fuse_abi::{
            fuse_forget_in, fuse_fsync_in, fuse_getattr_in, fuse_in_header, fuse_interrupt_in,
            fuse_rename2_in,
        };
        use crate::session::{FuseRequest, FuseResponse, FuseTask};
        use crate::FuseUtils;
        use bytes::{BufMut, BytesMut};
        use curvine_common::conf::FuseConf;
        use orpc::common::Metrics as m;
        use orpc::sync::channel::{AsyncChannel, AsyncReceiver};
        use orpc::sync::FastDashMap;

        // Each test uses a DISTINCT opcode: they run in parallel and assert deltas
        // on the shared process-global registry, so a shared opcode would make the
        // `==before+1` checks flaky.
        const OP_LOOKUP: u32 = 1;
        const OP_FORGET: u32 = 2;
        const OP_GETATTR: u32 = 3;
        const OP_STATFS: u32 = 17;
        const OP_ACCESS: u32 = 34;
        const OP_INTERRUPT: u32 = 36;

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
            make_request(
                OP_GETATTR,
                unique,
                1,
                FuseUtils::struct_as_bytes(&fuse_getattr_in::default()),
            )
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

        // A reply with a live metrics slot, wired to a real channel. `opcode` MUST match
        // the request's: labels derive from it, so a mismatch routes to a different
        // `request_duration_us` child and silently misses the assertion.
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

        // GetAttr Ok -> operation sample under status=success, not the enqueue IOResult.
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

        // Lookup ENOENT -> status=error from the stashed op_status, even though the
        // error is delivered as a reply frame so the send (and dispatch) returns Ok.
        #[tokio::test]
        async fn lookup_error_records_operation_error() {
            let before = op_dur_count("Lookup", "error");
            let pending = FastDashMap::default();
            let (reply, _rx) = metrics_reply(1002, "Lookup");
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

        // No-reply Forget still records an operation sample (status from
        // finish_no_reply) and enqueues no reply task.
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

        // Channel closed before dispatch: the FS op (StatFs) succeeds so the
        // operation sample stays status=success, independent of the failed delivery.
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

            assert_eq!(
                op_dur_count("StatFs", "success"),
                op_before + 1,
                "op succeeded -> operation status=success even when delivery fails"
            );
            // Delivery side: enqueue failed -> request finished early status=error
            // plus a reply_enqueue_errors_total bump. Both sides asserted proves the
            // operation-success vs request-error separation.
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

        // Parse failure records NO operation sample: an empty-payload Access fails
        // parse_operator's get_struct -> finish_early, before the operation timer.
        #[tokio::test]
        async fn parse_failure_records_no_operation_sample() {
            let s_before = op_dur_count("Access", "success");
            let e_before = op_dur_count("Access", "error");
            let pending = FastDashMap::default();
            let (reply, _rx) = metrics_reply(1005, "Access");

            let truncated = make_request(OP_ACCESS, 1005, 1, &[]);
            let _ = super::super::FuseReceiver::dispatch_meta(&pending, &fs(), &truncated, &reply)
                .await;

            assert_eq!(op_dur_count("Access", "success"), s_before);
            assert_eq!(op_dur_count("Access", "error"), e_before);
        }

        const OP_SETLKW: u32 = 33;

        use crate::fs::operator::SetLkW;
        use crate::fs::FileSystem;
        use crate::raw::fuse_abi::fuse_lk_in;
        use crate::FuseResult;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        // `set_lkw` blocks forever (never acquires the lock) and records whether it
        // was polled, so the `select!` interrupt branch deterministically wins.
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

        // SETLKW interrupted AFTER `set_lkw()` is polled (asserted via `polled`)
        // still records a wait sample. The complementary "set_lkw never reached"
        // case is `malformed_setlkw_records_wait_sample_without_set_lkw` below.
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

            // Gate on `polled`, not the pending_requests entry: the entry is
            // inserted before the `select!`, so notifying on it could win before
            // `set_lkw()` is polled and break the `polled==true` assertion.
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
            // set_lkw is polled and blocked; fire the interrupt so the notify wins.
            pending2
                .get(&2001)
                .expect("pending_requests entry exists once set_lkw is polled")
                .notify_one();

            let res = handle.await.unwrap();

            // The EINTR is delivered as a reply frame (a successful enqueue), so the
            // function returns Ok rather than propagating a Rust Err.
            assert!(res.is_ok(), "interrupt reply enqueued successfully");
            // The reply is tagged Interrupted from the source, not inferred from errno.
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
            assert!(
                polled.load(Ordering::SeqCst),
                "set_lkw was polled (the dispatch branch started)"
            );
            // Core invariant: a wait sample lands even though the lock poll never completed.
            assert!(
                setlkw_wait_count() > wait_before,
                "interrupted SETLKW still records a setlkw_wait_duration_us sample"
            );
            assert!(
                pending2.get(&2001).is_none(),
                "interrupt branch removed the pending_requests entry"
            );
        }

        #[tokio::test]
        async fn aborted_blocking_setlkw_removes_pending_request() {
            let polled = Arc::new(AtomicBool::new(false));
            let fs = Arc::new(BlockingSetlkwFs {
                polled: polled.clone(),
            });
            let pending: Arc<FastDashMap<u64, Arc<tokio::sync::Notify>>> =
                Arc::new(FastDashMap::default());
            let pending2 = pending.clone();
            let (reply, _rx) = metrics_reply(2003, "SetLkW");

            let handle = tokio::spawn(async move {
                super::super::FuseReceiver::dispatch_meta_interrupt(
                    fs,
                    pending,
                    setlkw_request(2003),
                    reply,
                )
                .await
            });

            // Wait until dispatch has entered the blocking SETLKW (registration live).
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
            assert!(
                pending2.get(&2003).is_some(),
                "SETLKW is registered while its dispatch is pending"
            );

            // Abort drops the dispatch future without selecting completion or
            // interrupt, so cleanup must come from the RAII guard.
            handle.abort();
            let join_err = handle.await.expect_err("the SETLKW task was aborted");
            assert!(join_err.is_cancelled(), "join error reports cancellation");
            assert!(
                pending2.get(&2003).is_none(),
                "aborting SETLKW removes its pending_requests entry"
            );
        }

        // Justifies hoisting the timer OUT of `set_lkw()`: a malformed SETLKW fails parse
        // before `set_lkw()` runs, yet the timer (built before the `select!`) still
        // records on drop. Inside `set_lkw()` this path would record nothing.
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

            // Opcode routes through dispatch_meta_interrupt (timer created), but the
            // empty payload makes parse_operator's get_struct::<fuse_lk_in> fail.
            let malformed = make_request(OP_SETLKW, 2002, 1, &[]);
            let res =
                super::super::FuseReceiver::dispatch_meta_interrupt(fs, pending, malformed, reply)
                    .await;

            assert!(res.is_err(), "malformed SETLKW returns the parse Err");
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

        // Opcode coverage: arm-wired vs intentional wildcard.
        const OP_RENAME2: u32 = 45;
        const OP_FSYNCDIR: u32 = 30;
        const OP_DESTROY: u32 = 38;
        const OP_BMAP: u32 = 37;

        // Drive dispatch_meta and return the enqueued RequestReply's
        // (unsupported_reason, errno). Panics if no RequestReply was enqueued.
        async fn dispatch_reason_errno(
            req: &FuseRequest,
            opcode: &'static str,
        ) -> (Option<&'static str>, i32) {
            let pending = FastDashMap::default();
            let (reply, mut rx) = metrics_reply(req.get_header().unwrap().unique, opcode);
            let _ = super::super::FuseReceiver::dispatch_meta(&pending, &fs(), req, &reply).await;
            match rx.try_recv().unwrap().expect("a RequestReply task") {
                FuseTask::RequestReply {
                    unsupported_reason,
                    errno,
                    ..
                } => (unsupported_reason, errno),
                _ => panic!("expected a RequestReply task"),
            }
        }

        // RENAME2 with flags==0 reaches the real arm, asserted via reason==None
        // (wired, not half-wired via the wildcard). errno is the trait ENOSYS.
        #[tokio::test]
        async fn rename2_flagless_reaches_dispatch_arm() {
            let arg = fuse_rename2_in {
                newdir: 2,
                flags: 0,
                padding: 0,
            };
            let mut payload = Vec::from(FuseUtils::struct_as_bytes(&arg));
            payload.extend_from_slice(b"old\0new\0");
            let req = make_request(OP_RENAME2, 3001, 1, &payload);
            let (reason, _errno) = dispatch_reason_errno(&req, "Rename2").await;
            assert_eq!(
                reason, None,
                "RENAME2 must reach its dispatch arm, not the wildcard"
            );
        }

        // FSYNCDIR is wired to the no-op fsync_dir default -> empty success reply.
        #[tokio::test]
        async fn fsyncdir_reaches_dispatch_arm_and_succeeds() {
            let req = make_request(
                OP_FSYNCDIR,
                3002,
                1,
                FuseUtils::struct_as_bytes(&fuse_fsync_in::default()),
            );
            let (reason, errno) = dispatch_reason_errno(&req, "FsyncDir").await;
            assert_eq!(reason, None, "FSYNCDIR must reach its dispatch arm");
            assert_eq!(errno, 0, "fsync_dir default is a no-op success");
        }

        // DESTROY is reply-expecting (send_rep): the no-op default acks with an
        // empty success reply.
        #[tokio::test]
        async fn destroy_reaches_dispatch_arm_and_succeeds() {
            let req = make_request(OP_DESTROY, 3003, 0, &[]);
            let (reason, errno) = dispatch_reason_errno(&req, "Destroy").await;
            assert_eq!(reason, None, "DESTROY must reach its dispatch arm");
            assert_eq!(errno, 0, "destroy default acks with an empty success reply");
        }

        // BMAP has no arm on purpose -> wildcard, tagged unimplemented_opcode.
        #[tokio::test]
        async fn bmap_falls_through_wildcard_as_unimplemented() {
            let req = make_request(OP_BMAP, 3004, 1, &[]);
            let (reason, errno) = dispatch_reason_errno(&req, "BMap").await;
            assert_eq!(
                reason,
                Some("unimplemented_opcode"),
                "BMAP is intentionally unsupported -> wildcard"
            );
            assert_eq!(errno, libc::ENOSYS);
        }
    }

    // Drive the real `send_stream_dispatch` with only `&fs` + a pre-built `FuseResponse`.
    // `TestFileSystem` returns ENOSYS without replying, so each op exercises the
    // pre-dispatch-error path: the error reply is enqueued INSIDE the RAII scope under test.
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
            // size=0: parses fine, and fs.write returns ENOSYS at dispatch (pre-task),
            // driving the io_dispatch path without a real writer task.
            make_request(
                OP_WRITE,
                unique,
                FuseUtils::struct_as_bytes(&fuse_write_in::default()),
            )
        }

        // Drive ONE stream op through `send_stream_dispatch`, FD-free (a real `FuseReceiver`
        // drags in a `Pipe2` whose Drop aborts with `IO Safety violation`). Assertions use
        // LOWER BOUNDS (io_type labels are process-global). `with_metrics_ctx` toggles the gate.
        fn dispatch_one(with_metrics_ctx: bool, req: FuseRequest) {
            use crate::fuse_metrics::{FuseReqCtx, FuseReqKind, FuseReqLabels};
            FuseMetrics::ensure_init().unwrap();
            let rt = AsyncRuntime::single();
            rt.block_on(async {
                let fs = Arc::new(TestFileSystem::new(
                    curvine_common::conf::FuseConf::default(),
                ));
                // Drainer so an enqueued error reply never blocks.
                let (tx, mut rx) = AsyncChannel::new(64).split();
                let drainer = tokio::spawn(async move { while rx.recv().await.is_some() {} });

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

        // A flush whose backend errors (pre-dispatch ENOSYS) still records both a
        // lifecycle attempt (counted before the match) and a duration sample (RAII
        // timer fires on drop, covering the in-scope error reply enqueue).
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

        // fsync lands under io_type=fsync, not flush: send_stream distinguishes them
        // by opcode, an ambiguity the writer task body can't — hence attribution here.
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

        // Release attributed at send_stream (one operator arm = one increment),
        // avoiding the reader+writer double-count a task-body approach would incur.
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

        // read/write record io_dispatch_duration_us{io_type} at send_stream. Backend
        // io_* lives in the task body, so a pre-dispatch ENOSYS records dispatch only.
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

        // SMOKE test: each stream family runs end-to-end with metrics off. Does NOT
        // prove "emits nothing" (shared io_type labels make `== before` flaky) — that
        // guarantee lives in the `*_gate` unit tests.
        #[test]
        fn disabled_send_stream_runs_clean_for_all_families() {
            dispatch_one(false, flush_request(7101));
            dispatch_one(false, fsync_request(7102));
            dispatch_one(false, release_request(7103));
            dispatch_one(false, read_request(7104));
            dispatch_one(false, write_request(7105));
        }

        // A malformed stream request whose `parse_operator()` fails AFTER the ctx was
        // built must finish the ctx early (drop guard, record decode error) and NOT enter
        // the RAII scope. Pins that no early return/await sits between ctx and scope.
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

            // "No io_dispatch/lifecycle sample on parse failure" is STRUCTURAL: the RAII
            // scope exists only after `parse_operator()` succeeds, so an Err returns via
            // `finish_early` first. Only the decode error shows here (shared → lower bound).
            assert!(
                mx.decode_errors_total
                    .with_label_values(&[DECODE_PHASE_PARSE, "other"])
                    .get()
                    > decode_before,
                "parse failure records decode_errors_total{{phase=parse}}"
            );
        }
    }

    // When a stream op fails BEFORE replying (pre-dispatch error), `send_stream_dispatch`
    // enqueues EXACTLY ONE error reply via its `if res.is_err()` fallback, never
    // double-replies. The per-test reply channel makes the `== 1` counts deterministic.
    mod stream_error_coverage {
        use crate::err_fuse;
        use crate::fs::operator::{FSync, Flush, Read, Release, Write};
        use crate::fs::FileSystem;
        use crate::fuse_metrics::{
            ActiveGuard, FuseMetrics, FuseReqCtx, FuseReqKind, FuseReqLabels,
        };
        use crate::raw::fuse_abi::{
            fuse_flush_in, fuse_fsync_in, fuse_in_header, fuse_read_in, fuse_release_in,
            fuse_write_in,
        };
        use crate::session::{FuseRequest, FuseResponse, FuseTask};
        use crate::{FuseResult, FuseUtils};
        use bytes::{BufMut, BytesMut};
        use orpc::runtime::{AsyncRuntime, RpcRuntime};
        use orpc::sync::channel::AsyncChannel;
        use std::sync::Arc;

        const OP_READ: u32 = 15;
        const OP_WRITE: u32 = 16;
        const OP_RELEASE: u32 = 18;
        const OP_FSYNC: u32 = 20;
        const OP_FLUSH: u32 = 25;

        // The real pre-dispatch errno: `node_state.rs` `find_handle` returns EBADF
        // (not EIO) on a handle-lookup miss.
        const ERRNO: i32 = libc::EBADF;

        // Stream ops fail WITHOUT touching `_reply` — the shape of a pre-dispatch
        // error. A dedicated mock lets the assertions pin EBADF on the wire.
        struct PreDispatchErrFs;
        impl FileSystem for PreDispatchErrFs {
            async fn read(&self, _op: Read<'_>, _reply: FuseResponse) -> FuseResult<()> {
                err_fuse!(ERRNO, "read: no handle")
            }
            async fn write(&self, _op: Write<'_>, _reply: FuseResponse) -> FuseResult<()> {
                err_fuse!(ERRNO, "write: no handle")
            }
            async fn flush(&self, _op: Flush<'_>, _reply: FuseResponse) -> FuseResult<()> {
                err_fuse!(ERRNO, "flush: no handle")
            }
            async fn release(&self, _op: Release<'_>, _reply: FuseResponse) -> FuseResult<()> {
                err_fuse!(ERRNO, "release: no handle")
            }
            async fn fsync(&self, _op: FSync<'_>, _reply: FuseResponse) -> FuseResult<()> {
                err_fuse!(ERRNO, "fsync: no handle")
            }
        }

        // `read` REPLIES and THEN returns `Err`, proving the `if res.is_err()` fallback
        // cannot double-reply end-to-end. `cfg(not(debug_assertions))` matches its only
        // (release-only) user, so the mock is not "never constructed" in debug.
        #[cfg(not(debug_assertions))]
        struct ReplyThenErrFs;
        #[cfg(not(debug_assertions))]
        impl FileSystem for ReplyThenErrFs {
            async fn read(&self, _op: Read<'_>, reply: FuseResponse) -> FuseResult<()> {
                reply.send_rep::<(), crate::FuseError>(Ok(())).await?;
                err_fuse!(ERRNO, "late error after a successful reply")
            }
        }

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

        fn read_request(unique: u64) -> FuseRequest {
            make_request(
                OP_READ,
                unique,
                FuseUtils::struct_as_bytes(&fuse_read_in::default()),
            )
        }
        fn write_request(unique: u64) -> FuseRequest {
            make_request(
                OP_WRITE,
                unique,
                FuseUtils::struct_as_bytes(&fuse_write_in::default()),
            )
        }
        fn flush_request(unique: u64) -> FuseRequest {
            make_request(
                OP_FLUSH,
                unique,
                FuseUtils::struct_as_bytes(&fuse_flush_in::default()),
            )
        }
        fn release_request(unique: u64) -> FuseRequest {
            make_request(
                OP_RELEASE,
                unique,
                FuseUtils::struct_as_bytes(&fuse_release_in::default()),
            )
        }
        fn fsync_request(unique: u64) -> FuseRequest {
            make_request(
                OP_FSYNC,
                unique,
                FuseUtils::struct_as_bytes(&fuse_fsync_in::default()),
            )
        }

        // Drive one stream op and return `(result, every enqueued FuseTask, the shared
        // reply handle)`. FD-free. Single-thread runtime fully awaits, so all tasks are
        // buffered in `rx` on return — drained synchronously, so a closed channel can't panic.
        fn dispatch_and_collect<F: FileSystem>(
            fs: Arc<F>,
            with_metrics_ctx: bool,
            req: FuseRequest,
        ) -> (FuseResult<()>, Vec<FuseTask>, FuseResponse) {
            FuseMetrics::ensure_init().unwrap();
            let rt = AsyncRuntime::single();
            rt.block_on(async {
                let (tx, mut rx) = AsyncChannel::new(64).split();
                let opcode = req.opcode().as_str();
                let ctx = if with_metrics_ctx {
                    let gauge = orpc::common::Metrics::new_gauge(
                        format!("sec_active_{}", req.unique()),
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
                // Clone shares the same `Arc<Mutex<..>>` slot (like the internal
                // `err_rep`), so we can assert finished/active after dispatch.
                let observer = rep.clone();

                let res =
                    super::super::FuseReceiver::<F>::send_stream_dispatch(&fs, req, rep).await;

                let mut tasks = Vec::new();
                while let Ok(Some(t)) = rx.try_recv() {
                    tasks.push(t);
                }
                (res, tasks, observer)
            })
        }

        // The single enqueued task is a `RequestReply { status: Error, errno }`.
        fn assert_single_error_reply(tasks: &[FuseTask]) {
            use crate::fuse_metrics::FuseReqStatus;
            assert_eq!(
                tasks.len(),
                1,
                "pre-dispatch error enqueues exactly one error reply"
            );
            match &tasks[0] {
                FuseTask::RequestReply { status, errno, .. } => {
                    assert_eq!(*status, FuseReqStatus::Error, "error frame, not success");
                    assert_eq!(*errno, ERRNO, "error reply carries the pre-dispatch errno");
                }
                FuseTask::NotifyReply { .. } => panic!("expected RequestReply, got NotifyReply"),
                FuseTask::Reply(_) => panic!("expected RequestReply, got legacy Reply"),
            }
        }

        // The shared slot finished exactly once and its active guard was taken (moved
        // onto the reply task) — no double-finish, no guard leak.
        fn assert_finished_once(observer: &FuseResponse) {
            let slot = observer.metrics.as_ref().unwrap().lock();
            assert!(slot.finished, "shared slot finished exactly once");
            assert!(
                slot.active.is_none(),
                "active guard was taken (moved onto the reply task), not leaked"
            );
        }

        // One test per stream family: dispatch returns Ok (error delivered as a reply
        // frame, not a Rust Err), exactly one EBADF error reply is enqueued, and the
        // shared slot finished exactly once.
        macro_rules! pre_dispatch_error_test {
            ($name:ident, $req:ident, $unique:expr) => {
                #[test]
                fn $name() {
                    let fs = Arc::new(PreDispatchErrFs);
                    let (res, tasks, observer) = dispatch_and_collect(fs, true, $req($unique));
                    assert!(
                        res.is_ok(),
                        "pre-dispatch error is delivered as a reply frame; dispatch returns Ok"
                    );
                    assert_single_error_reply(&tasks);
                    assert_finished_once(&observer);
                }
            };
        }

        pre_dispatch_error_test!(read_pre_dispatch_error_replies_once, read_request, 9001);
        pre_dispatch_error_test!(write_pre_dispatch_error_replies_once, write_request, 9002);
        pre_dispatch_error_test!(flush_pre_dispatch_error_replies_once, flush_request, 9003);
        pre_dispatch_error_test!(
            release_pre_dispatch_error_replies_once,
            release_request,
            9004
        );
        pre_dispatch_error_test!(fsync_pre_dispatch_error_replies_once, fsync_request, 9005);

        // The metrics-disabled path (no ctx -> metrics off) enqueues the same single
        // error reply, as the legacy `FuseTask::Reply` variant.
        #[test]
        fn disabled_pre_dispatch_error_replies_once() {
            let fs = Arc::new(PreDispatchErrFs);
            let (res, tasks, _observer) = dispatch_and_collect(fs, false, read_request(9006));
            assert!(
                res.is_ok(),
                "disabled path also delivers the error as a reply"
            );
            assert_eq!(
                tasks.len(),
                1,
                "exactly one error reply on the disabled path"
            );
            // Legacy Reply variant AND the same errno on the wire. The FUSE error frame
            // encodes the errno negated (header.error = -errno), so a pre-dispatch EBADF
            // surfaces as `-EBADF` — the enabled path's check, read off the raw frame.
            match &tasks[0] {
                FuseTask::Reply(d) => assert_eq!(
                    d.header().error,
                    -ERRNO,
                    "disabled path propagates the pre-dispatch errno on the wire"
                ),
                FuseTask::RequestReply { .. } => {
                    panic!("disabled path must use the legacy Reply variant, got RequestReply")
                }
                FuseTask::NotifyReply { .. } => {
                    panic!("disabled path must use the legacy Reply variant, got NotifyReply")
                }
            }
        }

        // End-to-end double-reply guard: an op that BOTH replies AND returns Err makes
        // the `err_rep` fallback a no-op, so the kernel gets one reply. Release-only: a
        // genuine double reply trips `commit_reply_task`'s `debug_assert!(!finished)`.
        #[test]
        #[cfg(not(debug_assertions))]
        fn err_rep_fallback_after_a_reply_is_noop() {
            let fs = Arc::new(ReplyThenErrFs);
            let (res, tasks, observer) = dispatch_and_collect(fs, true, read_request(9007));
            // The op already answered, so dispatch returns Ok even though the op body
            // returned Err (the fallback's send_rep on the finished slot is a no-op).
            assert!(
                res.is_ok(),
                "reply-then-error: the op already answered; dispatch returns Ok"
            );
            // Exactly one task on the wire — the op's success reply, no error second.
            assert_eq!(
                tasks.len(),
                1,
                "reply-then-error enqueues exactly one reply; the err_rep fallback is a no-op"
            );
            assert!(
                matches!(tasks[0], FuseTask::RequestReply { .. }),
                "the single reply is the op's own success reply"
            );
            // Shared slot finished exactly once (the fallback did not double-finish).
            assert_finished_once(&observer);
        }
    }
}
