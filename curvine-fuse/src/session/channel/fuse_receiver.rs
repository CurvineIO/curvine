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
use crate::fuse_metrics::{ActiveGuard, FuseReqCtx, FuseReqKind, FuseReqLabels};
use crate::raw::fuse_abi::fuse_out_header;
use crate::session::{FuseRequest, FuseResponse, FuseTask};
use crate::{err_fuse, FuseResult, FUSE_IN_HEADER_LEN};
use libc::{EAGAIN, EINTR, ENODEV, ENOENT};
use log::{debug, error, info};
use orpc::io::IOResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::AsyncSender;
use orpc::sync::FastDashMap;
use orpc::sys::pipe::{AsyncFd, Pipe2, PipeFd};
use orpc::{err_box, sys};
use std::sync::Arc;
use tokio::sync::{watch, Notify};
use tokio_util::bytes::BytesMut;

/// FuseReceiver provides the following functionality:
/// 1. Receive data from fuse fd using splice
/// 2. For metadata requests (mkdir, ls), spawn a task to execute
/// 3. For file read/write requests, send task to queue
pub struct FuseReceiver<T> {
    kernel_fd: Arc<AsyncFd>,
    fs: Arc<T>,
    rt: Arc<Runtime>,
    sender: AsyncSender<FuseTask>,
    pipe2: Pipe2,
    buf: BytesMut,
    fuse_len: usize,
    debug: bool,
    audit_logging_enabled: bool,
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
        pending_requests: Arc<FastDashMap<u64, Arc<Notify>>>,
    ) -> IOResult<Self> {
        let pipe2 = Pipe2::new(PipeFd::new(buf_size, false, false)?)?;
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
            pending_requests,
        };

        Ok(client)
    }

    // Read a data from fuse.
    pub async fn receive(&mut self) -> IOResult<BytesMut> {
        self.splice().await
    }

    // Use libc::read to read data, test it, and there are multiple memory copies.
    pub async fn read(&mut self) -> IOResult<BytesMut> {
        let len = self
            .kernel_fd
            .async_read(|fd| sys::read(fd.fd(), &mut self.buf))
            .await
            .unwrap();
        Ok(BytesMut::from(&self.buf[..len as usize]))
    }

    pub async fn splice(&mut self) -> IOResult<BytesMut> {
        let write_len = self
            .pipe2
            .write_io(&self.kernel_fd, None, self.fuse_len)
            .await
            .unwrap();

        self.buf.reserve(write_len);
        unsafe {
            self.buf.set_len(write_len);
        }

        let read_len = self.pipe2.read_buf(&mut self.buf[..write_len]).await?;
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

        let req_buf = self.buf.split_to(read_len);
        Ok(req_buf)
    }

    /// Build a reply handle for `unique`. When `labels` is `Some`, a metrics
    /// context (with a no-op `ActiveGuard` in Phase 1a-1) is created so the
    /// reply finishes in the sender; when `None`, the legacy disabled path.
    pub(crate) fn new_reply(&self, unique: u64, labels: Option<FuseReqLabels>) -> FuseResponse {
        let ctx = labels.map(|labels| FuseReqCtx {
            labels,
            active: Some(ActiveGuard::noop()),
        });
        FuseResponse::new_reply(unique, self.sender.clone(), self.debug, ctx)
    }

    /// Derive the copyable metrics labels for a decoded request. Phase 1a-1
    /// always builds these (metrics machinery is exercised but emits nothing);
    /// the disabled-mode `None` path arrives with the Phase 1 kill switch.
    fn req_labels(req: &FuseRequest) -> FuseReqLabels {
        let kind = if req.is_stream() {
            FuseReqKind::Stream
        } else {
            FuseReqKind::Metadata
        };
        let request_bytes = req.get_header().map(|h| h.len).unwrap_or(0);
        FuseReqLabels::new(req.opcode().as_str(), kind, request_bytes)
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
        // event, consistent with the metadata path.
        let labels = Self::req_labels(&req);
        let rep = self.new_reply(req.unique(), Some(labels));

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
        let res = match operator {
            FuseOperator::Read(op) => self.fs.read(op, rep).await,

            FuseOperator::Write(op) => self.fs.write(op, rep).await,

            FuseOperator::Flush(op) => self.fs.flush(op, rep).await,

            FuseOperator::Release(op) => self.fs.release(op, rep).await,

            FuseOperator::FSync(op) => self.fs.fsync(op, rep).await,

            _ => err_fuse!(libc::ENOSYS, "unsupported operation {:?}", req.opcode()),
        };

        if res.is_err() {
            err_rep.send_rep(res).await?;
        }
        Ok(())
    }

    pub async fn start(mut self, mut shutdown_rx: watch::Receiver<bool>) -> FuseResult<()> {
        debug!("fuse receiver started");
        loop {
            tokio::select! {
                res = self.receive() => {
                    match res {
                        Ok(buf) => {
                            let req = FuseRequest::from_bytes(buf.freeze())?;

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

                            if req.is_stream() {
                                if let Err(e) = self.send_stream(req).await {
                                    error!("failed to dispatch stream request: {}", e);
                                }
                            } else {
                                self.audit(&req);

                                let labels = Self::req_labels(&req);
                                let reply = self.new_reply(req.unique(), Some(labels));
                                let fs = self.fs.clone();
                                let pending_requests = self.pending_requests.clone();
                                self.rt.spawn(async move {
                                    if let Err(e) = Self::dispatch_meta_interrupt(fs, pending_requests, req, reply).await {
                                        error!("failed to dispatch meta request: {}", e);
                                    }
                                });
                            }
                        }

                        Err(e) => match e.raw_error().raw_os_error() {
                            Some(ENOENT) => continue,
                            Some(EINTR) => continue,
                            Some(EAGAIN) => continue,
                            Some(ENODEV) => break,
                            _ => return Err(e.into()),
                        },
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

        let res = tokio::select! {
            result = Self::dispatch_meta(&pending_requests, &fs, &req, &reply) => {
                pending_requests.remove(&req.unique());
                result
            }

            _ = notify.notified() => {
                pending_requests.remove(&req.unique());
                let err: FuseResult<()> = err_fuse!(EINTR, "operation interrupted");
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
                reply.send_rep(res).await
            }

            FuseOperator::Symlink(op) => reply.send_rep(fs.symlink(op).await).await,

            FuseOperator::Readlink(op) => reply.send_buf(fs.readlink(op).await).await,

            FuseOperator::GetLk(op) => reply.send_rep(fs.get_lk(op).await).await,

            FuseOperator::SetLk(op) => reply.send_rep(fs.set_lk(op).await).await,

            FuseOperator::SetLkW(op) => reply.send_rep(fs.set_lkw(op).await).await,

            _ => {
                // A parsed-but-unhandled opcode (e.g. Rename2, or any
                // `Notimplemented`): source-tagged as `unimplemented_opcode` so
                // it classifies as Unsupported, not a backend Error. (Phase 1a-2
                // reads the tag for `unsupported_total{reason}`.)
                let err: FuseResult<fuse_out_header> =
                    err_fuse!(libc::ENOSYS, "unsupported operation {:?}", req.opcode());
                reply
                    .send_rep_tagged(err, Some("unimplemented_opcode"), false)
                    .await
            }
        };

        res?;
        Ok(())
    }
}
