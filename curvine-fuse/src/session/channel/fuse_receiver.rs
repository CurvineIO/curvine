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
use crate::raw::fuse_abi::fuse_out_header;
use crate::session::{FuseRequest, FuseResponse, FuseTask};
use crate::{err_fuse, FuseResult, FUSE_IN_HEADER_LEN};
use libc::{EAGAIN, EINTR, ENODEV, ENOENT};
use log::{debug, error, info};
use orpc::io::IOResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::AsyncSender;
use orpc::sys::pipe::{AsyncFd, Pipe2, PipeFd};
use orpc::{err_box, sys};
use std::sync::Arc;
use tokio::sync::watch;
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
}

impl<T: FileSystem> FuseReceiver<T> {
    pub fn new(
        fs: Arc<T>,
        rt: Arc<Runtime>,
        kernel_fd: Arc<AsyncFd>,
        sender: AsyncSender<FuseTask>,
        buf_size: usize,
        debug: bool,
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

    pub fn new_replay(&self, unique: u64) -> FuseResponse {
        FuseResponse::new(unique, self.sender.clone(), self.debug)
    }

    pub async fn send_stream(&self, req: FuseRequest) -> FuseResult<()> {
        let operator = req.parse_operator()?;
        let rep = self.new_replay(req.unique());
        let res = match operator {
            FuseOperator::Read(op) => self.fs.read(op, rep).await,

            FuseOperator::Write(op) => self.fs.write(op, rep).await,

            FuseOperator::Flush(op) => self.fs.flush(op, rep).await,

            FuseOperator::Release(op) => self.fs.release(op, rep).await,

            FuseOperator::FSync(op) => self.fs.fsync(op, rep).await,

            _ => err_fuse!(libc::ENOSYS, "unsupported operation {:?}", req.opcode()),
        };

        if res.is_err() {
            self.new_replay(req.unique()).send_rep(res).await?;
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
                                let operator = req.parse_operator()?;
                                info!(
                                    "receive unique: {}, code: {:?}, op: {:?}",
                                    req.unique(),
                                    req.opcode(),
                                    operator
                                );
                            }

                            if req.is_stream() {
                                if let Err(e) = self.send_stream(req).await {
                                    error!("failed to dispatch stream request: {}", e);
                                }
                            } else {
                                let reply = self.new_replay(req.unique());
                                let fs = self.fs.clone();

                                self.rt.spawn(async move {
                                    if let Err(e) = Self::dispatch_meta(fs, req, reply).await {
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

    async fn dispatch_meta(fs: Arc<T>, req: FuseRequest, reply: FuseResponse) -> FuseResult<()> {
        let operator = req.parse_operator()?;

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

            FuseOperator::Open(op) => reply.send_rep(fs.open(op, &reply).await).await,

            FuseOperator::MkNod(op) => reply.send_rep(fs.mk_nod(op).await).await,

            FuseOperator::Create(op) => reply.send_rep(fs.create(op).await).await,

            FuseOperator::Unlink(op) => reply.send_rep(fs.unlink(op).await).await,

            FuseOperator::RmDir(op) => reply.send_rep(fs.rm_dir(op).await).await,

            FuseOperator::Link(op) => reply.send_rep(fs.link(op).await).await,

            FuseOperator::BatchForget(op) => reply.send_none(fs.batch_forget(op).await),

            FuseOperator::Rename(op) => reply.send_rep(fs.rename(op).await).await,

            FuseOperator::Interrupt(op) => reply.send_rep(fs.interrupt(op).await).await,

            FuseOperator::Symlink(op) => reply.send_rep(fs.symlink(op).await).await,

            FuseOperator::Readlink(op) => reply.send_buf(fs.readlink(op).await).await,

            _ => {
                let err: FuseResult<fuse_out_header> =
                    err_fuse!(libc::ENOSYS, "unsupported operation {:?}", req.opcode());
                reply.send_rep(err).await
            }
        };

        res?;
        Ok(())
    }
}
