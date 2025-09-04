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

#![allow(unused_variables, unused)]

use crate::fs::operator::FuseOperator;
use crate::fs::FileSystem;
use crate::raw::fuse_abi::*;
use crate::session::channel::{FuseChannel, FuseReceiver, FuseSender};
use crate::session::FuseRequest;
use crate::session::{FuseMnt, FuseResponse};
use crate::{err_fuse, FuseResult};
use curvine_common::conf::FuseConf;
use curvine_common::version::GIT_VERSION;
use libc::{EAGAIN, EINTR, ENODEV, ENOENT};
use log::{error, info, warn};
use orpc::io::IOResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::CommonResult;
use std::sync::Arc;

pub struct FuseSession<T> {
    rt: Arc<Runtime>,
    fs: Arc<T>,
    mnts: Vec<FuseMnt>,
    channels: Vec<FuseChannel>,
}

impl<T: FileSystem> FuseSession<T> {
    pub async fn new(rt: Arc<Runtime>, fs: T, conf: FuseConf) -> IOResult<Self> {
        let all_mnt_paths = conf.get_all_mnt_path()?;

        // Analyze the mount parameters.
        let fuse_opts = conf.parse_fuse_opts();
        let arg_ptrs = FuseConf::convert_fuse_args(&fuse_opts);
        let fuse_args = fuse_args {
            argc: arg_ptrs.len() as i32,
            argv: arg_ptrs.as_ptr(),
            allocated: 0,
        };

        // Create all mount points.
        let mut mnts = vec![];
        let auto_unmount = conf.auto_umount();
        for path in all_mnt_paths {
            mnts.push(FuseMnt::new(&path, &fuse_args, auto_unmount));
        }

        let mut channels = vec![];
        for mnt in &mut mnts {
            for _ in 0..conf.mnt_per_task {
                let fd_pair = mnt.create_async_task_fd(conf.clone_fd)?;
                let channel = FuseChannel::new(fd_pair, &conf)?;
                channels.push(channel);
            }
        }

        info!(
            "Create fuse session, git version: {}, mnt number: {}, loop task number: {},\
         io threads: {}, worker threads: {}, fuse channel size: {}, stream channel size: {}, fuse opts: {:?}",
            GIT_VERSION,
            conf.mnt_number,
            channels.len(),
            rt.io_threads(),
            rt.worker_threads(),
            conf.fuse_channel_size,
            conf.stream_channel_size,
            fuse_opts
        );

        let session = Self {
            rt,
            fs: Arc::new(fs),
            mnts,
            channels,
        };
        Ok(session)
    }

    pub async fn run(&mut self) -> CommonResult<()> {
        let ctrl_c = tokio::signal::ctrl_c();
        let channels = std::mem::take(&mut self.channels);
        let _mnts = std::mem::take(&mut self.mnts);

        #[cfg(target_os = "linux")]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut unix_sig = signal(SignalKind::terminate()).unwrap();

            tokio::select! {
                res = Self::run_all(self.rt.clone(), self.fs.clone(), channels) => {
                    if let Err(err) = res {
                        error!("fatal error, cause = {:?}", err);
                    }
                }

                _ = ctrl_c => {
                    info!("Receive ctrl_c signal, shutting down fuse");
                }

                _ = unix_sig.recv()  => {
                      info!("Received SIGTERM, shutting down fuse gracefully...");
                }
            }
        }

        self.fs.unmount();
        Ok(())
    }

    async fn run_all(rt: Arc<Runtime>, fs: Arc<T>, channels: Vec<FuseChannel>) -> CommonResult<()> {
        let mut handles = vec![];
        for channel in channels {
            let receiver_future =
                Self::fuse_receiver_future(rt.clone(), channel.receiver, fs.clone());
            let sender_future = Self::fuse_sender_future(channel.sender);
            let handle = rt.spawn(async move {
                if let Err(err) = receiver_future.await {
                    error!("failed to accept, cause = {:?}", err);
                }
            });
            handles.push(handle);

            rt.spawn(async move {
                if let Err(err) = sender_future.await {
                    error!("failed to send, cause = {:?}", err);
                }
            });
        }

        // Accepting any value is considered to require service cessation.
        for handle in handles {
            handle.await?;
        }

        Ok(())
    }

    // Write fuse message.
    async fn fuse_sender_future(mut sender: FuseSender) -> FuseResult<()> {
        while let Some(reply) = sender.recv().await {
            let id = reply.header.unique;
            if let Err(e) = sender.send(reply).await {
                warn!("error send: {} {}", e, id);
            }
        }
        Ok(())
    }

    // Read fuse messages and distribute them.
    async fn fuse_receiver_future(
        rt: Arc<Runtime>,
        mut receiver: FuseReceiver,
        fs: Arc<T>,
    ) -> FuseResult<()> {
        loop {
            match receiver.receive().await {
                Ok(buf) => {
                    // The parsing request failed, fuse may have been abnormal, and the program should be terminated.
                    let req = FuseRequest::from_bytes(buf.freeze())?;
                    let reply = receiver.new_reply(req.unique());
                    Self::dispatch(&rt, fs.clone(), req, reply).await;
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

        Ok(())
    }

    async fn dispatch(rt: &Runtime, fs: Arc<T>, req: FuseRequest, reply: FuseResponse) {
        let unique = req.unique();
        if reply.debug {
            info!(
                "receive unique: {}, code: {:?}, op: {:?}",
                unique,
                req.opcode(),
                req.parse_operator().unwrap()
            );
        }

        if req.is_stream() {
            // To avoid the scheduling overhead caused by creating a large number of futures when reading and writing data, the read and write data request will send the request to the queue.
            if let Err(e) = Self::dispatch_data(fs, req, reply).await {
                error!("Error dispatch request {}: {}", unique, e);
            }
        } else {
            // Metadata operation, start a future execution
            rt.spawn(async move {
                if let Err(e) = Self::dispatch_meta(fs, req, reply).await {
                    error!("Error dispatch request {}: {}", unique, e);
                };
            });
        }
    }

    async fn dispatch_data(fs: Arc<T>, req: FuseRequest, reply: FuseResponse) -> FuseResult<()> {
        let operator = req.parse_operator()?;
        let err_reply = reply.clone();
        let res: FuseResult<()> = match operator {
            FuseOperator::Write(op) => fs.write(op, reply).await,
            FuseOperator::Read(op) => fs.read(op, reply).await,
            _ => {
                err_fuse!(libc::ENOSYS, "Unsupported operation {:?}", req.opcode())
            }
        };

        if res.is_err() {
            err_reply.send_rep(res).await?;
        }
        Ok(())
    }

    async fn dispatch_meta(fs: Arc<T>, req: FuseRequest, reply: FuseResponse) -> FuseResult<()> {
        let operator = req.parse_operator()?;
        let res = match operator {
            FuseOperator::Init(op) => reply.send_rep(fs.init(op).await).await,

            FuseOperator::StatFs(op) => reply.send_rep(fs.stat_fs(op).await).await,

            FuseOperator::Access(op) => {
                fs.access(op).await?;
                reply.send_empty().await
            }

            FuseOperator::Lookup(op) => reply.send_rep(fs.lookup(op).await).await,

            FuseOperator::GetAttr(op) => reply.send_rep(fs.get_attr(op).await).await,

            FuseOperator::SetAttr(op) => reply.send_rep(fs.set_attr(op).await).await,

            FuseOperator::GetXAttr(op) => reply.send_buf(fs.get_xattr(op).await).await,

            FuseOperator::SetXAttr(op) => {
                fs.set_xattr(op).await?;
                reply.send_empty().await
            }

            FuseOperator::RemoveXAttr(op) => {
                fs.remove_xattr(op).await?;
                reply.send_empty().await
            }

            FuseOperator::ListXAttr(op) => reply.send_buf(fs.list_xattr(op).await).await,

            FuseOperator::OpenDir(op) => reply.send_rep(fs.open_dir(op).await).await,

            FuseOperator::Mkdir(op) => reply.send_rep(fs.mkdir(op).await).await,

            FuseOperator::FAllocate(op) => {
                fs.fuse_allocate(op).await?;
                reply.send_empty().await
            }

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

            FuseOperator::Flush(op) => reply.send_rep(fs.flush(op).await).await,

            FuseOperator::Release(op) => reply.send_rep(fs.release(op).await).await,

            FuseOperator::MkNod(op) => reply.send_rep(fs.mk_nod(op).await).await,

            FuseOperator::Create(op) => reply.send_rep(fs.create(op).await).await,

            FuseOperator::Unlink(op) => reply.send_rep(fs.unlink(op).await).await,

            FuseOperator::RmDir(op) => reply.send_rep(fs.rm_dir(op).await).await,

            FuseOperator::Link(op) => reply.send_rep(fs.link(op).await).await,

            FuseOperator::BatchForget(op) => {
                fs.batch_forget(op).await?;
                Ok(0)
            }

            FuseOperator::Rename(op) => reply.send_rep(fs.rename(op).await).await,

            FuseOperator::Interrupt(op) => {
                fs.interrupt(op).await?;
                Ok(0)
            }

            FuseOperator::FSync(op) => reply.send_rep(fs.fsync(op).await).await,

            FuseOperator::Symlink(op) => reply.send_rep(fs.symlink(op).await).await,

            FuseOperator::Readlink(op) => reply.send_buf(fs.readlink(op).await).await,

            _ => {
                let err: FuseResult<fuse_out_header> =
                    err_fuse!(libc::ENOSYS, "Unsupported operation {:?}", req.opcode());
                reply.send_rep(err).await
            }
        };

        res?;
        Ok(())
    }
}
