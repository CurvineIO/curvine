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
    channels: Vec<FuseChannel<T>>,
}

impl<T: FileSystem> FuseSession<T> {
    pub async fn new(rt: Arc<Runtime>, fs: T, conf: FuseConf) -> FuseResult<Self> {
        let all_mnt_paths = conf.get_all_mnt_path()?;

        // Analyze the mount parameters.
        let fuse_opts = conf.parse_fuse_opts();

        // Create all mount points.
        let mut mnts = vec![];
        for path in all_mnt_paths {
            mnts.push(FuseMnt::new(path, &conf));
        }

        let fs = Arc::new(fs);
        let mut channels = vec![];
        for mnt in &mut mnts {
            let channel = FuseChannel::new(fs.clone(), rt.clone(), mnt, &conf)?;
            channels.push(channel);
        }

        info!(
            "Create fuse session, git version: {}, mnt number: {}, loop task number: {},\
         io threads: {}, worker threads: {}, fuse channel size: {}, fuse opts: {:?}",
            GIT_VERSION,
            conf.mnt_number,
            channels.len(),
            rt.io_threads(),
            rt.worker_threads(),
            conf.fuse_channel_size,
            fuse_opts
        );

        let session = Self {
            rt,
            fs,
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

    async fn run_all(
        rt: Arc<Runtime>,
        fs: Arc<T>,
        channels: Vec<FuseChannel<T>>,
    ) -> CommonResult<()> {
        let mut handles = vec![];

        for channel in channels {
            for receiver in channel.receivers {
                let handle = rt.spawn(async move {
                    if let Err(err) = receiver.start().await {
                        error!("failed to accept, cause = {:?}", err);
                    }
                });
                handles.push(handle);
            }

            for sender in channel.senders {
                let handle = rt.spawn(async move {
                    if let Err(err) = sender.start().await {
                        error!("failed to send, cause = {:?}", err);
                    }
                });
                handles.push(handle);
            }
        }

        // Accepting any value is considered to require service cessation.
        for handle in handles {
            handle.await?;
        }

        Ok(())
    }
}
