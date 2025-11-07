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
use tokio::sync::watch;

pub struct FuseSession<T> {
    rt: Arc<Runtime>,
    fs: Arc<T>,
    mnts: Vec<FuseMnt>,
    channels: Vec<FuseChannel<T>>,
    shutdown_tx: watch::Sender<bool>,
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
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
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
            shutdown_tx,
        };
        Ok(session)
    }

    pub async fn run(&mut self) -> CommonResult<()> {
        info!("fuse session started running");
        let ctrl_c = tokio::signal::ctrl_c();
        let channels = std::mem::take(&mut self.channels);
        let mnts = std::mem::take(&mut self.mnts);

        #[cfg(target_os = "linux")]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).unwrap();
            let mut sigint = signal(SignalKind::interrupt()).unwrap();
            let mut sighup = signal(SignalKind::hangup()).unwrap();
            let mut sigquit = signal(SignalKind::quit()).unwrap();

            //check umount signal
            {
                let watch_fds: Vec<orpc::sys::RawIO> = mnts.iter().map(|m| m.fd).collect();
                self.spawn_fd_watcher(&watch_fds);
            }

            tokio::select! {
                res = Self::run_all(self.rt.clone(), self.fs.clone(), channels, self.shutdown_tx.subscribe()) => {
                    if let Err(err) = res {
                        error!("fatal error, cause = {:?}", err);
                    }
                    info!("run_all finished; proceeding to unmount and exit");
                }

                _ = ctrl_c => {
                    info!("received Ctrl-C (SIGINT via terminal), shutting down fuse");
                    let _ = self.shutdown_tx.send(true);
                }

                _ = sigterm.recv()  => {
                    info!("received SIGTERM, shutting down fuse gracefully...");
                    let _ = self.shutdown_tx.send(true);
                }

                _ = sigint.recv()  => {
                    info!("received SIGINT, shutting down fuse gracefully...");
                    let _ = self.shutdown_tx.send(true);
                }

                _ = sighup.recv()  => {
                    info!("received SIGHUP, shutting down fuse gracefully...");
                    let _ = self.shutdown_tx.send(true);
                }

                _ = sigquit.recv()  => {
                    info!("received SIGQUIT, shutting down fuse gracefully...");
                    let _ = self.shutdown_tx.send(true);
                }
            }
        }

        info!("calling fs.unmount() and finishing fuse session");
        self.fs.unmount();
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn spawn_fd_watcher(&self, watch_fds: &[orpc::sys::RawIO]) {
        // Spawn an independent watcher task to detect HUP/ERR on FUSE fd
        let shutdown_tx = self.shutdown_tx.clone();
        let watch_fds_cloned = watch_fds.to_owned();
        self.rt.spawn(async move {
            use libc::{poll, pollfd, POLLERR, POLLHUP};
            use std::time::Duration;
            let mut pfds: Vec<pollfd> = watch_fds_cloned
                .iter()
                .map(|fd| pollfd {
                    fd: *fd,
                    events: (POLLERR | POLLHUP) as i16,
                    revents: 0,
                })
                .collect();
            loop {
                // Non-blocking poll; do not stall the runtime
                let res = unsafe { poll(pfds.as_mut_ptr(), pfds.len() as u64, 0) };
                if res > 0 {
                    for p in &pfds {
                        let revents = p.revents as i16;
                        if (revents & ((POLLERR | POLLHUP) as i16)) != 0 {
                            info!("fd_watcher detected HUP/ERR on FUSE fd; broadcasting shutdown");
                            let _ = shutdown_tx.send(true);
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
    }

    async fn run_all(
        rt: Arc<Runtime>,
        fs: Arc<T>,
        channels: Vec<FuseChannel<T>>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> CommonResult<()> {
        let mut handles = vec![];

        for channel in channels {
            for receiver in channel.receivers {
                let mut shutdown_rx = shutdown_rx.clone();
                let handle = rt.spawn(async move {
                    if let Err(err) = receiver.start(shutdown_rx).await {
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
