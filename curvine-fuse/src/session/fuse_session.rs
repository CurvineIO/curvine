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
use curvine_common::conf::{ClusterConf, FuseConf};
use curvine_common::fs::{StateReader, StateWriter};
use curvine_common::utils::CommonUtils;
use curvine_common::version::GIT_VERSION;
use libc::{EAGAIN, EINTR, ENODEV, ENOENT};
use log::{debug, error, info, warn};
use orpc::common::{ByteUnit, TimeSpent};
use orpc::io::IOResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::{RawIO, SignalKind, SignalWatch};
use orpc::{err_box, err_msg, sys, CommonResult};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

pub struct FuseSession<T> {
    rt: Arc<Runtime>,
    fs: Arc<T>,
    mnts: Vec<FuseMnt>,
    channels: Vec<FuseChannel<T>>,
    shutdown_tx: watch::Sender<bool>,
    conf: FuseConf,
}

impl<T: FileSystem> FuseSession<T> {
    pub const STATE_PATH: &'static str = "CURVINE_FUSE_STATE_PATH";

    pub async fn new(rt: Arc<Runtime>, fs: T, conf: FuseConf) -> FuseResult<Self> {
        let mnts = Self::setup_mnts(&conf, &fs).await?;

        let fs = Arc::new(fs);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);

        let mut channels = vec![];
        for mnt in &mnts {
            let channel = FuseChannel::new(fs.clone(), rt.clone(), mnt, &conf)?;
            channels.push(channel);
        }

        info!(
            "Create fuse session, git version: {}, mnt number: {}, loop task number: {},\
         io threads: {}, worker threads: {}, fuse channel size: {}",
            GIT_VERSION,
            conf.mnt_number,
            channels[0].senders.len(),
            rt.io_threads(),
            rt.worker_threads(),
            conf.fuse_channel_size,
        );

        let session = Self {
            rt,
            fs,
            mnts,
            channels,
            shutdown_tx,
            conf,
        };
        Ok(session)
    }

    pub fn state_file(&self) -> String {
        let pid = std::process::id();
        format!("{}/curvine_fuse_state_{}.data", self.conf.state_dir, pid)
    }

    pub async fn run(&mut self) -> CommonResult<()> {
        info!("fuse session started running");
        let channels = std::mem::take(&mut self.channels);
        let mnts = std::mem::take(&mut self.mnts);

        #[cfg(target_os = "linux")]
        {
            //check umount signal
            let watch_fds: Vec<RawIO> = mnts.iter().map(|m| m.fd).collect();
            self.spawn_fd_watcher(&watch_fds);
        }

        let mut run_all_handle = tokio::spawn(Self::run_all(
            self.rt.clone(),
            self.fs.clone(),
            channels,
            self.shutdown_tx.subscribe(),
        ));

        tokio::select! {
            res = &mut run_all_handle => {
                match res {
                    Ok(Ok(())) => {
                        info!("run_all finished (likely due to umount or ENODEV); proceeding to unmount and exit");
                    }
                    Ok(Err(err)) => {
                        error!("fatal error in run_all, cause = {:?}", err);
                    }
                    Err(e) => {
                        error!("run_all task panicked: {:?}", e);
                    }
                }
            }

            signal_result = SignalWatch::wait_quit() => {
                match signal_result {
                    Ok(kind) => {
                        info!("received termination signal {}, initiating graceful shutdown of FUSE session...", kind);
                    }
                    Err(e) => {
                        error!("error waiting for signal: {:?}", e);
                    }
                }

                let _ = self.shutdown_tx.send(true);
                if let Err(e) = run_all_handle.await {
                    error!("run_all task panicked during shutdown: {:?}", e);
                }
            }

            signal_result = SignalWatch::wait_one(SignalKind::User1) => {
                 match signal_result {
                    Ok(kind) => {
                        info!("received user signal {}, initiating graceful shutdown and persisting FUSE session state...", kind);
                    }
                    Err(e) => {
                        error!("error waiting for signal: {:?}", e);
                    }
                }

                let _ = self.shutdown_tx.send(true);
                if let Err(e) = run_all_handle.await {
                    error!("run_all task panicked during shutdown: {:?}", e);
                }
                self.persist(mnts).await?;
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

    async fn setup_mnts(conf: &FuseConf, fs: &T) -> CommonResult<Vec<FuseMnt>> {
        if let Ok(state_file) = std::env::var(Self::STATE_PATH) {
            Self::restore(&state_file, conf, fs).await
        } else {
            let mut mnts = vec![];
            let all_mnt_paths = conf.get_all_mnt_path()?;
            for path in all_mnt_paths {
                mnts.push(FuseMnt::new(path, conf));
            }
            Ok(mnts)
        }
    }

    async fn persist(&self, mnts: Vec<FuseMnt>) -> CommonResult<()> {
        let mut writer = StateWriter::new(self.state_file())?;
        let ts = TimeSpent::new();
        info!("persist: task started, path={}", writer.path());

        // Handle mount point file descriptors
        // 1. Set auto_unmount to false to prevent automatic unmounting
        // 2. Clear FD_CLOEXEC flag to allow child process inheritance
        let mut fds = HashMap::new();
        for mut mnt in mnts {
            mnt.auto_unmount(false);

            let flags = sys::fcntl_get(mnt.fd)?;
            sys::fcntl_set(mnt.fd, flags & !libc::FD_CLOEXEC);

            fds.insert(mnt.fd, mnt.path.to_string_lossy().to_string());
        }

        // Save file descriptors and state information to file
        info!("persist: write mount fds {:?}", fds);
        writer.write_struct(&fds)?;
        self.fs.persist(&mut writer).await?;

        info!(
            "persist: task completed, path={}, size={}, elapsed={}ms",
            writer.path(),
            ByteUnit::byte_to_string(writer.len()),
            ts.used_ms()
        );

        // Set environment variable to pass state file path and start child process
        let mut env = HashMap::new();
        env.insert(Self::STATE_PATH.to_owned(), writer.path().to_owned());
        CommonUtils::reload_param(env)?;

        Ok(())
    }

    async fn restore(file: &str, conf: &FuseConf, fs: &T) -> CommonResult<Vec<FuseMnt>> {
        // If environment variable exists, restore state information from state file
        let mut mnts = vec![];
        let mut reader = StateReader::new(file)?;
        let ts = TimeSpent::new();
        info!("restore: task started, path={}", reader.path());

        // Read and process mount point file descriptors
        let fds: HashMap<RawIO, String> = reader.read_struct()?;
        info!("restore: write mount fds {:?}", fds);
        if fds.is_empty() {
            return err_box!("no fd found in state file {}", reader.path());
        }
        for (fd, path) in fds {
            let flags = sys::fcntl_get(fd)?;
            sys::fcntl_set(fd, flags | libc::FD_CLOEXEC)?;

            let path_buf = PathBuf::from(path);
            mnts.push(FuseMnt::from_fd(path_buf, conf, fd));
        }

        fs.restore(&mut reader).await?;

        info!(
            "restore: task completed, file_path={}, elapsed={}ms",
            reader.path(),
            ts.used_ms()
        );
        Ok(mnts)
    }
}
