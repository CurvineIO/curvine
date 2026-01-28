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

use std::collections::HashMap;
use std::path::PathBuf;
use crate::fs::operator::FuseOperator;
use crate::fs::FileSystem;
use crate::raw::fuse_abi::*;
use crate::session::channel::{FuseChannel, FuseReceiver, FuseSender};
use crate::session::FuseRequest;
use crate::session::{FuseMnt, FuseResponse};
use crate::{err_fuse, FuseResult};
use curvine_common::conf::{ClusterConf, FuseConf};
use curvine_common::version::GIT_VERSION;
use libc::{EAGAIN, EINTR, ENODEV, ENOENT};
use log::{debug, error, info, warn};
use orpc::io::IOResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::{RawIO, SignalKind, SignalWatch};
use orpc::{err_box, CommonResult, err_msg};
use std::sync::Arc;
use std::process::Command;
use std::time::Duration;
use tokio::sync::watch;
use curvine_common::fs::{StateReader, StateWriter};
use curvine_common::utils::CommonUtils;

pub struct FuseSession<T> {
    rt: Arc<Runtime>,
    fs: Arc<T>,
    mnts: Vec<FuseMnt>,
    channels: Vec<FuseChannel<T>>,
    shutdown_tx: watch::Sender<bool>,
    conf: FuseConf,
}

impl<T: FileSystem> FuseSession<T> {
    pub const STATE_PATH: &'static str = "CURVINE_FUSE_STAT_PATH";

    pub async fn new(rt: Arc<Runtime>, fs: T, conf: FuseConf) -> FuseResult<Self> {
        let mnts = Self::restore(&conf, &fs).await?;

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
            channels.len(),
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

    async fn reload(&self, mnts: &[FuseMnt]) -> CommonResult<()> {
        let mut fds = HashMap::new();
        for mnt in mnts {
            fds.insert(mnt.fd, mnt.path.to_string_lossy().to_string());

            // Clear FD_CLOEXEC flag so fds are inherited by child process (fork+exec)
            #[cfg(target_os = "linux")]
            {
                unsafe {
                    let flags = libc::fcntl(mnt.fd, libc::F_GETFD);
                    if flags >= 0 {
                        let _ = libc::fcntl(mnt.fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC);
                    }
                }
            }
        }

        let mut writer = StateWriter::new(self.state_file())?;
        writer.write_struct(&fds)?;
        self.fs.persist(&mut writer).await?;

        let mut env = HashMap::new();
        env.insert(Self::STATE_PATH.to_owned(), writer.path().to_owned());

        // Start new process (fds will be inherited via fork+exec)
        CommonUtils::reload_param(env)?;

        Ok(())
    }

    async fn restore(conf: &FuseConf, fs: &T) -> CommonResult<Vec<FuseMnt>> {
        let mut mnts = vec![];
        if let Ok(state_file) = std::env::var(Self::STATE_PATH) {
            let mut reader = StateReader::new(&state_file)?;

            let fds: HashMap<RawIO, String> = reader.read_struct()?;
            if fds.is_empty() {
                return err_box!("no fd found in state file {}", state_file);
            }

            // Try to use inherited fds, fallback to remount if invalid
            for (fd, path) in fds {
                let path_buf = PathBuf::from(path);
                // Verify fd is valid and mount point is still mounted
                #[cfg(target_os = "linux")]
                {
                    unsafe {
                        // Check if fd is valid
                        let flags = libc::fcntl(fd, libc::F_GETFD);
                        if flags >= 0 {
                            debug!("fd {} is valid, flags={}", fd, flags);
                            // Check if mount point is still mounted by reading /proc/mounts
                            use std::fs;
                            use std::io::{BufRead, BufReader};
                            if let Ok(mounts) = fs::File::open("/proc/mounts") {
                                let reader = BufReader::new(mounts);
                                let path_str = path_buf.to_string_lossy();
                                let mut found_mount = false;
                                for line in reader.lines() {
                                    if let Ok(line) = line {
                                        let parts: Vec<&str> = line.split_whitespace().collect();
                                        if parts.len() >= 2 && parts[1] == path_str {
                                            debug!("found mount point in /proc/mounts: {}", line);
                                            found_mount = true;
                                            break;
                                        }
                                    }
                                }
                                if found_mount {
                                    info!("using inherited fd {} for mount path {:?}", fd, path_buf);
                                    mnts.push(FuseMnt::restore(path_buf, conf, fd));
                                    continue;
                                } else {
                                    warn!("fd {} is valid but mount point {:?} is not found in /proc/mounts", fd, path_buf);
                                }
                            } else {
                                warn!("failed to read /proc/mounts to verify mount point {:?}", path_buf);
                            }
                        } else {
                            warn!("fd {} is invalid (fcntl returned {}), cannot use inherited fd", fd, flags);
                        }
                    }
                }
                // Fallback to remount if fd is invalid or mount point is not mounted
                warn!("inherited fd {} is invalid or mount point {:?} is not mounted, remounting", fd, path_buf);
                mnts.push(FuseMnt::new(path_buf, conf));
            }

            fs.restore(&mut reader).await?;
        } else {
            let all_mnt_paths = conf.get_all_mnt_path()?;
            for path in all_mnt_paths {
                mnts.push(FuseMnt::new(path, conf));
            }
        }

        Ok(mnts)
    }

    pub async fn run(&mut self) -> CommonResult<()> {
        info!("fuse session started running");
        let channels = std::mem::take(&mut self.channels);
        let mnts = std::mem::take(&mut self.mnts);

        #[cfg(target_os = "linux")]
        {
            //check umount signal
           /* let watch_fds: Vec<RawIO> = mnts.iter().map(|m| m.fd).collect();
            self.spawn_fd_watcher(&watch_fds);*/
        }

        tokio::select! {
            res = Self::run_all(self.rt.clone(), self.fs.clone(), channels, self.shutdown_tx.subscribe()) => {
                if let Err(err) = res {
                    error!("fatal error, cause = {:?}", err);
                }
                info!("run_all finished; proceeding to unmount and exit");
            }

            signal_result = SignalWatch::wait_quit() => {
                match signal_result {
                    Ok(kind) => {
                        info!("received {}, shutting down fuse gracefully...", kind);
                    }
                    Err(e) => {
                        error!("error waiting for signal: {:?}", e);
                    }
                }
                let _ = self.shutdown_tx.send(true);
            }

            signal_result = SignalWatch::wait_one(SignalKind::User1) => {
                let _ = self.shutdown_tx.send(true);
                tokio::time::sleep(Duration::from_secs(1)).await;

                match signal_result {
                    Ok(kind) => {
                        info!("received {}, shutting down fuse gracefully...", kind);
                        self.reload(&mnts).await?;
                    }
                    Err(e) => {
                        error!("error waiting for signal: {:?}", e);
                    }
                }
            }
        }

        debug!("calling fs.unmount() and finishing fuse session");
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
