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
use crate::fuse_metrics::{
    FuseMetrics, ShutdownOnce, StateStageTimer, SESSION_INIT_ERROR, SESSION_INIT_SUCCESS,
    SHUTDOWN_COMPLETED, SHUTDOWN_FD_WATCHER, SHUTDOWN_RUN_ALL_ERROR, SHUTDOWN_RUN_ALL_PANIC,
    SHUTDOWN_SIGUSR1_PERSIST, SHUTDOWN_TERM_SIGNAL, STATE_STAGE_MOUNT_FDS, STATE_STATUS_ERROR,
    STATE_STATUS_SUCCESS,
};
use crate::raw::fuse_abi::*;
use crate::session::channel::{FuseChannel, FuseReceiver, FuseSender};
use crate::session::FuseRequest;
use crate::session::{FuseMnt, FuseResponse};
use crate::{err_fuse, FuseResult};
use curvine_config::{ClusterConf, FuseConf};
use curvine_fs_api::{StateReader, StateWriter};
use curvine_runtime::common::CommonUtils;
use curvine_sys::version::GIT_VERSION;
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

/// closes out a `run()` invocation on EVERY exit path — normal completion,
/// signal arms, and the SIGUSR1 run-all/persist error returns.
/// No await/join/panic-prone work in Drop.
struct RunCleanupGuard {
    watcher: Option<tokio::task::JoinHandle<()>>,
    enabled: bool,
}

impl Drop for RunCleanupGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.watcher.take() {
            handle.abort();
        }
        close_out_health(self.enabled, |healthy| {
            FuseMetrics::with(|m| m.set_kernel_fd_health(healthy))
        });
    }
}

fn close_out_health<F: FnOnce(bool)>(enabled: bool, set_health: F) {
    if enabled {
        set_health(false);
    }
}

/// Flatten the spawned `run_all` join result and its inner task result.
fn flatten_run_all_result(
    result: Result<CommonResult<()>, tokio::task::JoinError>,
) -> CommonResult<()> {
    match result {
        Ok(inner) => inner,
        Err(err) => Err(err.into()),
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, PartialEq, Eq)]
enum FdWatcherPollResult {
    Healthy,
    Interrupted,
    UnhealthyEvent { fd: RawIO, revents: i16 },
    PollError { errno: Option<i32> },
}

#[cfg(target_os = "linux")]
fn classify_fd_watcher_poll(
    result: libc::c_int,
    pfds: &[libc::pollfd],
    errno: Option<i32>,
) -> FdWatcherPollResult {
    if result < 0 {
        return if errno == Some(libc::EINTR) {
            FdWatcherPollResult::Interrupted
        } else {
            FdWatcherPollResult::PollError { errno }
        };
    }

    if result == 0 {
        return FdWatcherPollResult::Healthy;
    }

    let fatal_events = (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) as i16;
    pfds.iter()
        .find(|pfd| pfd.revents & fatal_events != 0)
        .map_or(FdWatcherPollResult::Healthy, |pfd| {
            FdWatcherPollResult::UnhealthyEvent {
                fd: pfd.fd,
                revents: pfd.revents,
            }
        })
}

impl<T: FileSystem> FuseSession<T> {
    pub const STATE_PATH: &'static str = "CURVINE_FUSE_STATE_PATH";

    pub async fn new(rt: Arc<Runtime>, fs: T, conf: FuseConf) -> FuseResult<Self> {
        let enabled = conf.metrics_enabled;
        let result = Self::new_inner(rt, fs, conf).await;
        if enabled {
            let res = if result.is_ok() {
                SESSION_INIT_SUCCESS
            } else {
                SESSION_INIT_ERROR
            };
            FuseMetrics::with(|m| m.record_session_init(res));
            if result.is_ok() {
                // Set health only after the session is fully built.
                FuseMetrics::with(|m| m.set_kernel_fd_health(true));
            }
        }
        result
    }

    async fn new_inner(rt: Arc<Runtime>, fs: T, conf: FuseConf) -> FuseResult<Self> {
        let mnts = Self::setup_mnts(&conf, &fs).await?;

        if let Some(kb) = conf.max_readahead_kb {
            for mnt in &mnts {
                crate::session::bdi::apply_max_readahead_kb(&mnt.path, kb);
            }
        }

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
        let enabled = self.conf.metrics_enabled;

        // one ShutdownOnce records session_shutdown_total exactly
        // once — the first cause (fd_watcher / signal / run_all completion) wins.
        let shutdown_once = ShutdownOnce::new(enabled);

        #[cfg(target_os = "linux")]
        let watcher_handle = {
            let watch_fds: Vec<RawIO> = mnts.iter().map(|m| m.fd).collect();
            Some(self.spawn_fd_watcher(&watch_fds, shutdown_once.clone(), enabled))
        };
        #[cfg(not(target_os = "linux"))]
        let watcher_handle: Option<tokio::task::JoinHandle<()>> = None;
        let _cleanup = RunCleanupGuard {
            watcher: watcher_handle,
            enabled,
        };

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
                        shutdown_once.record_once(SHUTDOWN_COMPLETED);
                    }
                    Ok(Err(err)) => {
                        error!("fatal error in run_all, cause = {:?}", err);
                        shutdown_once.record_once(SHUTDOWN_RUN_ALL_ERROR);
                    }
                    Err(e) => {
                        error!("run_all task panicked: {:?}", e);
                        shutdown_once.record_once(SHUTDOWN_RUN_ALL_PANIC);
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

                shutdown_once.record_once(SHUTDOWN_TERM_SIGNAL);
                let _ = self.shutdown_tx.send(true);
                if let Err(e) = flatten_run_all_result(run_all_handle.await) {
                    error!("run_all failed during termination shutdown: {:?}", e);
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

                // Record the shutdown intent BEFORE persist: sigusr1_persist denotes intent,
                // not a completed unmount (persist success/failure is a separate
                // state_persist_total{status}).
                shutdown_once.record_once(SHUTDOWN_SIGUSR1_PERSIST);
                let _ = self.shutdown_tx.send(true);
                if let Err(e) = flatten_run_all_result(run_all_handle.await) {
                    error!(
                        "run_all failed during SIGUSR1 shutdown; refusing to persist state: {:?}",
                        e
                    );
                    return Err(e);
                }
                self.persist(mnts).await?;
            }
        }

        info!("calling fs.unmount() and finishing fuse session");
        self.fs.unmount();
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn spawn_fd_watcher(
        &self,
        watch_fds: &[orpc::sys::RawIO],
        shutdown_once: ShutdownOnce,
        enabled: bool,
    ) -> tokio::task::JoinHandle<()> {
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
                let result = unsafe { poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, 0) };
                let errno = if result < 0 {
                    std::io::Error::last_os_error().raw_os_error()
                } else {
                    None
                };

                let unhealthy = match classify_fd_watcher_poll(result, &pfds, errno) {
                    FdWatcherPollResult::Healthy => false,
                    FdWatcherPollResult::Interrupted => continue,
                    FdWatcherPollResult::UnhealthyEvent { fd, revents } => {
                        info!(
                            "fd_watcher detected fatal event on FUSE fd {}, revents={:#x}; broadcasting shutdown",
                            fd, revents
                        );
                        true
                    }
                    FdWatcherPollResult::PollError { errno } => {
                        error!(
                            "fd_watcher poll failed, errno={:?}; broadcasting shutdown",
                            errno
                        );
                        true
                    }
                };

                if unhealthy {
                    // Record shutdown and health before broadcasting receiver shutdown.
                    shutdown_once.record_once(SHUTDOWN_FD_WATCHER);
                    if enabled {
                        FuseMetrics::with(|m| m.set_kernel_fd_health(false));
                    }
                    let _ = shutdown_tx.send(true);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
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
                        error!("fuse receiver exited with error: {}", err);
                    }
                });
                handles.push(handle);
            }

            for sender in channel.senders {
                let handle = rt.spawn(async move {
                    if let Err(err) = sender.start().await {
                        error!("fuse sender exited with error: {}", err);
                    }
                });
                handles.push(handle);
            }
        }

        // Wait for all receiver/sender tasks to finish.
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
                mnts.push(FuseMnt::new(path, conf)?);
            }
            Ok(mnts)
        }
    }

    async fn persist(&self, mnts: Vec<FuseMnt>) -> CommonResult<()> {
        let enabled = self.conf.metrics_enabled;
        let result = self.persist_inner(mnts, enabled).await;
        if enabled {
            let status = if result.is_ok() {
                STATE_STATUS_SUCCESS
            } else {
                STATE_STATUS_ERROR
            };
            FuseMetrics::with(|m| m.record_state_total(true, status));
        }
        result
    }

    async fn persist_inner(&self, mnts: Vec<FuseMnt>, enabled: bool) -> CommonResult<()> {
        let mut writer = StateWriter::new(self.state_file())?;
        let ts = TimeSpent::new();
        info!("persist: task started, path={}", writer.path());

        {
            let stage = StateStageTimer::start(enabled, true, STATE_STAGE_MOUNT_FDS);
            // Persist mount fds with auto_unmount disabled and FD_CLOEXEC cleared.
            let mut fds = HashMap::new();
            for mut mnt in mnts {
                mnt.auto_unmount(false);

                let flags = sys::fcntl_get(mnt.fd)?;
                // Propagate the F_SETFD failure: if clearing FD_CLOEXEC fails the
                // persisted fd cannot be inherited by the child, so the persist must
                // fail rather than silently report success with an unusable state file.
                sys::fcntl_set(mnt.fd, flags & !libc::FD_CLOEXEC)?;

                fds.insert(mnt.fd, mnt.path.to_string_lossy().to_string());
            }

            // Save file descriptors and state information to file
            info!("persist: write mount fds {:?}", fds);
            writer.write_struct(&fds)?;
            if let Some(stage) = stage {
                stage.success();
            }
        }

        self.fs.persist(&mut writer).await?;

        // Flush before spawning the restore child, which immediately opens this state file.
        writer.flush()?;

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
        let enabled = conf.metrics_enabled;
        let result = Self::restore_inner(file, conf, fs, enabled).await;
        if enabled {
            let status = if result.is_ok() {
                STATE_STATUS_SUCCESS
            } else {
                STATE_STATUS_ERROR
            };
            FuseMetrics::with(|m| m.record_state_total(false, status));
        }
        result
    }

    async fn restore_inner(
        file: &str,
        conf: &FuseConf,
        fs: &T,
        enabled: bool,
    ) -> CommonResult<Vec<FuseMnt>> {
        // If environment variable exists, restore state information from state file
        let mut mnts = vec![];
        let mut reader = StateReader::new(file)?;
        let ts = TimeSpent::new();
        info!("restore: task started, path={}", reader.path());

        {
            let stage = StateStageTimer::start(enabled, false, STATE_STAGE_MOUNT_FDS);
            // Read and process mount point file descriptors
            let fds: HashMap<RawIO, String> = reader.read_struct()?;
            info!("restore: read mount fds {:?}", fds);
            if fds.is_empty() {
                return err_box!("no fd found in state file {}", reader.path());
            }
            for (fd, path) in fds {
                let flags = sys::fcntl_get(fd)?;
                sys::fcntl_set(fd, flags | libc::FD_CLOEXEC)?;

                let path_buf = PathBuf::from(path);
                mnts.push(FuseMnt::from_fd(path_buf, conf, fd)?);
            }
            if let Some(stage) = stage {
                stage.success();
            }
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

#[cfg(all(test, target_os = "linux"))]
mod fd_watcher_tests {
    use super::{classify_fd_watcher_poll, FdWatcherPollResult};
    use libc::{pollfd, EINTR, EIO, POLLERR, POLLHUP, POLLIN, POLLNVAL};

    #[test]
    fn pollnval_is_unhealthy() {
        let pfds = [pollfd {
            fd: 42,
            events: (POLLERR | POLLHUP) as i16,
            revents: POLLNVAL as i16,
        }];

        assert_eq!(
            classify_fd_watcher_poll(1, &pfds, None),
            FdWatcherPollResult::UnhealthyEvent {
                fd: 42,
                revents: POLLNVAL as i16,
            }
        );
    }

    #[test]
    fn interrupted_poll_is_retried() {
        assert_eq!(
            classify_fd_watcher_poll(-1, &[], Some(EINTR)),
            FdWatcherPollResult::Interrupted
        );
    }

    #[test]
    fn non_eintr_poll_error_is_unhealthy() {
        assert_eq!(
            classify_fd_watcher_poll(-1, &[], Some(EIO)),
            FdWatcherPollResult::PollError { errno: Some(EIO) }
        );
    }

    #[test]
    fn non_fatal_revents_remain_healthy() {
        let pfds = [pollfd {
            fd: 42,
            events: POLLIN as i16,
            revents: POLLIN as i16,
        }];

        assert_eq!(
            classify_fd_watcher_poll(1, &pfds, None),
            FdWatcherPollResult::Healthy
        );
    }
}

#[cfg(test)]
mod cleanup_guard_tests {
    use super::RunCleanupGuard;

    #[tokio::test]
    async fn drop_aborts_the_watcher_handle() {
        // A long-lived task that would never finish on its own.
        let handle = tokio::spawn(async {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        });
        let abort_handle = handle.abort_handle();
        assert!(!abort_handle.is_finished(), "task runs before guard drop");

        {
            let _guard = RunCleanupGuard {
                watcher: Some(handle),
                enabled: false, // avoid touching the process-global health gauge
            };
        } // guard drops here -> aborts the task

        // Give the runtime a tick to process the abort.
        tokio::task::yield_now().await;
        assert!(
            abort_handle.is_finished(),
            "guard Drop must abort the watcher task"
        );
    }

    #[test]
    fn drop_without_watcher_is_a_noop() {
        let _guard = RunCleanupGuard {
            watcher: None,
            enabled: false,
        };
        // dropping here must not panic
    }

    #[test]
    fn close_out_health_sets_zero_only_when_enabled() {
        use std::cell::Cell;
        let set: Cell<Option<bool>> = Cell::new(None);

        super::close_out_health(true, |h| set.set(Some(h)));
        assert_eq!(set.get(), Some(false), "enabled => sets health to 0");

        set.set(None);
        super::close_out_health(false, |h| set.set(Some(h)));
        assert_eq!(set.get(), None, "disabled => does not touch health");
    }
}

#[cfg(test)]
mod run_all_shutdown_result_tests {
    use super::flatten_run_all_result;

    #[test]
    fn clean_inner_result_succeeds() {
        assert!(flatten_run_all_result(Ok(Ok(()))).is_ok());
    }

    #[test]
    fn inner_run_all_error_is_preserved() {
        let inner: orpc::CommonResult<()> = Err(std::io::Error::other("receiver failed").into());

        let err = flatten_run_all_result(Ok(inner)).expect_err("inner error must be returned");

        assert_eq!(err.to_string(), "receiver failed");
    }

    #[tokio::test]
    async fn join_error_is_preserved() {
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
            Ok::<(), orpc::CommonError>(())
        });
        handle.abort();

        let err = flatten_run_all_result(handle.await).expect_err("join error must be returned");

        assert!(err.to_string().contains("cancelled"));
    }
}
