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

use super::fuse_mnt::describe_fuse_fd_error;
use crate::fs::operator::FuseOperator;
use crate::fs::FileSystem;
use crate::fuse_metrics::{
    FuseMetrics, ShutdownCause, ShutdownOnce, StateStageTimer, SESSION_INIT_ERROR,
    SESSION_INIT_SUCCESS, STATE_STAGE_MOUNT_FDS, STATE_STATUS_ERROR, STATE_STATUS_SUCCESS,
};
use crate::raw::fuse_abi::*;
use crate::session::channel::{FuseChannel, FuseReceiver, FuseSender};
use crate::session::FuseRequest;
use crate::session::{FuseMnt, FuseResponse};
use crate::{err_fuse, FuseError, FuseResult};
use curvine_config::{ClusterConf, FuseConf};
use curvine_core_error::CommonResult;
use curvine_fs_api::{StateReader, StateWriter};
use curvine_io::IOResult;
use curvine_runtime::common::CommonUtils;
use curvine_runtime::common::{ByteUnit, TimeSpent};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_sys as sys;
use curvine_sys::version::GIT_VERSION;
use curvine_sys::{RawIO, SignalKind, SignalWatch};
use libc::{EAGAIN, EINTR, ENODEV, ENOENT};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;

/// Drive the worker set until every worker has been joined.
///
/// Cancellation ownership stays here: on the first real worker failure (or on
/// the persistent force signal) every worker is aborted and then joined before
/// this function returns, so no task is left detached while the caller releases
/// the mounts.
async fn supervise_workers(
    mut workers: JoinSet<FuseResult<()>>,
    mut force_rx: watch::Receiver<bool>,
) -> CommonResult<()> {
    let mut force_open = true;
    let mut first_error: Option<curvine_core_error::CommonError> = None;

    loop {
        tokio::select! {
            biased;

            changed = force_rx.changed(), if force_open => {
                match changed {
                    Ok(()) if *force_rx.borrow() => {
                        workers.abort_all();
                        while let Some(_joined) = workers.join_next().await {}
                        return curvine_core_error::err_box!("fuse workers force-cancelled");
                    }
                    Ok(()) => {}
                    // Every sender is gone: stop polling this branch instead of
                    // spinning on an immediately-ready error.
                    Err(_) => force_open = false,
                }
            }

            joined = workers.join_next() => {
                match joined {
                    None => {
                        return match first_error {
                            Some(err) => Err(err),
                            None => Ok(()),
                        }
                    }
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(err))) => {
                        if first_error.is_none() {
                            first_error = Some(err.into());
                        }
                        workers.abort_all();
                    }
                    Some(Err(join_err)) => {
                        if first_error.is_none() {
                            first_error = Some(join_err.into());
                        }
                        workers.abort_all();
                    }
                }
            }
        }
    }
}

/// Graceful window first, then a persistent force signal; resources are only
/// released by the caller once `run_all` has joined every worker.
async fn drain_after_device_lost(
    run_all: &mut tokio::task::JoinHandle<CommonResult<()>>,
    force_tx: &watch::Sender<bool>,
    event: DeviceLostEvent,
    grace: Duration,
    warn_interval: Duration,
) -> CommonResult<()> {
    match tokio::time::timeout(grace, &mut *run_all).await {
        Ok(joined) => {
            return session_exit_result(DeviceLostTeardown::Completed(flatten_run_all_result(
                joined,
            )))
        }
        Err(_) => warn!(
            "fuse session did not drain within {:?} after {}; forcing worker cancellation",
            grace,
            event.describe()
        ),
    }

    let _ = force_tx.send(true);

    loop {
        tokio::select! {
            joined = &mut *run_all => {
                return session_exit_result(DeviceLostTeardown::Forced {
                    worker_result: flatten_run_all_result(joined),
                    event,
                });
            }
            _ = tokio::time::sleep(warn_interval) => {
                warn!("fuse workers are still draining after a forced cancel; keeping mounts alive");
            }
        }
    }
}

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

impl RunCleanupGuard {
    /// Stop the watcher before the mounts are released: it polls the mount's raw
    /// fds, which must not outlive the mount.
    async fn stop(&mut self) {
        if let Some(handle) = self.watcher.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
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

/// How long a session may take to drain after the device disappeared before its
/// workers are aborted and the loss is reported as an error.
const DEVICE_LOST_TEARDOWN_GRACE: Duration = Duration::from_secs(10);

/// How often a forced teardown logs that workers are still draining. Resources
/// are kept alive until they are joined; the supervisor decides when to kill.
const FORCE_WARN_INTERVAL: Duration = Duration::from_secs(5);

/// A fatal condition reported by the FUSE fd watcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeviceLostEvent {
    /// Watched fd, or `-1` when `poll(2)` itself failed or the watcher vanished.
    pub fd: RawIO,
    /// `revents` that made the fd unhealthy (0 when poll failed).
    pub revents: i16,
    /// `errno` from `poll(2)`, when it returned -1.
    pub errno: Option<i32>,
    /// The watcher task ended without reporting a device event.
    gone: bool,
}

impl DeviceLostEvent {
    /// The watcher is gone; treat it as an internal failure rather than
    /// "no device problem".
    fn watcher_gone() -> Self {
        Self {
            fd: -1,
            revents: 0,
            errno: None,
            gone: true,
        }
    }

    fn describe(&self) -> String {
        if self.gone {
            return "fd watcher exited without a verdict".to_string();
        }
        match self.errno {
            Some(errno) => format!("poll on the FUSE fd failed (errno={})", errno),
            None => format!("fuse fd {} reported revents={:#x}", self.fd, self.revents),
        }
    }
}

/// How the session ended after the fd watcher fired.
#[derive(Debug)]
pub(crate) enum DeviceLostTeardown {
    /// `run_all` finished within the teardown grace period.
    Completed(CommonResult<()>),
    /// `run_all` stalled on the dead device and had to be forced.
    ///
    /// `worker_result` keeps the real failure when a worker reported one while
    /// being cancelled, so the root cause is not replaced by the device event.
    Forced {
        worker_result: CommonResult<()>,
        event: DeviceLostEvent,
    },
}

/// Decide what `run()` reports when the watcher sees a fatal fd event.
///
/// A clean `umount` also shows up as POLLERR/POLLHUP, so the event alone must
/// not fail the session: when the workers still wind down on their own, the
/// session exits successfully exactly as before. Only a session that cannot
/// tear itself down (the dead-device case) is an error.
fn session_exit_result(teardown: DeviceLostTeardown) -> CommonResult<()> {
    match teardown {
        DeviceLostTeardown::Completed(result) => result,
        // A worker error outranks the forced-teardown verdict, and both outrank
        // the bare device event: keep the most actionable cause.
        DeviceLostTeardown::Forced {
            worker_result: Err(err),
            event,
        } => curvine_core_error::err_box!("fuse device lost ({}): {}", event.describe(), err),
        DeviceLostTeardown::Forced {
            worker_result: Ok(()),
            event,
        } => curvine_core_error::err_box!(
            "fuse device lost: {}; session did not shut down within {:?}",
            event.describe(),
            DEVICE_LOST_TEARDOWN_GRACE
        ),
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

        // One ShutdownOnce records the first shutdown cause; metrics stay a
        // single increment per session.
        let shutdown_once = ShutdownOnce::new(enabled);

        #[cfg(target_os = "linux")]
        let (device_lost_tx, device_lost_rx) = oneshot::channel::<DeviceLostEvent>();
        #[cfg(target_os = "linux")]
        let watcher = {
            let watch_fds: Vec<RawIO> = mnts.iter().map(|m| m.fd).collect();
            Some(self.spawn_fd_watcher(&watch_fds, shutdown_once.clone(), enabled, device_lost_tx))
        };
        #[cfg(not(target_os = "linux"))]
        let watcher: Option<tokio::task::JoinHandle<()>> = None;
        let mut cleanup = RunCleanupGuard { watcher, enabled };

        // `shutdown_rx` asks workers to stop accepting work; `force_rx` is the
        // persistent "cancel now" state set once the graceful window expires.
        let (force_tx, force_rx) = watch::channel(false);
        let mut run_all_handle = tokio::spawn(Self::run_all(
            channels,
            self.shutdown_tx.subscribe(),
            force_rx,
        ));

        // On Linux the watcher verdict drives the exit path; elsewhere the arm
        // never resolves so `select!` keeps one shape on every platform.
        #[cfg(target_os = "linux")]
        let device_lost = async move { device_lost_rx.await.ok() };
        #[cfg(not(target_os = "linux"))]
        let device_lost = std::future::pending::<Option<DeviceLostEvent>>();

        let run_result: CommonResult<()> = tokio::select! {
            res = &mut run_all_handle => {
                match res {
                    Ok(Ok(())) => {
                        info!("run_all finished (likely due to umount or ENODEV); proceeding to unmount and exit");
                        let _ = shutdown_once.record(ShutdownCause::Completed);
                        Ok(())
                    }
                    Ok(Err(err)) => {
                        error!("fatal error in run_all, cause = {:?}", err);
                        let _ = shutdown_once.record(ShutdownCause::RunAllFailed);
                        // A worker failure is never downgraded to success.
                        Err(err)
                    }
                    Err(e) => {
                        error!("run_all task panicked: {:?}", e);
                        let _ = shutdown_once.record(ShutdownCause::RunAllPanic);
                        Err(e.into())
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

                let _ = shutdown_once.record(ShutdownCause::TermSignal);
                let _ = self.shutdown_tx.send(true);
                // A real worker failure during shutdown still fails the session.
                flatten_run_all_result(run_all_handle.await)
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
                let _ = shutdown_once.record(ShutdownCause::User1Persist);
                let _ = self.shutdown_tx.send(true);
                match flatten_run_all_result(run_all_handle.await) {
                    Err(e) => {
                        error!(
                            "run_all failed during SIGUSR1 shutdown; refusing to persist state: {:?}",
                            e
                        );
                        Err(e)
                    }
                    // `persist` errors also flow out through `run_result` so the
                    // common unmount path below always runs.
                    Ok(()) => self.persist(mnts).await,
                }
            }

            // The watcher fired, so the fd is gone. A normal umount closes the
            // fd too, therefore the event alone is not an error: give the
            // workers a bounded window to wind down and only report a device
            // loss when the session cannot stop on its own.
            lost = device_lost => {
                match lost {
                    Some(event) => {
                        info!(
                            "fd watcher reported a fatal event ({}); waiting up to {:?} for the session to drain",
                            event.describe(),
                            DEVICE_LOST_TEARDOWN_GRACE
                        );
                        drain_after_device_lost(
                            &mut run_all_handle,
                            &force_tx,
                            event,
                            DEVICE_LOST_TEARDOWN_GRACE,
                            FORCE_WARN_INTERVAL,
                        )
                        .await
                    }
                    // The watcher vanished without a verdict: that is an internal
                    // failure, not "no device problem".
                    None => {
                        error!("fd watcher exited without a verdict; forcing a bounded teardown");
                        let _ = shutdown_once.record(ShutdownCause::WatcherGone);
                        let _ = self.shutdown_tx.send(true);
                        drain_after_device_lost(
                            &mut run_all_handle,
                            &force_tx,
                            DeviceLostEvent::watcher_gone(),
                            DEVICE_LOST_TEARDOWN_GRACE,
                            FORCE_WARN_INTERVAL,
                        )
                        .await
                    }
                }
            }
        };

        // Stop the watcher before unmounting: it polls the mount's raw fds.
        cleanup.stop().await;
        info!(
            "fuse session shutting down, cause = {:?}",
            shutdown_once.cause()
        );
        info!("calling fs.unmount() and finishing fuse session");
        self.fs.unmount();
        run_result
    }

    #[cfg(target_os = "linux")]
    fn spawn_fd_watcher(
        &self,
        watch_fds: &[curvine_sys::RawIO],
        shutdown_once: ShutdownOnce,
        enabled: bool,
        lost_tx: oneshot::Sender<DeviceLostEvent>,
    ) -> tokio::task::JoinHandle<()> {
        // Spawn an independent watcher task to detect HUP/ERR on FUSE fd
        let shutdown_tx = self.shutdown_tx.clone();
        let watch_fds_cloned = watch_fds.to_owned();
        self.rt.spawn(watch_fuse_fds(
            watch_fds_cloned,
            shutdown_tx,
            shutdown_once,
            enabled,
            lost_tx,
        ))
    }
}

/// Poll the mounted FUSE fds until one reports a fatal event.
///
/// On such an event the watcher records the shutdown cause, flips the health
/// gauge and hands the verdict to `run()`, which is what turns a dead device
/// into a visible session error instead of a silent stall.
#[cfg(target_os = "linux")]
async fn watch_fuse_fds(
    watch_fds: Vec<RawIO>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_once: ShutdownOnce,
    enabled: bool,
    lost_tx: oneshot::Sender<DeviceLostEvent>,
) {
    use libc::{poll, pollfd, POLLERR, POLLHUP};
    let mut pfds: Vec<pollfd> = watch_fds
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

        let lost = match classify_fd_watcher_poll(result, &pfds, errno) {
            FdWatcherPollResult::Healthy => None,
            FdWatcherPollResult::Interrupted => continue,
            FdWatcherPollResult::UnhealthyEvent { fd, revents } => {
                info!(
                    "fd_watcher detected fatal event on FUSE fd {}, revents={:#x}; broadcasting shutdown",
                    fd, revents
                );
                Some(DeviceLostEvent {
                    fd,
                    revents,
                    errno: None,
                    gone: false,
                })
            }
            FdWatcherPollResult::PollError { errno } => {
                error!(
                    "fd_watcher poll failed, errno={:?}; broadcasting shutdown",
                    errno
                );
                Some(DeviceLostEvent {
                    fd: -1,
                    revents: 0,
                    errno,
                    gone: false,
                })
            }
        };

        if let Some(event) = lost {
            // Record shutdown and health before broadcasting receiver shutdown.
            let _ = shutdown_once.record(ShutdownCause::FdWatcher);
            if enabled {
                FuseMetrics::with(|m| m.set_kernel_fd_health(false));
            }
            let _ = shutdown_tx.send(true);
            // Hand the verdict to `run()`: without it a dead device can leave
            // the session running, or exiting as if it were clean.
            let _ = lost_tx.send(event);
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

impl<T: FileSystem> FuseSession<T> {
    async fn run_all(
        channels: Vec<FuseChannel<T>>,
        shutdown_rx: watch::Receiver<bool>,
        force_rx: watch::Receiver<bool>,
    ) -> CommonResult<()> {
        let mut workers: JoinSet<FuseResult<()>> = JoinSet::new();

        for channel in channels {
            for receiver in channel.receivers {
                let shutdown_rx = shutdown_rx.clone();
                workers.spawn(async move { receiver.start(shutdown_rx).await });
            }

            for sender in channel.senders {
                workers.spawn(async move { sender.start().await });
            }
        }

        supervise_workers(workers, force_rx).await
    }

    async fn setup_mnts(conf: &FuseConf, fs: &T) -> FuseResult<Vec<FuseMnt>> {
        if let Ok(state_file) = std::env::var(Self::STATE_PATH) {
            Self::restore(&state_file, conf, fs).await
        } else {
            let mut mnts = vec![];
            let all_mnt_paths = conf.get_all_mnt_path()?;
            for path in all_mnt_paths {
                mnts.push(FuseMnt::new(path, conf).map_err(FuseError::from_startup_io_error)?);
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

                let flags = sys::fcntl_get(mnt.fd).map_err(|error| {
                    describe_fuse_fd_error(
                        "read FUSE fd flags for reload persistence",
                        mnt.fd,
                        error.into(),
                    )
                })?;
                // Propagate the F_SETFD failure: if clearing FD_CLOEXEC fails the
                // persisted fd cannot be inherited by the child, so the persist must
                // fail rather than silently report success with an unusable state file.
                sys::fcntl_set(mnt.fd, flags & !libc::FD_CLOEXEC).map_err(|error| {
                    describe_fuse_fd_error(
                        "clear FD_CLOEXEC for reload persistence",
                        mnt.fd,
                        error.into(),
                    )
                })?;

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

    async fn restore(file: &str, conf: &FuseConf, fs: &T) -> FuseResult<Vec<FuseMnt>> {
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
    ) -> FuseResult<Vec<FuseMnt>> {
        // If environment variable exists, restore state information from state file
        let mut mnts = vec![];
        let mut reader = StateReader::new(file).map_err(FuseError::from_startup_io_error)?;
        let ts = TimeSpent::new();
        info!("restore: task started, path={}", reader.path());

        {
            let stage = StateStageTimer::start(enabled, false, STATE_STAGE_MOUNT_FDS);
            // Read and process mount point file descriptors
            let fds: HashMap<RawIO, String> = reader
                .read_struct()
                .map_err(FuseError::from_startup_io_error)?;
            info!("restore: read mount fds {:?}", fds);
            if fds.is_empty() {
                return Err(FuseError::from(format!(
                    "no fd found in state file {}",
                    reader.path()
                )));
            }
            for (fd, path) in fds {
                let flags = sys::fcntl_get(fd).map_err(|error| {
                    FuseError::from_startup_io_error(describe_fuse_fd_error(
                        "read FUSE fd flags during reload restore",
                        fd,
                        error.into(),
                    ))
                })?;
                sys::fcntl_set(fd, flags | libc::FD_CLOEXEC).map_err(|error| {
                    FuseError::from_startup_io_error(describe_fuse_fd_error(
                        "set FD_CLOEXEC during reload restore",
                        fd,
                        error.into(),
                    ))
                })?;

                let path_buf = PathBuf::from(path);
                mnts.push(
                    FuseMnt::from_fd(path_buf, conf, fd)
                        .map_err(FuseError::from_startup_io_error)?,
                );
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
        let inner: curvine_core_error::CommonResult<()> =
            Err(std::io::Error::other("receiver failed").into());

        let err = flatten_run_all_result(Ok(inner)).expect_err("inner error must be returned");

        assert_eq!(err.to_string(), "receiver failed");
    }

    #[tokio::test]
    async fn join_error_is_preserved() {
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
            Ok::<(), curvine_core_error::CommonError>(())
        });
        handle.abort();

        let err = flatten_run_all_result(handle.await).expect_err("join error must be returned");

        assert!(err.to_string().contains("cancelled"));
    }
}

#[cfg(all(test, target_os = "linux"))]
mod device_lost_tests {
    use super::{session_exit_result, DeviceLostEvent, DeviceLostTeardown};

    fn stalled_event() -> DeviceLostEvent {
        DeviceLostEvent {
            fd: 42,
            revents: 0x10,
            errno: None,
            gone: false,
        }
    }

    #[test]
    fn a_clean_teardown_stays_successful() {
        // A normal umount closes /dev/fuse as well, so the watcher fires on
        // every unmount: as long as the workers wind down, the session is fine.
        assert!(session_exit_result(DeviceLostTeardown::Completed(Ok(()))).is_ok());
    }

    #[test]
    fn an_existing_run_all_error_is_preserved() {
        let err = session_exit_result(DeviceLostTeardown::Completed(Err(std::io::Error::other(
            "receiver failed",
        )
        .into())))
        .expect_err("run_all error must survive");

        assert_eq!(err.to_string(), "receiver failed");
    }

    #[test]
    fn a_forced_teardown_is_a_device_lost_error() {
        let err = session_exit_result(DeviceLostTeardown::Forced {
            worker_result: Ok(()),
            event: stalled_event(),
        })
        .expect_err("a session that cannot stop on a dead device must fail");

        assert!(err.to_string().contains("device lost"), "got: {err}");
        assert!(err.to_string().contains("fd 42"), "got: {err}");
        assert!(err.to_string().contains("0x10"), "got: {err}");
    }

    #[test]
    fn a_forced_teardown_keeps_the_worker_error() {
        let err = session_exit_result(DeviceLostTeardown::Forced {
            worker_result: Err(std::io::Error::other("receiver failed").into()),
            event: stalled_event(),
        })
        .expect_err("a forced teardown with a worker error must fail");

        // The worker root cause outranks the bare device event.
        assert!(err.to_string().contains("receiver failed"), "got: {err}");
        assert!(err.to_string().contains("fd 42"), "got: {err}");
    }

    #[test]
    fn a_watcher_gone_teardown_names_the_event() {
        let err = session_exit_result(DeviceLostTeardown::Forced {
            worker_result: Ok(()),
            event: DeviceLostEvent::watcher_gone(),
        })
        .expect_err("a watcher without a verdict must fail");

        assert!(err.to_string().contains("without a verdict"), "got: {err}");
    }

    #[tokio::test]
    async fn the_watcher_hands_the_verdict_of_a_dead_fd_to_the_session() {
        use super::{watch_fuse_fds, ShutdownOnce};
        use std::time::Duration;

        let mut fds = [0i32; 2];
        assert_eq!(
            unsafe { libc::pipe(fds.as_mut_ptr()) },
            0,
            "pipe() must succeed"
        );
        let (read_fd, write_fd) = (fds[0], fds[1]);

        let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
        let (lost_tx, lost_rx) = tokio::sync::oneshot::channel();
        let watcher = tokio::spawn(watch_fuse_fds(
            vec![read_fd],
            shutdown_tx,
            ShutdownOnce::new(false),
            false,
            lost_tx,
        ));

        // Closing the write end is what a disappearing device looks like to
        // poll(2): the read end reports POLLHUP.
        unsafe { libc::close(write_fd) };

        let event = tokio::time::timeout(Duration::from_secs(2), lost_rx)
            .await
            .expect("the watcher must report a dead fd")
            .expect("the verdict channel must be fed");

        assert_eq!(event.fd, read_fd, "the event must name the watched fd");
        assert!(
            event.revents & (libc::POLLHUP as i16) != 0
                || event.revents & (libc::POLLERR as i16) != 0,
            "expected POLLHUP/POLLERR, got revents={:#x}",
            event.revents
        );

        unsafe { libc::close(read_fd) };
        watcher.await.expect("watcher task must finish");
    }
}

#[cfg(all(test, target_os = "linux"))]
mod teardown_orchestration_tests {
    use super::{drain_after_device_lost, supervise_workers, DeviceLostEvent, RunCleanupGuard};
    use crate::FuseError;
    use curvine_core_error::CommonError;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;
    use tokio::task::{JoinHandle, JoinSet};

    fn event() -> DeviceLostEvent {
        DeviceLostEvent {
            fd: 42,
            revents: 0x10,
            errno: None,
            gone: false,
        }
    }

    fn worker_error(message: &'static str) -> FuseError {
        FuseError::from_errno_msg(libc::EIO, std::io::Error::other(message).into())
    }

    fn cancellation_flag() -> (Arc<AtomicBool>, impl Fn() -> bool) {
        let flag = Arc::new(AtomicBool::new(false));
        let reader = flag.clone();
        (flag, move || reader.load(Ordering::SeqCst))
    }

    /// A worker that never finishes on its own; `Drop` records that it was
    /// actually cancelled and joined (not merely dropped by the runtime).
    fn pending_worker(
        flag: Arc<AtomicBool>,
    ) -> impl std::future::Future<Output = crate::FuseResult<()>> {
        struct MarkOnDrop(Arc<AtomicBool>);
        impl Drop for MarkOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        async move {
            let _guard = MarkOnDrop(flag);
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_worker_error_cancels_and_joins_the_rest() {
        let mut workers: JoinSet<crate::FuseResult<()>> = JoinSet::new();
        workers.spawn(async { Err(worker_error("receiver failed")) });
        let (cancelled, was_cancelled) = cancellation_flag();
        workers.spawn(pending_worker(cancelled));

        // Keep the sender alive so the force branch never fires: the first
        // worker error alone must stop the rest.
        let (_force_tx, force_rx) = watch::channel(false);
        let err = supervise_workers(workers, force_rx)
            .await
            .expect_err("the first worker error must surface");

        assert!(err.to_string().contains("receiver failed"), "got: {err}");
        assert!(
            was_cancelled(),
            "the remaining worker must be aborted and joined before returning"
        );
    }

    #[tokio::test]
    async fn force_cancels_and_joins_every_worker() {
        let mut workers: JoinSet<crate::FuseResult<()>> = JoinSet::new();
        let (cancelled_a, was_a) = cancellation_flag();
        let (cancelled_b, was_b) = cancellation_flag();
        workers.spawn(pending_worker(cancelled_a));
        workers.spawn(pending_worker(cancelled_b));

        let (force_tx, force_rx) = watch::channel(false);
        let supervisor = tokio::spawn(supervise_workers(workers, force_rx));
        force_tx.send(true).expect("force receiver is alive");

        let err = tokio::time::timeout(Duration::from_secs(2), supervisor)
            .await
            .expect("force must not hang")
            .expect("supervisor must not panic")
            .expect_err("a forced teardown must fail the session");

        assert!(err.to_string().contains("force-cancelled"), "got: {err}");
        assert!(
            was_a() && was_b(),
            "every worker must be cancelled and joined"
        );
    }

    #[tokio::test]
    async fn a_dropped_force_sender_does_not_spin() {
        let mut workers: JoinSet<crate::FuseResult<()>> = JoinSet::new();
        workers.spawn(async { Ok(()) });

        let (force_tx, force_rx) = watch::channel(false);
        drop(force_tx);

        let out =
            tokio::time::timeout(Duration::from_secs(2), supervise_workers(workers, force_rx))
                .await
                .expect("a closed force channel must not busy-loop");

        assert!(out.is_ok(), "workers finished cleanly");
    }

    #[tokio::test]
    async fn drain_preserves_a_fast_worker_error() {
        let mut run_all: JoinHandle<curvine_core_error::CommonResult<()>> = tokio::spawn(async {
            Err::<(), CommonError>(std::io::Error::other("receiver failed").into())
        });
        let (force_tx, _force_rx) = watch::channel(false);

        let err = drain_after_device_lost(
            &mut run_all,
            &force_tx,
            event(),
            Duration::from_millis(50),
            Duration::from_millis(20),
        )
        .await
        .expect_err("a worker error must survive the device-lost path");

        assert!(err.to_string().contains("receiver failed"), "got: {err}");
    }

    #[tokio::test]
    async fn drain_forces_a_stalled_session() {
        // Stand-in for run_all: only exits once force is set, like a session
        // whose workers ignore the graceful shutdown request.
        let (force_tx, mut force_rx) = watch::channel(false);
        let mut run_all: JoinHandle<curvine_core_error::CommonResult<()>> =
            tokio::spawn(async move {
                while !*force_rx.borrow() {
                    if force_rx.changed().await.is_err() {
                        break;
                    }
                }
                Err::<(), CommonError>(std::io::Error::other("force-cancelled").into())
            });

        let started = std::time::Instant::now();
        let err = drain_after_device_lost(
            &mut run_all,
            &force_tx,
            event(),
            Duration::from_millis(50),
            Duration::from_millis(20),
        )
        .await
        .expect_err("a stalled session must fail");

        assert!(err.to_string().contains("force-cancelled"), "got: {err}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the forced path must be bounded"
        );
    }

    #[tokio::test]
    async fn mounts_outlive_every_worker() {
        let released = Arc::new(AtomicBool::new(false));
        let mount_released_at_worker_exit = Arc::new(AtomicBool::new(true));

        struct MountGuard(Arc<AtomicBool>);
        impl Drop for MountGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let mount = MountGuard(released.clone()); // stands in for `mnts`

        let mut workers: JoinSet<crate::FuseResult<()>> = JoinSet::new();
        let released_in_worker = released.clone();
        let observed = mount_released_at_worker_exit.clone();
        workers.spawn(async move {
            struct RecordOnDrop(Arc<AtomicBool>, Arc<AtomicBool>);
            impl Drop for RecordOnDrop {
                fn drop(&mut self) {
                    self.1
                        .store(self.0.load(Ordering::SeqCst), Ordering::SeqCst);
                }
            }
            let _record = RecordOnDrop(released_in_worker, observed);
            std::future::pending::<()>().await;
            Ok(())
        });

        let (force_tx, force_rx) = watch::channel(false);
        let supervisor = tokio::spawn(supervise_workers(workers, force_rx));
        force_tx.send(true).expect("force receiver is alive");
        let _ = supervisor.await;

        assert!(
            !mount_released_at_worker_exit.load(Ordering::SeqCst),
            "the mount must still be alive when the worker exits"
        );

        drop(mount);
        assert!(released.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn the_cleanup_guard_stops_the_watcher_before_unmount() {
        let mut guard = RunCleanupGuard {
            watcher: Some(tokio::spawn(async {
                std::future::pending::<()>().await;
            })),
            enabled: false,
        };

        guard.stop().await;

        assert!(guard.watcher.is_none(), "stop() must take the handle");
    }
}
