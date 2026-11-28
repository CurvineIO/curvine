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

use crate::fs::FileSystem;
use crate::session::FuseMnt;
use crate::{FuseError, FuseResult, FuseUtils, FUSE_BUFFER_HEADER_SIZE};
use curvine_config::FuseConf;
use curvine_core_error::err_msg;
use curvine_io::IOError;
use curvine_runtime::runtime::Runtime;
use curvine_runtime::sync::channel::AsyncChannel;
use curvine_runtime::sync::FastDashMap;
use curvine_sys::pipe::{Pipe2, PipeFd};
use log::warn;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;

mod fuse_receiver;
pub use self::fuse_receiver::FuseReceiver;

mod fuse_sender;
pub use self::fuse_sender::FuseSender;

pub struct FuseChannel<T> {
    pub senders: Vec<FuseSender<T>>,
    pub receivers: Vec<FuseReceiver<T>>,
}

/// Shared exit decision for the receive/send loops.
///
/// Both loops must stop when the owner broadcasts shutdown, and also when the
/// watch channel closes (the owner is gone). Blocking forever on a task queue
/// that a detached request keeps open would hang the whole teardown path.
pub(super) fn should_exit_on_shutdown<E>(
    changed: Result<(), E>,
    shutdown_rx: &watch::Receiver<bool>,
) -> bool {
    match changed {
        Ok(()) if *shutdown_rx.borrow() => true,
        Ok(()) => false,
        Err(_) => true,
    }
}

/// Detached metadata-task handles owned by one receiver.
///
/// Metadata requests are spawned detached so the receiver can keep reading, and
/// every in-flight task holds a clone of the task-queue producer. A request that
/// blocks (for example a `SETLKW` waiting on a lock) therefore keeps the queue
/// open, so a sender waiting on `recv()` never observes channel closure and the
/// session hangs before `unmount()`. Tracking the handles lets the receiver
/// cancel and join them on every exit path.
#[derive(Default)]
pub(super) struct MetaTaskTracker {
    tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl MetaTaskTracker {
    /// Prune finished handles only once the list grows past this, so the hot
    /// path stays a single push under a short-lived lock.
    const PRUNE_THRESHOLD: usize = 64;

    pub(super) fn track(&self, handle: JoinHandle<()>) {
        let mut tasks = self.lock();
        if tasks.len() >= Self::PRUNE_THRESHOLD {
            tasks.retain(|task| !task.is_finished());
        }
        tasks.push(handle);
    }

    /// Abort every tracked task and wait at most `grace` for them to unwind.
    ///
    /// Returns how many tasks were cancelled. A task that ignores cancellation
    /// is abandoned after `grace` instead of blocking teardown again.
    pub(super) async fn shutdown(&self, grace: Duration) -> usize {
        let handles: Vec<JoinHandle<()>> = {
            let mut tasks = self.lock();
            tasks.drain(..).collect()
        };
        if handles.is_empty() {
            return 0;
        }

        let cancelled = handles.len();
        for handle in &handles {
            handle.abort();
        }
        let joined = tokio::time::timeout(grace, async {
            for handle in handles {
                let _ = handle.await;
            }
        })
        .await;
        if joined.is_err() {
            warn!("{cancelled} metadata task(s) still running after {grace:?}; abandoning them");
        }
        cancelled
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<JoinHandle<()>>> {
        match self.tasks.lock() {
            Ok(tasks) => tasks,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[derive(Debug)]
pub(super) enum SplicePipeSetupError {
    Capacity(IOError),
    AsyncRegistration(IOError),
}

impl SplicePipeSetupError {
    fn io_error(&self) -> &IOError {
        match self {
            Self::Capacity(error) | Self::AsyncRegistration(error) => error,
        }
    }
}

pub(super) fn new_splice_pipe(buf_size: usize) -> Result<Pipe2, SplicePipeSetupError> {
    let pipe_fd = PipeFd::new(buf_size, false, false)
        .map_err(|error| SplicePipeSetupError::Capacity(error.into()))?;
    Pipe2::new(pipe_fd).map_err(|error| SplicePipeSetupError::AsyncRegistration(error.into()))
}

impl<T: FileSystem> FuseChannel<T> {
    pub fn new(fs: Arc<T>, rt: Arc<Runtime>, mnt: &FuseMnt, conf: &FuseConf) -> FuseResult<Self> {
        let max_readahead = conf.max_readahead_kb.unwrap_or(0).saturating_mul(1024);
        let buf_size =
            FuseUtils::get_fuse_buf_size().max(max_readahead as usize + FUSE_BUFFER_HEADER_SIZE);

        // Resolve `tasks_per_mnt == 0` ("follow io_threads") here, on read, rather than
        // normalizing it in FuseConf::init. Resolving on read lets this consumer always
        // see the current io_threads.
        let tasks_per_mnt = conf.effective_tasks_per_mnt();
        let mut receivers = Vec::with_capacity(tasks_per_mnt);
        let mut senders = Vec::with_capacity(tasks_per_mnt);
        let pending_requests = Arc::new(FastDashMap::default());
        let mnt_label = mnt.path.to_string_lossy().into_owned();
        for idx in 0..tasks_per_mnt {
            let (tx, rx) = AsyncChannel::new(conf.fuse_channel_size).split();
            let fd = mnt.create_async_task_fd(conf.clone_fd)?;

            let sender = FuseSender::new(
                fs.clone(),
                rt.clone(),
                fd.clone(),
                rx,
                buf_size,
                conf.debug,
                conf.enable_splice,
                &mnt_label,
                idx,
                conf.metrics_enabled,
            )
            .map_err(|err| splice_pipe_error("sender", err, buf_size))?;

            let receiver = FuseReceiver::new(
                fs.clone(),
                rt.clone(),
                fd,
                tx,
                buf_size,
                conf.debug,
                conf.audit_logging_enabled,
                conf.metrics_enabled,
                pending_requests.clone(),
                conf.enable_splice,
            )
            .map_err(|err| splice_pipe_error("receiver", err, buf_size))?;

            senders.push(sender);
            receivers.push(receiver);
        }

        Ok(Self { senders, receivers })
    }
}

fn splice_pipe_error(
    component: &str,
    error: SplicePipeSetupError,
    requested_size: usize,
) -> FuseError {
    let errno = error
        .io_error()
        .raw_error()
        .raw_os_error()
        .unwrap_or(libc::EIO);
    let message = splice_pipe_error_message(component, &error, requested_size, pipe_max_size());
    FuseError::from_errno_msg(errno, err_msg!("{}", message).into())
}

fn splice_pipe_error_message(
    component: &str,
    error: &SplicePipeSetupError,
    requested_size: usize,
    pipe_max_size: Option<usize>,
) -> String {
    let limit = pipe_max_size
        .map(|value| format!(", /proc/sys/fs/pipe-max-size={value}"))
        .unwrap_or_else(|| ", /proc/sys/fs/pipe-max-size is unavailable".to_string());

    match error {
        SplicePipeSetupError::Capacity(error)
            if error.raw_error().raw_os_error() == Some(libc::EPERM) =>
        {
            format!(
                "FUSE mount succeeded, but zero-copy splice {component} pipe setup failed: F_SETPIPE_SZ was denied while requesting {requested_size} bytes{limit}. \
                 This commonly occurs in a restricted container. Grant CAP_SYS_RESOURCE, raise the host pipe-max-size, \
                 or disable zero-copy FUSE splice with [fuse] enable_splice = false. Original error: {error}"
            )
        }
        SplicePipeSetupError::Capacity(error) => format!(
            "FUSE mount succeeded, but zero-copy splice {component} pipe capacity setup failed while requesting {requested_size} bytes{limit}. \
             Pipe creation or capacity initialization error: {error}"
        ),
        SplicePipeSetupError::AsyncRegistration(error) => format!(
            "FUSE mount succeeded, but zero-copy splice {component} pipe asynchronous I/O registration failed after pipe setup. \
             This is not a pipe-capacity failure. Original error: {error}"
        ),
    }
}

fn pipe_max_size() -> Option<usize> {
    std::fs::read_to_string("/proc/sys/fs/pipe-max-size")
        .ok()?
        .trim()
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::{
        should_exit_on_shutdown, splice_pipe_error, splice_pipe_error_message, MetaTaskTracker,
        SplicePipeSetupError,
    };
    use curvine_io::IOError;
    use std::time::Duration;
    use tokio::sync::watch;

    #[test]
    fn shutdown_decision_follows_the_broadcast() {
        let (tx, rx) = watch::channel(false);
        assert!(!should_exit_on_shutdown::<()>(Ok(()), &rx));
        tx.send(true).unwrap();
        assert!(should_exit_on_shutdown::<()>(Ok(()), &rx));
    }

    #[test]
    fn shutdown_decision_exits_when_the_owner_is_gone() {
        let (tx, rx) = watch::channel(false);
        drop(tx);
        assert!(should_exit_on_shutdown::<()>(Err(()), &rx));
    }

    // A detached metadata task that blocks forever must not keep teardown alive:
    // shutdown aborts it and returns instead of waiting for its natural end.
    #[tokio::test]
    async fn tracker_cancels_and_joins_in_flight_tasks() {
        let tracker = MetaTaskTracker::default();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.unwrap();
        tracker.track(handle);

        let cancelled = tokio::time::timeout(
            Duration::from_secs(1),
            tracker.shutdown(Duration::from_secs(1)),
        )
        .await
        .expect("shutdown must not wait for a blocked task");
        assert_eq!(cancelled, 1);
    }

    #[tokio::test]
    async fn tracker_shutdown_without_tasks_is_noop() {
        let tracker = MetaTaskTracker::default();
        assert_eq!(tracker.shutdown(Duration::from_secs(1)).await, 0);
    }

    #[test]
    fn pipe_capacity_permission_error_explains_container_remediation() {
        let error = SplicePipeSetupError::Capacity(IOError::from(
            std::io::Error::from_raw_os_error(libc::EPERM),
        ));
        let message = splice_pipe_error_message("sender", &error, 1_052_672, Some(1_048_576));

        assert!(message.contains("mount succeeded"), "message: {message}");
        assert!(message.contains("sender"), "message: {message}");
        assert!(message.contains("F_SETPIPE_SZ"), "message: {message}");
        assert!(message.contains("1052672"), "message: {message}");
        assert!(message.contains("1048576"), "message: {message}");
        assert!(message.contains("CAP_SYS_RESOURCE"), "message: {message}");
        assert!(
            message.contains("enable_splice = false"),
            "message: {message}"
        );
    }

    #[test]
    fn splice_pipe_error_preserves_errno_and_diagnostic() {
        let error = SplicePipeSetupError::Capacity(IOError::from(
            std::io::Error::from_raw_os_error(libc::EPERM),
        ));
        let fuse_error = splice_pipe_error("receiver", error, 1_052_672);

        assert_eq!(fuse_error.errno, libc::EPERM);
        let message = fuse_error.error.to_string();
        assert!(message.contains("mount succeeded"), "message: {message}");
        assert!(message.contains("receiver"), "message: {message}");
        assert!(message.contains("CAP_SYS_RESOURCE"), "message: {message}");
    }

    #[test]
    fn async_pipe_registration_error_does_not_claim_capacity_denial() {
        let error = SplicePipeSetupError::AsyncRegistration(IOError::from(
            std::io::Error::from_raw_os_error(libc::EPERM),
        ));
        let message = splice_pipe_error_message("receiver", &error, 1_052_672, Some(1_048_576));

        assert!(
            message.contains("asynchronous I/O registration"),
            "message: {message}"
        );
        assert!(
            !message.contains("F_SETPIPE_SZ was denied"),
            "message: {message}"
        );
        assert!(!message.contains("CAP_SYS_RESOURCE"), "message: {message}");
    }

    #[test]
    fn generic_pipe_capacity_error_does_not_claim_linux_fcntl_failure() {
        let error = SplicePipeSetupError::Capacity(IOError::from(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "unsupported operation",
        )));
        let message = splice_pipe_error_message("sender", &error, 1_052_672, None);

        assert!(message.contains("Pipe creation or capacity initialization"));
        assert!(!message.contains("F_SETPIPE_SZ"), "message: {message}");
    }
}
