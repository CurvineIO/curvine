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
use std::sync::Arc;

mod fuse_receiver;
pub use self::fuse_receiver::FuseReceiver;

mod fuse_sender;
pub use self::fuse_sender::FuseSender;

pub struct FuseChannel<T> {
    pub senders: Vec<FuseSender<T>>,
    pub receivers: Vec<FuseReceiver<T>>,
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
    use super::{splice_pipe_error, splice_pipe_error_message, SplicePipeSetupError};
    use curvine_io::IOError;

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
