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

use crate::fuse_metrics::{
    FuseMetrics, FuseReqCtx, FuseReqStatus, FuseRespMetrics, ENQUEUE_REASON_CHANNEL_CLOSED,
    NOTIFY_ENQUEUE_FAILED, REPLY_TYPE_NO_REPLY,
};
use crate::raw::fuse_abi::{
    fuse_notify_inval_entry_out, fuse_notify_inval_inode_out, fuse_out_header,
};
use crate::session::{FuseNotifyCode, FuseTask};
use crate::{FuseError, FuseResult, FuseUtils};
use crate::{FUSE_NOTIFY_UNIQUE, FUSE_OUT_HEADER_LEN, FUSE_SUCCESS};
use bytes::BytesMut;
use curvine_core_error::ternary;
use curvine_io::DataSlice;
use curvine_io::IOResult;
use curvine_runtime::sync::channel::AsyncSender;
use log::{info, warn};
use parking_lot::Mutex;
use std::fmt::Debug;
use std::io::IoSlice;
use std::sync::{Arc, OnceLock};
use std::vec;

#[cfg(target_os = "linux")]
const FALLBACK_IOV_MAX: usize = libc::UIO_MAXIOV as usize;
#[cfg(not(target_os = "linux"))]
const FALLBACK_IOV_MAX: usize = 16;

#[derive(Debug)]
pub struct ResponseData {
    header: fuse_out_header,
    data: Vec<DataSlice>,
}

impl ResponseData {
    pub fn unique(&self) -> u64 {
        self.header.unique
    }

    pub fn header(&self) -> &fuse_out_header {
        &self.header
    }

    pub fn len(&self) -> u32 {
        self.header.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iovec_max() -> usize {
        static IOV_MAX: OnceLock<usize> = OnceLock::new();
        *IOV_MAX.get_or_init(|| {
            let limit = nix::unistd::sysconf(nix::unistd::SysconfVar::IOV_MAX)
                .ok()
                .flatten();
            Self::iovec_max_or_fallback(limit)
        })
    }

    fn iovec_max_or_fallback(limit: Option<libc::c_long>) -> usize {
        match limit {
            Some(limit) if limit > 0 => limit as usize,
            _ => FALLBACK_IOV_MAX,
        }
    }

    fn checked_iovec_count(data: &[DataSlice]) -> IOResult<usize> {
        let count = match data.len().checked_add(1) {
            Some(count) => count,
            None => return curvine_core_error::err_box!("FUSE response iovec count overflow"),
        };
        let max = Self::iovec_max();
        if count > max {
            return curvine_core_error::err_box!(
                "FUSE response iovec count {} exceeds IOV_MAX {}",
                count,
                max
            );
        }
        Ok(count)
    }

    fn checked_frame_len(data: &[DataSlice]) -> IOResult<usize> {
        let mut len = FUSE_OUT_HEADER_LEN;
        for slice in data {
            len = match len.checked_add(slice.len()) {
                Some(len) => len,
                None => return curvine_core_error::err_box!("FUSE response length overflow"),
            };
        }
        Ok(len)
    }

    pub fn as_iovec(&self) -> IOResult<(usize, Vec<IoSlice<'_>>)> {
        let count = Self::checked_iovec_count(&self.data)?;
        let actual_len = Self::checked_frame_len(&self.data)?;
        if actual_len != self.header.len as usize {
            return curvine_core_error::err_box!(
                "FUSE response length mismatch: header {}, actual {}",
                self.header.len,
                actual_len
            );
        }

        let mut iovec: Vec<IoSlice<'_>> = Vec::with_capacity(count);

        let header_bytes = FuseUtils::struct_as_bytes(&self.header);
        iovec.push(IoSlice::new(header_bytes));

        for data in &self.data {
            // FUSE iovec replies require memory-backed data, not fd-backed IOSlice regions.
            if matches!(data, DataSlice::IOSlice(_)) {
                return curvine_core_error::err_box!(
                    "DataSlice::IOSlice is not supported in FUSE iovec responses"
                );
            }
            iovec.push(IoSlice::new(data.as_slice()));
        }
        Ok((actual_len, iovec))
    }

    fn create(unique: u64, error: i32, data: Vec<DataSlice>) -> IOResult<Self> {
        Self::checked_iovec_count(&data)?;
        let frame_len = Self::checked_frame_len(&data)?;
        let frame_len = match u32::try_from(frame_len) {
            Ok(frame_len) => frame_len,
            Err(_) => {
                return curvine_core_error::err_box!(
                    "FUSE response length {} exceeds u32::MAX",
                    frame_len
                )
            }
        };
        let error = ternary!(unique == FUSE_NOTIFY_UNIQUE, error, -error);

        // The fuse error code is the negative number of the os error code.
        let header = fuse_out_header {
            len: frame_len,
            error,
            unique,
        };

        Ok(Self { header, data })
    }
}

#[derive(Clone)]
pub struct FuseResponse {
    pub(crate) unique: u64,
    pub(crate) sender: AsyncSender<FuseTask>,
    pub(crate) debug: bool,
    pub(crate) metrics: Option<Arc<Mutex<FuseRespMetrics>>>,
}

impl FuseResponse {
    pub(crate) fn new_reply(
        unique: u64,
        sender: AsyncSender<FuseTask>,
        debug: bool,
        ctx: Option<FuseReqCtx>,
    ) -> Self {
        let metrics = ctx.map(|c| Arc::new(Mutex::new(FuseRespMetrics::new(c))));
        Self {
            unique,
            sender,
            debug,
            metrics,
        }
    }

    pub fn unique(&self) -> u64 {
        self.unique
    }

    /// FS-operation status stashed by the finish path.
    pub(crate) fn metrics_op_status(&self) -> Option<FuseReqStatus> {
        self.metrics.as_ref().and_then(|m| m.lock().op_status)
    }

    fn rep_log(&self, e: &FuseError) {
        if self.debug
            || !matches!(
                e.errno,
                libc::ENOENT | libc::ENODATA | libc::ENOSYS | libc::ENOTEMPTY
            )
        {
            warn!("send_rep unique {}: {}", self.unique, e);
        }
    }

    fn create_success_response(
        &self,
        data: Vec<DataSlice>,
    ) -> IOResult<(ResponseData, FuseReqStatus, i32)> {
        match ResponseData::create(self.unique, FUSE_SUCCESS, data) {
            Ok(data) => Ok((data, FuseReqStatus::Success, 0)),
            Err(error) => {
                warn!(
                    "failed to build FUSE response unique {}: {}; replying EIO",
                    self.unique, error
                );
                let errno = libc::EIO;
                let data = ResponseData::create(self.unique, errno, vec![])?;
                Ok((data, FuseReqStatus::Error, errno))
            }
        }
    }

    /// Classify a *non-Ok* reply into a `FuseReqStatus` from the explicit source tag —
    /// **never from errno alone** (a backend `ENOSYS`/`EINTR` with no tag stays `Error`).
    fn err_status(unsupported_reason: Option<&'static str>, interrupted: bool) -> FuseReqStatus {
        // Source tags are mutually exclusive; catch future mis-wiring.
        debug_assert!(
            unsupported_reason.is_none() || !interrupted,
            "send_rep_tagged: unsupported_reason and interrupted are mutually exclusive"
        );
        if unsupported_reason.is_some() {
            FuseReqStatus::Unsupported
        } else if interrupted {
            FuseReqStatus::Interrupted
        } else {
            FuseReqStatus::Error
        }
    }

    async fn finish_request(
        &self,
        data: ResponseData,
        status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
    ) -> IOResult<()> {
        let slot = match &self.metrics {
            None => {
                return self
                    .sender
                    .send(FuseTask::Reply(data))
                    .await
                    .map_err(Into::into);
            }
            Some(slot) => slot,
        };

        // Reserve first, without touching the slot: a bounded send().await can
        // suspend on a full channel, and a cancel there would drop the ActiveGuard
        // (decrementing active_requests) while emitting NO terminal metric. The
        // reserve is the only suspend point; if cancelled the slot stays unfinished
        // and the guard is dropped cleanly, no half-finished state.
        if self.sender.is_bounded() {
            let permit = match self.sender.reserve().await {
                Ok(p) => p,
                Err(e) => {
                    // Channel closed before reserve: finish the pending slot now.
                    self.finish_enqueue_failure(slot, status, errno, unsupported_reason);
                    return Err(e.into());
                }
            };
            // Permit send is synchronous; commit and enqueue have no await gap.
            let task = match self.commit_reply_task(slot, data, status, errno, unsupported_reason) {
                Some(task) => task,
                None => return Ok(()), // double reply: warned/asserted in commit.
            };
            permit.send(task);
            return Ok(());
        }

        // Unbounded fast path: commit before send is safe because this branch has
        // no await point before enqueue.
        let task = match self.commit_reply_task(slot, data, status, errno, unsupported_reason) {
            Some(task) => task,
            None => return Ok(()),
        };
        let send_result = self.sender.send(task).await;
        if send_result.is_err() {
            self.record_enqueue_failure_metrics(slot, status, errno, unsupported_reason);
        }
        Ok(send_result?)
    }

    fn commit_reply_task(
        &self,
        slot: &Arc<Mutex<FuseRespMetrics>>,
        data: ResponseData,
        status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
    ) -> Option<FuseTask> {
        let (labels, active) = {
            let mut m = slot.lock();
            if m.finished {
                // Double reply on an already-finished context is a logic bug.
                debug_assert!(
                    !m.finished,
                    "double reply on an already-finished FuseResponse (unique {})",
                    self.unique
                );
                warn!(
                    "double reply on an already-finished FuseResponse (unique {})",
                    self.unique
                );
                return None;
            }
            m.op_status = Some(status);
            m.request_status = Some(status);
            m.errno = errno;
            m.unsupported_reason = unsupported_reason;
            m.finished = true;
            let active = m
                .active
                .take()
                .unwrap_or_else(crate::fuse_metrics::ActiveGuard::noop);
            (m.labels, active)
        };
        let queue_guard = FuseMetrics::reply_queue_guard();
        Some(FuseTask::RequestReply {
            data,
            labels,
            active,
            status,
            errno,
            unsupported_reason,
            queue_guard,
        })
    }

    /// Finish a still-pending slot after bounded reserve sees a closed channel.
    fn finish_enqueue_failure(
        &self,
        slot: &Arc<Mutex<FuseRespMetrics>>,
        status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
    ) {
        {
            let mut m = slot.lock();
            if m.finished {
                return;
            }
            m.op_status = Some(status);
            m.request_status = Some(FuseReqStatus::Error);
            m.errno = errno;
            m.unsupported_reason = unsupported_reason;
            m.finished = true;
            let _ = m.active.take();
        }
        self.record_enqueue_failure_metrics(slot, status, errno, unsupported_reason);
    }

    fn record_enqueue_failure_metrics(
        &self,
        slot: &Arc<Mutex<FuseRespMetrics>>,
        status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
    ) {
        let labels = {
            let mut m = slot.lock();
            m.request_status = Some(FuseReqStatus::Error);
            m.labels
        };
        let metrics = FuseMetrics::get();
        metrics.record_reply_enqueue_error(labels.opcode, ENQUEUE_REASON_CHANNEL_CLOSED);
        metrics.record_request_duration(
            labels.opcode,
            labels.kind,
            FuseReqStatus::Error,
            labels.elapsed_us(),
        );
        metrics.record_op_terminal(
            labels.opcode,
            labels.kind,
            status,
            errno,
            unsupported_reason,
        );
    }

    fn finish_no_reply(&self, res: FuseResult<()>) {
        if let Some(slot) = &self.metrics {
            // Classify no-reply results explicitly, independent of prior slot state.
            let status = match &res {
                Ok(_) => FuseReqStatus::Success,
                Err(_) => FuseReqStatus::Error,
            };
            let labels = {
                let mut m = slot.lock();
                if m.finished {
                    return;
                }
                m.op_status = Some(status);
                m.request_status = Some(status);
                m.finished = true;
                let _ = m.active.take();
                m.labels
            }; // lock dropped before recording metrics.

            let metrics = FuseMetrics::get();
            metrics.record_request_total(labels.opcode, labels.kind, REPLY_TYPE_NO_REPLY, status);
            metrics.record_request_duration(
                labels.opcode,
                labels.kind,
                status,
                labels.elapsed_us(),
            );
        }
    }

    /// Finish a no-reply request whose opcode payload could not be decoded.
    pub(crate) fn finish_early(&self, errno: i32, reason: &'static str) {
        if let Some(slot) = &self.metrics {
            {
                let mut m = slot.lock();
                if m.finished {
                    return;
                }
                m.op_status = Some(FuseReqStatus::Error);
                m.request_status = Some(FuseReqStatus::Error);
                m.errno = errno;
                m.parse_reason = Some(reason);
                m.finished = true;
                let _ = m.active.take();
            } // lock dropped before recording the metric.

            // Parse failed after ctx creation: record decode error, not `requests_total`.
            FuseMetrics::get().record_parse_error(reason);
        }
    }

    /// Complete a request whose header was decoded but whose opcode payload was malformed.
    pub(crate) async fn send_parse_error(
        &self,
        reason: &'static str,
        message: String,
    ) -> IOResult<()> {
        if let Some(slot) = &self.metrics {
            {
                let mut m = slot.lock();
                m.parse_reason = Some(reason);
            }
            FuseMetrics::get().record_parse_error(reason);
        }

        let error: FuseResult<()> = Err(FuseError::from_errno_msg(
            libc::EPROTO,
            format!("Malformed FUSE request payload: {message}").into(),
        ));
        self.send_rep(error).await
    }

    pub async fn send_rep<T: Debug, E: Into<FuseError> + Debug>(
        &self,
        res: Result<T, E>,
    ) -> IOResult<()> {
        self.send_rep_tagged(res, None, false).await
    }

    /// Like `send_rep`, but lets callers tag unsupported or interrupted error sources explicitly.
    pub async fn send_rep_tagged<T: Debug, E: Into<FuseError> + Debug>(
        &self,
        res: Result<T, E>,
        unsupported_reason: Option<&'static str>,
        interrupted: bool,
    ) -> IOResult<()> {
        let (data, status, errno) = match res {
            Ok(v) => {
                if self.debug {
                    info!("send_rep unique {}, res: {:?}", self.unique, v);
                }

                let data = if size_of::<T>() == 0 {
                    vec![]
                } else {
                    vec![DataSlice::buffer(FuseUtils::struct_as_buf(&v))]
                };
                self.create_success_response(data)?
            }

            Err(e) => {
                let e = e.into();
                self.rep_log(&e);
                let errno = e.errno;
                let status = Self::err_status(unsupported_reason, interrupted);
                (
                    ResponseData::create(self.unique, errno, vec![])?,
                    status,
                    errno,
                )
            }
        };

        self.finish_request(data, status, errno, unsupported_reason)
            .await
    }

    pub async fn send_notify(&self, code: FuseNotifyCode, data: Vec<DataSlice>) -> IOResult<()> {
        if self.debug {
            info!("send_notify code {:?}", code);
        }

        let data = ResponseData::create(FUSE_NOTIFY_UNIQUE, code.into(), data)?;
        if self.metrics.is_some() {
            self.send_notify_metrics(code, data).await
        } else {
            self.sender
                .send(FuseTask::Reply(data))
                .await
                .map_err(Into::into)
        }
    }

    async fn send_notify_metrics(&self, code: FuseNotifyCode, data: ResponseData) -> IOResult<()> {
        let code_str = code.as_str();

        if self.sender.is_bounded() {
            let permit = match self.sender.reserve().await {
                Ok(p) => p,
                Err(e) => {
                    // Closed channel before reserve means the notify never reaches sender.
                    FuseMetrics::get().record_notify_result(code_str, NOTIFY_ENQUEUE_FAILED);
                    return Err(e.into());
                }
            };
            permit.send(FuseTask::NotifyReply {
                data,
                code: code_str,
                queue_guard: FuseMetrics::reply_queue_guard(),
            });
            return Ok(());
        }

        let send_result = self
            .sender
            .send(FuseTask::NotifyReply {
                data,
                code: code_str,
                queue_guard: FuseMetrics::reply_queue_guard(),
            })
            .await;
        if send_result.is_err() {
            FuseMetrics::get().record_notify_result(code_str, NOTIFY_ENQUEUE_FAILED);
        }
        Ok(send_result?)
    }

    // `send_buf` / `send_data` have no source-tag variant; tagged errors use `send_rep_tagged`.
    pub async fn send_buf(&self, res: FuseResult<BytesMut>) -> IOResult<()> {
        let (data, status, errno) = match res {
            Ok(v) => {
                if self.debug {
                    info!("send_buf unique {}, data len: {}", self.unique, v.len());
                }
                self.create_success_response(vec![DataSlice::Buffer(v)])?
            }

            Err(e) => {
                self.rep_log(&e);
                let errno = e.errno;
                (
                    ResponseData::create(self.unique, errno, vec![])?,
                    FuseReqStatus::Error,
                    errno,
                )
            }
        };

        self.finish_request(data, status, errno, None).await
    }

    pub async fn send_data(&self, res: FuseResult<Vec<DataSlice>>) -> IOResult<()> {
        let (data, status, errno) = match res {
            Ok(v) => {
                if self.debug {
                    let len = v.iter().map(|x| x.len()).sum::<usize>();
                    info!("send_data unique {}, data len: {}", self.unique, len);
                }
                self.create_success_response(v)?
            }

            Err(e) => {
                self.rep_log(&e);
                let errno = e.errno;
                (
                    ResponseData::create(self.unique, errno, vec![])?,
                    FuseReqStatus::Error,
                    errno,
                )
            }
        };

        self.finish_request(data, status, errno, None).await
    }

    pub fn send_none(&self, res: FuseResult<()>) -> IOResult<()> {
        // Protocol no-reply operations still finish their request context.
        self.finish_no_reply(res);
        Ok(())
    }

    pub async fn send_inode_out(&self, ino: u64, off: i64, len: i64) -> IOResult<()> {
        let arg = fuse_notify_inval_inode_out { ino, off, len };
        let data = vec![DataSlice::buffer(FuseUtils::struct_as_buf(&arg))];
        self.send_notify(FuseNotifyCode::FUSE_NOTIFY_INVAL_INODE, data)
            .await
    }

    pub async fn send_rep_then_inval_inode<T: Debug, E: Into<FuseError> + Debug>(
        &self,
        res: Result<T, E>,
        ino: u64,
        off: i64,
        len: i64,
    ) -> IOResult<()> {
        self.send_rep(res).await?;
        self.send_inode_out(ino, off, len).await
    }

    pub async fn send_rep_then_inval_entry<E: Into<FuseError> + Debug>(
        &self,
        res: Result<(), E>,
        parent: u64,
        name: &str,
    ) -> IOResult<()> {
        self.send_rep(res).await?;
        self.send_entry_out(parent, name).await
    }

    pub async fn send_entry_out(&self, parent: u64, name: &str) -> IOResult<()> {
        let arg = fuse_notify_inval_entry_out {
            parent,
            namelen: name.len() as u32,
            flags: 0,
        };

        let mut name_buf = BytesMut::with_capacity(name.len() + 1);
        name_buf.extend_from_slice(name.as_bytes());
        name_buf.extend_from_slice(b"\0");

        let data = vec![
            DataSlice::buffer(FuseUtils::struct_as_buf(&arg)),
            DataSlice::buffer(name_buf),
        ];
        self.send_notify(FuseNotifyCode::FUSE_NOTIFY_INVAL_ENTRY, data)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fuse_metrics::{
        ActiveGuard, FuseMetrics, FuseReqKind, FuseReqLabels, DECODE_PHASE_PARSE,
        ENQUEUE_REASON_CHANNEL_CLOSED, NOTIFY_ENQUEUE_FAILED, REPLY_TYPE_NO_REPLY,
        REPLY_TYPE_REPLIED,
    };
    use curvine_metrics::{Gauge, Metrics as m};
    use curvine_runtime::sync::channel::{AsyncChannel, AsyncReceiver};

    #[test]
    fn as_iovec_rejects_io_slice_without_panicking() {
        let response = ResponseData {
            header: fuse_out_header {
                len: (FUSE_OUT_HEADER_LEN + 1) as u32,
                error: 0,
                unique: 1,
            },
            data: vec![DataSlice::io_slice(-1, None, 1)],
        };

        let error = match response.as_iovec() {
            Ok(_) => panic!("IOSlice response must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("DataSlice::IOSlice"));
    }

    #[test]
    fn as_iovec_accepts_memory_backed_data() {
        let payload = b"reply";
        let response = ResponseData {
            header: fuse_out_header {
                len: (FUSE_OUT_HEADER_LEN + payload.len()) as u32,
                error: 0,
                unique: 1,
            },
            data: vec![DataSlice::buffer(BytesMut::from(&payload[..]))],
        };

        let (len, iovec) = response.as_iovec().unwrap();
        assert_eq!(len, FUSE_OUT_HEADER_LEN + payload.len());
        assert_eq!(iovec.len(), 2);
        assert_eq!(&*iovec[1], payload);
    }

    #[test]
    fn create_rejects_response_length_over_u32_max() {
        let response =
            ResponseData::create(1, 0, vec![DataSlice::io_slice(-1, None, u32::MAX as usize)]);

        let error = response.expect_err("oversized response must be rejected");
        assert!(error.to_string().contains("exceeds u32::MAX"));
    }

    #[test]
    fn create_rejects_iovec_count_over_platform_limit() {
        let data = std::iter::repeat_with(DataSlice::empty)
            .take(ResponseData::iovec_max())
            .collect();

        let error =
            ResponseData::create(1, 0, data).expect_err("excessive iovec count must be rejected");
        assert!(error.to_string().contains("exceeds IOV_MAX"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_iovec_fallback_uses_uio_maxiov() {
        assert_eq!(
            ResponseData::iovec_max_or_fallback(None),
            libc::UIO_MAXIOV as usize
        );
        assert_eq!(ResponseData::iovec_max_or_fallback(Some(0)), 1024);
    }

    #[test]
    fn as_iovec_rejects_header_length_mismatch() {
        let response = ResponseData {
            header: fuse_out_header {
                len: FUSE_OUT_HEADER_LEN as u32,
                error: 0,
                unique: 1,
            },
            data: vec![DataSlice::bytes(bytes::Bytes::from_static(b"payload"))],
        };

        let error = response
            .as_iovec()
            .expect_err("mismatched response length must be rejected");
        assert!(error.to_string().contains("length mismatch"));
    }

    // The finish paths (`finish_no_reply` / `finish_early` / enqueue-failure)
    // now read `FuseMetrics::get()`, which panics if the process-global registry
    // was never initialized. `ensure_init` is idempotent, so every test that
    // exercises a real finish path calls this first.
    fn init_metrics() {
        FuseMetrics::ensure_init().expect("init FuseMetrics for tests");
    }

    // Build a FuseResponse whose active guard is backed by `gauge`, so tests can
    // assert "guard dropped exactly once" as a concrete `gauge.get()` count.
    fn reply_with_gauge(unique: u64, gauge: &Gauge) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        reply_with_gauge_opcode(unique, gauge, "Lookup")
    }

    // Like `reply_with_gauge` but with a caller-chosen opcode label. Value-
    // assertion tests use a UNIQUE opcode each so their counter children never
    // collide with another (parallel) test's deltas on the shared registry.
    fn reply_with_gauge_opcode(
        unique: u64,
        gauge: &Gauge,
        opcode: &'static str,
    ) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        reply_with_gauge_opcode_kind(unique, gauge, opcode, FuseReqKind::Metadata)
    }

    // Like `reply_with_gauge_opcode` but also lets the test choose the kind, so
    // stream-path tests can assert `kind="stream"` labels.
    fn reply_with_gauge_opcode_kind(
        unique: u64,
        gauge: &Gauge,
        opcode: &'static str,
        kind: FuseReqKind,
    ) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        // Metrics-enabled reply path now resolves `reply_queue_guard()` via
        // `get()` (strict), so the singleton must be initialized for any enabled
        // fixture. Idempotent.
        init_metrics();
        let (tx, rx) = AsyncChannel::new(16).split();
        let labels = FuseReqLabels::new(opcode, kind, 64);
        let ctx = FuseReqCtx {
            labels,
            active: Some(ActiveGuard::new(gauge.clone())), // inc to 1 now
        };
        (FuseResponse::new_reply(unique, tx, false, Some(ctx)), rx)
    }

    fn disabled_reply(unique: u64) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        let (tx, rx) = AsyncChannel::new(16).split();
        (FuseResponse::new_reply(unique, tx, false, None), rx)
    }

    #[tokio::test]
    async fn oversized_success_response_falls_back_to_eio_reply() {
        let g = m::new_gauge("oversized_response_active", "test").unwrap();
        let (reply, mut rx) =
            reply_with_gauge_opcode_kind(64, &g, "OversizedRead", FuseReqKind::Stream);
        let data = std::iter::repeat_with(DataSlice::empty)
            .take(ResponseData::iovec_max())
            .collect();

        reply.send_data(Ok(data)).await.unwrap();

        let task = rx.try_recv().unwrap().expect("an EIO reply was enqueued");
        match &task {
            FuseTask::RequestReply {
                data,
                status,
                errno,
                ..
            } => {
                assert_eq!(data.header().error, -libc::EIO);
                assert_eq!(data.len() as usize, FUSE_OUT_HEADER_LEN);
                assert_eq!(*status, FuseReqStatus::Error);
                assert_eq!(*errno, libc::EIO);
            }
            _ => panic!("expected FuseTask::RequestReply"),
        }
        assert!(
            reply.metrics.as_ref().unwrap().lock().finished,
            "fallback reply must finish request metrics"
        );
        drop(task);
        assert_eq!(g.get(), 0, "fallback reply drops the active guard once");
    }

    // T1: a normal metadata reply produces a RequestReply, finishes the slot
    // exactly once, and the active guard is NOT dropped until the task is
    // (i.e. the count is still 1 while the task is in flight, 0 after).
    #[tokio::test]
    async fn t1_request_reply_finishes_once_and_holds_guard_until_task_drops() {
        let g = m::new_gauge("t1_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge(1, &g);
        assert_eq!(g.get(), 1, "guard live after ctx creation");

        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();

        // The slot is finished, and the guard was moved onto the task (still 1).
        {
            let slot = reply.metrics.as_ref().unwrap().lock();
            assert!(slot.finished, "slot marked finished after reply");
            assert!(
                slot.active.is_none(),
                "guard taken out of slot exactly once"
            );
        }
        assert_eq!(g.get(), 1, "guard rides on the task, not yet dropped");

        let task = rx.try_recv().unwrap().expect("a task was enqueued");
        assert!(
            matches!(task, FuseTask::RequestReply { .. }),
            "produced RequestReply"
        );
        drop(task); // sender finish: dropping the task drops the guard
        assert_eq!(g.get(), 0, "guard dropped exactly once at task drop");
    }

    // T13: a real second reply on an already-finished slot is a no-op — no
    // second task enqueued, the guard is not double-taken or double-dropped.
    // Release-only: a double reply trips `debug_assert!(!finished)` in debug
    // builds (see `double_reply_panics_in_debug`); the release behavior is the
    // safe warn+no-op asserted here.
    #[tokio::test]
    #[cfg(not(debug_assertions))]
    async fn t13_real_double_reply_is_noop() {
        let g = m::new_gauge("t13_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge(2, &g);

        // First reply: takes the guard, finishes, enqueues a RequestReply.
        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();
        let t1 = rx.try_recv().unwrap().expect("first task");
        assert!(matches!(t1, FuseTask::RequestReply { .. }));
        assert_eq!(g.get(), 1, "guard rides on the first task");

        // Second reply: slot already finished → no-op, no second task.
        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();
        assert!(rx.try_recv().unwrap().is_none(), "no second task enqueued");

        drop(t1);
        assert_eq!(g.get(), 0, "exactly one guard, dropped once");
    }

    // Debug counterpart: a double reply is a logic bug and must trip the
    // debug_assert. (Release turns this into a safe warn+no-op — see T13.)
    #[tokio::test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "double reply")]
    async fn double_reply_panics_in_debug() {
        let g = m::new_gauge("dbl_reply_dbg_active", "test").unwrap();
        let (reply, _rx) = reply_with_gauge(2, &g);
        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();
        // Second reply on the finished slot trips debug_assert!(!finished).
        let _ = reply.send_rep::<(), FuseError>(Ok(())).await;
    }

    // T6: Forget/BatchForget — finish_no_reply inspects the result, drops the
    // guard, and enqueues NO task. Run for both Ok and Err.
    #[tokio::test]
    async fn t6_no_reply_finishes_without_task_for_ok_and_err() {
        init_metrics();
        // Ok case
        let g_ok = m::new_gauge("t6_ok_active", "test").unwrap();
        let (reply_ok, mut rx_ok) = reply_with_gauge(3, &g_ok);
        reply_ok.send_none(Ok(())).unwrap();
        assert_eq!(g_ok.get(), 0, "no-reply drops the guard");
        assert!(
            rx_ok.try_recv().unwrap().is_none(),
            "no task enqueued on no-reply"
        );
        {
            let slot = reply_ok.metrics.as_ref().unwrap().lock();
            assert!(slot.finished);
            assert_eq!(slot.op_status, Some(FuseReqStatus::Success));
        }

        // Err case — must classify as Error, not a phantom success.
        let g_err = m::new_gauge("t6_err_active", "test").unwrap();
        let (reply_err, mut rx_err) = reply_with_gauge(4, &g_err);
        reply_err.send_none(Err(FuseError::from("boom"))).unwrap();
        assert_eq!(g_err.get(), 0);
        assert!(rx_err.try_recv().unwrap().is_none());
        {
            let slot = reply_err.metrics.as_ref().unwrap().lock();
            assert!(slot.finished);
            assert_eq!(slot.op_status, Some(FuseReqStatus::Error));
        }
    }

    // T7: send_rep_then_inval_inode — the request reply finishes once (a
    // RequestReply), the trailing notification is a NotifyReply that does NOT
    // re-finish the request slot.
    #[tokio::test]
    async fn t7_rep_then_inval_splits_request_and_notify() {
        let g = m::new_gauge("t7_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge(5, &g);

        reply
            .send_rep_then_inval_inode::<(), FuseError>(Ok(()), 1, 0, 0)
            .await
            .unwrap();

        let first = rx.try_recv().unwrap().expect("request reply");
        assert!(
            matches!(first, FuseTask::RequestReply { .. }),
            "first = RequestReply"
        );
        let second = rx.try_recv().unwrap().expect("trailing notify");
        assert!(
            matches!(second, FuseTask::NotifyReply { .. }),
            "second = NotifyReply"
        );

        // Request slot finished exactly once; notify did not touch it again.
        {
            let slot = reply.metrics.as_ref().unwrap().lock();
            assert!(slot.finished);
            assert!(slot.active.is_none());
        }
        drop(first);
        assert_eq!(
            g.get(),
            0,
            "request guard dropped once; notify carried none"
        );
    }

    // T8: malformed no-reply operator — `finish_early` drops the guard, marks
    // the metrics slot finished, and enqueues NO task. No requests_total is emitted.
    #[tokio::test]
    async fn t8_finish_early_drops_guard_no_task() {
        init_metrics();
        let g = m::new_gauge("t8_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge(6, &g);
        reply.finish_early(libc::EINVAL, "other");
        assert_eq!(g.get(), 0, "guard dropped on early finish (no leak)");
        assert!(rx.try_recv().unwrap().is_none(), "no request task enqueued");
        {
            let slot = reply.metrics.as_ref().unwrap().lock();
            assert!(slot.finished);
            assert_eq!(slot.errno, libc::EINVAL, "errno stashed for decode_errors");
            assert_eq!(
                slot.parse_reason,
                Some("other"),
                "parse reason stashed for decode_errors"
            );
            assert_eq!(slot.op_status, Some(FuseReqStatus::Error));
        }
    }

    // T11: metrics disabled — produces the legacy Reply, constructs no metrics
    // slot, and notifications also fall back to Reply AND emit no notify metric
    // (R8d: the disabled production path records nothing).
    #[tokio::test]
    async fn t11_disabled_uses_legacy_reply() {
        init_metrics();
        let code = FuseNotifyCode::FUSE_NOTIFY_INVAL_INODE.as_str();
        let notify_before = FuseMetrics::get()
            .notify_total
            .with_label_values(&[code, "success"])
            .get();

        let (reply, mut rx) = disabled_reply(7);
        assert!(reply.metrics.is_none(), "no metrics slot when disabled");

        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();
        let task = rx.try_recv().unwrap().expect("a task");
        assert!(
            matches!(task, FuseTask::Reply(_)),
            "disabled path = legacy Reply"
        );

        reply
            .send_notify(FuseNotifyCode::FUSE_NOTIFY_INVAL_INODE, vec![])
            .await
            .unwrap();
        let n = rx.try_recv().unwrap().expect("a notify task");
        assert!(
            matches!(n, FuseTask::Reply(_)),
            "disabled notify = legacy Reply"
        );

        // The disabled notify went out as a legacy Reply, so notify_total is
        // untouched (and the sender's Reply arm never records notify metrics).
        assert_eq!(
            FuseMetrics::get()
                .notify_total
                .with_label_values(&[code, "success"])
                .get(),
            notify_before,
            "disabled notify must not increment notify_total"
        );
    }

    // Clone shares the single slot: finishing via a clone marks the original.
    #[tokio::test]
    async fn t13_clone_shares_one_slot() {
        let g = m::new_gauge("t13_clone_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge(8, &g);
        let clone = reply.clone();

        // Finish via the clone.
        clone.send_rep::<(), FuseError>(Ok(())).await.unwrap();

        // The original sees finished=true and the guard gone — shared slot.
        {
            let slot = reply.metrics.as_ref().unwrap().lock();
            assert!(slot.finished, "clone and original share one slot");
            assert!(slot.active.is_none());
        }
        let task = rx.try_recv().unwrap().expect("one task");
        drop(task);
        assert_eq!(g.get(), 0, "single guard, dropped once");
    }

    // #3: reply enqueue failure splits op_status (FS result) from request_status
    // (delivery). FS op succeeds but the channel is closed → request_status=Error
    // while op_status stays Success; guard dropped exactly once (with the
    // consumed task).
    #[tokio::test]
    async fn enqueue_failure_sets_request_status_error_keeps_op_status() {
        init_metrics();
        let g = m::new_gauge("enq_fail_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge(9, &g);
        drop(rx); // close the channel so send() fails

        let send_result = reply.send_rep::<(), FuseError>(Ok(())).await;
        assert!(
            send_result.is_err(),
            "enqueue must fail on a closed channel"
        );

        let slot = reply.metrics.as_ref().unwrap().lock();
        assert!(slot.finished);
        assert_eq!(
            slot.op_status,
            Some(FuseReqStatus::Success),
            "FS op succeeded"
        );
        assert_eq!(
            slot.request_status,
            Some(FuseReqStatus::Error),
            "delivery failed → request_status=Error"
        );
        drop(slot);
        assert_eq!(g.get(), 0, "guard dropped once (with the consumed task)");
    }

    // #4/#5: status is classified from the explicit source tag, never errno.
    #[tokio::test]
    async fn status_classification_from_source_tag_not_errno() {
        // backend ENOSYS with no tag → Error (not laundered into Unsupported).
        let g1 = m::new_gauge("tag_backend_enosys", "test").unwrap();
        let (r1, mut rx1) = reply_with_gauge(10, &g1);
        let err: FuseResult<()> = Err(FuseError::new(libc::ENOSYS, "backend".into()));
        r1.send_rep_tagged(err, None, false).await.unwrap();
        let _ = rx1.try_recv();
        assert_eq!(
            r1.metrics.as_ref().unwrap().lock().op_status,
            Some(FuseReqStatus::Error),
            "untagged ENOSYS is Error"
        );

        // tagged unimplemented_opcode → Unsupported.
        let g2 = m::new_gauge("tag_unimpl", "test").unwrap();
        let (r2, mut rx2) = reply_with_gauge(11, &g2);
        let err: FuseResult<()> = Err(FuseError::new(libc::ENOSYS, "unimpl".into()));
        r2.send_rep_tagged(err, Some("unimplemented_opcode"), false)
            .await
            .unwrap();
        let _ = rx2.try_recv();
        assert_eq!(
            r2.metrics.as_ref().unwrap().lock().op_status,
            Some(FuseReqStatus::Unsupported),
            "tagged path is Unsupported"
        );

        // ordinary EINTR with no interrupt tag → Error (not Interrupted).
        let g3 = m::new_gauge("tag_plain_eintr", "test").unwrap();
        let (r3, mut rx3) = reply_with_gauge(12, &g3);
        let err: FuseResult<()> = Err(FuseError::new(libc::EINTR, "plain".into()));
        r3.send_rep_tagged(err, None, false).await.unwrap();
        let _ = rx3.try_recv();
        assert_eq!(
            r3.metrics.as_ref().unwrap().lock().op_status,
            Some(FuseReqStatus::Error),
            "untagged EINTR is Error"
        );

        // interrupt source tag → Interrupted.
        let g4 = m::new_gauge("tag_interrupt", "test").unwrap();
        let (r4, mut rx4) = reply_with_gauge(13, &g4);
        let err: FuseResult<()> = Err(FuseError::new(libc::EINTR, "setlkw".into()));
        r4.send_rep_tagged(err, None, true).await.unwrap();
        let _ = rx4.try_recv();
        assert_eq!(
            r4.metrics.as_ref().unwrap().lock().op_status,
            Some(FuseReqStatus::Interrupted),
            "interrupt-tagged path is Interrupted"
        );
    }

    // The process-global registry accumulates across tests, so value assertions
    // read a child's counter/histogram before and after and check the delta.
    fn requests_total(opcode: &str, kind: &str, reply_type: &str, status: &str) -> i64 {
        FuseMetrics::get()
            .requests_total
            .with_label_values(&[opcode, kind, reply_type, status])
            .get()
    }
    fn request_duration_count(opcode: &str, kind: &str, status: &str) -> u64 {
        FuseMetrics::get()
            .request_duration_us
            .with_label_values(&[opcode, kind, status])
            .get_sample_count()
    }
    fn errors_total(opcode: &str, kind: &str, errno: &str) -> i64 {
        FuseMetrics::get()
            .errors_total
            .with_label_values(&[opcode, kind, errno])
            .get()
    }
    fn unsupported_total(opcode: &str, reason: &str) -> i64 {
        FuseMetrics::get()
            .unsupported_total
            .with_label_values(&[opcode, reason])
            .get()
    }
    fn interrupted_total(opcode: &str) -> i64 {
        FuseMetrics::get()
            .interrupted_total
            .with_label_values(&[opcode])
            .get()
    }

    // B2 / test 4: enqueue failure records `reply_enqueue_errors_total` +
    // `request_duration_us{status=error}` exactly once, and does NOT count
    // toward `requests_total` (QPS) or `errors_total` (no OS errno).
    #[tokio::test]
    async fn enqueue_failure_emits_enqueue_error_and_duration_not_requests_total() {
        init_metrics();
        const OP: &str = "EnqFailTest";
        let metrics = FuseMetrics::get();
        let dur_before = request_duration_count(OP, "metadata", "error");

        let g = m::new_gauge("enq_emit_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(20, &g, OP);
        drop(rx); // close channel so enqueue fails
        assert!(reply.send_rep::<(), FuseError>(Ok(())).await.is_err());

        assert_eq!(
            metrics
                .reply_enqueue_errors_total
                .with_label_values(&[OP, ENQUEUE_REASON_CHANNEL_CLOSED])
                .get(),
            1,
            "one reply_enqueue_errors_total channel_closed"
        );
        assert_eq!(
            request_duration_count(OP, "metadata", "error"),
            dur_before + 1,
            "request_duration_us error observed once"
        );
        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_REPLIED, "error"),
            0,
            "enqueue failure must NOT count toward requests_total"
        );
        assert_eq!(g.get(), 0, "guard dropped once with the consumed task");
    }

    // enqueue failure layered on a FAILED op must still record the
    // op-level terminal counter with the real FS errno — the channel error must
    // not swallow the operation failure (symmetric with the sender write-failure
    // path's op/request status split).
    #[tokio::test]
    async fn enqueue_failure_on_failed_op_still_records_errors_total() {
        init_metrics();
        const OP: &str = "EnqFailOpErr";
        let before = errors_total(OP, "metadata", "EIO");

        let g = m::new_gauge("enq_op_err_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(30, &g, OP);
        drop(rx); // close channel so enqueue fails
        let err: FuseResult<()> = Err(FuseError::new(libc::EIO, "backend".into()));
        assert!(reply.send_rep(err).await.is_err());

        assert_eq!(
            errors_total(OP, "metadata", "EIO"),
            before + 1,
            "failed op + enqueue failure still records errors_total with FS errno"
        );
        // enqueue error recorded too; QPS still excluded.
        assert_eq!(
            FuseMetrics::get()
                .reply_enqueue_errors_total
                .with_label_values(&[OP, ENQUEUE_REASON_CHANNEL_CLOSED])
                .get(),
            1
        );
        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_REPLIED, "error"),
            0
        );
        assert_eq!(g.get(), 0);
    }

    // enqueue failure layered on a tagged-unsupported op still records
    // unsupported_total{reason}.
    #[tokio::test]
    async fn enqueue_failure_on_unsupported_op_still_records_unsupported_total() {
        init_metrics();
        const OP: &str = "EnqFailUnsup";
        let before = unsupported_total(OP, "unimplemented_opcode");

        let g = m::new_gauge("enq_unsup_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(31, &g, OP);
        drop(rx);
        let err: FuseResult<()> = Err(FuseError::new(libc::ENOSYS, "unimpl".into()));
        assert!(reply
            .send_rep_tagged(err, Some("unimplemented_opcode"), false)
            .await
            .is_err());

        assert_eq!(
            unsupported_total(OP, "unimplemented_opcode"),
            before + 1,
            "unsupported op + enqueue failure still records unsupported_total"
        );
        assert_eq!(g.get(), 0);
    }

    // enqueue failure layered on an interrupted op still records
    // interrupted_total.
    #[tokio::test]
    async fn enqueue_failure_on_interrupted_op_still_records_interrupted_total() {
        init_metrics();
        const OP: &str = "EnqFailIntr";
        let before = interrupted_total(OP);

        let g = m::new_gauge("enq_intr_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(32, &g, OP);
        drop(rx);
        let err: FuseResult<()> = Err(FuseError::new(libc::EINTR, "setlkw".into()));
        assert!(reply.send_rep_tagged(err, None, true).await.is_err());

        assert_eq!(
            interrupted_total(OP),
            before + 1,
            "interrupted op + enqueue failure still records interrupted_total"
        );
        assert_eq!(g.get(), 0);
    }

    // A stream worker holds the `FuseResponse` and replies from inside the task.
    // If the reply channel is closed by then, `send_*().await` returns Err and the
    // worker exits via `?` — but the finish must still happen: the active guard
    // drops (no leak) and the enqueue error + duration{error} are recorded.
    #[tokio::test]
    async fn stream_worker_send_data_enqueue_failure_finishes_without_leak() {
        init_metrics();
        const OP: &str = "StreamWorkerRead";
        let g = m::new_gauge("stream_worker_read_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(40, &g, OP);
        assert_eq!(g.get(), 1, "guard live while the worker holds the reply");
        drop(rx); // sender gone: the worker's reply enqueue will fail.

        // The reader worker replies with data; enqueue fails on the closed channel.
        let data: FuseResult<Vec<DataSlice>> = Ok(vec![]);
        assert!(reply.send_data(data).await.is_err());

        assert_eq!(
            g.get(),
            0,
            "active guard dropped on worker enqueue failure (no leak)"
        );
        assert_eq!(
            FuseMetrics::get()
                .reply_enqueue_errors_total
                .with_label_values(&[OP, ENQUEUE_REASON_CHANNEL_CLOSED])
                .get(),
            1,
            "worker enqueue failure records reply_enqueue_errors_total"
        );
        // The fixture's labels carry kind=metadata; the worker enqueue-failure
        // finish records request_duration_us{error} for whatever kind the ctx
        // holds. (The reader/writer kind is exercised end-to-end, not here.)
        assert!(
            request_duration_count(OP, "metadata", "error") >= 1,
            "worker enqueue failure records request_duration_us error"
        );
        // The op itself succeeded (data was Ok), so no op-level errors_total.
        assert_eq!(errors_total(OP, "metadata", "OTHER"), 0);
    }

    // companion: the writer worker's `send_rep` path on a closed channel.
    #[tokio::test]
    async fn stream_worker_send_rep_enqueue_failure_finishes_without_leak() {
        init_metrics();
        const OP: &str = "StreamWorkerWrite";
        let g = m::new_gauge("stream_worker_write_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode(41, &g, OP);
        drop(rx);

        assert!(reply.send_rep::<(), FuseError>(Ok(())).await.is_err());

        assert_eq!(g.get(), 0, "active guard dropped on writer enqueue failure");
        assert_eq!(
            FuseMetrics::get()
                .reply_enqueue_errors_total
                .with_label_values(&[OP, ENQUEUE_REASON_CHANNEL_CLOSED])
                .get(),
            1
        );
    }

    // the worker enqueue-failure finish records under the real
    // stream kind label — verifies `request_duration_us{kind="stream",error}`.
    #[tokio::test]
    async fn stream_worker_enqueue_failure_records_stream_kind_duration() {
        init_metrics();
        const OP: &str = "StreamWorkerKind";
        let before = request_duration_count(OP, "stream", "error");

        let g = m::new_gauge("stream_worker_kind_active", "test").unwrap();
        let (reply, rx) = reply_with_gauge_opcode_kind(42, &g, OP, FuseReqKind::Stream);
        drop(rx);
        let data: FuseResult<Vec<DataSlice>> = Ok(vec![]);
        assert!(reply.send_data(data).await.is_err());

        assert_eq!(g.get(), 0, "stream worker guard dropped, no leak");
        assert_eq!(
            request_duration_count(OP, "stream", "error"),
            before + 1,
            "stream-kind worker enqueue failure records request_duration_us kind=stream error"
        );
    }

    // Build a reply backed by an explicitly-bounded channel of the given
    // capacity, so bounded-path tests don't depend on the default helper's
    // internal `AsyncChannel::new(16)`.
    fn bounded_reply_with_capacity(
        unique: u64,
        gauge: &Gauge,
        opcode: &'static str,
        cap: usize,
    ) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        init_metrics();
        let (tx, rx) = AsyncChannel::new(cap).split();
        debug_assert!(tx.is_bounded(), "cap>0 must yield a bounded channel");
        let labels = FuseReqLabels::new(opcode, FuseReqKind::Metadata, 64);
        let ctx = FuseReqCtx {
            labels,
            active: Some(ActiveGuard::new(gauge.clone())),
        };
        (FuseResponse::new_reply(unique, tx, false, Some(ctx)), rx)
    }

    // Build a bounded size-1 reply slot whose only buffer position is already
    // filled, so the next `send` must suspend on `reserve().await`.
    fn full_bounded_reply(
        unique: u64,
        gauge: &Gauge,
        opcode: &'static str,
    ) -> (FuseResponse, AsyncReceiver<FuseTask>) {
        init_metrics();
        let (tx, rx) = AsyncChannel::new(1).split();
        // Fill the single slot so a subsequent reserve()/send() blocks.
        tx.try_reserve()
            .unwrap()
            .expect("one permit available")
            .send(FuseTask::Reply(
                ResponseData::create(unique, 0, vec![]).unwrap(),
            ));
        let labels = FuseReqLabels::new(opcode, FuseReqKind::Metadata, 64);
        let ctx = FuseReqCtx {
            labels,
            active: Some(ActiveGuard::new(gauge.clone())),
        };
        (FuseResponse::new_reply(unique, tx, false, Some(ctx)), rx)
    }

    // on a bounded channel, if the task is cancelled while the
    // reply is suspended on `reserve().await` (channel full), the request must
    // NOT enter a "silent finished" state — the slot stays `finished=false` and
    // the guard is released by passive Drop, so a retry/cleanup is still possible
    // and no half-finished state corrupts the gauges.
    #[tokio::test]
    async fn bounded_reserve_cancellation_leaves_slot_unfinished() {
        init_metrics();
        const OP: &str = "BoundedCancel";
        let g = m::new_gauge("bounded_cancel_active", "test").unwrap();
        let (reply, _rx) = full_bounded_reply(99, &g, OP);
        assert_eq!(g.get(), 1, "guard live before the reply");

        // The slot we will inspect after cancellation.
        let slot = reply.metrics.as_ref().unwrap().clone();

        // Spawn the reply; it suspends on reserve() because the channel is full.
        let handle = tokio::spawn(async move {
            let _ = reply.send_rep::<(), FuseError>(Ok(())).await;
        });
        // Give it a moment to reach the suspended reserve().
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Cancel the suspended task: its future (and the `FuseResponse`) drops.
        handle.abort();
        let _ = handle.await;

        // The critical invariant: NO silent finish. The slot was never committed,
        // so the guard is still IN the slot (not moved onto a task, not dropped),
        // and a real terminal path (retry / teardown cleanup) can still run.
        {
            let m = slot.lock();
            assert!(
                !m.finished,
                "cancellation during reserve() must NOT mark the slot finished"
            );
            assert!(
                m.active.is_some(),
                "guard stays in the unfinished slot, available for a real terminal path"
            );
        }
        assert_eq!(g.get(), 1, "guard still held by the unfinished slot");

        // Dropping the last slot reference releases the guard by Drop — so even
        // the abandoned request does not leak `active_requests`.
        drop(slot);
        assert_eq!(
            g.get(),
            0,
            "guard released once the slot is finally dropped"
        );
    }

    // bounded channel, reserve succeeds (slot free) -> the reply
    // finishes normally (RequestReply enqueued, slot finished, guard rides task).
    #[tokio::test]
    async fn bounded_reserve_success_finishes_normally() {
        init_metrics();
        const OP: &str = "BoundedOk";
        let g = m::new_gauge("bounded_ok_active", "test").unwrap();
        // Explicitly bounded with a free slot, so reserve() succeeds immediately.
        let (reply, mut rx) = bounded_reply_with_capacity(100, &g, OP, 4);

        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();

        let task = rx.try_recv().unwrap().expect("a task was enqueued");
        assert!(matches!(task, FuseTask::RequestReply { .. }));
        {
            let slot = reply.metrics.as_ref().unwrap().lock();
            assert!(slot.finished, "reserve-success path finishes the slot");
        }
        assert_eq!(g.get(), 1, "guard rides on the task");
        drop(task);
        assert_eq!(g.get(), 0, "guard dropped once at task drop");
    }

    // B3 / test 6: no-reply forget emits requests_total{reply_type=no_reply} +
    // duration for both Ok and Err, and never errors_total.
    #[tokio::test]
    async fn no_reply_emits_requests_total_no_reply_for_ok_and_err() {
        init_metrics();
        const OP: &str = "NoReplyTest";
        let err_errors_before = errors_total(OP, "metadata", "OTHER");

        let g_ok = m::new_gauge("nr_emit_ok", "test").unwrap();
        let (reply_ok, _rx_ok) = reply_with_gauge_opcode(21, &g_ok, OP);
        reply_ok.send_none(Ok(())).unwrap();

        let g_err = m::new_gauge("nr_emit_err", "test").unwrap();
        let (reply_err, _rx_err) = reply_with_gauge_opcode(22, &g_err, OP);
        reply_err.send_none(Err(FuseError::from("boom"))).unwrap();

        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_NO_REPLY, "success"),
            1,
            "Ok forget increments requests_total no_reply success"
        );
        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_NO_REPLY, "error"),
            1,
            "Err forget increments requests_total no_reply error"
        );
        assert_eq!(
            errors_total(OP, "metadata", "OTHER"),
            err_errors_before,
            "no-reply error must NOT emit errors_total"
        );
    }

    // B4 / test 8: malformed no-reply early finish emits decode_errors_total
    // {phase=parse,reason=other} once and NO requests_total.
    #[tokio::test]
    async fn finish_early_emits_decode_error_not_requests_total() {
        init_metrics();
        const OP: &str = "FinishEarlyTest";
        let metrics = FuseMetrics::get();
        // `decode_errors_total` is opcode-free (phase,reason), so other parallel
        // tests could also bump {parse,other}; assert a delta, not an absolute.
        let decode_before = metrics
            .decode_errors_total
            .with_label_values(&[DECODE_PHASE_PARSE, "other"])
            .get();

        let g = m::new_gauge("fe_emit_active", "test").unwrap();
        let (reply, _rx) = reply_with_gauge_opcode(23, &g, OP);
        reply.finish_early(libc::EINVAL, "other");

        assert!(
            metrics
                .decode_errors_total
                .with_label_values(&[DECODE_PHASE_PARSE, "other"])
                .get()
                > decode_before,
            "decode_errors_total parse other incremented at least once"
        );
        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_REPLIED, "error"),
            0,
            "parse-after-ctx must NOT emit requests_total"
        );
        assert_eq!(g.get(), 0, "guard dropped on early finish");
    }

    // --- reply_queue_depth task-embedded guard ---
    //
    // `reply_queue_guard()` increments the process-global `reply_queue_depth`
    // gauge shared by every parallel test, so these tests assert only the
    // structural invariant deterministic under parallelism ("is the guard on the
    // right task variant?") and leave the numeric inc/dec balance to
    // `active_guard_inc_dec_balances` in fuse_metrics.

    // B1: `metrics_op_status()` reads back the stashed op_status that the
    // `operation_duration_us` timer uses after the dispatch_meta match. It is the
    // FS-operation result (here: an untagged error → Error), NOT the enqueue
    // outcome, and is None when there is no metrics slot (disabled) or before any
    // finish.
    #[tokio::test]
    async fn metrics_op_status_reads_stashed_fs_result() {
        init_metrics();
        let g = m::new_gauge("op_status_active", "test").unwrap();

        // Before any finish: nothing stashed yet.
        let (reply, mut rx) = reply_with_gauge_opcode(60, &g, "OpStatusOp");
        assert_eq!(
            reply.metrics_op_status(),
            None,
            "no op_status before the reply path runs"
        );

        // A failed FS op stashes Error (read by operation_duration as status=error).
        let err: FuseResult<()> = Err(FuseError::new(libc::EIO, "backend".into()));
        reply.send_rep(err).await.unwrap();
        let _ = rx.try_recv();
        assert_eq!(
            reply.metrics_op_status(),
            Some(FuseReqStatus::Error),
            "metrics_op_status reflects the FS op result after the reply path"
        );

        // Disabled reply has no slot, so the accessor is None.
        let (disabled, _rx2) = disabled_reply(61);
        assert_eq!(disabled.metrics_op_status(), None, "disabled has no slot");
    }

    // B2 test 3(a): a metrics-enabled reply produces a RequestReply that CARRIES a
    // queue guard (so the sender's `mark_dequeued` has something to drop at the
    // dequeue point).
    #[tokio::test]
    async fn request_reply_carries_queue_guard() {
        init_metrics();
        let g = m::new_gauge("rq_req_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge_opcode(50, &g, "RqReqOp");
        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();

        let task = rx.try_recv().unwrap().expect("a task");
        match task {
            FuseTask::RequestReply { queue_guard, .. } => {
                assert!(
                    queue_guard.is_some(),
                    "metrics-enabled RequestReply carries a reply_queue_depth guard"
                );
            }
            other => panic!("expected RequestReply, got {}", as_variant(&other)),
        }
    }

    // B2 test 3(d): the disabled legacy `Reply` carries no queue guard at all —
    // the variant has no guard field, so disabled mode cannot touch
    // reply_queue_depth. (Structural, not a gauge read.)
    #[tokio::test]
    async fn disabled_reply_carries_no_queue_guard() {
        init_metrics();
        let (reply, mut rx) = disabled_reply(52);
        reply.send_rep::<(), FuseError>(Ok(())).await.unwrap();
        assert!(
            matches!(rx.try_recv().unwrap().unwrap(), FuseTask::Reply(_)),
            "disabled path produces the legacy Reply (no queue guard field)"
        );
    }

    // B2 test (notify): a metrics-enabled notify produces a NotifyReply carrying a
    // queue guard, same discipline as the request path.
    #[tokio::test]
    async fn notify_reply_carries_queue_guard() {
        init_metrics();
        let g = m::new_gauge("rq_notify_active", "test").unwrap();
        let (reply, mut rx) = reply_with_gauge_opcode(53, &g, "RqNotifyOp");
        reply
            .send_notify(FuseNotifyCode::FUSE_NOTIFY_INVAL_INODE, vec![])
            .await
            .unwrap();

        let task = rx.try_recv().unwrap().expect("a notify task");
        match task {
            FuseTask::NotifyReply { queue_guard, .. } => {
                assert!(queue_guard.is_some(), "NotifyReply carries a queue guard");
            }
            other => panic!("expected NotifyReply, got {}", as_variant(&other)),
        }
    }

    // B2 test 18(a): on a bounded channel, a `reserve()` that fails with a closed
    // channel records `notify_total{status=enqueue_failed}` — the NEW reserve-path
    // failure point (previously only the send() error was counted). `notify_total`
    // is keyed by a code unique to this test, so the delta is parallel-safe.
    #[tokio::test]
    async fn notify_bounded_reserve_closed_records_enqueue_failed() {
        init_metrics();
        // FUSE_NOTIFY_INVAL_ENTRY is the code this test owns for its delta; no
        // other test enqueue_failed's this code.
        let code = FuseNotifyCode::FUSE_NOTIFY_INVAL_ENTRY.as_str();
        let before = FuseMetrics::get()
            .notify_total
            .with_label_values(&[code, NOTIFY_ENQUEUE_FAILED])
            .get();

        // Bounded channel, then close it by dropping the receiver so reserve()
        // returns a closed-channel error.
        let g = m::new_gauge("rq_notify_closed_active", "test").unwrap();
        let (tx, rx) = AsyncChannel::new(1).split();
        debug_assert!(tx.is_bounded());
        let labels = FuseReqLabels::new("RqNotifyClosed", FuseReqKind::Metadata, 64);
        let ctx = FuseReqCtx {
            labels,
            active: Some(ActiveGuard::new(g.clone())),
        };
        let reply = FuseResponse::new_reply(54, tx, false, Some(ctx));
        drop(rx);

        assert!(reply
            .send_notify(FuseNotifyCode::FUSE_NOTIFY_INVAL_ENTRY, vec![])
            .await
            .is_err());

        assert_eq!(
            FuseMetrics::get()
                .notify_total
                .with_label_values(&[code, NOTIFY_ENQUEUE_FAILED])
                .get(),
            before + 1,
            "bounded reserve-closed records notify enqueue_failed"
        );
    }

    // Small helper so panic messages name the unexpected variant without deriving
    // Debug on FuseTask (which carries non-Debug fields).
    fn as_variant(task: &FuseTask) -> &'static str {
        match task {
            FuseTask::RequestReply { .. } => "RequestReply",
            FuseTask::NotifyReply { .. } => "NotifyReply",
            FuseTask::Reply(_) => "Reply",
        }
    }

    // --- reply_queue_depth REAL queue lifecycle ---
    //
    // These drive an actual channel with locally-gauged guards (NOT the global
    // `reply_queue_depth`, which parallel tests share and would make asserts flaky).
    // The queue guard is an `ActiveGuard`; its behavior is fully determined by where
    // it is created (enqueue boundary) and dropped (sender dequeue, or task drop).
    // The `RequestReply` task carries `active` and `queue` guards on distinct local
    // gauges so the two scopes can be asserted apart.

    // Build a RequestReply task whose active/queue guards are backed by the two
    // given gauges, so a test can watch each scope independently.
    fn request_reply_task(unique: u64, active_g: &Gauge, queue_g: &Gauge) -> FuseTask {
        let labels = FuseReqLabels::new("QDepthOp", FuseReqKind::Metadata, 64);
        FuseTask::RequestReply {
            data: ResponseData::create(unique, 0, vec![]).unwrap(),
            labels,
            active: ActiveGuard::new(active_g.clone()),
            status: FuseReqStatus::Success,
            errno: 0,
            unsupported_reason: None,
            queue_guard: Some(ActiveGuard::new(queue_g.clone())),
        }
    }

    // recv dequeues -> the queue guard drops at the dequeue point, so
    // queue depth returns to baseline BEFORE any splice; the active guard is a
    // SEPARATE scope and is still held (active stays 1 while queue is back to 0).
    #[tokio::test]
    async fn reply_queue_depth_drops_at_dequeue_active_still_held() {
        let active_g = m::new_gauge("qd_dequeue_active", "test").unwrap();
        let queue_g = m::new_gauge("qd_dequeue_queue", "test").unwrap();
        let (tx, mut rx) = AsyncChannel::<FuseTask>::new(16).split();

        tx.send(request_reply_task(1, &active_g, &queue_g))
            .await
            .unwrap();
        // Enqueued and not yet received: both scopes are live.
        assert_eq!(queue_g.get(), 1, "queue depth +1 while task is in channel");
        assert_eq!(active_g.get(), 1, "active +1 from ctx creation");

        // Sender dequeues: take the task, drop ONLY the queue guard (== sender's
        // `mark_dequeued`). The active guard rides on the task until sender finish.
        let task = rx.try_recv().unwrap().expect("task");
        match task {
            FuseTask::RequestReply {
                queue_guard,
                active,
                ..
            } => {
                drop(queue_guard); // mark_dequeued
                assert_eq!(queue_g.get(), 0, "queue depth back to 0 at dequeue");
                assert_eq!(
                    active_g.get(),
                    1,
                    "active still held after dequeue (separate scope, splice not done)"
                );
                drop(active); // sender finish
                assert_eq!(active_g.get(), 0, "active released at sender finish");
            }
            other => panic!("expected RequestReply, got {}", as_variant(&other)),
        }
    }

    // a task enqueued but NEVER received (sender/channel dropped) still
    // balances — the queue guard rides the task and drops with it, no leak.
    #[tokio::test]
    async fn reply_queue_depth_unreceived_task_drop_balances() {
        let active_g = m::new_gauge("qd_unrecv_active", "test").unwrap();
        let queue_g = m::new_gauge("qd_unrecv_queue", "test").unwrap();
        let (tx, rx) = AsyncChannel::<FuseTask>::new(16).split();

        tx.send(request_reply_task(2, &active_g, &queue_g))
            .await
            .unwrap();
        assert_eq!(queue_g.get(), 1, "enqueued: +1");

        // Drop both ends without recv: the queued task (and both its guards) is
        // dropped with the channel.
        drop(rx);
        drop(tx);
        assert_eq!(
            queue_g.get(),
            0,
            "un-received task drop balances queue depth"
        );
        assert_eq!(active_g.get(), 0, "and the active guard too");
    }

    // bounded-full reserve-first does NOT inflate queue depth: the queue guard is
    // created only AFTER a permit is acquired, so a producer parked in
    // `reserve().await` holds none. A PROTOCOL-LEVEL STAND-IN — it asserts the
    // invariant against a full channel (`try_reserve()` -> no permit -> gauge 0),
    // not a genuinely parked producer (which would need the shared global gauge or
    // a waker harness). The cancellation half is covered by
    // `bounded_reserve_cancellation_leaves_slot_unfinished`.
    #[tokio::test]
    async fn reply_queue_depth_bounded_full_does_not_inflate() {
        let queue_g = m::new_gauge("qd_bounded_full_queue", "test").unwrap();
        let (tx, _rx) = AsyncChannel::<FuseTask>::new(1).split();
        debug_assert!(tx.is_bounded());

        // Fill the single slot.
        let permit = tx.try_reserve().unwrap().expect("one permit");
        permit.send(FuseTask::Reply(ResponseData::create(0, 0, vec![]).unwrap()));

        // Channel now full: a producer would block in `reserve().await`. Reserve-
        // first means the queue guard is created only after a permit is in hand,
        // so right now NO guard exists and queue depth is untouched.
        assert!(
            tx.try_reserve().unwrap().is_none(),
            "full bounded channel yields no permit"
        );
        assert_eq!(
            queue_g.get(),
            0,
            "no permit -> no queue guard built -> depth not inflated"
        );
    }
}
