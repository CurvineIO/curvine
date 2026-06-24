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

mod fuse_response;
pub use self::fuse_response::FuseResponse;
pub use self::fuse_response::ResponseData;

mod fuse_session;
pub use fuse_session::FuseSession;

mod fuse_request;
pub use self::fuse_request::FuseRequest;

mod fuse_decoder;
pub use self::fuse_decoder::FuseDecoder;

pub mod channel;

mod fuse_mnt;
pub use self::fuse_mnt::*;

mod fuse_op_code;
pub use self::fuse_op_code::FuseOpCode;

mod fuse_buf;
pub use fuse_buf::FuseBuf;

mod fuse_notify_code;
pub use self::fuse_notify_code::FuseNotifyCode;

pub mod bdi;

use crate::fuse_metrics::{ActiveGuard, FuseReqLabels, FuseReqStatus};

pub(crate) enum FuseTask {
    /// A reply to a real FUSE request with metrics enabled. Owns the move-only
    /// `ActiveGuard`; the sender finishes the request metrics and drops the
    /// guard after the kernel-fd write (the E2E finish point).
    RequestReply {
        data: ResponseData,
        labels: FuseReqLabels,
        active: ActiveGuard,
        status: FuseReqStatus,
        errno: i32,
        /// Source tag for `status == Unsupported` (`unknown_opcode` /
        /// `unimplemented_opcode` / `trait_default`), carried so Phase 1a-2 can
        /// emit `unsupported_total{reason}` at the sender finish point without
        /// reaching back into the metrics slot. `None` for non-unsupported
        /// replies. (Phase 1a-1 sets it but reads nothing.)
        unsupported_reason: Option<&'static str>,
    },
    /// A kernel notification (cache invalidation). No originating request, so no
    /// labels/guard; carries its own `FuseNotifyCode::as_str()` code for
    /// sender-side attribution.
    NotifyReply {
        data: ResponseData,
        code: &'static str,
    },
    /// Legacy fast path, used when metrics are disabled: no context, no guard,
    /// the sender does no request-finish work. Keeps `metrics_enabled=false`
    /// byte-identical to the pre-metrics behaviour.
    Reply(ResponseData),
}
