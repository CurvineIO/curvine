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

use crate::fuse_error::{errno_of, FuseError};
use crate::session::FuseResponse;
use curvine_common::FsResult;
use orpc::sync::channel::CallSender;

/// Deliver a backend stream-op result (`Flush`/`Complete`/`Resize`) to the
/// correct single consumer, which differs by whether this op originated from a
/// kernel request (`reply = Some`) or an internal caller (`reply = None`).
///
/// Why the split (PR #1201 review): the kernel reply and the `tx` completion
/// channel are BOTH ways a result travels back, but for a kernel request they
/// are two ends of the SAME FUSE request. If we sent the real `Err` on `tx`,
/// the caller (`flush()/complete()/resize()`) would return it up the dispatch
/// chain, where `send_stream_dispatch` treats any `Err` as a pre-reply dispatch
/// failure and calls `err_rep.send_rep(res)` — enqueuing a SECOND kernel reply
/// for the same request. So the helper is the single decision point:
///
///   * `reply = Some` (kernel request): the kernel reply is the authoritative
///     response. Send the real (errno-mapped) result to the kernel, and hand
///     the in-process caller a bare `Ok(())` so it does NOT re-propagate the
///     error and trigger a duplicate reply. Ordering (issue #1118): notify `tx`
///     FIRST, then the kernel reply, so a blocked/failing reply channel cannot
///     gate the caller's completion notification.
///   * `reply = None` (internal caller: read-path dirty-read flush, flush_writer,
///     release's `complete(None)`, resize): `tx` is the ONLY channel back, so
///     the real backend result MUST travel on it — otherwise a backend failure
///     is silently swallowed (the bug issue #1118 fixed).
///
/// `FsError` is not `Clone`, but that is no longer a constraint here: on the
/// `reply = Some` path only the kernel reply needs the error (via a borrowed
/// `errno_of`), and the caller receives a value-free `Ok(())` signal; on the
/// `reply = None` path the single `res` is simply moved onto `tx`.
pub(crate) async fn deliver_stream_result(
    res: FsResult<()>,
    tx: CallSender<FsResult<()>>,
    reply: Option<FuseResponse>,
) -> FsResult<()> {
    match reply {
        // Kernel-request path: kernel reply is authoritative; caller gets Ok(())
        // ("reply already handled") so it never re-propagates and causes a
        // duplicate reply. tx first, then kernel reply (issue #1118 ordering).
        Some(reply) => {
            let kernel_rep: Result<(), FuseError> = match &res {
                Ok(()) => Ok(()),
                Err(e) => Err(FuseError::from_errno_msg(errno_of(e), e.to_string().into())),
            };
            tx.send(Ok(()))?;
            reply.send_rep(kernel_rep).await?;
        }

        // Internal-caller path: tx is the only way back, so the real backend
        // result must travel on it (do not swallow the error — issue #1118).
        None => {
            tx.send(res)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::deliver_stream_result;
    use curvine_common::error::FsError;
    use curvine_common::FsResult;
    use orpc::sync::channel::CallChannel;

    // reply=None is the internal-caller path (read-path dirty-read flush,
    // flush_writer, release's complete(None), resize). On this path `tx` is the
    // ONLY channel back, so `deliver_stream_result` must move the real backend
    // result onto it. These tests drive the production function and assert the
    // caller-visible result — pinning the #1118 contract that a backend failure
    // is propagated (not swallowed as the old `tx.send(1)` did) and a success
    // stays a success.
    #[tokio::test]
    async fn deliver_reply_none_propagates_backend_err_to_caller() {
        let (tx, rx) = CallChannel::channel::<FsResult<()>>();
        let backend: FsResult<()> = Err(FsError::common("backend flush failed"));

        deliver_stream_result(backend, tx, None).await.unwrap();

        // The caller (e.g. FuseWriter::flush(None) via `receive().await??`) must
        // observe the backend Err.
        let got = rx.receive().await.expect("channel delivered a result");
        assert!(
            got.is_err(),
            "reply=None: a backend failure must reach the caller, not be swallowed"
        );
    }

    #[tokio::test]
    async fn deliver_reply_none_propagates_backend_ok_to_caller() {
        let (tx, rx) = CallChannel::channel::<FsResult<()>>();
        let backend: FsResult<()> = Ok(());

        deliver_stream_result(backend, tx, None).await.unwrap();

        let got = rx.receive().await.expect("channel delivered a result");
        assert!(got.is_ok(), "reply=None: a backend success stays a success");
    }

    // reply=Some is the kernel-request path (e.g. fsync -> flush(Some), a kernel
    // flush -> complete(Some)). On this path the kernel reply is the single
    // authoritative response for the FUSE request, so two properties are pinned:
    //   1. tx receives `Ok(())` ("reply already handled") EVEN when the backend
    //      failed — so the caller does NOT re-propagate the error up the dispatch
    //      chain and cause `send_stream_dispatch` to enqueue a SECOND reply for
    //      the same request (PR #1201 review).
    //   2. the kernel reply carries the mapped POSIX errno (the real failure).
    // The disabled reply path (ctx=None) enqueues a `FuseTask::Reply(ResponseData)`
    // whose `header.error` is the negated errno; we drain and inspect it.
    #[tokio::test]
    async fn deliver_reply_some_reports_ok_to_caller_and_errno_to_kernel() {
        use crate::session::{FuseResponse, FuseTask};
        use orpc::sync::channel::AsyncChannel;

        let (task_tx, mut task_rx) = AsyncChannel::<FuseTask>::new(16).split();
        // ctx=None -> disabled fast path -> a plain `FuseTask::Reply(ResponseData)`.
        let reply = FuseResponse::new_reply(1, task_tx, false, None);

        let (tx, rx) = CallChannel::channel::<FsResult<()>>();
        // `FsError::common` matches no specific kind and carries no permission/
        // unsupported keyword, so `errno_of` falls back to EIO.
        let backend: FsResult<()> = Err(FsError::common("backend flush failed"));

        deliver_stream_result(backend, tx, Some(reply))
            .await
            .unwrap();

        // (1) The in-process caller must receive Ok(()) so it does NOT surface
        // the error to the dispatch layer (which would send a duplicate reply).
        let got = rx.receive().await.expect("channel delivered a result");
        assert!(
            got.is_ok(),
            "reply=Some: caller must get Ok(()) so the error is not re-propagated \
             into a duplicate kernel reply"
        );

        // (2) The kernel reply must carry the mapped errno (EIO), negated in the
        // FUSE out header.
        let task = task_rx.recv().await.expect("a kernel reply was enqueued");
        match task {
            FuseTask::Reply(data) => assert_eq!(
                data.header.error,
                -libc::EIO,
                "reply=Some: kernel reply must carry the mapped errno (negated)"
            ),
            _ => panic!("expected FuseTask::Reply"),
        }
    }

    #[tokio::test]
    async fn deliver_reply_some_reports_ok_to_kernel() {
        use crate::session::{FuseResponse, FuseTask};
        use orpc::sync::channel::AsyncChannel;

        let (task_tx, mut task_rx) = AsyncChannel::<FuseTask>::new(16).split();
        let reply = FuseResponse::new_reply(1, task_tx, false, None);

        let (tx, rx) = CallChannel::channel::<FsResult<()>>();
        let backend: FsResult<()> = Ok(());

        deliver_stream_result(backend, tx, Some(reply))
            .await
            .unwrap();

        let got = rx.receive().await.expect("channel delivered a result");
        assert!(got.is_ok(), "reply=Some: caller sees success");

        let task = task_rx.recv().await.expect("a kernel reply was enqueued");
        match task {
            FuseTask::Reply(data) => assert_eq!(
                data.header.error, 0,
                "reply=Some: a successful op replies error=0 to the kernel"
            ),
            _ => panic!("expected FuseTask::Reply"),
        }
    }
}
