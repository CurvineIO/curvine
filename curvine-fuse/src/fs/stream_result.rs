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

/// Deliver a backend stream-op result (`Flush`/`Complete`/`Resize`) to both the
/// waiting in-process caller (`tx`) and, when present, the kernel (`reply`).
///
/// Ordering (issue #1118): notify `tx` FIRST, then send the kernel reply. The
/// caller parks on `tx`, so sending the kernel reply first would let a blocked
/// or failing reply channel gate/break the caller's completion notification.
///
/// `FsError` is not `Clone`, so the single `res` cannot be handed to both
/// consumers by value. We borrow it to compute an errno-only kernel reply, then
/// move the original `res` onto `tx` so the caller sees the real success/failure.
pub(crate) async fn deliver_stream_result(
    res: FsResult<()>,
    tx: CallSender<FsResult<()>>,
    reply: Option<FuseResponse>,
) -> FsResult<()> {
    // Build the kernel reply payload by borrowing `res`, before moving it to `tx`.
    let kernel_rep: Result<(), FuseError> = match &res {
        Ok(()) => Ok(()),
        Err(e) => Err(FuseError::from_errno_msg(errno_of(e), e.to_string().into())),
    };

    // Caller first: hand the real result to whoever awaits `flush()/complete()/resize()`.
    tx.send(res)?;

    // Then the kernel reply, if this op originated from a kernel request.
    if let Some(reply) = reply {
        reply.send_rep(kernel_rep).await?;
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
    // flush -> complete(Some)). On this path the result must reach BOTH the
    // in-process caller (tx) AND the kernel (reply). Two properties are pinned:
    //   1. tx still receives the real backend result, so the caller's
    //      `receive().await??` cannot misjudge a backend failure as success.
    //   2. the kernel reply carries the mapped POSIX errno.
    // The disabled reply path (ctx=None) enqueues a `FuseTask::Reply(ResponseData)`
    // whose `header.error` is the negated errno; we drain and inspect it.
    #[tokio::test]
    async fn deliver_reply_some_propagates_err_to_caller_and_kernel() {
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

        // (1) The in-process caller must still observe the backend Err.
        let got = rx.receive().await.expect("channel delivered a result");
        assert!(
            got.is_err(),
            "reply=Some: the caller (tx) must still see the backend failure"
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
