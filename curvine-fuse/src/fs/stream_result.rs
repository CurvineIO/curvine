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
use log::warn;
use orpc::sync::channel::CallSender;

/// Deliver a backend stream-op result (`Flush`/`Complete`/`Resize`) to the one
/// correct consumer, which differs by origin:
///
///   * `reply = Some` (kernel request): the kernel reply is authoritative. Send
///     the real (errno-mapped) result to the kernel and hand the in-process
///     caller a bare `Ok(())` — if the caller re-propagated the `Err`,
///     `send_stream_dispatch` would treat it as a dispatch failure and enqueue a
///     SECOND kernel reply for the same request. Notify `tx` FIRST, then reply,
///     so a blocked reply channel cannot gate the caller's completion (#1118).
///   * `reply = None` (internal caller: dirty-read flush, flush_writer,
///     release's `complete(None)`, resize): `tx` is the ONLY channel back, so the
///     real result MUST travel on it, else a backend failure is swallowed (#1118).
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
            if let Err(e) = tx.send(Ok(())) {
                // The dispatch future was cancelled after submitting the task.
                // Still attempt the authoritative kernel reply, and keep the
                // stream worker alive for subsequent operations.
                warn!("failed to deliver stream result to internal caller: {}", e);
            }
            if let Err(e) = reply.send_rep(kernel_rep).await {
                // The backend work and internal notification are already done.
                // A closed reply channel is local to this request and must not
                // terminate the long-lived reader/writer worker.
                warn!("failed to send FUSE stream reply: {}", e);
            }
        }

        // Internal-caller path: tx is the only way back, so the real backend
        // result must travel on it (do not swallow the error — issue #1118).
        None => {
            if let Err(e) = tx.send(res) {
                // An internal caller can be cancelled while its backend task is
                // in flight. Treat the abandoned receiver like a reply-send
                // failure instead of killing the shared worker.
                warn!("failed to deliver stream result to internal caller: {}", e);
            }
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

    #[tokio::test]
    async fn closed_reply_channel_does_not_fail_internal_completion() {
        use crate::session::{FuseResponse, FuseTask};
        use orpc::sync::channel::AsyncChannel;

        let (task_tx, task_rx) = AsyncChannel::<FuseTask>::new(1).split();
        drop(task_rx);
        let reply = FuseResponse::new_reply(1, task_tx, false, None);
        let (tx, rx) = CallChannel::channel::<FsResult<()>>();

        deliver_stream_result(Ok(()), tx, Some(reply))
            .await
            .expect("reply-send failure is best effort");

        assert!(rx
            .receive()
            .await
            .expect("internal completion is delivered first")
            .is_ok());
    }
}
