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

use crate::handler::{
    Frame, GlobalLimit, LimitConf, LimitPermit, MessageHandler, ReadFrame, WriteFrame,
};
use crate::message::{Builder, Message};
use crate::ServerConf;
use curvine_io::{IOError, IOResult};
use curvine_metrics::{Gauge, Metrics};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::debug;
use once_cell::sync::Lazy;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::TryAcquireError;
use tokio::time::sleep;

static BLOCKED_REQUESTS: Lazy<Gauge> = Lazy::new(|| {
    Metrics::new_gauge(
        "buffer_handler_blocked_requests",
        "Current number of BufferHandler requests waiting for the global concurrency limit",
    )
    .expect("buffer handler blocked requests metric must be valid")
});

struct BlockedRequestGuard;

impl BlockedRequestGuard {
    fn new() -> Self {
        BLOCKED_REQUESTS.inc();
        Self
    }
}

impl Drop for BlockedRequestGuard {
    fn drop(&mut self) {
        BLOCKED_REQUESTS.dec();
    }
}

/// Connection-local reactor for RPC services that explicitly support
/// concurrent request dispatch.
///
/// The read and write halves remain owned by this task. Request futures are
/// polled through `FuturesUnordered`, so responses are written in completion
/// order while each frame is still serialized by a single writer.
pub struct BufferHandler<F, M> {
    rt: Arc<Runtime>,
    frame: F,
    handler: Arc<M>,
    close_idle: bool,
    timeout: Duration,
    limit: LimitConf,
}

impl<F: Frame, M: MessageHandler> BufferHandler<F, M> {
    pub fn new(
        rt: Arc<Runtime>,
        frame: F,
        handler: M,
        conf: &ServerConf,
        limit: LimitConf,
    ) -> Self {
        Lazy::force(&BLOCKED_REQUESTS);
        Self {
            rt,
            frame,
            handler: Arc::new(handler),
            close_idle: conf.close_idle,
            timeout: Duration::from_millis(conf.timeout_ms),
            limit,
        }
    }

    async fn process_request(
        rt: &Runtime,
        handler: &Arc<M>,
        global_limit: &Arc<GlobalLimit>,
        request: Message,
    ) -> IOResult<Message> {
        let permit = Self::acquire_limit(global_limit).await?;

        let response = if handler.is_sync(&request) {
            let exec_rt = handler.get_rt(&request).unwrap_or(rt);
            let handler = handler.clone();
            exec_rt
                .spawn_blocking(move || {
                    let _permit = permit;
                    match handler.handle(&request) {
                        Err(e) => {
                            debug!("handler request {} error: {}", request.req_id(), e);
                            request.error_ext(&e)
                        }
                        Ok(v) => v,
                    }
                })
                .await?
        } else {
            let _permit = permit;
            let protocol = request.protocol;
            match handler.async_handle(request).await {
                Ok(v) => v,
                Err(e) => {
                    debug!("handler request {} error: {}", protocol.req_id, e);
                    Builder::protocol(protocol).build().error_ext(&e)
                }
            }
        };

        Ok(response)
    }

    async fn acquire_limit(global_limit: &Arc<GlobalLimit>) -> IOResult<LimitPermit> {
        let semaphore = global_limit.semaphore();
        match semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok(global_limit.track_permit(permit)),
            Err(TryAcquireError::NoPermits) => {
                let _blocked = BlockedRequestGuard::new();
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|e| IOError::create(e.to_string()))?;
                Ok(global_limit.track_permit(permit))
            }
            Err(TryAcquireError::Closed) => err_box!("global limit is closed"),
        }
    }

    async fn send_response(writer: &mut WriteFrame, response: IOResult<Message>) -> IOResult<()> {
        let response = response?;
        if response.not_empty() {
            writer.send(&response).await?;
        }
        Ok(())
    }

    async fn receive_request<R>(
        reader: &mut ReadFrame,
        writer: &mut WriteFrame,
        inflight: &mut FuturesUnordered<R>,
        conn_limit: &AtomicUsize,
        close_idle: bool,
        idle_timeout: Duration,
    ) -> IOResult<Message>
    where
        R: Future<Output = IOResult<Message>>,
    {
        let receive = reader.receive();
        tokio::pin!(receive);

        loop {
            // Read per iteration rather than once per connection: an edit has to
            // reach connections that are already established.
            let conn_limit = conn_limit.load(Ordering::Acquire);
            tokio::select! {
                response = inflight.next(), if !inflight.is_empty() => {
                    if let Some(response) = response {
                        Self::send_response(writer, response).await?;
                    }
                }

                received = &mut receive, if inflight.len() < conn_limit => return received,

                _ = sleep(idle_timeout), if close_idle && inflight.is_empty() => {
                    return Ok(Message::empty());
                }
            }
        }
    }

    pub async fn run(self) -> IOResult<()> {
        let Self {
            rt,
            frame,
            handler,
            close_idle,
            timeout: idle_timeout,
            limit,
        } = self;

        let (mut reader, mut writer) = frame.split();
        // Declared before `inflight`: the futures below borrow it, so it has to
        // outlive the set of requests that are still running.
        let global_limit = limit.global_limit().clone();
        let conn_limit = limit.conn_limit_cell();
        let mut inflight = FuturesUnordered::new();

        loop {
            let request = Self::receive_request(
                &mut reader,
                &mut writer,
                &mut inflight,
                &conn_limit,
                close_idle,
                idle_timeout,
            )
            .await?;

            if request.is_empty() {
                return Ok(());
            }

            inflight.push(Self::process_request(&rt, &handler, &global_limit, request));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockedRequestGuard, BufferHandler, BLOCKED_REQUESTS};
    use crate::handler::{GlobalLimit, MessageHandler, RpcFrame, TestMessageHandler};
    use crate::message::Message;
    use curvine_core_error::CommonErrorExt;
    use curvine_runtime::runtime::{RpcRuntime, Runtime};
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::oneshot;

    struct BlockingHandler {
        started: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    struct RuntimeRoutingHandler {
        rt: Runtime,
        executed_on: mpsc::Sender<String>,
    }

    impl MessageHandler for RuntimeRoutingHandler {
        type Error = CommonErrorExt;

        fn handle(&self, _msg: &Message) -> Result<Message, Self::Error> {
            let thread_name = std::thread::current()
                .name()
                .unwrap_or_default()
                .to_string();
            self.executed_on.send(thread_name).unwrap();
            Ok(Message::empty())
        }

        fn get_rt(&self, _msg: &Message) -> Option<&Runtime> {
            Some(&self.rt)
        }
    }

    impl MessageHandler for BlockingHandler {
        type Error = CommonErrorExt;

        fn handle(&self, _msg: &Message) -> Result<Message, Self::Error> {
            if let Some(started) = self.started.lock().unwrap().take() {
                let _ = started.send(());
            }
            self.release.lock().unwrap().recv().unwrap();
            Ok(Message::empty())
        }
    }

    #[test]
    fn blocked_request_guard_tracks_current_waiters() {
        let initial = BLOCKED_REQUESTS.get();
        {
            let _blocked = BlockedRequestGuard::new();
            assert_eq!(BLOCKED_REQUESTS.get(), initial + 1);
        }
        assert_eq!(BLOCKED_REQUESTS.get(), initial);
    }

    #[test]
    fn closed_global_limit_returns_error() {
        let runtime = Runtime::current_thread("closed-global-limit-test");
        let limit = GlobalLimit::new(1);
        limit.close();

        let result =
            runtime.block_on(BufferHandler::<RpcFrame, TestMessageHandler>::acquire_limit(&limit));
        assert!(result.is_err());
    }

    #[test]
    fn sync_task_keeps_permit_after_outer_future_is_cancelled() {
        let runtime = Arc::new(Runtime::current_thread("sync-permit-cancellation-test"));
        let limit = GlobalLimit::new(1);
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let handler = Arc::new(BlockingHandler {
            started: Mutex::new(Some(started_tx)),
            release: Mutex::new(release_rx),
        });

        runtime.clone().block_on(async move {
            let task_runtime = runtime.clone();
            let task_limit = limit.clone();
            let task = tokio::spawn(async move {
                BufferHandler::<RpcFrame, BlockingHandler>::process_request(
                    &task_runtime,
                    &handler,
                    &task_limit,
                    Message::empty(),
                )
                .await
            });

            started_rx.await.unwrap();
            task.abort();
            tokio::task::yield_now().await;
            assert_eq!(limit.available_permits(), 0);

            release_tx.send(()).unwrap();
            for _ in 0..100 {
                if limit.available_permits() == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            panic!("blocking task did not release its permit after completion");
        });
    }

    /// A shrink that could not be applied while permits were in flight has to be
    /// finished by the requests that release them, otherwise a lowered limit
    /// would stay over-provisioned until the service restarts.
    #[test]
    fn released_permit_finishes_a_deferred_shrink() {
        let runtime = Runtime::current_thread("deferred-shrink-test");
        let limit = GlobalLimit::new(4);

        runtime.block_on(async {
            let first = BufferHandler::<RpcFrame, TestMessageHandler>::acquire_limit(&limit)
                .await
                .unwrap();
            let second = BufferHandler::<RpcFrame, TestMessageHandler>::acquire_limit(&limit)
                .await
                .unwrap();
            let third = BufferHandler::<RpcFrame, TestMessageHandler>::acquire_limit(&limit)
                .await
                .unwrap();
            assert_eq!(limit.in_flight(), 3);
            assert_eq!(limit.available_permits(), 1);

            // Only one permit is idle, so the cap lands on three and the rest of
            // the shrink is owed by whoever releases next.
            limit.resize(1);
            assert_eq!(limit.capacity(), 3);
            assert_eq!(limit.available_permits(), 0);
            assert!(limit.is_settling());

            drop(third);
            assert_eq!(limit.capacity(), 2);
            assert_eq!(limit.available_permits(), 0);
            assert!(limit.is_settling());

            drop(second);
            assert_eq!(limit.capacity(), 1);
            assert_eq!(limit.available_permits(), 0);
            assert!(!limit.is_settling());

            drop(first);
            assert_eq!(limit.capacity(), 1);
            assert_eq!(limit.available_permits(), 1);
        });
    }

    #[test]
    fn sync_request_uses_handler_runtime() {
        let runtime = Runtime::current_thread("buffer-handler-default-runtime");
        let (executed_on_tx, executed_on_rx) = mpsc::channel();
        let handler = Arc::new(RuntimeRoutingHandler {
            rt: Runtime::new("buffer-handler-selected-runtime", 1, 1),
            executed_on: executed_on_tx,
        });
        let limit = GlobalLimit::new(1);

        runtime.block_on(async {
            BufferHandler::<RpcFrame, RuntimeRoutingHandler>::process_request(
                &runtime,
                &handler,
                &limit,
                Message::empty(),
            )
            .await
            .unwrap();
        });

        assert!(executed_on_rx
            .recv()
            .unwrap()
            .starts_with("buffer-handler-selected-runtime"));
    }
}
