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

use crate::handler::stream_handler::dispatch;
use crate::handler::{LimitConf, MessageHandler, ReadFrame, WriteFrame};
use crate::message::Message;
use crate::ServerConf;
use curvine_io::{IOError, IOResult};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use log::warn;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep_until, timeout, Instant};

/// A finished request and the connection permit it holds until written.
type Reply = (IOResult<Message>, OwnedSemaphorePermit);

/// Connection handler that processes requests of one connection concurrently.
///
/// [`StreamHandler`](crate::handler::StreamHandler) finishes one request before
/// reading the next, so a slow request blocks every later request on the same
/// connection. `BufferHandler` instead:
///
/// 1. reads requests in a loop and spawns one task per request;
/// 2. bounds work with two semaphores: `LimitConf::conn_limit` per connection
///    covers a request from dispatch until its reply is written, and the shared
///    `LimitConf::global_limit` covers execution only, so a peer that stops
///    reading holds just its own connection's permits;
/// 3. sends responses from a single writer task as soon as they are ready, so
///    responses can be out of order. Clients must match responses by `req_id`
///    (as `BufferClient` does).
///
/// Only use it for services whose requests are independent of each other on a
/// connection (e.g. master metadata RPC). Stateful streams such as worker block
/// reads/writes must keep using `StreamHandler`.
///
/// The read side is buffered (`ReadFrame`/`WriteFrame`), so splice and sendfile
/// are not used.
pub struct BufferHandler<M> {
    rt: Arc<Runtime>,
    read_frame: ReadFrame,
    write_frame: Option<WriteFrame>,
    handler: Arc<M>,
    conn_limit: Arc<Semaphore>,
    global_limit: Arc<Semaphore>,
    queue_size: usize,
    close_idle: bool,
    timeout: Duration,
}

impl<M: MessageHandler> BufferHandler<M> {
    pub fn new(
        rt: Arc<Runtime>,
        frames: (ReadFrame, WriteFrame),
        handler: M,
        limit: LimitConf,
        conf: &ServerConf,
    ) -> Self {
        let (read_frame, write_frame) = frames;
        BufferHandler {
            rt,
            read_frame,
            write_frame: Some(write_frame),
            handler: Arc::new(handler),
            conn_limit: Arc::new(Semaphore::new(limit.conn_limit)),
            global_limit: limit.global_limit,
            queue_size: limit.conn_limit,
            close_idle: conf.close_idle,
            timeout: Duration::from_millis(conf.timeout_ms),
        }
    }

    pub async fn run(&mut self) -> IOResult<()> {
        let write_frame = match self.write_frame.take() {
            Some(v) => v,
            None => return Ok(()),
        };

        let (tx, rx) = mpsc::channel(self.queue_size);
        let writer = self.rt.spawn(Self::write_loop(write_frame, rx));

        let read_res = self.read_loop(tx).await;

        // `read_loop` dropped its sender. The writer exits after in-flight
        // requests have sent their responses, or right away if it failed.
        let write_res = writer.await?;
        read_res.and(write_res)
    }

    fn permit_timeout(&self, kind: &str) -> IOResult<()> {
        warn!(
            "no {} permit within {:?}, closing connection",
            kind, self.timeout
        );
        Ok(())
    }

    async fn read_loop(&mut self, tx: mpsc::Sender<Reply>) -> IOResult<()> {
        loop {
            let res = tokio::select! {
                // The writer stopped (write error or failed request task):
                // stop reading so the connection closes.
                _ = tx.closed() => return Ok(()),
                res = timeout(self.timeout, self.read_frame.receive()) => res,
            };

            let request = match res {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => return Err(e),
                Err(_) if self.close_idle => return Ok(()),
                Err(_) => continue,
            };
            if request.is_empty() {
                return Ok(());
            }

            // Acquire the connection permit first so one busy connection waits
            // on its own limit instead of holding global permits.
            //
            // While waiting, this loop does not read the socket, so it cannot
            // see the peer close the connection. Bound the wait: when it
            // expires, stop reading and close the connection. In-flight
            // requests still write their replies first; the caller fails the
            // undispatched request and can retry on a new connection.
            let deadline = Instant::now() + self.timeout;
            let conn_permit = tokio::select! {
                _ = tx.closed() => return Ok(()),
                _ = sleep_until(deadline) => return self.permit_timeout("connection"),
                v = self.conn_limit.clone().acquire_owned() => v,
            };
            let global_permit = tokio::select! {
                _ = tx.closed() => return Ok(()),
                _ = sleep_until(deadline) => return self.permit_timeout("global"),
                v = self.global_limit.clone().acquire_owned() => v,
            };
            // The semaphores are never closed.
            let (conn_permit, global_permit) = (conn_permit.unwrap(), global_permit.unwrap());

            let rt = self.rt.clone();
            let handler = self.handler.clone();
            let tx = tx.clone();
            self.rt.spawn(async move {
                // Run dispatch in its own task so a panic anywhere in the
                // handler (is_sync, get_rt, handle, async_handle) becomes an
                // `Err` that closes the connection, instead of leaving the
                // caller waiting for a reply that never comes.
                let task_rt = rt.clone();
                let response = rt
                    .spawn(async move { dispatch(&task_rt, &handler, request).await })
                    .await
                    .map_err(IOError::from)
                    .and_then(|v| v);

                // Execution is done: free the global slot before queueing, so
                // a peer that stops reading cannot hold slots other
                // connections need.
                drop(global_permit);

                // The connection permit travels with the reply and is released
                // after it is written (or dropped). That bounds queued plus
                // writing replies per connection to `conn_limit`.
                // `send` fails only when the writer already stopped.
                let _ = tx.send((response, conn_permit)).await;
            });
        }
    }

    async fn write_loop(
        mut write_frame: WriteFrame,
        mut rx: mpsc::Receiver<Reply>,
    ) -> IOResult<()> {
        while let Some((response, _conn_permit)) = rx.recv().await {
            let response = match response {
                Ok(v) => v,
                Err(e) => {
                    // Same as StreamHandler: a failed handler task closes the
                    // connection and the client fails its in-flight calls.
                    warn!("request task failed, closing connection: {}", e);
                    return Err(e);
                }
            };

            if response.not_empty() {
                write_frame.send(&response).await?;
            }
        }
        Ok(())
    }
}
