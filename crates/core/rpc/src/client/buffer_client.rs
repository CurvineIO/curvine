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

use crate::client::dispatch::{CallMap, Envelope};
use crate::client::raw_client::RawClient;
use crate::client::{ClientConf, ClientState};
use crate::handler::{Frame, ReadFrame, WriteFrame};
use crate::message::{BoxMessage, Message, RefMessage};
use crate::IOUtils;
use curvine_io::{IOError, IOResult};
use curvine_net::net::InetAddr;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use log::{debug, error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time::timeout;

// BufferClient uses queue communication, which is thread-safe and can be shared among multiple threads.
#[derive(Clone)]
pub struct BufferClient {
    pub(crate) sender: Sender<Envelope>,
    pub(crate) state: Arc<ClientState>,
}

impl BufferClient {
    const BATCH_MAX: usize = 8;

    pub async fn new(rt: Arc<Runtime>, addr: &InetAddr, conf: &ClientConf) -> IOResult<Self> {
        let raw = RawClient::new(addr, conf).await?;
        let call_state = Arc::new(CallMap::new());
        let client_state = Arc::new(raw.state);
        let (sender, receiver) = mpsc::channel(conf.message_size);
        let (read_frame, write_frame) = raw.frame.split();
        let timeout = Duration::from_millis(conf.rpc_timeout_ms);

        let request_future = Self::request_future(
            client_state.clone(),
            timeout,
            conf.close_idle,
            receiver,
            write_frame,
            call_state.clone(),
        );
        rt.spawn(async move {
            if let Err(e) = request_future.await {
                error!("request_future error: {}", e)
            }
        });

        let response_future = Self::response_future(
            client_state.clone(),
            timeout,
            read_frame,
            call_state.clone(),
        );
        rt.spawn(async move {
            if let Err(e) = response_future.await {
                error!("response_future error: {}", e)
            }
        });

        let client = Self {
            sender,
            state: client_state,
        };

        Ok(client)
    }

    // Get data from the channel, send data to the server, and set a callback.
    async fn request_future(
        client_state: Arc<ClientState>,
        dur: Duration,
        close_idle: bool,
        mut receiver: mpsc::Receiver<Envelope>,
        mut write_frame: WriteFrame,
        call_map: Arc<CallMap>,
    ) -> IOResult<()> {
        let mut msgs: Vec<BoxMessage> = Vec::with_capacity(Self::BATCH_MAX);

        loop {
            if client_state.is_closed() {
                break;
            }

            let res = timeout(dur, receiver.recv()).await;
            let opt_env = match res {
                Ok(v) => v,
                Err(_) => {
                    if close_idle {
                        // Channel has no data, and this timeout connection is automatically closed. .
                        info!("close idle connection: {}", client_state.conn_info());
                        client_state.set_closed();
                        // Dropping the write half alone leaves the socket open
                        // until the peer's own idle timeout, because the read
                        // half still owns it. A half-close ends both.
                        if let Err(e) = write_frame.shutdown().await {
                            info!("idle shutdown {}: {}", client_state.conn_info(), e);
                        }

                        break;
                    } else {
                        // Maintain heartbeat.
                        let msg = BoxMessage::Msg(Message::heartbeat());
                        if let Err(e) = write_frame.send_timeout(&msg, dur).await {
                            let err = IOError::with_msg(e.into_raw(), "heartbeat sending failed");
                            client_state.set_error(err);
                            break;
                        }
                        continue;
                    }
                }
            };

            let env = match opt_env {
                None => break,
                Some(v) => v,
            };

            if client_state.is_closed() {
                let error: IOResult<Message> =
                    err_box!("connection {} closed", client_state.conn_info());
                let _ = env.send(error);
                break;
            }

            // Drain whatever else is already queued so one write can carry
            // several requests; metadata workloads arrive in bursts, so this
            // turns N writer wakeups + N syscalls into one. Each callback is
            // registered as its message is dequeued, before the first byte
            // goes out, so the reader task can never see a response ahead of
            // its registration.
            msgs.clear();
            let mut dequeued = 1;
            if env.is_canceled() {
                info!(
                    "Request({},{}) has been canceled",
                    env.msg.req_id(),
                    client_state.conn_info()
                );
            } else {
                let Envelope { msg, cb } = env;
                call_map.insert(msg.req_id(), cb);
                msgs.push(msg);
            }
            while dequeued < Self::BATCH_MAX {
                dequeued += 1;
                match receiver.try_recv() {
                    Ok(next) => {
                        if next.is_canceled() {
                            info!(
                                "Request({},{}) has been canceled",
                                next.msg.req_id(),
                                client_state.conn_info()
                            );
                        } else {
                            let Envelope { msg, cb } = next;
                            call_map.insert(msg.req_id(), cb);
                            msgs.push(msg);
                        }
                    }
                    Err(_) => break,
                }
            }
            if msgs.is_empty() {
                continue;
            }

            if let Err(e) = write_frame.send_batch_timeout(&msgs, dur).await {
                // Network exception, setting connection has been closed. All
                // in-flight callbacks, including this batch's, are failed by
                // check_calls when the loop exits.
                client_state.set_error(e);
                break;
            }

            msgs.clear();
        }

        Self::check_calls(call_map, client_state)
    }

    /// Obtain the future task of the server，
    /// 1. Read network data.
    /// 2. Get the callback function to be executed based on the request id from the state.
    /// 3. Execute this callback
    async fn response_future(
        client_state: Arc<ClientState>,
        dur: Duration,
        mut read_frame: ReadFrame,
        call_map: Arc<CallMap>,
    ) -> IOResult<()> {
        loop {
            let msg = match read_frame.receive_timeout(dur).await {
                Ok(msg) => msg,

                Err(e) if IOUtils::is_timeout(&e) => {
                    // A failed or closed connection takes no new requests, so
                    // the read half has nothing left to wait for.
                    if client_state.has_error() || client_state.is_closed() {
                        break;
                    }
                    continue;
                }

                Err(e) => {
                    client_state.set_error(e);
                    break;
                }
            };

            if msg.is_empty() {
                client_state.set_closed();
                break;
            }

            let req_id = msg.req_id();
            match call_map.remove(req_id) {
                None => {
                    error!(
                        "Request({},{}) not found callback",
                        req_id,
                        client_state.conn_info()
                    );
                }

                Some(cb) => {
                    // The callback notification failed, which may be because the caller has been destroyed, and the response_future only prints the log and the task does not end.
                    if let Err(e) = cb.send(Ok(msg)) {
                        debug!(
                            "Request({},{}) caller cancelled the request, drop response: {:?}",
                            req_id,
                            client_state.conn_info(),
                            e
                        );
                    }
                }
            }
        }

        Self::check_calls(call_map, client_state)
    }

    fn check_calls(map: Arc<CallMap>, client_state: Arc<ClientState>) -> IOResult<()> {
        // Check whether there is any completed request.
        let inflight = map.clear();
        let len = inflight.len();

        let err_msg = match client_state.take_error() {
            Some(err) => err_msg!("connection {}: {}", client_state.conn_info(), err),
            None => err_msg!("connection {} closed", client_state.conn_info()),
        };
        for (id, cb) in inflight {
            let error: IOResult<Message> = Err(err_msg.clone().into());
            if let Err(e) = cb.send(error) {
                debug!(
                    "Request({},{}) caller cancelled the request, drop connection-close notice: {:?}",
                    id,
                    client_state.conn_info(),
                    e
                );
            }
        }

        if len == 0 {
            Ok(())
        } else {
            err_box!(
                "Still have {} requests not completed when connection from {} is closed",
                len,
                client_state.conn_info()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BufferClient;
    use crate::client::dispatch::{CallMap, Envelope};
    use crate::client::ClientState;
    use crate::handler::{FrameBuf, ReadFrame, WriteFrame};
    use crate::message::{Builder, Message, ResponseStatus};
    use bytes::BytesMut;
    use curvine_io::IOResult;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::timeout;

    /// #1908: a peer close must mark the client state closed, otherwise the
    /// dead connection is reused and every request burns a full rpc timeout.
    #[tokio::test]
    async fn peer_close_must_mark_client_state_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();

        // Keep the write half alive so only the peer's FIN drives EOF.
        let (read_half, _write_half) = tokio::io::split(client);
        let read_frame = ReadFrame::new(read_half, FrameBuf::new(0));

        let state = Arc::new(ClientState::new(addr.into(), addr.into()));
        let call_map = Arc::new(CallMap::new());

        // Peer closes an idle connection.
        drop(server);

        let outcome = timeout(
            Duration::from_secs(5),
            BufferClient::response_future(
                state.clone(),
                Duration::from_millis(200),
                read_frame,
                call_map,
            ),
        )
        .await
        .expect("response_future did not return after the peer closed the connection");
        outcome.expect("response_future should exit cleanly on EOF");

        assert!(
            state.is_closed(),
            "peer closed the connection but client_state is still Normal; \
             the dead connection stays active and is reused, so every later \
             request waits a full rpc timeout instead of failing fast"
        );
    }

    /// #1908: a timeout inside a frame cannot be resumed (the consumed bytes are
    /// gone), so the connection must be dropped instead of reused.
    #[tokio::test]
    async fn mid_frame_timeout_closes_the_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();

        let (read_half, _write_half) = tokio::io::split(client);
        let read_frame = ReadFrame::new(read_half, FrameBuf::new(0));

        let state = Arc::new(ClientState::new(addr.into(), addr.into()));
        let call_map = Arc::new(CallMap::new());

        // Part of a frame head, then silence: the deadline fires mid-frame.
        server.write_all(&[1, 2, 3, 4]).await.unwrap();

        let res = timeout(
            Duration::from_secs(5),
            BufferClient::response_future(
                state.clone(),
                Duration::from_millis(200),
                read_frame,
                call_map,
            ),
        )
        .await
        .expect("response_future did not return after a mid-frame timeout");

        res.expect("the mid-frame timeout must end the response task cleanly");
        assert!(
            state.has_error(),
            "half a frame was consumed, the stream is desynced, but the \
             connection was kept alive"
        );
    }

    /// #1908: once the read side has given up on a connection, a request that
    /// arrives afterwards must fail fast. Writing it into the dead socket makes
    /// the caller burn a full rpc timeout before it learns nobody will answer.
    #[tokio::test]
    async fn request_on_a_dead_connection_fails_fast() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (_server, _peer) = listener.accept().await.unwrap();

        let (_read_half, write_half) = tokio::io::split(client);
        let write_frame = WriteFrame::new(write_half, FrameBuf::new(0));

        let state = Arc::new(ClientState::new(addr.into(), addr.into()));
        let call_map = Arc::new(CallMap::new());
        let (sender, receiver) = mpsc::channel(8);

        // Far beyond the assertion window: a pass must not be the idle tick
        // doing the work.
        let dur = Duration::from_secs(30);

        let task = tokio::spawn(BufferClient::request_future(
            state.clone(),
            dur,
            false,
            receiver,
            write_frame,
            call_map,
        ));

        // Let the request side settle into its receive loop while the
        // connection still looks healthy.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The read side gives up: EOF, a desynced stream, an IO error.
        state.set_closed();

        let (cb, promise) = oneshot::channel::<IOResult<Message>>();
        let env = Envelope::new(Builder::new_rpc(1i8).build(), cb);

        // Either the queue is already closed, or the request is dequeued and
        // rejected: both mean the caller learns immediately.
        if sender.send(env).await.is_ok() {
            let res = timeout(Duration::from_secs(2), promise)
                .await
                .expect("a request sent to a closed connection never got an answer")
                .expect("the callback was dropped");
            assert!(
                res.is_err(),
                "a request on a closed connection must not be accepted"
            );
        }

        let exit = timeout(Duration::from_secs(5), task)
            .await
            .expect("request_future kept running on a closed connection")
            .expect("request_future must not panic");
        // The request side leaves through `break`, so stopping is a clean exit.
        assert!(
            exit.is_ok(),
            "request_future must stop cleanly once the connection is closed, got {exit:?}"
        );
    }

    /// An idle connection has nothing to receive, so an empty receive window is
    /// normal and must not tear it down.
    #[tokio::test]
    async fn an_idle_connection_survives_the_receive_window() {
        const REQ_ID: i64 = 7;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (mut server, _peer) = listener.accept().await.unwrap();

        let (read_half, _write_half) = tokio::io::split(client);
        let read_frame = ReadFrame::new(read_half, FrameBuf::new(0));

        let state = Arc::new(ClientState::new(addr.into(), addr.into()));
        let call_map = Arc::new(CallMap::new());

        // Nothing is outstanding, so the loop must ride out several windows.
        let task = tokio::spawn(BufferClient::response_future(
            state.clone(),
            Duration::from_millis(100),
            read_frame,
            call_map.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(
            !state.is_closed(),
            "an idle connection must survive empty receive windows"
        );

        // A request goes out, and the peer answers it.
        let (cb, promise) = oneshot::channel::<IOResult<Message>>();
        call_map.insert(REQ_ID, cb);

        let mut frame = BytesMut::new();
        Builder::new_rpc(1i8)
            .response(ResponseStatus::Success)
            .req_id(REQ_ID)
            .build()
            .encode(&mut frame)
            .unwrap();
        server.write_all(&frame).await.unwrap();

        let msg = timeout(Duration::from_secs(5), promise)
            .await
            .expect("the response was never delivered: an idle window killed a live connection")
            .expect("the callback was dropped")
            .expect("the response must decode");
        assert_eq!(msg.req_id(), REQ_ID);
        assert!(
            !state.is_closed(),
            "idle windows must not mark a live connection closed"
        );

        task.abort();
    }

    /// A call is bounded by its own deadline, not by the per-connection receive
    /// window: a call issued late in a window inherits only the remainder of
    /// that window, and answering it after the window expires must still
    /// deliver. Failing it here would kill a healthy call (and every other call
    /// multiplexed on the connection) before its `timeout_rpc` budget ran out.
    #[tokio::test]
    async fn a_late_response_is_delivered_after_the_receive_window() {
        const REQ_ID: i64 = 7;
        const WINDOW: Duration = Duration::from_millis(400);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (mut server, _peer) = listener.accept().await.unwrap();

        let (read_half, _write_half) = tokio::io::split(client);
        let read_frame = ReadFrame::new(read_half, FrameBuf::new(0));

        let state = Arc::new(ClientState::new(addr.into(), addr.into()));
        let call_map = Arc::new(CallMap::new());

        let task = tokio::spawn(BufferClient::response_future(
            state.clone(),
            WINDOW,
            read_frame,
            call_map.clone(),
        ));

        // Issue the call near the end of the current window: it does not get a
        // window of its own, only whatever is left of this one.
        tokio::time::sleep(WINDOW.mul_f32(0.75)).await;
        let (cb, mut promise) = oneshot::channel::<IOResult<Message>>();
        call_map.insert(REQ_ID, cb);

        // The window the call inherited has now expired. It must still be
        // waiting: only the call's own deadline may fail it.
        tokio::time::sleep(WINDOW.mul_f32(0.5)).await;
        let still_waiting = timeout(Duration::from_millis(1), &mut promise)
            .await
            .is_err();
        assert!(
            still_waiting,
            "the receive window expired and failed a call that was still inside its own \
             deadline; on main that call simply kept waiting"
        );

        // The peer answers after the window expired. The response must arrive.
        let mut frame = BytesMut::new();
        Builder::new_rpc(1i8)
            .response(ResponseStatus::Success)
            .req_id(REQ_ID)
            .build()
            .encode(&mut frame)
            .unwrap();
        server.write_all(&frame).await.unwrap();

        let msg = timeout(Duration::from_secs(5), promise)
            .await
            .expect("the response was never delivered after the window expired")
            .expect("the callback was dropped")
            .expect("the response must decode");
        assert_eq!(msg.req_id(), REQ_ID);
        assert!(
            !state.is_closed(),
            "a late but healthy call must not close the connection"
        );

        task.abort();
    }

    /// Requests queued together must go out in queue order with their own
    /// req_id, so every response can be paired with the right callback.
    #[tokio::test]
    async fn a_batch_is_written_in_queue_order() {
        const N: i64 = 8;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _peer) = listener.accept().await.unwrap();

        let (_read_half, write_half) = tokio::io::split(client);
        let (server_read, _server_write) = tokio::io::split(server);
        let write_frame = WriteFrame::new(write_half, FrameBuf::new(0));
        let mut read_frame = ReadFrame::new(server_read, FrameBuf::new(0));

        let state = Arc::new(ClientState::new(addr.into(), addr.into()));
        let call_map = Arc::new(CallMap::new());
        let (sender, receiver) = mpsc::channel(16);

        let task = tokio::spawn(BufferClient::request_future(
            state,
            Duration::from_secs(30),
            false,
            receiver,
            write_frame,
            call_map,
        ));

        let mut promises = Vec::with_capacity(N as usize);
        for req_id in 0..N {
            let (cb, promise) = oneshot::channel::<IOResult<Message>>();
            // Keep the callbacks alive: dropping them would cancel the requests.
            promises.push(promise);
            sender
                .send(Envelope::new(
                    Builder::new_rpc(1i8).req_id(req_id).build(),
                    cb,
                ))
                .await
                .unwrap();
        }

        for req_id in 0..N {
            let got = timeout(Duration::from_secs(5), read_frame.receive())
                .await
                .expect("a queued request was never written")
                .expect("the frame must decode");
            assert_eq!(got.req_id(), req_id, "frames must keep queue order");
        }

        task.abort();
        drop(promises);
    }

    /// A backlog of cancelled requests must not hold up the next live one.
    #[tokio::test]
    async fn a_cancelled_backlog_does_not_starve_a_live_request() {
        const CANCELLED: i64 = 100;
        const LIVE_ID: i64 = 42;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _peer) = listener.accept().await.unwrap();

        let (_read_half, write_half) = tokio::io::split(client);
        let (server_read, _server_write) = tokio::io::split(server);
        let write_frame = WriteFrame::new(write_half, FrameBuf::new(0));
        let mut read_frame = ReadFrame::new(server_read, FrameBuf::new(0));

        let state = Arc::new(ClientState::new(addr.into(), addr.into()));
        let call_map = Arc::new(CallMap::new());
        let (sender, receiver) = mpsc::channel(16);

        let task = tokio::spawn(BufferClient::request_future(
            state,
            Duration::from_secs(30),
            false,
            receiver,
            write_frame,
            call_map,
        ));

        for req_id in 0..CANCELLED {
            let (cb, promise) = oneshot::channel::<IOResult<Message>>();
            // The caller gave up, so its callback is already closed.
            drop(promise);
            sender
                .send(Envelope::new(
                    Builder::new_rpc(1i8).req_id(-req_id - 1).build(),
                    cb,
                ))
                .await
                .unwrap();
        }

        let (cb, promise) = oneshot::channel::<IOResult<Message>>();
        sender
            .send(Envelope::new(
                Builder::new_rpc(1i8).req_id(LIVE_ID).build(),
                cb,
            ))
            .await
            .unwrap();

        let got = timeout(Duration::from_secs(5), read_frame.receive())
            .await
            .expect("cancelled requests held up a live one")
            .expect("the frame must decode");
        assert_eq!(got.req_id(), LIVE_ID);

        task.abort();
        drop(promise);
    }

    /// Every callback registered for a connection that dies must be answered with
    /// an error, otherwise its caller waits out the full rpc timeout.
    #[tokio::test]
    async fn check_calls_fails_every_registered_callback() {
        const N: i64 = 4;

        let addr = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap();
        let state = Arc::new(ClientState::new(addr.into(), addr.into()));
        let call_map = Arc::new(CallMap::new());

        let mut promises = Vec::new();
        for req_id in 0..N {
            let (cb, promise) = oneshot::channel::<IOResult<Message>>();
            call_map.insert(req_id, cb);
            promises.push(promise);
        }

        let exit = BufferClient::check_calls(call_map, state);
        assert!(
            exit.is_err(),
            "leaving requests behind must be reported as an error"
        );

        for promise in promises {
            let outcome = promise
                .await
                .expect("the callback was dropped instead of answered");
            assert!(
                outcome.is_err(),
                "a dead connection must fail its outstanding calls"
            );
        }
    }
}
