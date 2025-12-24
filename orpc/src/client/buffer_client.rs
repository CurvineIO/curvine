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
use crate::err_box;
use crate::handler::{ReadFrame, WriteFrame};
use crate::io::net::InetAddr;
use crate::io::{IOError, IOResult};
use crate::message::{BoxMessage, Message, RefMessage};
use crate::runtime::{RpcRuntime, Runtime};
use log::{error, info, warn};
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
    pub async fn new(rt: Arc<Runtime>, addr: &InetAddr, conf: &ClientConf) -> IOResult<Self> {
        let raw = RawClient::new(addr, conf).await?;
        let call_state = Arc::new(CallMap::new());
        let client_state = Arc::new(raw.state);
        let (sender, receiver) = mpsc::channel(conf.message_size);
        let (read_frame, write_frame) = raw.frame.split();
        let timout = Duration::from_millis(conf.rpc_timeout_ms);

        let request_future = Self::request_future(
            client_state.clone(),
            timout,
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

        let response_future =
            Self::response_future(client_state.clone(), timout, read_frame, call_state.clone());
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
        loop {
            let res = timeout(dur, receiver.recv()).await;
            let opt_env = match res {
                Ok(v) => v,
                Err(_) => {
                    if close_idle {
                        // Channel has no data, and this timeout connection is automatically closed. .
                        info!("close idle connection: {}", client_state.conn_info());
                        client_state.set_closed();

                        break;
                    } else {
                        // Maintain heartbeat.
                        let msg = BoxMessage::Msg(Message::heartbeat());
                        if let Err(e) = write_frame.send(&msg).await {
                            let err = IOError::with_msg(e.into_raw(), "heartbeat sending failed");
                            client_state.set_error(err);
                            break;
                        } else {
                            continue;
                        }
                    }
                }
            };

            let env = match opt_env {
                None => break,
                Some(v) => v,
            };

            let req_id = env.msg.req_id();
            if env.is_canceled() {
                // The callback has been cancelled, and the next request is executed.
                info!(
                    "Request({},{}) has been canceled",
                    req_id,
                    client_state.conn_info()
                );
                continue;
            }

            // Set the callback.
            call_map.insert(req_id, env.cb);

            if let Err(e) = write_frame.send(&env.msg).await {
                // send failed, send a failed callback notification directly.
                match call_map.remove(req_id) {
                    None => {
                        warn!(
                            "Request({},{}) not found callback",
                            req_id,
                            client_state.conn_info()
                        );
                    }

                    Some(cb) => {
                        let error: IOResult<Message> = err_box!(
                            "Request({},{}) send failed:  {}",
                            req_id,
                            client_state.conn_info(),
                            e
                        );
                        if let Err(e) = cb.send(error) {
                            info!(
                                "Request({},{}) callback execute fail: {:?}",
                                req_id,
                                client_state.conn_info(),
                                e
                            );
                        }
                    }
                }

                // Network exception, setting connection has been closed.
                client_state.set_error(e);
                break;
            }
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
            let res = timeout(dur, read_frame.receive()).await;
            let read_res = match res {
                Ok(v) => v,

                Err(_) => {
                    // If an error occurs in the network channel, the future ends.
                    if client_state.has_error() {
                        break;
                    } else {
                        continue;
                    }
                }
            };

            let msg = read_res?;
            if msg.is_empty() {
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
                        info!(
                            "Request({},{}) callback execute fail: {:?}",
                            req_id,
                            client_state.conn_info(),
                            e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn check_calls(map: Arc<CallMap>, client_state: Arc<ClientState>) -> IOResult<()> {
        // Check whether there is any completed request.
        let inflight = map.clear();
        let len = inflight.len();

        for (id, cb) in inflight {
            let error: IOResult<Message> =
                err_box!("Connection {} closed", client_state.conn_info());
            if let Err(e) = cb.send(error) {
                info!(
                    "Request({},{}) callback execute fail: {:?}",
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
