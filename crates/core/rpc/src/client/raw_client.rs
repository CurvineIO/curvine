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

use crate::client::{ClientConf, ClientState};
use crate::handler::RpcFrame;
use crate::message::Message;
use curvine_io::{IOError, IOResult};
use curvine_net::net::InetAddr;
use log::warn;
use socket2::SockRef;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time;

// Low-level connection, direct connection to tcp
pub struct RawClient {
    pub(crate) frame: RpcFrame,
    pub(crate) state: ClientState,
}

impl RawClient {
    pub async fn new(addr: &InetAddr, conf: &ClientConf) -> IOResult<Self> {
        let stream = Self::conn(addr, conf).await?;
        let sock_ref = SockRef::from(&stream);
        sock_ref.set_tcp_nodelay(true)?;
        sock_ref.set_keepalive(true)?;
        // Bound unacknowledged/zero-window writes on Linux with the existing
        // RPC budget. This is not a receive deadline: if all bytes are ACKed
        // but the peer stops replying, the application timeout is still needed.
        #[cfg(target_os = "linux")]
        sock_ref.set_tcp_user_timeout(Some(Duration::from_millis(conf.rpc_timeout_ms)))?;

        let local_addr = stream.local_addr()?.into();
        let frame = RpcFrame::with_client(stream, conf.buffer_size);
        let client = Self {
            frame,
            state: ClientState::new(addr.clone(), local_addr),
        };

        Ok(client)
    }

    pub async fn conn(addr: &InetAddr, conf: &ClientConf) -> IOResult<TcpStream> {
        let timeout = Duration::from_millis(conf.conn_timeout_ms);
        let stream = time::timeout(timeout, TcpStream::connect(addr.as_pair())).await??;
        Ok(stream)
    }

    pub async fn conn_retry(addr: &InetAddr, conf: &ClientConf) -> IOResult<TcpStream> {
        let mut policy = conf.conn_retry_policy();
        let timeout = Duration::from_millis(conf.conn_timeout_ms);

        let mut last_error: Option<IOError> = None;
        while policy.attempt().await {
            let stream = time::timeout(timeout, TcpStream::connect(addr.as_pair())).await;

            match stream {
                Ok(e) => match e {
                    Ok(s) => return Ok(s),

                    Err(e) => {
                        warn!(
                            "Failed to connect {}(retry times {}): {}",
                            addr,
                            policy.count(),
                            e
                        );
                        let _ = last_error.replace(e.into());
                    }
                },

                Err(e) => {
                    warn!(
                        "Failed to connect {}(retry times {}): {}",
                        addr,
                        policy.count(),
                        e
                    );
                    let _ = last_error.replace(e.into());
                }
            }
        }

        let msg = format!(
            "Failed to connect {} after {} attempts",
            addr,
            policy.count()
        );
        Err(IOError::with_opt_msg(last_error, msg))
    }

    pub fn check_response(req_id: i64, seq_id: i32, response: &Message) -> IOResult<()> {
        if response.req_id() != req_id {
            err_box!(
                "Returned req_id did not match, expected {}, actual {}",
                req_id,
                response.req_id()
            )
        } else if response.seq_id() != seq_id {
            err_box!(
                "Returned seq_id did not match, expected {}, actual {}",
                seq_id,
                response.seq_id()
            )
        } else {
            Ok(())
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn tcp_user_timeout_follows_rpc_budget() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: InetAddr = listener.local_addr().unwrap().into();
        for millis in [1234, 0] {
            let conf = ClientConf {
                rpc_timeout_ms: millis,
                ..ClientConf::default()
            };
            let (client, peer) = tokio::join!(RawClient::new(&addr, &conf), listener.accept());
            let client = client.unwrap();
            let _peer = peer.unwrap().0;
            let expected = if millis == 0 {
                None
            } else {
                Some(Duration::from_millis(millis))
            };
            assert_eq!(
                SockRef::from(client.frame.io()).tcp_user_timeout().unwrap(),
                expected,
            );
        }
    }
}
