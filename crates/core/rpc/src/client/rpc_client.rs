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

use crate::client::client_state::InnerState;
use crate::client::dispatch::Envelope;
use crate::client::{BufferClient, ClientConf, ClientState, RawClient};
use crate::handler::{Frame, RpcFrame};
use crate::message::{Message, RefMessage};
use curvine_io::{IOError, IOResult};
use curvine_net::net::InetAddr;
use curvine_net::retry::TimeBondedRetry;
use curvine_runtime::runtime::Runtime;
use curvine_sys::RawPtr;
use log::warn;
use socket2::SockRef;
use std::io::ErrorKind;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::timeout;

enum BoxSender {
    Frame(RawPtr<RpcFrame>),
    Channel(Sender<Envelope>),
}

impl Clone for BoxSender {
    fn clone(&self) -> Self {
        match self {
            BoxSender::Frame(_) => panic!("The streaming client does not allow cloning"),
            BoxSender::Channel(v) => BoxSender::Channel(v.clone()),
        }
    }
}

// Unified access APIs for RawClient and BufferClient.
// RawClient uses BoxRaw to solve mutable reference issues, but it is not thread-safe and does not allow cloning.
#[derive(Clone)]
pub struct RpcClient {
    sender: BoxSender,
    state: Arc<ClientState>,
}

impl RpcClient {
    pub async fn new(
        buffer: bool,
        rt: Arc<Runtime>,
        addr: &InetAddr,
        conf: &ClientConf,
    ) -> IOResult<Self> {
        let client = if buffer {
            Self::with_buffer(rt, addr, conf).await?
        } else {
            Self::with_raw(addr, conf).await?
        };

        Ok(client)
    }

    pub async fn with_buffer(
        rt: Arc<Runtime>,
        addr: &InetAddr,
        conf: &ClientConf,
    ) -> IOResult<Self> {
        let client = BufferClient::new(rt.clone(), addr, conf).await?;
        Ok(Self {
            sender: BoxSender::Channel(client.sender),
            state: client.state,
        })
    }

    pub async fn with_raw(addr: &InetAddr, conf: &ClientConf) -> IOResult<Self> {
        let client = RawClient::new(addr, conf).await?;
        Ok(Self {
            sender: BoxSender::Frame(RawPtr::from_owned(client.frame)),
            state: Arc::new(client.state),
        })
    }

    pub async fn rpc(&self, msg: impl RefMessage) -> IOResult<Message> {
        let req_id = msg.req_id();
        let seq_id = msg.seq_id();

        let rep_msg = match &self.sender {
            BoxSender::Frame(f) => {
                let frame = f.as_mut();
                if let Err(e) = frame.send(msg).await {
                    self.set_closed();
                    return Err(e);
                }

                let msg = match frame.receive().await {
                    Ok(msg) => msg,
                    Err(e) => {
                        self.set_closed();
                        return Err(e);
                    }
                };

                if msg.is_empty() {
                    self.set_closed();
                    return err_box!("Connection {} is closed", self.state.conn_info());
                } else {
                    msg
                }
            }

            BoxSender::Channel(c) => {
                let (tx, rx) = oneshot::channel();
                try_err!(c.send(Envelope::new(msg.into_box(), tx)).await);
                try_err!(rx.await)?
            }
        };

        RawClient::check_response(req_id, seq_id, &rep_msg)?;
        Ok(rep_msg)
    }

    pub async fn timeout_rpc(&self, dur: Duration, msg: impl RefMessage) -> IOResult<Message> {
        timeout(dur, self.rpc(msg)).await?
    }

    // @todo Next consider using unsafe instead of arc.
    // Note: io-level errors will cause retry.
    // client will not check whether the response msg contains errors, and this part of the error should be handled by the business code.
    pub async fn retry_rpc(
        &self,
        dur: Duration,
        mut policy: TimeBondedRetry,
        msg: Message,
    ) -> IOResult<Message> {
        let msg = msg.into_arc();
        while policy.attempt().await {
            let rep = timeout(dur, self.rpc(msg.clone())).await;
            let rep = match rep {
                Ok(v) => v,
                Err(e) => {
                    // Timeout log log and try again.
                    warn!(
                        "Request failed, id {}(retries {} times): {}",
                        msg.req_id(),
                        policy.count(),
                        e
                    );
                    continue;
                }
            };

            match rep {
                Ok(response) => return Ok(response),

                Err(e) => {
                    warn!(
                        "Request failed, id {}(retries {} times): {}",
                        msg.req_id(),
                        policy.count(),
                        e
                    )
                }
            }
        }

        err_box!(
            "Request failed {} retries {} times",
            self.state.conn_info(),
            policy.count()
        )
    }

    pub fn is_raw(&self) -> bool {
        match &self.sender {
            BoxSender::Frame(_) => true,
            BoxSender::Channel(_) => false,
        }
    }

    pub fn is_buffer(&self) -> bool {
        match &self.sender {
            BoxSender::Frame(_) => false,
            BoxSender::Channel(_) => true,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    pub fn is_active(&self) -> bool {
        if self.is_closed() {
            return false;
        }

        // Raw clients have no background receiver to observe an idle peer's
        // FIN/RST. The local flag alone can therefore return a dead connection
        // to the block pool after a Worker restarts. Tokio TCP sockets are
        // nonblocking: peek detects an already-observed close without a network
        // round trip or consuming any pending response bytes. This does not
        // retry an RPC or eliminate a disconnect racing with the next request.
        if let BoxSender::Frame(frame) = &self.sender {
            let mut byte = [MaybeUninit::uninit(); 1];
            match SockRef::from(frame.as_ref().io()).peek(&mut byte) {
                Ok(0) => {}
                Ok(_) => return true,
                Err(error) if error.kind() == ErrorKind::WouldBlock => return true,
                Err(_) => {}
            }
            self.set_closed();
            return false;
        }

        true
    }

    pub fn set_closed(&self) {
        self.state.set_closed()
    }

    pub fn remote_addr(&self) -> &InetAddr {
        &self.state.remote_addr
    }

    pub fn local_addr(&self) -> &InetAddr {
        &self.state.local_addr
    }

    pub fn state(&self) -> InnerState {
        self.state.state.state()
    }

    pub fn take_error(&self) -> Option<IOError> {
        self.state.take_error()
    }
}

#[cfg(test)]
mod idle_raw_client_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn connected_pair() -> (RpcClient, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: InetAddr = listener.local_addr().unwrap().into();
        let conf = ClientConf::default();
        let (client, peer) = tokio::join!(RpcClient::with_raw(&addr, &conf), listener.accept());
        (client.unwrap(), peer.unwrap().0)
    }

    async fn readable(client: &RpcClient) {
        let BoxSender::Frame(frame) = &client.sender else {
            panic!("raw client required")
        };
        timeout(Duration::from_secs(2), frame.as_ref().io().readable())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn idle_raw_client_keeps_live_connection() {
        let (client, _peer) = connected_pair().await;
        assert!(client.is_active());
    }

    #[tokio::test]
    async fn idle_raw_client_rejects_peer_closed_before_another_rpc() {
        let (client, mut peer) = connected_pair().await;
        peer.shutdown().await.unwrap();
        readable(&client).await;
        assert!(
            !client.is_active(),
            "pool must detect EOF before issuing another write RPC"
        );
    }

    #[tokio::test]
    async fn idle_raw_client_liveness_check_preserves_pending_bytes() {
        let (client, mut peer) = connected_pair().await;
        peer.write_all(b"pending").await.unwrap();
        readable(&client).await;
        assert!(client.is_active());
        let BoxSender::Frame(frame) = &client.sender else {
            panic!("raw client required")
        };
        let mut bytes = [0; 7];
        timeout(
            Duration::from_secs(2),
            frame.as_mut().io_mut().read_exact(&mut bytes),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&bytes, b"pending");
    }

    #[tokio::test]
    async fn idle_raw_client_respects_explicit_closed_state() {
        let (client, _peer) = connected_pair().await;
        client.set_closed();
        assert!(!client.is_active());
    }
}
