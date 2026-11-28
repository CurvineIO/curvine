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
use std::net::Shutdown;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::timeout;

enum BoxSender {
    // The count belongs to this non-cloneable raw frame. Relaxed atomics only
    // provide interior mutability; they do not make concurrent raw I/O safe.
    Frame(RawPtr<RpcFrame>, AtomicUsize),
    Channel(Sender<Envelope>),
}

impl Clone for BoxSender {
    fn clone(&self) -> Self {
        match self {
            BoxSender::Frame(..) => panic!("The streaming client does not allow cloning"),
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

// Raw frames may be partially transmitted or consumed at an await point.
// Cancellation bypasses normal error handling, so only a completed operation
// may leave the connection reusable. Buffered clients have their own dispatcher
// and must not poison a shared connection when one caller is cancelled.
struct InFlightGuard<'a> {
    client: &'a RpcClient,
    armed: bool,
}

impl<'a> InFlightGuard<'a> {
    fn new(client: &'a RpcClient) -> Self {
        Self {
            client,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.client.set_closed();
        }
    }
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
            sender: BoxSender::Frame(RawPtr::from_owned(client.frame), AtomicUsize::new(0)),
            state: Arc::new(client.state),
        })
    }

    pub async fn rpc(&self, msg: impl RefMessage) -> IOResult<Message> {
        self.check_open()?;
        let req_id = msg.req_id();
        let seq_id = msg.seq_id();

        match &self.sender {
            BoxSender::Frame(..) => {
                self.send(msg).await?;
                self.receive(req_id, seq_id).await
            }

            BoxSender::Channel(c) => {
                let (tx, rx) = oneshot::channel();
                try_err!(c.send(Envelope::new(msg.into_box(), tx)).await);
                let rep_msg = try_err!(rx.await)?;
                RawClient::check_response(req_id, seq_id, &rep_msg)?;
                Ok(rep_msg)
            }
        }
    }

    /// Send a request without waiting for its response. Raw clients only: the
    /// server answers a connection's requests in order, so pair every send with
    /// a later [`Self::receive`], in the same order.
    pub async fn send(&self, msg: impl RefMessage) -> IOResult<()> {
        let BoxSender::Frame(f, outstanding) = &self.sender else {
            return err_box!("pipelined send needs a raw client");
        };
        self.check_open()?;
        let mut guard = InFlightGuard::new(self);
        f.as_mut().send(msg).await?;
        // Keep successful sends busy across the gap before receive is polled.
        outstanding.fetch_add(1, Ordering::Relaxed);
        guard.disarm();
        Ok(())
    }

    /// Receive the response to the oldest request sent with [`Self::send`].
    pub async fn receive(&self, req_id: i64, seq_id: i32) -> IOResult<Message> {
        let BoxSender::Frame(f, outstanding) = &self.sender else {
            return err_box!("pipelined receive needs a raw client");
        };
        self.check_open()?;
        let mut guard = InFlightGuard::new(self);
        let msg = f.as_mut().receive().await?;
        if msg.is_empty() {
            return err_box!("Connection {} is closed", self.state.conn_info());
        }
        RawClient::check_response(req_id, seq_id, &msg)?;
        // Existing callers may receive without a tracked send. Do not let
        // those successful receives underflow the outstanding count.
        let _ = outstanding.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            Some(n.saturating_sub(1))
        });
        guard.disarm();
        Ok(msg)
    }

    pub async fn timeout_send(&self, dur: Duration, msg: impl RefMessage) -> IOResult<()> {
        timeout(dur, self.send(msg)).await?
    }

    pub async fn timeout_receive(
        &self,
        dur: Duration,
        req_id: i64,
        seq_id: i32,
    ) -> IOResult<Message> {
        timeout(dur, self.receive(req_id, seq_id)).await?
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
            BoxSender::Frame(..) => true,
            BoxSender::Channel(_) => false,
        }
    }

    pub fn is_buffer(&self) -> bool {
        match &self.sender {
            BoxSender::Frame(..) => false,
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
        if let BoxSender::Frame(frame, _) = &self.sender {
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

    /// A live raw connection is reusable by a new owner only after every
    /// successfully sent request has had its response received. This check
    /// does not close a busy connection: its current owner may keep pipelining.
    pub fn is_reusable(&self) -> bool {
        if let BoxSender::Frame(_, outstanding) = &self.sender {
            if outstanding.load(Ordering::Relaxed) != 0 {
                return false;
            }
        }
        self.is_active()
    }

    fn check_open(&self) -> IOResult<()> {
        if self.is_closed() {
            return err_box!("Connection {} is closed", self.state.conn_info());
        }
        Ok(())
    }

    pub fn set_closed(&self) {
        self.state.set_closed();
        if let BoxSender::Frame(frame, _) = &self.sender {
            // Marking state alone leaves the peer waiting for the rest of a
            // frame until this client is dropped. Shut down both directions
            // now, including when a cancelled client has not reached the pool.
            let _ = SockRef::from(frame.as_ref().io()).shutdown(Shutdown::Both);
        }
    }

    pub(crate) fn same_connection(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
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
        let BoxSender::Frame(frame, _) = &client.sender else {
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
        let BoxSender::Frame(frame, _) = &client.sender else {
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

#[cfg(test)]
mod pipelined_tests {
    use super::*;
    use crate::message::Builder;
    use crate::test::SimpleServer;
    use curvine_io::DataSlice;
    use curvine_runtime::runtime::RpcRuntime;

    /// Requests sent back to back on one raw connection are answered in order,
    /// each matched to its own req/seq id.
    #[test]
    fn pipelined_requests_are_answered_in_order() {
        let server = SimpleServer::default();
        let addr = server.bind_addr().clone();
        server.start(0);
        let conf = ClientConf::default();
        let rt = Arc::new(conf.create_runtime());
        rt.block_on(async move {
            let mut client = None;
            for _ in 0..50 {
                if let Ok(c) = RpcClient::with_raw(&addr, &conf).await {
                    client = Some(c);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let client = client.expect("server failed to start");

            // SimpleHandler fails the first request of each req_id to exercise
            // retries; spend it before pipelining.
            let warmup = Builder::new_rpc(1).req_id(7).seq_id(-1).build();
            let _ = client.rpc(warmup).await;

            let words = ["a", "bb", "ccc"];
            for (i, w) in words.iter().enumerate() {
                let msg = Builder::new_rpc(1)
                    .req_id(7)
                    .seq_id(i as i32)
                    .data(DataSlice::from_str(w))
                    .build();
                client.send(msg).await.unwrap();
            }
            for (i, w) in words.iter().enumerate() {
                let rep = client.receive(7, i as i32).await.unwrap();
                assert_eq!(rep.data.as_slice(), w.to_uppercase().as_bytes());
            }
        });
    }

    #[test]
    fn pipelined_send_rejects_buffer_client() {
        let server = SimpleServer::default();
        let addr = server.bind_addr().clone();
        server.start(0);
        let conf = ClientConf::default();
        let rt = Arc::new(conf.create_runtime());
        let rt2 = rt.clone();
        rt.block_on(async move {
            let mut client = None;
            for _ in 0..50 {
                if let Ok(c) = RpcClient::with_buffer(rt2.clone(), &addr, &conf).await {
                    client = Some(c);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let client = client.expect("server failed to start");
            let msg = Builder::new_rpc(1).req_id(8).seq_id(0).build();
            assert!(client.send(msg).await.is_err());
        });
    }
}
