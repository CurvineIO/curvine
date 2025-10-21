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
use crate::io::net::InetAddr;
use crate::io::retry::TimeBondedRetry;
use crate::io::{IOError, IOResult};
use crate::message::{Message, RefMessage};
use crate::runtime::Runtime;
use crate::sys::RawPtr;
use crate::{err_box, try_err};
use log::warn;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::timeout;
#[cfg(feature = "ucp")]
use crate::ucp::reactor::UcpFrame;
#[cfg(feature = "ucp")]
use crate::ucp::reactor::UcpRuntime;


enum BoxSender {
    Frame(RawPtr<RpcFrame>),
    Channel(Sender<Envelope>),
    #[cfg(feature = "ucp")]
    UcpFrame(RawPtr<UcpFrame>),
}

impl Clone for BoxSender {
    fn clone(&self) -> Self {
        match self {
            BoxSender::Channel(v) => BoxSender::Channel(v.clone()),
            _ => panic!("The streaming client does not allow cloning"),
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


    #[cfg(feature = "ucp")]
    pub async fn with_ucp(
        rt: &UcpRuntime,
        addr: &InetAddr,
        conf: &ClientConf,
    ) -> IOResult<Self> {
        use crate::ucp::reactor::RmaType;
        let mut endpoint = rt.connect_async(addr)?;
        endpoint.handshake_request(RmaType::Both, conf.buffer_size).await?;
        let frame = UcpFrame::with_client(endpoint, conf);

        Ok(Self {
            sender: BoxSender::UcpFrame(RawPtr::from_owned(frame)),
            state: Arc::new(ClientState::default()),
        })
    }

    pub async fn rpc(&self, msg: impl RefMessage) -> IOResult<Message> {
        let req_id = msg.req_id();
        let seq_id = msg.seq_id();
        let msg = msg.into_box();

        let rep_msg = match &self.sender {
            BoxSender::Frame(f) => {
                let frame = f.as_mut();
                frame.send(msg).await?;
                let msg = frame.receive().await?;
                if msg.is_empty() {
                    return err_box!("Connection {} is closed", self.state.conn_info());
                } else {
                    msg
                }
            }

            #[cfg(feature = "ucp")]
            BoxSender::UcpFrame(f) => {
                let frame = f.as_mut();
                frame.send(msg).await?;
                let msg = frame.receive().await?;
                if msg.is_empty() {
                    return err_box!("Connection {} is closed", self.state.conn_info());
                } else {
                    msg
                }
            }

            BoxSender::Channel(c) => {
                let (tx, rx) = oneshot::channel();
                try_err!(c.send(Envelope::new(msg, tx)).await);
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
            BoxSender::Channel(_) => false,
            _ => true,
        }
    }

    pub fn is_buffer(&self) -> bool {
        !self.is_raw()
    }

    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    pub fn is_active(&self) -> bool {
        !self.is_closed()
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
mod tests {
    use crate::client::{ClientConf, RpcClient};
    use crate::io::net::InetAddr;
    use crate::io::IOResult;
    use crate::message::Builder;
    use crate::runtime::{RpcRuntime, Runtime};
    use crate::sys::DataSlice;
    use crate::test::SimpleServer;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test1() -> IOResult<()> {
        let server = SimpleServer::default();
        let addr = server.bind_addr().clone();
        server.start(10000);

        let conf = ClientConf::default();
        let rt = Arc::new(conf.create_runtime());

        rt.block_on(task(&addr, true, true, &conf, rt.clone()));
        rt.block_on(task(&addr, false, true, &conf, rt.clone()));

        Ok(())
    }

    async fn task(addr: &InetAddr, buffer: bool, retry: bool, conf: &ClientConf, rt: Arc<Runtime>) {
        let client = RpcClient::new(buffer, rt, addr, conf).await.unwrap();

        assert_eq!(buffer, client.is_buffer());

        let msg = Builder::new_rpc(1).data(DataSlice::from_str("abc")).build();

        let rep = if retry {
            let dur = Duration::from_millis(conf.rpc_timeout_ms);
            let policy = conf.io_retry_policy();
            client.retry_rpc(dur, policy, msg).await.unwrap()
        } else {
            client.rpc(msg).await.unwrap()
        };

        println!("rep = {:?}", rep);
    }
}
