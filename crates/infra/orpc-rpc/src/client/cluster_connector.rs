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

use crate::client::{ClientConf, ClientFactory, RpcClient, SyncClient};
use crate::error::ErrorExt;
use crate::io::net::{InetAddr, NodeAddr};
use crate::io::retry::{TimeBondedRetry, TimeBondedRetryBuilder};
use crate::io::{IOError, IOResult};
use crate::message::{Message, MessageBuilder, RefMessage};
use crate::runtime::Runtime;
use crate::sync::FastDashMap;
use crate::{err_box, err_msg, CommonError};
use futures::future::select_all;
use log::{debug, warn};
use prost::Message as PMessage;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Cluster connection manager
// 1. Request the master, automatically obtain the active master node, handle retries and master switching
// 2. Create a connection with the worker node
pub struct ClusterConnector {
    factory: ClientFactory,
    leader_id: AtomicU64,
    group: FastDashMap<u64, Arc<NodeAddr>>,
    rpc_timeout: Duration,
    data_timeout: Duration,
    retry_builder: TimeBondedRetryBuilder,
}

impl ClusterConnector {
    pub const DEFAULT_LEADER_ID: u64 = 0;

    pub fn new(factory: ClientFactory) -> Self {
        let rpc_timeout = Duration::from_millis(factory.conf.rpc_timeout_ms);
        let data_timeout = Duration::from_millis(factory.conf.data_timeout_ms);
        let retry_builder = factory.conf.io_retry_builder();
        Self {
            factory,
            leader_id: AtomicU64::new(Self::DEFAULT_LEADER_ID),
            group: FastDashMap::default(),
            rpc_timeout,
            data_timeout,
            retry_builder,
        }
    }

    pub fn with_rt(conf: ClientConf, rt: Arc<Runtime>) -> Self {
        Self::new(ClientFactory::with_rt(conf, rt))
    }

    pub fn add_node(&self, peer: NodeAddr) -> IOResult<()> {
        if peer.id == Self::DEFAULT_LEADER_ID {
            err_box!("Node id cannot be {}", Self::DEFAULT_LEADER_ID)
        } else {
            self.group.insert(peer.id, Arc::new(peer));
            Ok(())
        }
    }

    pub fn factory(&self) -> &ClientFactory {
        &self.factory
    }

    pub fn leader_id(&self) -> Option<u64> {
        let id = self.leader_id.load(Ordering::SeqCst);
        if id == Self::DEFAULT_LEADER_ID {
            None
        } else {
            Some(id)
        }
    }

    pub fn change_leader(&self, id: u64) {
        self.leader_id.store(id, Ordering::SeqCst)
    }

    pub async fn create_client(&self, addr: &InetAddr, buffer: bool) -> IOResult<RpcClient> {
        self.factory.create(addr, buffer).await
    }

    pub async fn get_client(&self, addr: &InetAddr) -> IOResult<RpcClient> {
        self.factory.get(addr).await
    }

    pub async fn get_client_with_id(&self, id: u64) -> IOResult<RpcClient> {
        let addr = self.get_addr(id)?;
        self.factory.get(&addr.addr).await
    }

    pub fn create_sync_with_id(&self, id: u64) -> IOResult<SyncClient> {
        let addr = self.get_addr(id)?;
        self.factory.create_sync(&addr.addr)
    }

    pub fn get_addr(&self, id: u64) -> IOResult<Arc<NodeAddr>> {
        match self.group.get(&id) {
            None => err_box!("Node {} not exist", id),
            Some(v) => Ok(v.clone()),
        }
    }

    /// When obtaining node_list, it means that the leader id access failed and all nodes need to be traversed.
    fn node_list(&self, leader_first: bool) -> Vec<u64> {
        let mut vec = vec![];

        let leader_id = self.leader_id.load(Ordering::SeqCst);
        if leader_id != Self::DEFAULT_LEADER_ID && leader_first {
            vec.push(leader_id);
        }

        for item in self.group.iter() {
            if leader_id == item.id {
                continue;
            }
            vec.push(item.id)
        }

        if leader_id != Self::DEFAULT_LEADER_ID && !leader_first {
            vec.push(leader_id);
        }

        vec
    }

    pub fn conf(&self) -> &ClientConf {
        &self.factory.conf
    }

    fn get_addr_string(&self, id: u64) -> String {
        self.group
            .get(&id)
            .map(|x| x.addr.to_string())
            .unwrap_or("None".to_string())
    }

    fn remove_client_with_id(&self, id: u64) {
        match self.group.get(&id) {
            None => (),
            Some(v) => self.factory.remove(&v.addr),
        }
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.factory.clone_runtime()
    }

    pub fn rt(&self) -> &Runtime {
        self.factory.rt()
    }

    pub async fn timeout_rpc<E>(&self, id: u64, msg: impl RefMessage) -> Result<Message, (bool, E)>
    where
        E: ErrorExt + From<IOError> + From<CommonError>,
    {
        let client = match self.get_client_with_id(id).await {
            Err(e) => return Err((true, e.into())),
            Ok(v) => v,
        };

        match client.timeout_rpc(self.rpc_timeout, msg).await {
            Ok(v) => match v.check_error_ext::<E>() {
                Err(e) => Err((e.should_retry(), e)),
                Ok(_) => Ok(v),
            },

            Err(e) => {
                client.set_closed();
                self.remove_client_with_id(id);
                Err((true, e.into()))
            }
        }
    }

    // Send a retry request to the specified node.
    pub async fn retry_rpc<E>(&self, id: u64, msg: Message) -> Result<Message, E>
    where
        E: ErrorExt + From<IOError> + From<CommonError>,
    {
        let msg = msg.into_arc();
        let mut last_error: Option<E> = None;
        let mut policy = self.retry_builder.build();
        while policy.attempt().await {
            match self.timeout_rpc::<E>(id, msg.clone()).await {
                Ok(v) => return Ok(v),

                Err((retry, e)) => {
                    if !retry {
                        return Err(e);
                    } else {
                        warn!(
                            "rpc({}) call failed to node {}: {}",
                            msg.req_id(),
                            self.get_addr_string(id),
                            e
                        );
                    }
                    let _ = last_error.insert(e);
                }
            }
        }

        let err = err_msg!(
            "Failed to {} determine after {}, attempts: {:?}",
            self.get_addr_string(id),
            policy.count(),
            last_error
        );
        Err(IOError::create(err).into())
    }

    pub async fn rpc<E>(&self, msg: Message) -> Result<Message, E>
    where
        E: ErrorExt + From<IOError> + From<CommonError>,
    {
        let mut last_error: Option<E> = None;
        let msg = msg.into_arc();

        // Send a request to the current leader node.
        if let Some(id) = self.leader_id() {
            match self.timeout_rpc(id, msg.clone()).await {
                Ok(v) => return Ok(v),

                Err((false, e)) => {
                    return Err(e);
                }

                Err((true, e)) => {
                    warn!(
                        "rpc({}) call failed to leader {}: {}",
                        msg.req_id(),
                        self.get_addr_string(id),
                        e
                    );
                    last_error.replace(e);
                }
            }
        }

        // Poll to send requests to all nodes until timeout.
        // If the client returns that the current node is not the leader, we still perform polling and retry.
        // At this time, the server may be performing the master selection operation, and it is not advisable to fail directly to return.
        self.change_leader(Self::DEFAULT_LEADER_ID);
        let mut policy = self.retry_builder.build();
        let node_list = self.node_list(false);
        if node_list.is_empty() {
            let err = err_msg!("no nodes available in cluster");
            return Err(IOError::create(err).into());
        }

        while policy.attempt().await {
            let mut futures: Vec<_> = node_list
                .iter()
                .map(|&id| {
                    let msg = msg.clone();
                    Box::pin(async move {
                        let res = self.timeout_rpc::<E>(id, msg).await;
                        (id, res)
                    })
                })
                .collect();

            let mut failed_msgs = vec![];
            loop {
                if futures.is_empty() {
                    break;
                }

                let ((id, res), _index, remaining) = select_all(futures).await;
                match res {
                    Ok(v) => {
                        self.change_leader(id);
                        return Ok(v);
                    }

                    Err((false, e)) => {
                        self.change_leader(id);
                        return Err(e);
                    }

                    Err((true, e)) => {
                        futures = remaining;

                        let error_msg = format!(
                            "rpc({}) node {} is unavailable: {}",
                            msg.req_id(),
                            self.get_addr_string(id),
                            e
                        );
                        failed_msgs.push(error_msg);
                        last_error.replace(e);
                    }
                }
            }

            if !failed_msgs.is_empty() {
                if self.leader_id().is_some() {
                    debug!("{}", failed_msgs.join("; "));
                } else {
                    warn!("{}", failed_msgs.join("; "));
                }
            }
        }

        let err = err_msg!(
            "failed to determine after {} attempts: {:?}",
            policy.count(),
            last_error
        );
        Err(IOError::create(err).into())
    }

    pub async fn proto_rpc<T, R, E>(&self, code: impl Into<i8>, header: T) -> Result<R, E>
    where
        T: PMessage + Default,
        R: PMessage + Default,
        E: ErrorExt + From<IOError> + From<CommonError>,
    {
        let msg = MessageBuilder::new_rpc(code.into())
            .proto_header(header)
            .build();

        let rep = self.rpc::<E>(msg).await?;

        match rep.parse_header() {
            Ok(v) => Ok(v),
            Err(e) => Err(e.into()),
        }
    }

    pub fn rpc_timeout(&self) -> Duration {
        self.rpc_timeout
    }

    pub fn data_timeout(&self) -> Duration {
        self.data_timeout
    }

    pub fn retry_builder(&self) -> TimeBondedRetryBuilder {
        self.retry_builder.clone()
    }

    pub fn retry_inst(&self) -> TimeBondedRetry {
        self.retry_builder.build()
    }
}

impl Default for ClusterConnector {
    fn default() -> Self {
        let factory = ClientFactory::default();
        Self::new(factory)
    }
}
