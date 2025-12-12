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
use futures::future::select_ok;
use log::warn;
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
                            "Rpc({}) call failed to node {}: {}",
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

        // Step 1: Try current leader first (fast path)
        if let Some(id) = self.leader_id() {
            match self.timeout_rpc::<E>(id, msg.clone()).await {
                Ok(v) => return Ok(v),

                Err((retry, e)) => {
                    if !retry {
                        return Err(e);
                    } else {
                        warn!(
                            "Rpc({}) call failed to leader {}: {}",
                            msg.req_id(),
                            self.get_addr_string(id),
                            e
                        );

                        // Optimization: If NotLeaderMaster error, trigger concurrent RPC
                        if self.is_not_leader_master(&e) {
                            warn!(
                                "Rpc({}) detected NotLeaderMaster, starting concurrent polling...",
                                msg.req_id()
                            );
                            if let crate::message::BoxMessage::Arc(arc_msg) = msg.clone() {
                                match self.concurrent_rpc::<E>(arc_msg).await {
                                    Ok((node_id, response)) => {
                                        warn!(
                                            "Rpc({}) succeeded via concurrent polling on node {}",
                                            msg.req_id(),
                                            self.get_addr_string(node_id)
                                        );
                                        self.change_leader(node_id);
                                        return Ok(response);
                                    }
                                    Err(_) => {
                                        warn!(
                                            "Rpc({}) concurrent polling failed, falling back to sequential polling",
                                            msg.req_id()
                                        );
                                        // Fallback to sequential polling
                                    }
                                }
                            }
                        }

                        let _ = last_error.insert(e);
                    }
                }
            }
        }

        // Step 2: Sequential polling with immediate switching (fallback)
        // Optimization: Only wait between rounds, not between nodes in the same round
        let mut policy = self.retry_builder.build();
        let node_list = self.node_list(false);
        let mut node_iter = node_list.iter().copied().cycle();

        while policy.attempt().await {
            // In each round, try all nodes immediately without waiting
            for _ in 0..node_list.len() {
                let id = node_iter.next().expect("cycle iterator never ends");

                match self.timeout_rpc::<E>(id, msg.clone()).await {
                    Ok(v) => {
                        self.change_leader(id);
                        return Ok(v);
                    }

                    Err((retry, e)) => {
                        if !retry {
                            self.change_leader(id);
                            return Err(e);
                        } else {
                            warn!(
                                "Rpc({}) failed at {}, switching to next node: {}",
                                msg.req_id(),
                                self.get_addr_string(id),
                                e
                            );
                            let _ = last_error.insert(e);
                            // Continue immediately to next node in the same round (no wait)
                            // Node1 failed → immediately → Node2 failed → immediately → Node3
                        }
                    }
                }
            }
            // Only after traversing all nodes, wait for the next round (100ms-2s)
        }

        let err = err_msg!(
            "Failed to determine after {} attempts: {:?}",
            policy.count(),
            last_error
        );
        Err(IOError::create(err).into())
    }

    fn is_not_leader_master<E: ErrorExt>(&self, e: &E) -> bool {
        e.should_retry() && e.should_continue()
    }

    /// Concurrently send RPC requests to all known master nodes
    /// Returns the first successful response along with the node ID
    /// Uses futures::select_ok to race all futures and get the first successful response
    async fn concurrent_rpc<E>(&self, msg: Arc<Message>) -> Result<(u64, Message), E>
    where
        E: ErrorExt + From<IOError> + From<CommonError>,
    {
        let node_list = self.node_list(false);

        // Create futures for all nodes
        let futures: Vec<_> = node_list
            .iter()
            .map(|&id| {
                let msg_clone = msg.clone();
                let connector = self;
                Box::pin(async move {
                    use crate::message::BoxMessage;
                    let box_msg = BoxMessage::Arc(msg_clone);
                    connector
                        .timeout_rpc::<E>(id, box_msg)
                        .await
                        .map(|response| (id, response))
                })
            })
            .collect();

        // Use select_ok to race all futures and get the first successful response
        match select_ok(futures).await {
            Ok((result, _remaining)) => Ok(result),
            Err(_) => {
                // All requests failed, convert the last error
                // Since select_ok doesn't give us the error details, we'll return a generic error
                Err(IOError::create(err_msg!("All concurrent RPC requests failed")).into())
            }
        }
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
