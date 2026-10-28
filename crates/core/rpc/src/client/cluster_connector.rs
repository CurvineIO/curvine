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

use crate::client::{ClientConf, ClientFactory, RpcClient};
use crate::message::{Message, MessageBuilder, RefMessage};
use curvine_core_error::{CommonError, ErrorExt};
use curvine_io::{IOError, IOResult};
use curvine_net::net::{InetAddr, NodeAddr};
use curvine_net::retry::{TimeBondedRetry, TimeBondedRetryBuilder};
use curvine_runtime::runtime::Runtime;
use futures::future::select_all;
use log::{debug, warn};
use prost::Message as PMessage;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

// Cluster connection manager
// 1. Request the master, automatically obtain the active master node, handle retries and master switching
// 2. Create a connection with the worker node
pub struct ClusterConnector {
    factory: ClientFactory,
    membership: RwLock<ConnectorMembership>,
    rpc_timeout: Duration,
    data_timeout: Duration,
    retry_builder: TimeBondedRetryBuilder,
}

#[derive(Default)]
struct ConnectorMembership {
    leader_id: u64,
    peers: HashMap<u64, Arc<NodeAddr>>,
}

impl ClusterConnector {
    pub const DEFAULT_LEADER_ID: u64 = 0;

    pub fn new(factory: ClientFactory) -> Self {
        let rpc_timeout = Duration::from_millis(factory.conf.rpc_timeout_ms);
        let data_timeout = Duration::from_millis(factory.conf.data_timeout_ms);
        let retry_builder = factory.conf.io_retry_builder();
        Self {
            factory,
            membership: RwLock::new(ConnectorMembership::default()),
            rpc_timeout,
            data_timeout,
            retry_builder,
        }
    }

    pub fn with_rt(conf: ClientConf, rt: Arc<Runtime>) -> Self {
        Self::new(ClientFactory::with_rt(conf, rt))
    }

    pub fn add_node(&self, peer: NodeAddr) -> IOResult<()> {
        Self::validate_peer(&peer)?;

        let mut membership = self.membership.write().unwrap();
        let old = membership.peers.insert(peer.id, Arc::new(peer.clone()));
        if old.as_deref().is_some_and(|old| old != &peer) {
            if membership.leader_id == peer.id {
                membership.leader_id = Self::DEFAULT_LEADER_ID;
            }
            if let Some(old) = old {
                Self::remove_unused_client(&self.factory, old.addr(), &membership.peers);
            }
        }
        Ok(())
    }

    /// Atomically replace the connector's complete set of known peers.
    ///
    /// The peer ID is the stable identity: an address change for an existing ID
    /// replaces the old endpoint and clears a cached leader hint for that node.
    pub fn reconcile_nodes<I>(&self, peers: I) -> IOResult<()>
    where
        I: IntoIterator<Item = NodeAddr>,
    {
        let mut next = HashMap::new();
        for peer in peers {
            Self::validate_peer(&peer)?;
            if let Some(previous) = next.insert(peer.id, Arc::new(peer.clone())) {
                if previous.as_ref() != &peer {
                    return err_box!("Conflicting addresses for node id {}", peer.id);
                }
            }
        }

        let mut membership = self.membership.write().unwrap();
        let leader_changed = membership
            .peers
            .get(&membership.leader_id)
            .is_some_and(|old| next.get(&membership.leader_id).is_none_or(|new| old != new));
        if leader_changed {
            membership.leader_id = Self::DEFAULT_LEADER_ID;
        }

        let retained_addresses: Vec<_> = next.values().map(|peer| peer.addr().clone()).collect();
        let removed_addresses: Vec<_> = membership
            .peers
            .values()
            .filter(|old| next.get(&old.id).is_none_or(|new| *old != new))
            .map(|peer| peer.addr().clone())
            .collect();

        membership.peers = next;
        for addr in removed_addresses {
            if !retained_addresses.contains(&addr) {
                self.factory.remove(&addr);
            }
        }
        Ok(())
    }

    /// Remove one peer from the current membership. Removing an absent peer is
    /// idempotent; node zero is reserved as the no-leader sentinel.
    pub fn remove_node(&self, id: u64) -> IOResult<()> {
        if id == Self::DEFAULT_LEADER_ID {
            return err_box!("Node id cannot be {}", Self::DEFAULT_LEADER_ID);
        }

        let mut membership = self.membership.write().unwrap();
        if let Some(removed) = membership.peers.remove(&id) {
            if membership.leader_id == id {
                membership.leader_id = Self::DEFAULT_LEADER_ID;
            }
            Self::remove_unused_client(&self.factory, removed.addr(), &membership.peers);
        }
        Ok(())
    }

    /// Return the current membership in stable ID order.
    pub fn peer_snapshot(&self) -> Vec<NodeAddr> {
        let membership = self.membership.read().unwrap();
        let mut peers: Vec<_> = membership
            .peers
            .values()
            .map(|peer| peer.as_ref().clone())
            .collect();
        peers.sort_by_key(|peer| peer.id);
        peers
    }

    fn validate_peer(peer: &NodeAddr) -> IOResult<()> {
        if peer.id == Self::DEFAULT_LEADER_ID {
            err_box!("Node id cannot be {}", Self::DEFAULT_LEADER_ID)
        } else {
            Ok(())
        }
    }

    fn remove_unused_client(
        factory: &ClientFactory,
        addr: &InetAddr,
        remaining: &HashMap<u64, Arc<NodeAddr>>,
    ) {
        if !remaining.values().any(|peer| peer.addr() == addr) {
            factory.remove(addr);
        }
    }

    pub fn factory(&self) -> &ClientFactory {
        &self.factory
    }

    pub fn leader_id(&self) -> Option<u64> {
        let id = self.membership.read().unwrap().leader_id;
        if id == Self::DEFAULT_LEADER_ID {
            None
        } else {
            Some(id)
        }
    }

    pub fn change_leader(&self, id: u64) {
        let mut membership = self.membership.write().unwrap();
        membership.leader_id =
            if id == Self::DEFAULT_LEADER_ID || membership.peers.contains_key(&id) {
                id
            } else {
                Self::DEFAULT_LEADER_ID
            };
    }

    fn change_leader_if_current(&self, peer: &NodeAddr) {
        let mut membership = self.membership.write().unwrap();
        if membership
            .peers
            .get(&peer.id)
            .is_some_and(|current| current.as_ref() == peer)
        {
            membership.leader_id = peer.id;
        }
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

    pub async fn create_client_with_id(&self, id: u64, buffer: bool) -> IOResult<RpcClient> {
        let addr = self.get_addr(id)?;
        self.factory.create(&addr.addr, buffer).await
    }

    pub fn get_addr(&self, id: u64) -> IOResult<Arc<NodeAddr>> {
        self.membership
            .read()
            .unwrap()
            .peers
            .get(&id)
            .cloned()
            .ok_or_else(|| IOError::create(format!("Node {} not exist", id)))
    }

    pub fn conf(&self) -> &ClientConf {
        &self.factory.conf
    }

    pub fn get_addr_string(&self, id: u64) -> String {
        self.membership
            .read()
            .unwrap()
            .peers
            .get(&id)
            .map(|peer| peer.addr.to_string())
            .unwrap_or("None".to_string())
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
        let peer = match self.get_addr(id) {
            Err(e) => return Err((true, e.into())),
            Ok(v) => v,
        };
        self.timeout_rpc_to_peer::<E>(&peer, msg).await
    }

    /// Send one RPC with a deadline that covers both pooled-client acquisition
    /// (including connection establishment) and the request/response exchange.
    /// This is used by opportunistic follower reads so a blackholed peer cannot
    /// consume the leader's remaining request budget.
    pub async fn timeout_rpc_with_timeout<E>(
        &self,
        id: u64,
        msg: impl RefMessage,
        timeout: Duration,
    ) -> Result<Message, (bool, E)>
    where
        E: ErrorExt + From<IOError> + From<CommonError>,
    {
        let peer = match self.get_addr(id) {
            Err(e) => return Err((true, e.into())),
            Ok(v) => v,
        };
        self.timeout_rpc_to_peer_with_timeout::<E>(&peer, msg, timeout)
            .await
    }

    async fn timeout_rpc_to_peer<E>(
        &self,
        peer: &NodeAddr,
        msg: impl RefMessage,
    ) -> Result<Message, (bool, E)>
    where
        E: ErrorExt + From<IOError> + From<CommonError>,
    {
        // A snapshot-based caller may have been queued while membership
        // changed. Avoid opening a connection to an endpoint that is already
        // obsolete when the request starts.
        let is_current = self
            .membership
            .read()
            .unwrap()
            .peers
            .get(&peer.id)
            .is_some_and(|current| current.as_ref() == peer);
        if !is_current {
            return Err((
                true,
                IOError::create(format!(
                    "Node {} is no longer in the connector membership",
                    peer.id
                ))
                .into(),
            ));
        }

        let client = match self.factory.get(&peer.addr).await {
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
                self.factory.remove_client(&peer.addr, &client);
                Err((true, e.into()))
            }
        }
    }

    async fn timeout_rpc_to_peer_with_timeout<E>(
        &self,
        peer: &NodeAddr,
        msg: impl RefMessage,
        timeout: Duration,
    ) -> Result<Message, (bool, E)>
    where
        E: ErrorExt + From<IOError> + From<CommonError>,
    {
        // Reject stale peer snapshots before spending any of the caller's
        // deadline opening a connection to an endpoint that is already gone.
        let is_current = self
            .membership
            .read()
            .unwrap()
            .peers
            .get(&peer.id)
            .is_some_and(|current| current.as_ref() == peer);
        if !is_current {
            return Err((
                true,
                IOError::create(format!(
                    "Node {} is no longer in the connector membership",
                    peer.id
                ))
                .into(),
            ));
        }

        let deadline = tokio::time::Instant::now() + timeout;
        let client = match tokio::time::timeout_at(deadline, self.factory.get(&peer.addr)).await {
            Ok(Ok(client)) => client,
            Ok(Err(error)) => return Err((true, error.into())),
            Err(_) => {
                return Err((
                    true,
                    IOError::create(format!(
                        "timed out connecting to follower node {} after {:?}",
                        peer.id, timeout
                    ))
                    .into(),
                ));
            }
        };

        match tokio::time::timeout_at(deadline, client.rpc(msg)).await {
            Ok(Ok(response)) => match response.check_error_ext::<E>() {
                Err(error) => Err((error.should_retry(), error)),
                Ok(_) => Ok(response),
            },
            Ok(Err(error)) => {
                client.set_closed();
                self.factory.remove_client(&peer.addr, &client);
                Err((true, error.into()))
            }
            Err(_) => {
                client.set_closed();
                self.factory.remove_client(&peer.addr, &client);
                Err((
                    true,
                    IOError::create(format!(
                        "timed out requesting follower node {} after {:?}",
                        peer.id, timeout
                    ))
                    .into(),
                ))
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

    /// Send to one peer using the configured RPC timeout and retry budget.
    /// This API never reads or updates the cached leader hint.
    pub async fn rpc_to_peer<E>(&self, id: u64, msg: Message) -> Result<Message, E>
    where
        E: ErrorExt + From<IOError> + From<CommonError>,
    {
        self.retry_rpc(id, msg).await
    }

    pub async fn rpc<E>(&self, msg: Message) -> Result<Message, E>
    where
        E: ErrorExt + From<IOError> + From<CommonError>,
    {
        let mut last_error: Option<E> = None;
        let msg = msg.into_arc();

        // Send a request to the current leader node.
        if let Some(id) = self.leader_id() {
            if let Ok(peer) = self.get_addr(id) {
                match self.timeout_rpc_to_peer::<E>(&peer, msg.clone()).await {
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
        }

        // Poll to send requests to all nodes until timeout.
        // If the client returns that the current node is not the leader, we still perform polling and retry.
        // At this time, the server may be performing the master selection operation, and it is not advisable to fail directly to return.
        self.change_leader(Self::DEFAULT_LEADER_ID);
        let mut policy = self.retry_builder.build();
        while policy.attempt().await {
            // Membership can change while a request is retrying. Re-read the
            // complete snapshot for each attempt so a watch update takes effect
            // without waiting for the whole retry budget to expire.
            let node_list = self.peer_snapshot();
            if node_list.is_empty() {
                let err = err_msg!("no nodes available in cluster");
                return Err(IOError::create(err).into());
            }

            let mut futures: Vec<_> = node_list
                .iter()
                .map(|peer| {
                    let peer = peer.clone();
                    let msg = msg.clone();
                    Box::pin(async move {
                        let res = self.timeout_rpc_to_peer::<E>(&peer, msg).await;
                        (peer, res)
                    })
                })
                .collect();

            let mut failed_msgs = vec![];
            loop {
                if futures.is_empty() {
                    break;
                }

                let ((peer, res), _index, remaining) = select_all(futures).await;
                match res {
                    Ok(v) => {
                        self.change_leader_if_current(&peer);
                        return Ok(v);
                    }

                    Err((false, e)) => {
                        self.change_leader_if_current(&peer);
                        return Err(e);
                    }

                    Err((true, e)) => {
                        futures = remaining;

                        let error_msg =
                            format!("rpc({}) node {} is unavailable: {}", msg.req_id(), peer, e);
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

#[cfg(test)]
mod tests {
    use super::{ClientConf, ClientFactory, ClusterConnector, NodeAddr};
    use crate::handler::{HandlerService, MessageHandler};
    use crate::message::{Message, MessageBuilder};
    use crate::server::{RpcServer, ServerConf};
    use curvine_core_error::{CommonErrorExt, CommonResultExt};
    use curvine_io::DataSlice;
    use curvine_net::net::{ConnState, InetAddr, NetUtils};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn test_connector() -> ClusterConnector {
        let conf = ClientConf {
            io_threads: 1,
            worker_threads: 1,
            ..Default::default()
        };
        ClusterConnector::new(ClientFactory::new(conf))
    }

    fn peer(id: u64, hostname: &str, port: u16) -> NodeAddr {
        NodeAddr::new(id, hostname, port)
    }

    struct EchoService;

    struct EchoHandler;

    impl MessageHandler for EchoHandler {
        type Error = CommonErrorExt;

        fn handle(&self, msg: &Message) -> CommonResultExt<Message> {
            Ok(msg.success())
        }
    }

    impl HandlerService for EchoService {
        type Item = EchoHandler;

        fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
            EchoHandler
        }
    }

    fn start_echo_server() -> InetAddr {
        let port = NetUtils::hold_available_port();
        let mut conf = ServerConf::with_hostname("127.0.0.1", port);
        conf.io_threads = 1;
        conf.worker_threads = 1;
        let server = RpcServer::new(conf, EchoService);
        let addr = server.bind_addr().clone();
        thread::spawn(move || server.block_on_start());
        addr
    }

    #[test]
    fn reconcile_replaces_membership_and_invalidates_stale_leader() {
        let connector = test_connector();
        let initial = vec![peer(2, "master-2", 9020), peer(1, "master-1", 9010)];
        connector.reconcile_nodes(initial.clone()).unwrap();
        connector.change_leader(1);

        assert_eq!(connector.leader_id(), Some(1));
        assert_eq!(
            connector.peer_snapshot(),
            vec![peer(1, "master-1", 9010), peer(2, "master-2", 9020)]
        );

        // An identical watch snapshot does not disturb a usable leader hint.
        connector.reconcile_nodes(initial).unwrap();
        assert_eq!(connector.leader_id(), Some(1));

        // The stable ID remains, but its new endpoint invalidates the old hint.
        connector
            .reconcile_nodes(vec![
                peer(1, "master-1-new", 9110),
                peer(3, "master-3", 9030),
            ])
            .unwrap();
        assert_eq!(connector.leader_id(), None);
        assert_eq!(
            connector.peer_snapshot(),
            vec![peer(1, "master-1-new", 9110), peer(3, "master-3", 9030)]
        );

        connector.change_leader(3);
        connector.remove_node(3).unwrap();
        assert_eq!(connector.leader_id(), None);
        assert!(connector.get_addr(3).is_err());
        connector.remove_node(3).unwrap(); // Removing an absent peer is idempotent.
        assert!(connector.remove_node(0).is_err());
    }

    #[test]
    fn invalid_snapshot_does_not_partially_replace_membership() {
        let connector = test_connector();
        let initial = vec![peer(1, "master-1", 9010)];
        connector.reconcile_nodes(initial.clone()).unwrap();
        connector.change_leader(1);

        assert!(connector
            .reconcile_nodes(vec![
                peer(2, "master-2-a", 9020),
                peer(2, "master-2-b", 9021)
            ])
            .is_err());
        assert!(connector
            .reconcile_nodes(vec![peer(0, "invalid", 1)])
            .is_err());

        assert_eq!(connector.peer_snapshot(), initial);
        assert_eq!(connector.leader_id(), Some(1));
    }

    #[test]
    fn add_node_remains_an_idempotent_upsert_for_static_callers() {
        let connector = test_connector();
        connector.add_node(peer(1, "master-1", 9010)).unwrap();
        connector.add_node(peer(2, "master-2", 9020)).unwrap();
        connector.change_leader(1);

        connector.add_node(peer(2, "master-2", 9020)).unwrap();
        assert_eq!(connector.leader_id(), Some(1));
        assert_eq!(
            connector.peer_snapshot(),
            vec![peer(1, "master-1", 9010), peer(2, "master-2", 9020)]
        );

        connector.add_node(peer(1, "master-1-new", 9110)).unwrap();
        assert_eq!(connector.leader_id(), None);
        assert_eq!(
            connector.get_addr(1).unwrap().addr,
            peer(1, "master-1-new", 9110).addr
        );
    }

    #[test]
    fn concurrent_reconciliation_exposes_only_complete_snapshots() {
        let connector = Arc::new(test_connector());
        let first = vec![peer(1, "set-a", 1), peer(2, "set-a", 2)];
        let second = vec![peer(3, "set-b", 3), peer(4, "set-b", 4)];
        connector.reconcile_nodes(first.clone()).unwrap();

        let writer_connector = connector.clone();
        let writer_first = first.clone();
        let writer_second = second.clone();
        let writer = thread::spawn(move || {
            for index in 0..1_000 {
                let snapshot = if index % 2 == 0 {
                    writer_second.clone()
                } else {
                    writer_first.clone()
                };
                writer_connector.reconcile_nodes(snapshot).unwrap();
            }
        });

        let mut readers = Vec::new();
        for _ in 0..4 {
            let reader_connector = connector.clone();
            let allowed = [first.clone(), second.clone()];
            readers.push(thread::spawn(move || {
                for _ in 0..2_000 {
                    let snapshot = reader_connector.peer_snapshot();
                    assert!(
                        allowed.contains(&snapshot),
                        "partial membership: {snapshot:?}"
                    );
                }
            }));
        }

        writer.join().unwrap();
        for reader in readers {
            reader.join().unwrap();
        }
    }

    #[test]
    fn direct_peer_rpc_keeps_cached_leader_unchanged() {
        let server_addr = start_echo_server();

        let conf = ClientConf {
            io_threads: 1,
            worker_threads: 1,
            rpc_timeout_ms: 1_000,
            io_retry_max_duration_ms: 2_000,
            io_retry_min_sleep_ms: 1,
            io_retry_max_sleep_ms: 10,
            ..Default::default()
        };
        let connector = ClusterConnector::new(ClientFactory::new(conf));
        connector
            .reconcile_nodes(vec![
                peer(1, "127.0.0.1", 1),
                NodeAddr::from_addr(2, server_addr),
            ])
            .unwrap();
        connector.change_leader(1);

        let request = MessageBuilder::new_rpc(1)
            .data(DataSlice::from_str("follower-target"))
            .build();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let response = runtime.block_on(async {
            connector
                .rpc_to_peer::<CommonErrorExt>(2, request)
                .await
                .unwrap()
        });

        assert!(response.is_success());
        assert_eq!(connector.leader_id(), Some(1));
    }

    #[test]
    fn leader_route_refreshes_membership_between_retries() {
        let server_addr = start_echo_server();

        let conf = ClientConf {
            io_threads: 1,
            worker_threads: 1,
            conn_retry_max_duration_ms: 100,
            conn_retry_min_sleep_ms: 1,
            conn_retry_max_sleep_ms: 10,
            rpc_timeout_ms: 500,
            io_retry_max_duration_ms: 3_000,
            io_retry_min_sleep_ms: 20,
            io_retry_max_sleep_ms: 50,
            ..Default::default()
        };
        let connector = Arc::new(ClusterConnector::new(ClientFactory::new(conf)));
        let unavailable = peer(1, "127.0.0.1", 0);
        connector.reconcile_nodes(vec![unavailable]).unwrap();

        let request = MessageBuilder::new_rpc(1)
            .data(DataSlice::from_str("updated-membership"))
            .build();
        let request_connector = connector.clone();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let response = runtime.block_on(async {
            let request =
                tokio::spawn(async move { request_connector.rpc::<CommonErrorExt>(request).await });

            tokio::time::sleep(Duration::from_millis(250)).await;
            connector
                .reconcile_nodes(vec![NodeAddr::from_addr(2, server_addr)])
                .unwrap();

            tokio::time::timeout(Duration::from_secs(3), request)
                .await
                .expect("RPC should pick up the updated peer snapshot")
                .unwrap()
                .unwrap()
        });
        assert!(response.is_success());
        assert_eq!(connector.leader_id(), Some(2));
    }
}
