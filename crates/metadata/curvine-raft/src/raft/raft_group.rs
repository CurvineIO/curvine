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

use crate::conf::JournalConf;
use crate::proto::raft::RaftPeerProto;
use crate::raft::{NodeId, RaftPeer};
use curvine_core_error::{err_box, CommonResult};
use curvine_net::net::InetAddr;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// raft group
/// In curvine, all masters form a raft group.
/// In this group, when raft request is initiated, it is necessary to automatically determine which leader is currently.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RaftGroup {
    // Group name
    pub(crate) name: String,

    // Node address list.
    pub(crate) peers: HashMap<NodeId, RaftPeer>,
}

impl RaftGroup {
    pub fn new<T: AsRef<str>>(name: T, peers: HashMap<NodeId, RaftPeer>) -> Self {
        Self {
            name: name.as_ref().to_string(),
            peers,
        }
    }

    pub fn from_conf(conf: &JournalConf) -> Self {
        let peers = Self::get_peers(conf);
        Self::new(conf.group_name.as_str(), peers)
    }

    pub fn get_addr_string(&self, id: u64) -> String {
        match self.peers.get(&id) {
            None => format!("{}", id),
            Some(v) => format!("{}", v),
        }
    }

    pub fn get_addr_only_string(&self, id: u64) -> String {
        if id == 0 {
            return "unknown".to_string();
        }
        match self.peers.get(&id) {
            None => format!("{}", id),
            Some(v) => format!("{}:{}", v.hostname, v.port),
        }
    }

    pub fn voters(&self) -> Vec<u64> {
        self.peers.iter().map(|x| *x.0).collect()
    }

    pub fn from_proto<T: AsRef<str>>(name: T, list: Vec<RaftPeerProto>) -> Self {
        let mut peers = HashMap::new();
        for item in list {
            let mut peer = RaftPeer::new(item.node_id, item.hostname, item.port as u16);
            peer.snapshot_port = item.snapshot_port.and_then(|port| u16::try_from(port).ok());
            peers.insert(item.node_id, peer);
        }

        Self::new(name, peers)
    }

    pub fn to_proto(&self) -> Vec<RaftPeerProto> {
        let mut vec = vec![];
        for peer in self.peers.values() {
            vec.push(RaftPeerProto {
                node_id: peer.id,
                hostname: peer.hostname.to_string(),
                port: peer.port as u32,
                snapshot_port: peer.snapshot_port.map(u32::from),
            })
        }
        vec
    }

    // Get the node id of the current node
    pub fn get_node_id(&self, addr: &InetAddr) -> CommonResult<NodeId> {
        let find = self
            .peers
            .iter()
            .find(|x| x.1.hostname == addr.hostname && x.1.port == addr.port);

        match find {
            None => err_box!("Not a master role, address {}", addr),
            Some(v) => Ok(*v.0),
        }
    }

    fn get_peers(conf: &JournalConf) -> HashMap<NodeId, RaftPeer> {
        let mut map = HashMap::new();
        for peer in &conf.journal_addrs {
            let mut peer = peer.clone();
            peer.snapshot_port.get_or_insert(conf.snapshot_rpc_port);
            map.insert(peer.id, peer);
        }
        map
    }

    pub fn get_addr_check(&self, id: &NodeId) -> CommonResult<InetAddr> {
        match self.peers.get(id) {
            None => err_box!("Node {} not exists", id),
            Some(v) => Ok(v.to_addr()),
        }
    }

    pub fn get_addr(&self, id: &NodeId) -> Option<InetAddr> {
        self.peers.get(id).map(|x| x.to_addr())
    }

    pub fn insert(&mut self, id: NodeId, addr: &InetAddr) {
        let snapshot_port = self.peers.get(&id).and_then(|peer| peer.snapshot_port);
        let mut peer = RaftPeer::new(id, addr.hostname.clone(), addr.port);
        peer.snapshot_port = snapshot_port;
        self.peers.insert(peer.id, peer);
    }

    pub fn remove(&mut self, id: &NodeId) {
        self.peers.remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_addresses_survive_control_protocol_round_trip() {
        let conf = JournalConf {
            snapshot_rpc_port: 9000,
            journal_addrs: vec![
                RaftPeer::new(1, "master-1", 8996),
                RaftPeer::new(2, "master-2", 8996).with_snapshot_port(9002),
            ],
            ..Default::default()
        };
        let original = RaftGroup::from_conf(&conf);
        let restored = RaftGroup::from_proto("test", original.to_proto());
        assert_eq!(restored.peers[&1].snapshot_port, Some(9000));
        assert_eq!(restored.peers[&2].snapshot_port, Some(9002));
        assert_eq!(restored.peers[&2].port, 8996);
    }

    #[test]
    fn get_addr_only_string_returns_address_when_peer_exists() {
        let mut peers = HashMap::new();
        peers.insert(1, RaftPeer::new(1, "master-1", 8996));

        let group = RaftGroup::new("test", peers);

        assert_eq!("master-1:8996", group.get_addr_only_string(1));
    }

    #[test]
    fn get_addr_only_string_falls_back_to_id_when_peer_is_unknown() {
        let group = RaftGroup::new("test", HashMap::new());

        assert_eq!("2", group.get_addr_only_string(2));
    }

    #[test]
    fn get_addr_only_string_returns_unknown_for_zero_leader_id() {
        let group = RaftGroup::new("test", HashMap::new());

        assert_eq!("unknown", group.get_addr_only_string(0));
    }
}
