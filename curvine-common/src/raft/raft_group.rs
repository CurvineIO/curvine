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
use orpc::io::net::InetAddr;
use orpc::{err_box, CommonResult};
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

    pub fn from_proto<T: AsRef<str>>(name: T, list: Vec<RaftPeerProto>) -> Self {
        let mut peers = HashMap::new();
        for item in list {
            peers.insert(
                item.node_id,
                RaftPeer::new(item.node_id, item.hostname, item.port as u16),
            );
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
            None => {
                let peers_str = self
                    .peers
                    .values()
                    .map(|p| format!("{}:{}", p.hostname, p.port))
                    .collect::<Vec<_>>()
                    .join(", ");
                err_box!(
                    "Not a master role, address {}. Available peers: [{}]",
                    addr,
                    peers_str
                )
            }
            Some(v) => Ok(*v.0),
        }
    }

    fn get_peers(conf: &JournalConf) -> HashMap<NodeId, RaftPeer> {
        let mut map = HashMap::new();
        for peer in &conf.journal_addrs {
            map.insert(peer.id, peer.clone());
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
        let peer = RaftPeer::new(id, addr.hostname.clone(), addr.port);
        self.peers.insert(peer.id, peer);
    }

    pub fn remove(&mut self, id: &NodeId) {
        self.peers.remove(id);
    }
}
