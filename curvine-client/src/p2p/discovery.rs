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

use curvine_common::conf::ClientP2pConf;

#[derive(Debug, Clone, Default)]
pub struct DiscoverySnapshot {
    pub mdns_peers: usize,
    pub dht_peers: usize,
    pub bootstrap_connected: usize,
}

#[derive(Debug, Clone)]
pub struct DiscoveryService {
    conf: ClientP2pConf,
}

impl DiscoveryService {
    pub fn new(conf: ClientP2pConf) -> Self {
        Self { conf }
    }

    pub fn bootstrap_peers(&self) -> &[String] {
        &self.conf.bootstrap_peers
    }

    pub fn snapshot_with_active_peers(&self, active_peers: usize) -> DiscoverySnapshot {
        let mdns_peers = if self.conf.enable_mdns {
            active_peers
        } else {
            0
        };
        let dht_peers = if self.conf.enable_dht {
            active_peers
        } else {
            0
        };
        let bootstrap_connected = self.conf.bootstrap_peers.len().min(active_peers);

        DiscoverySnapshot {
            mdns_peers,
            dht_peers,
            bootstrap_connected,
        }
    }

    pub fn snapshot(&self) -> DiscoverySnapshot {
        self.snapshot_with_active_peers(0)
    }
}
