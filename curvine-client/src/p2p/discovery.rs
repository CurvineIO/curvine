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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryPeerSnapshot {
    pub peer_id: String,
    pub addrs: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiscoverySnapshot {
    pub mdns_peers: Vec<DiscoveryPeerSnapshot>,
    pub dht_peers: Vec<DiscoveryPeerSnapshot>,
    pub bootstrap_peers: Vec<DiscoveryPeerSnapshot>,
}
