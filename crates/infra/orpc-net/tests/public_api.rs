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

use orpc_net::net::{InetAddr, NodeAddr};
use orpc_net::retry::{LimitedRetry, TimeoutRetry};

#[test]
fn exposes_network_and_retry_primitives() {
    let inet_addr = InetAddr::new("127.0.0.1", 8080);
    let node_addr = NodeAddr::from_addr(1, inet_addr.clone());

    assert_eq!(node_addr.addr(), &inet_addr);
    assert_eq!(LimitedRetry::new(1, 0).count(), 0);
    assert!(TimeoutRetry::new(1, 0).attempt_blocking());
}
