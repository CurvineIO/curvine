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

use curvine_master::master::MasterMonitor;
use curvine_raft::raft::RoleState;
use curvine_runtime::sync::StateCtl;

fn leader(term: u64) -> MasterMonitor {
    let monitor = MasterMonitor::new(StateCtl::new(RoleState::Leader.into()), StateCtl::new(0));
    monitor.set_term_for_test(term);
    monitor
}

#[test]
fn serving_waits_for_an_entry_of_the_current_term() {
    let monitor = leader(7);

    monitor.set_applied_term_for_test(6);
    assert!(!monitor.is_serving());

    monitor.set_applied_term_for_test(7);
    assert!(monitor.is_serving());

    monitor.set_term_for_test(8);
    assert!(!monitor.is_serving());
}

#[test]
fn followers_never_serve_writes() {
    let monitor = MasterMonitor::new(StateCtl::new(RoleState::Follower.into()), StateCtl::new(0));
    monitor.set_term_for_test(3);
    monitor.set_applied_term_for_test(3);
    assert!(!monitor.is_serving());
}
