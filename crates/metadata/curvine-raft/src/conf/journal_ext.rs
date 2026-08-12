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

use crate::raft::RaftGroup;
use crate::FsResult;
use curvine_config::JournalConf;

pub trait JournalConfExt {
    fn node_id(&self) -> FsResult<u64>;

    fn new_raft_conf(&self, id: u64, applied: u64) -> raft::Config;
}

impl JournalConfExt for JournalConf {
    fn node_id(&self) -> FsResult<u64> {
        let group = RaftGroup::from_conf(self);
        let id = group.get_node_id(&self.local_addr())?;
        Ok(id)
    }

    fn new_raft_conf(&self, id: u64, applied: u64) -> raft::Config {
        raft::Config {
            id,
            election_tick: self.raft_election_tick,
            heartbeat_tick: self.raft_heartbeat_tick,
            min_election_tick: self.raft_min_election_ticks,
            max_election_tick: self.raft_max_election_ticks,
            max_size_per_msg: self.raft_max_size_per_msg,
            max_inflight_msgs: self.raft_max_inflight_msgs,
            applied,
            max_committed_size_per_ready: self.raft_max_committed_size_per_ready,

            check_quorum: self.raft_check_quorum,
            skip_bcast_commit: true,
            pre_vote: true,
            batch_append: true,
            ..Default::default()
        }
    }
}
