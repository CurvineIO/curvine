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

use crate::proto::raft::{FsmState, SnapshotData};
use std::collections::HashMap;
use crate::raft::DEFAULT_LEADER_ID;

/// Map of node_id -> FsmState (committed/applied), used for safe log compact.
#[derive(Clone, Default)]
pub struct FsmStateMap {
    inner: HashMap<u64, FsmState>,
    fsm_state_leader: FsmState,
}

impl FsmStateMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, node_id: u64, leader_id: u64, committed: u64, applied: u64) {
        let state = FsmState { committed, applied };

        let is_leader = leader_id != DEFAULT_LEADER_ID && node_id == leader_id;
        self.inner.insert(node_id, state.clone());
        if is_leader {
            self.fsm_state_leader = state;
        }
    }

    pub fn min_applied(&self) -> u64 {
        let min_applied = self.inner.values().map(|s| s.applied).min().unwrap_or(0);
        min_applied.min(self.fsm_state_leader.applied)
    }

    pub fn leader_applied(&self) -> u64 {
        self.fsm_state_leader.applied
    }

    pub fn as_map(&self) -> &HashMap<u64, FsmState> {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut HashMap<u64, FsmState> {
        &mut self.inner
    }

    pub fn set_snapshot(self, snap: &mut SnapshotData)  {
        snap.fsm_state_map = self.inner;
        snap.fsm_state_leader = self.fsm_state_leader;
    }

    pub fn from_snapshot(snap: &SnapshotData) -> Self {
        Self {
            inner: snap.fsm_state_map.clone(),
            fsm_state_leader: snap.fsm_state_leader.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_sets_leader_applied_when_sender_is_leader() {
        let mut m = FsmStateMap::new();
        // node_id=1 is leader (leader_id=1), update sets fsm_state_leader
        m.update(1, 1, 10, 8);
        assert_eq!(m.leader_applied(), 8);
        assert_eq!(m.as_map().get(&1).unwrap().applied, 8);
    }

    #[test]
    fn test_update_does_not_set_leader_applied_when_no_leader() {
        let mut m = FsmStateMap::new();
        m.update(1, DEFAULT_LEADER_ID, 10, 8);
        assert_eq!(m.leader_applied(), 0, "fsm_state_leader should stay default");
        assert_eq!(m.as_map().get(&1).unwrap().applied, 8);
    }

    #[test]
    fn test_update_does_not_set_leader_applied_when_sender_is_follower() {
        let mut m = FsmStateMap::new();
        // leader is 1, sender is 2 (follower)
        m.update(2, 1, 10, 5);
        assert_eq!(m.leader_applied(), 0);
        assert_eq!(m.as_map().get(&2).unwrap().applied, 5);
    }

    #[test]
    fn test_min_applied_is_min_of_all_and_leader() {
        let mut m = FsmStateMap::new();
        m.update(1, 1, 10, 10); // leader applied 10
        m.update(2, 1, 10, 5);  // follower applied 5
        assert_eq!(m.min_applied(), 5);
        assert_eq!(m.leader_applied(), 10);
    }

    #[test]
    fn test_min_applied_empty_is_zero() {
        let m = FsmStateMap::new();
        assert_eq!(m.min_applied(), 0);
        assert_eq!(m.leader_applied(), 0);
    }

    #[test]
    fn test_snapshot_roundtrip_preserves_leader_applied_and_min() {
        let mut m = FsmStateMap::new();
        m.update(1, 1, 10, 10);
        m.update(2, 1, 10, 6);

        let mut snap = SnapshotData {
            snapshot_id: 0,
            node_id: 0,
            create_time: 0,
            ..Default::default()
        };
        m.clone().set_snapshot(&mut snap);

        let restored = FsmStateMap::from_snapshot(&snap);
        assert_eq!(restored.leader_applied(), 10);
        assert_eq!(restored.min_applied(), 6);
        assert_eq!(restored.as_map().len(), 2);
        assert_eq!(restored.as_map().get(&1).unwrap().applied, 10);
        assert_eq!(restored.as_map().get(&2).unwrap().applied, 6);
    }
}

