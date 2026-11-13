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

use crate::master::fs::DeleteResult;
use curvine_model::{DeleteBlockCmd, WorkerCommand};
use std::collections::{HashMap, HashSet};

// Manage all blocks in the cluster
pub struct BlockMap {
    // Mark the block that needs to be deleted.
    remove_blocks: HashMap<u32, HashSet<i64>>,
}

impl Default for BlockMap {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockMap {
    pub fn new() -> Self {
        Self {
            remove_blocks: Default::default(),
        }
    }

    pub fn remove_block(&mut self, worker_id: u32, block_id: i64) {
        self.remove_blocks
            .entry(worker_id)
            .or_default()
            .insert(block_id);
    }

    // Indicates the block that needs to be deleted.
    pub fn remove_blocks(&mut self, del_res: &DeleteResult) {
        for (block_id, locs) in &del_res.blocks {
            for loc in locs {
                let entry = self.remove_blocks.entry(loc.worker_id).or_default();

                entry.insert(*block_id);
            }
        }
    }

    // Handle heartbeat and return the relevant commands that the worker needs to execute.
    // @todo Exception scene, the heartbeat response failed, the delete block data has been cleared, the next time the heartbeat is successful, but the worker block will not be deleted.
    // hdfs solution: regular block full reporting.
    // alluxio solution: remove only after block report is deleted.This solution is adopted.
    pub fn handle_heartbeat(&mut self, worker_id: u32) -> Vec<WorkerCommand> {
        let mut cmds = vec![];
        let sets = match self.remove_blocks.get(&worker_id) {
            None => return cmds,
            Some(v) => v.iter().copied(),
        };
        let cmd = DeleteBlockCmd {
            blocks: Vec::from_iter(sets),
        };
        cmds.push(WorkerCommand::DeleteBlock(cmd));
        cmds
    }

    /// Acknowledges only a deletion requested for this replica. Unsolicited
    /// and repeated reports must not settle a different reservation.
    pub fn deleted_block(&mut self, worker_id: u32, block_id: i64) -> bool {
        let Some(blocks) = self.remove_blocks.get_mut(&worker_id) else {
            return false;
        };
        let removed = blocks.remove(&block_id);
        // Once drained, omit the worker so heartbeats stop sending empty
        // DeleteBlock commands. Keep the match result for reservation settlement.
        if blocks.is_empty() {
            self.remove_blocks.remove(&worker_id);
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deleted_block_matches_pending_replica_once() {
        let mut map = BlockMap::new();
        map.remove_block(1, 10);
        map.remove_block(2, 10);
        assert!(!map.deleted_block(3, 10));
        assert!(!map.deleted_block(1, 11));
        assert!(map.deleted_block(1, 10));
        assert!(!map.deleted_block(1, 10));
        assert!(map.deleted_block(2, 10));
    }

    #[test]
    fn deleted_block_drops_empty_worker_set() {
        let mut map = BlockMap::new();
        map.remove_block(1, 10);
        map.remove_block(1, 11);
        map.remove_block(2, 20);

        assert!(map.deleted_block(1, 10));
        let commands = map.handle_heartbeat(1);
        let [WorkerCommand::DeleteBlock(cmd)] = commands.as_slice() else {
            panic!("the remaining block must still be scheduled for deletion");
        };
        assert_eq!(cmd.blocks, vec![11]);

        assert!(map.deleted_block(1, 11));
        assert!(map.handle_heartbeat(1).is_empty());
        assert!(!map.deleted_block(1, 11));
        assert!(map.handle_heartbeat(1).is_empty());

        let commands = map.handle_heartbeat(2);
        let [WorkerCommand::DeleteBlock(cmd)] = commands.as_slice() else {
            panic!("another worker's pending deletion must remain intact");
        };
        assert_eq!(cmd.blocks, vec![20]);
        assert!(map.deleted_block(2, 20));
        assert!(map.handle_heartbeat(2).is_empty());
    }
}
