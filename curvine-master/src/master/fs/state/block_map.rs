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
    /// Workers holding a quarantined replica. In memory only; rebuilt from full reports.
    quarantined: HashMap<i64, HashSet<u32>>,
    /// Times a marker was set per (block, worker); never decremented. A
    /// replication confirm captures it before asking the target and releases
    /// the marker only if it is unchanged, so evidence that arrived while the
    /// answer was in flight is not erased.
    quarantine_epoch: HashMap<(i64, u32), u64>,
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
            quarantined: Default::default(),
            quarantine_epoch: Default::default(),
        }
    }

    pub fn remove_block(&mut self, worker_id: u32, block_id: i64) {
        self.remove_blocks
            .entry(worker_id)
            .or_default()
            .insert(block_id);
    }

    /// Queue a delete on every location, and on quarantined workers (not
    /// locations) so they learn the block is gone. The marker goes with the block.
    /// Also reached from truncate/free/resize, where the inode survives: the
    /// worker's next full report then re-marks and re-arms one repair for a
    /// block the file no longer holds (`block_inode_state` is per inode).
    pub fn remove_blocks(&mut self, del_res: &DeleteResult) {
        for (block_id, locs) in &del_res.blocks {
            for loc in locs {
                let entry = self.remove_blocks.entry(loc.worker_id).or_default();

                entry.insert(*block_id);
            }
            if let Some(workers) = self.quarantined.remove(block_id) {
                for worker_id in workers {
                    self.remove_block(worker_id, *block_id);
                }
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

    /// Mark the replica quarantined and drop its pending delete (the worker
    /// parks it anyway). `remove_blocks` re-issues it when the block goes; that
    /// delete stays pending until the worker releases the extent.
    /// Returns true if the marker is new.
    pub fn quarantine_block(&mut self, worker_id: u32, block_id: i64) -> bool {
        let inserted = self
            .quarantined
            .entry(block_id)
            .or_default()
            .insert(worker_id);
        *self
            .quarantine_epoch
            .entry((block_id, worker_id))
            .or_default() += 1;
        // BlockMap-level drop on purpose: the extent still uses space, so the
        // scheduled-bytes reservation is not settled here.
        self.deleted_block(worker_id, block_id);
        inserted
    }

    /// The worker no longer holds a quarantined replica (deleted or finalized).
    pub fn release_quarantine(&mut self, worker_id: u32, block_id: i64) {
        if let Some(workers) = self.quarantined.get_mut(&block_id) {
            workers.remove(&worker_id);
            if workers.is_empty() {
                self.quarantined.remove(&block_id);
            }
        }
    }

    /// Marker-set count for the pair; 0 if never marked.
    pub fn quarantine_epoch(&self, worker_id: u32, block_id: i64) -> u64 {
        self.quarantine_epoch
            .get(&(block_id, worker_id))
            .copied()
            .unwrap_or(0)
    }

    pub fn quarantined_workers(&self, block_id: i64) -> Vec<u32> {
        self.quarantined
            .get(&block_id)
            .map(|w| w.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Every quarantined (block, workers) pair, for operator views.
    pub fn quarantined_blocks(&self) -> Vec<(i64, Vec<u32>)> {
        self.quarantined
            .iter()
            .map(|(block, workers)| (*block, workers.iter().copied().collect()))
            .collect()
    }

    /// Drop all markers of a removed worker.
    pub fn clear_worker_quarantine(&mut self, worker_id: u32) {
        self.quarantined.retain(|_, workers| {
            workers.remove(&worker_id);
            !workers.is_empty()
        });
        self.quarantine_epoch.retain(|(_, w), _| *w != worker_id);
    }

    /// Drop `worker_id`'s markers for blocks not in its full report `reported`.
    pub fn retain_worker_quarantine(&mut self, worker_id: u32, reported: &HashSet<i64>) {
        self.quarantined.retain(|block_id, workers| {
            if !reported.contains(block_id) {
                workers.remove(&worker_id);
            }
            !workers.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::BlockLocation;

    fn pending(map: &mut BlockMap, worker_id: u32) -> Vec<i64> {
        let mut ids: Vec<i64> = map
            .handle_heartbeat(worker_id)
            .into_iter()
            .flat_map(|cmd| match cmd {
                WorkerCommand::DeleteBlock(cmd) => cmd.blocks,
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Deleting the inode tells the quarantined worker and drops the marker.
    #[test]
    fn remove_blocks_reissues_delete_to_quarantined_workers_and_clears_marker() {
        let mut map = BlockMap::new();
        map.remove_block(200, 7);
        assert!(map.quarantine_block(200, 7));
        assert!(
            pending(&mut map, 200).is_empty(),
            "quarantine parks the delete"
        );
        assert!(!map.quarantine_block(200, 7), "re-report is not new");

        let mut del_res = DeleteResult::default();
        del_res.blocks.insert(7, vec![BlockLocation::with_id(100)]);
        map.remove_blocks(&del_res);

        assert_eq!(pending(&mut map, 100), vec![7]);
        assert_eq!(
            pending(&mut map, 200),
            vec![7],
            "quarantined worker is told too"
        );
        assert!(map.quarantined_workers(7).is_empty());
        assert!(map.quarantined_blocks().is_empty());
    }

    /// A full report that omits the block prunes that worker's marker only.
    #[test]
    fn retain_worker_quarantine_prunes_unreported_markers_of_that_worker() {
        let mut map = BlockMap::new();
        map.quarantine_block(200, 7);
        map.quarantine_block(300, 7);
        map.quarantine_block(200, 8);

        map.retain_worker_quarantine(200, &HashSet::from([8]));

        assert_eq!(map.quarantined_workers(7), vec![300]);
        assert_eq!(map.quarantined_workers(8), vec![200]);

        map.retain_worker_quarantine(300, &HashSet::new());
        assert!(map.quarantined_workers(7).is_empty());
        assert_eq!(map.quarantined_blocks(), vec![(8, vec![200])]);
    }

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

    /// Clearing a lost worker's markers must leave other workers' markers on
    /// the same blocks in place; only a block marked by that worker alone
    /// disappears.
    #[test]
    fn clear_worker_quarantine_keeps_other_workers_markers() {
        let mut map = BlockMap::new();
        assert!(map.quarantine_block(200, 7));
        assert!(map.quarantine_block(300, 7));
        assert!(map.quarantine_block(200, 8));

        map.clear_worker_quarantine(200);

        assert_eq!(map.quarantined_workers(7), vec![300]);
        assert!(map.quarantined_workers(8).is_empty());
        assert_eq!(map.quarantined_blocks(), vec![(7, vec![300])]);
    }
}
