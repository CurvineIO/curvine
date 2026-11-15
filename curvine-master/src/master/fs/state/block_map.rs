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
    /// Workers holding a quarantined replica: not a location, never a target.
    /// In memory only; rebuilt from full reports.
    quarantined: HashMap<i64, HashSet<u32>>,
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
        }
    }

    pub fn remove_block(&mut self, worker_id: u32, block_id: i64) {
        self.remove_blocks
            .entry(worker_id)
            .or_default()
            .insert(block_id);
    }

    /// Queue a delete on every location of each block. A worker holding the
    /// block quarantined is not a location, so it is queued separately: it
    /// will refuse the delete and park it until the extent is released, but
    /// it has to learn the block is gone. The marker goes with the inode.
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

    pub fn deleted_block(&mut self, worker_id: u32, block_id: i64) {
        self.remove_blocks
            .entry(worker_id)
            .or_default()
            .remove(&block_id);
    }

    /// Mark the replica quarantined and drop the pending delete: the worker
    /// parks a delete for a quarantined block anyway, and `remove_blocks`
    /// re-issues it when the inode goes. A future release path must not
    /// assume the worker ever received the dropped command.
    /// Returns true if the marker is new, so a re-sent report does not re-queue repair.
    pub fn quarantine_block(&mut self, worker_id: u32, block_id: i64) -> bool {
        let inserted = self
            .quarantined
            .entry(block_id)
            .or_default()
            .insert(worker_id);
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
    }

    /// A full report from `worker_id` listed exactly `reported`: drop its
    /// markers for every other block (metadata lost, extent gone).
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

    /// Deleting the inode re-issues the delete to the quarantined worker
    /// (which is no longer a location) and drops the marker.
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
}
