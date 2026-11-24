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

use crate::master::journal::{AddBlockEntry, JournalBatch, JournalEntry, JournalRecord};
use crate::master::meta::{BlockMeta, InodeId};
use crate::master::{Master, MasterMetrics};
use curvine_config::JournalConf;
use curvine_core_error::CommonResult;
use curvine_error::FsResult;
use curvine_model::{BlockLocation, CommitBlock};
use curvine_raft::raft::RaftClient;
use curvine_runtime::common::SerdeUtils;
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::sync::channel::BlockingReceiver;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

pub struct SenderTask {
    pub(crate) client: RaftClient,
    /// Raft id of the master whose handler enqueued these entries. The receiver
    /// may only skip FSM apply when this matches its own id.
    pub(crate) proposer_id: u64,
    pub(crate) batch: JournalBatch,
    /// Restore epoch of the entries currently in the batch. The receiver makes
    /// one skip decision per propose, so a batch never mixes origins: an entry
    /// from another epoch flushes the batch first. `None` means the entries
    /// were not pre-applied and must be replayed. Only meaningful while the
    /// batch is non-empty.
    pub(crate) batch_origin: Option<u64>,
    pub(crate) flush_batch_ms: u64,
    pub(crate) flush_batch_size: u64,
    pub(crate) flush_batch_bytes: u64,
    pub(crate) batch_bytes: u64,
    /// Encoded size of the entry that currently sits at the tail of the batch,
    /// as accounted in `batch_bytes`. A coalesce rewrites that entry in place,
    /// so its old share of the byte budget has to be known to replace it.
    pub(crate) tail_bytes: u64,
    /// Fold consecutive `AddBlock` ops of the same inode into one record.
    /// Env kill switch: `CV_JOURNAL_COALESCE_ADD_BLOCK=0`.
    pub(crate) coalesce_add_block: bool,
    pub(crate) last_flush_ms: u64,
    pub(crate) metrics: &'static MasterMetrics,
}

impl SenderTask {
    pub fn new(
        client: RaftClient,
        conf: &JournalConf,
        batch_seq_id: u64,
        proposer_id: u64,
    ) -> CommonResult<Self> {
        let sender = Self {
            client,
            proposer_id,
            batch: JournalBatch::new(batch_seq_id),
            batch_origin: None,
            flush_batch_ms: conf.writer_flush_batch_ms,
            flush_batch_size: conf.writer_flush_batch_size,
            flush_batch_bytes: conf.writer_flush_batch_bytes,
            batch_bytes: 0,
            tail_bytes: 0,
            coalesce_add_block: conf.coalesce_add_block,
            last_flush_ms: LocalTime::mills(),
            metrics: Master::get_metrics()?,
        };

        Ok(sender)
    }

    // Start a thread to execute sender task
    pub fn spawn(self, receiver: BlockingReceiver<JournalRecord>) -> FsResult<()> {
        let poll = Duration::from_millis(self.flush_batch_ms);
        let name = "journal-writer".to_string();
        let task = self;
        thread::Builder::new().name(name.clone()).spawn(move || {
            if let Err(e) = Self::loop0(receiver, poll, task) {
                log::error!("thread {} stop: {:?}; aborting master", name, e);
                std::process::abort();
            }
        })?;
        Ok(())
    }

    fn loop0(
        receiver: BlockingReceiver<JournalRecord>,
        poll: Duration,
        mut task: SenderTask,
    ) -> FsResult<()> {
        // The thread should not end.
        loop {
            // Byte-budget gate: the batch has reached the byte ceiling. Flush
            // it before accepting the next entry so a single Propose never
            // approaches the peer's 64 MiB frame limit, regardless of how many
            // blocks each op carries.
            if task.flush_batch_bytes > 0
                && !task.batch.is_empty()
                && task.batch_bytes >= task.flush_batch_bytes
            {
                task.flush_pending()?;
                continue;
            }

            let event = match receiver.recv_timeout(poll) {
                Ok(v) => Some(v),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => {
                    task.flush_pending()?;
                    return Ok(());
                }
            };
            task.handle(event)?;
        }
    }

    pub fn handle(&mut self, record: Option<JournalRecord>) -> FsResult<()> {
        let force = if let Some((entry, epoch)) = record {
            // One propose, one origin: the receiver decides skip-or-replay for
            // the whole batch, so an entry carrying another restore epoch must
            // not join the pending one.
            if !self.batch.is_empty() && self.batch_origin != epoch {
                self.flush_pending()?;
            }

            let force = matches!(entry, JournalEntry::Snapshot(_));
            let budgeted = !force && self.flush_batch_bytes > 0;

            // Fold a consecutive same-inode `AddBlock` into the batch tail
            // before accounting bytes: the merged record is what ships, so the
            // budget must measure it, not the pieces it replaced.
            let pending = match self.fold_add_block_tail(entry, budgeted)? {
                Some(v) => v,
                None => {
                    self.metrics.journal_queue_len.dec();
                    return self.flush_if_due(force);
                }
            };

            if budgeted {
                let size = pending.encode_size()? as u64;
                // Flush a non-empty batch before accepting an entry that
                // would push it past the byte ceiling, so a frame never
                // combines a large entry with an already-large batch. An
                // entry larger than the ceiling still ships alone.
                if !self.batch.is_empty()
                    && self.batch_bytes.saturating_add(size) > self.flush_batch_bytes
                {
                    self.flush_pending()?;
                }
                self.batch_bytes = self.batch_bytes.saturating_add(size);
                self.tail_bytes = size;
            }
            self.batch_origin = epoch;
            self.batch.push(pending);
            self.metrics.journal_queue_len.dec();
            force
        } else {
            false
        };

        self.flush_if_due(force)
    }

    /// Apply the op-count, time and forced-flush triggers to the current batch.
    fn flush_if_due(&mut self, force: bool) -> FsResult<()> {
        let len = self.batch.len() as u64;
        if len == 0 {
            return Ok(());
        }

        if force || len >= self.flush_batch_size || self.flush_interval_elapsed() {
            self.flush_pending()?;
        }

        Ok(())
    }

    /// Merge `next` into the tail of the batch when the two are consecutive
    /// `AddBlock` ops of the same inode.
    ///
    /// Replay does `mem::replace(&mut file.blocks, entry.blocks)` (last full
    /// `blocks` wins) followed by `apply_new_block(commit_block)` (`add_location`
    /// is an idempotent put), so one record carrying the last `blocks` and the
    /// union of the committed locations replays exactly like the chain it
    /// replaced. `op_id` takes the max (`update_op_id` is monotonic), `rpc_id`
    /// and `path` the last.
    ///
    /// Two ops fold only when they are the same inode (block id high bits, as
    /// replay resolves it) *and* the same path, so a rename or a block id that
    /// does not decode to this file can never merge two files.
    ///
    /// Returns `Ok(None)` when the entry was folded (the tail grew in place, so
    /// the caller must not push anything), `Ok(Some(entry))` when it is not
    /// mergeable and must be pushed unchanged.
    fn fold_add_block_tail(
        &mut self,
        next: JournalEntry,
        budgeted: bool,
    ) -> CommonResult<Option<JournalEntry>> {
        if !self.coalesce_add_block {
            return Ok(Some(next));
        }
        let next = match next {
            JournalEntry::AddBlock(v) => v,
            other => return Ok(Some(other)),
        };
        let Some(next_inode) = inode_of(&next.blocks) else {
            return Ok(Some(JournalEntry::AddBlock(next)));
        };
        let tail = match self.batch.batch.last_mut() {
            // Same inode *and* same path: two different files must never fold
            // together, and neither must a renamed one.
            Some(JournalEntry::AddBlock(v))
                if v.path == next.path && inode_of(&v.blocks) == Some(next_inode) =>
            {
                v
            }
            _ => return Ok(Some(JournalEntry::AddBlock(next))),
        };

        merge_add_block(tail, next);

        // Only the byte budget needs the size; with the budget disabled the
        // whole accounting stays at zero regardless of folding.
        if budgeted {
            let merged = JournalEntry::AddBlock(tail.clone()).encode_size()? as u64;
            self.batch_bytes = self
                .batch_bytes
                .saturating_sub(self.tail_bytes)
                .saturating_add(merged);
            self.tail_bytes = merged;
        }
        Ok(None)
    }

    fn flush_interval_elapsed(&self) -> bool {
        LocalTime::mills() - self.last_flush_ms >= self.flush_batch_ms
    }

    fn flush_pending(&mut self) -> FsResult<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let spend = TimeSpent::new();
        let entries = self.batch.len() as i64;
        let _ = entries; // read only by fault-injection contexts

        crate::fault_point! {
            sync,
            name: "master.journal.before_flush",
            description: "Before a queued journal batch is proposed to raft",
            context: {"node" => self.proposer_id as i64, "entries" => entries},
        }
        let bytes = SerdeUtils::serialize(&self.batch)?;
        self.client
            .block_on_send_propose_from(bytes, Some(self.proposer_id), self.batch_origin)?;
        crate::fault_point! {
            sync,
            name: "master.journal.after_flush",
            description: "After a queued journal batch was proposed to raft",
            context: {"node" => self.proposer_id as i64, "entries" => entries},
        }
        self.batch_bytes = 0;
        self.tail_bytes = 0;
        self.batch_origin = None;

        self.metrics.journal_flush_count.inc();
        self.metrics
            .journal_flush_time
            .inc_by(spend.used_us() as i64);

        // Scroll to the next batch.
        self.batch.next();
        self.last_flush_ms = LocalTime::mills();

        Ok(())
    }
}

/// The inode an `AddBlock` belongs to: the high bits of a block id are the
/// inode id, the same encoding `journal_loader::resolve_file_inode` decodes.
fn inode_of(blocks: &[BlockMeta]) -> Option<i64> {
    blocks.first().map(|v| InodeId::get_id(v.id))
}

/// Fold `next` into `tail`: the last full `blocks` vector wins, the committed
/// locations are unioned, `op_id` takes the max.
fn merge_add_block(tail: &mut AddBlockEntry, next: AddBlockEntry) {
    tail.op_id = tail.op_id.max(next.op_id);
    tail.rpc_id = next.rpc_id;
    tail.path = next.path;
    tail.blocks = next.blocks;
    merge_commit_blocks(&mut tail.commit_block, next.commit_block);
}

/// Union committed locations of the merged ops.
///
/// Replay only reads `commit_block` to `add_location` each location, which puts
/// one record per `(block_id, worker_id)`. So the sequential chain leaves the
/// union of every location ever committed for a block id, and the later write
/// wins per worker. `block_len` does not reach the store at all (the file's
/// block lengths come from `blocks`), it is only carried along.
fn merge_commit_blocks(tail: &mut Vec<CommitBlock>, next: Vec<CommitBlock>) {
    for commit in next {
        match tail.iter_mut().find(|v| v.block_id == commit.block_id) {
            Some(slot) => {
                slot.block_len = commit.block_len;
                merge_locations(&mut slot.locations, commit.locations);
            }
            None => tail.push(commit),
        }
    }
}

fn merge_locations(tail: &mut Vec<BlockLocation>, next: Vec<BlockLocation>) {
    for loc in next {
        match tail.iter_mut().find(|v| v.worker_id == loc.worker_id) {
            Some(slot) => *slot = loc,
            None => tail.push(loc),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::AddBlockEntry;
    use crate::master::meta::BlockMeta;

    const FILE_ID: i64 = 1_000;
    const FILE_PATH: &str = "/coalesce/file";

    fn workers() -> Vec<curvine_model::WorkerAddress> {
        (1..=2u32)
            .map(|id| curvine_model::WorkerAddress {
                worker_id: 1000 + id,
                hostname: format!("worker-{id}"),
                ip_addr: format!("10.0.0.{id}"),
                rpc_port: 8997,
                web_port: 9002,
            })
            .collect()
    }

    /// Block id of the `seq`-th block of file `FILE_ID`: the inode id lives in
    /// the high bits, exactly what replay re-derives in `resolve_file_inode`.
    fn block_id(seq: i64) -> i64 {
        InodeId::create_block_id(FILE_ID, seq).expect("block id")
    }

    /// A `SenderTask` wired to an unreachable peer: nothing can be proposed, so
    /// only batch assembly (fold + byte accounting) is under test.
    fn idle_sender(coalesce: bool, flush_batch_bytes: u64) -> SenderTask {
        Master::init_test_metrics();
        let conf = JournalConf {
            hostname: "127.0.0.1".to_string(),
            rpc_port: 1,
            journal_addrs: vec![curvine_config::RaftPeer::new(1, "127.0.0.1", 1)],
            io_threads: 1,
            worker_threads: 1,
            conn_timeout_ms: 200,
            io_timeout_ms: 200,
            conn_retry_max_duration_ms: 0,
            rpc_retry_max_duration_ms: 0,
            conn_retry_min_sleep_ms: 1,
            conn_retry_max_sleep_ms: 1,
            rpc_retry_min_sleep_ms: 1,
            rpc_retry_max_sleep_ms: 1,
            writer_flush_batch_size: u64::MAX,
            writer_flush_batch_ms: 60_000,
            writer_flush_batch_bytes: flush_batch_bytes,
            ..Default::default()
        };

        let rt = conf.create_runtime();
        let client = RaftClient::from_conf(rt, &conf);
        let mut task = SenderTask::new(client, &conf, 0, 1).expect("sender");
        task.coalesce_add_block = coalesce;
        task
    }

    /// A journal record with no pre-apply origin; these tests only exercise
    /// batch assembly, not the replay decision the origin feeds.
    fn rec(entry: JournalEntry) -> Option<JournalRecord> {
        Some((entry, None))
    }

    fn add_block(
        op_id: u64,
        blocks: Vec<BlockMeta>,
        commit_block: Vec<CommitBlock>,
    ) -> JournalEntry {
        JournalEntry::AddBlock(AddBlockEntry {
            op_id,
            rpc_id: op_id as i64,
            path: FILE_PATH.to_string(),
            blocks,
            commit_block,
        })
    }

    fn prealloc(seq: i64) -> BlockMeta {
        BlockMeta::with_pre(block_id(seq), &workers())
    }

    fn commit_of(block: &BlockMeta) -> CommitBlock {
        CommitBlock {
            block_id: block.id,
            block_len: 4 << 20,
            locations: vec![BlockLocation::with_id(1001)],
        }
    }

    /// Sum of the per-entry encoded sizes, i.e. exactly what the sender's byte
    /// budget is supposed to hold for a batch.
    fn accounted_bytes(batch: &JournalBatch) -> u64 {
        batch
            .batch
            .iter()
            .map(|entry| entry.encode_size().expect("encode_size") as u64)
            .sum()
    }

    #[test]
    fn byte_budget_accumulates_encoded_size() {
        let mut task = idle_sender(true, 1024);
        let entry = JournalEntry::AddBlock(AddBlockEntry {
            op_id: 1,
            rpc_id: 0,
            path: "/f".to_string(),
            blocks: (0..100).map(|b| BlockMeta::new(1000 + b, 1024)).collect(),
            commit_block: vec![],
        });
        let size = entry.encode_size().expect("encode_size");
        assert!(size > 0, "an AddBlock entry always serializes to bytes");

        task.handle(rec(entry)).expect("handle");
        assert_eq!(
            task.batch.len(),
            1,
            "below op-count/time thresholds, no flush"
        );
        assert_eq!(
            task.batch_bytes, size as u64,
            "the byte budget must record the entry's encoded size"
        );
    }

    #[test]
    fn coalesce_folds_consecutive_add_block_of_one_inode() {
        let mut old = prealloc(0);
        let first_commit = commit_of(&old);
        old.commit(&first_commit);
        let b0 = old;
        let b1 = prealloc(1);

        let mut task = idle_sender(true, 4 << 20);
        task.handle(rec(add_block(10, vec![b0.clone()], Vec::new())))
            .expect("handle");
        assert_eq!(task.batch.len(), 1, "the first op opens the record");

        // Commit of b0 plus allocation of b1 folds into the same record.
        task.handle(rec(add_block(
            40,
            vec![b0.clone(), b1.clone()],
            vec![first_commit.clone()],
        )))
        .expect("handle");
        task.handle(rec(add_block(25, vec![b0.clone(), b1.clone()], Vec::new())))
            .expect("handle");

        assert_eq!(
            task.batch.len(),
            1,
            "one inode run collapses into one record"
        );
        let JournalEntry::AddBlock(merged) = &task.batch.batch[0] else {
            panic!("expected the merged AddBlock to be the only entry")
        };
        // blocks: last full replace wins.
        let ids: Vec<i64> = merged.blocks.iter().map(|b| b.id).collect();
        assert_eq!(ids, vec![b0.id, b1.id]);
        // op_id: max (update_op_id is monotonic), rpc_id/path: last.
        assert_eq!(merged.op_id, 40);
        assert_eq!(merged.rpc_id, 25);
        assert_eq!(merged.path, FILE_PATH);
        // commit_block: put-union.
        assert_eq!(merged.commit_block.len(), 1);
        let mut locs: Vec<u32> = merged.commit_block[0]
            .locations
            .iter()
            .map(|v| v.worker_id)
            .collect();
        locs.sort_unstable();
        assert_eq!(locs, vec![1001], "one location, kept from the commit");
        assert_eq!(
            task.batch_bytes,
            accounted_bytes(&task.batch),
            "the budget must track the merged record"
        );
        assert_eq!(task.batch_bytes, task.tail_bytes);
    }

    #[test]
    fn coalesce_unions_commit_locations_and_keeps_last_block_len() {
        let b0 = prealloc(0);
        let other = prealloc(1);
        let mut task = idle_sender(true, 4 << 20);

        task.handle(rec(add_block(
            1,
            vec![b0.clone()],
            vec![CommitBlock {
                block_id: b0.id,
                block_len: 1,
                locations: vec![BlockLocation::with_id(1001)],
            }],
        )))
        .expect("handle");
        task.handle(rec(add_block(
            2,
            vec![b0.clone(), other.clone()],
            vec![
                CommitBlock {
                    block_id: b0.id,
                    block_len: 9,
                    locations: vec![BlockLocation::with_id(1002)],
                },
                CommitBlock {
                    block_id: b0.id,
                    block_len: 9,
                    locations: vec![BlockLocation::with_id(1001)],
                },
            ],
        )))
        .expect("handle");

        let JournalEntry::AddBlock(merged) = &task.batch.batch[0] else {
            panic!("expected the merged AddBlock")
        };
        assert_eq!(merged.commit_block.len(), 1, "one entry per block id");
        let commit = &merged.commit_block[0];
        assert_eq!(commit.block_len, 9, "the last commit wins the length");
        let mut locs: Vec<u32> = commit.locations.iter().map(|v| v.worker_id).collect();
        locs.sort_unstable();
        assert_eq!(
            locs,
            vec![1001, 1002],
            "every committed location survives, exactly like N put-based replays"
        );
    }

    #[test]
    fn coalesce_stops_at_other_inodes_and_at_other_file_ops() {
        use crate::master::journal::CompleteFileEntry;
        use crate::master::meta::inode::InodeFile;

        let mut task = idle_sender(true, 4 << 20);
        let a0 = prealloc(0);
        let b_other = BlockMeta::with_pre(
            InodeId::create_block_id(FILE_ID + 1, 0).unwrap(),
            &workers(),
        );

        task.handle(rec(add_block(1, vec![a0.clone()], Vec::new())))
            .expect("handle");
        task.handle(rec(add_block(2, vec![a0.clone(), prealloc(1)], Vec::new())))
            .expect("handle");
        assert_eq!(task.batch.len(), 1);

        // A different inode interrupts the run.
        task.handle(rec(JournalEntry::AddBlock(AddBlockEntry {
            op_id: 3,
            rpc_id: 3,
            path: "/coalesce/other".to_string(),
            blocks: vec![b_other.clone()],
            commit_block: Vec::new(),
        })))
        .expect("handle");
        assert_eq!(task.batch.len(), 2);

        // Same inode, but a CompleteFile sits in between: no folding.
        let mut file = InodeFile::new(FILE_ID, 100);
        file.blocks = vec![a0.clone()];
        task.handle(rec(JournalEntry::CompleteFile(CompleteFileEntry {
            op_id: 4,
            rpc_id: 4,
            path: FILE_PATH.to_string(),
            file,
            commit_blocks: Vec::new(),
        })))
        .expect("handle");
        assert_eq!(task.batch.len(), 3);
        task.handle(rec(add_block(5, vec![a0.clone()], Vec::new())))
            .expect("handle");
        assert_eq!(
            task.batch.len(),
            4,
            "an AddBlock after CompleteFile opens a new record"
        );
        assert!(matches!(task.batch.batch[3], JournalEntry::AddBlock(_)));

        // Same inode but a different path (a rename in between) must not fold
        // into the record that named the old path.
        let before = task.batch.len();
        task.handle(rec(JournalEntry::AddBlock(AddBlockEntry {
            op_id: 6,
            rpc_id: 6,
            path: "/coalesce/renamed".to_string(),
            blocks: vec![a0.clone(), prealloc(2)],
            commit_block: Vec::new(),
        })))
        .expect("handle");
        assert_eq!(task.batch.len(), before + 1, "a rename breaks the run");
    }

    #[test]
    fn coalesce_is_inert_when_disabled() {
        let a0 = prealloc(0);
        let mut task = idle_sender(false, 4 << 20);
        task.handle(rec(add_block(1, vec![a0.clone()], Vec::new())))
            .expect("handle");
        task.handle(rec(add_block(2, vec![a0.clone()], Vec::new())))
            .expect("handle");
        assert_eq!(task.batch.len(), 2, "kill switch must keep every op");
    }

    /// A dense `AddBlock` entry of a controlled encoded size: `n` pre-assigned
    /// blocks of the same file, so the entry never folds with the one before it
    /// (different path) and the byte gate alone decides whether to flush.
    fn sized_add_block(op_id: u64, n: usize) -> JournalEntry {
        let blocks = (0..n).map(|seq| prealloc(seq as i64)).collect::<Vec<_>>();
        JournalEntry::AddBlock(AddBlockEntry {
            op_id,
            rpc_id: op_id as i64,
            path: format!("/coalesce/sized-{op_id}"),
            blocks,
            commit_block: Vec::new(),
        })
    }

    // One propose carries one origin because the receiver decides skip-or-replay
    // per batch (#2488 review): records folded across a restore epoch must ship
    // separately, so a record sampled at another epoch forces a pre-push flush
    // instead of joining the pending batch.
    #[test]
    fn batch_origin_splits_a_batch_across_epochs() {
        let b0 = prealloc(0);
        let b1 = prealloc(1);
        let mut task = idle_sender(true, 4 << 20);

        task.handle(Some((add_block(10, vec![b0.clone()], Vec::new()), Some(3))))
            .expect("handle");
        task.handle(Some((
            add_block(20, vec![b0.clone(), b1.clone()], Vec::new()),
            Some(3),
        )))
        .expect("handle");
        assert_eq!(task.batch.len(), 1, "the same epoch still coalesces");
        assert_eq!(task.batch_origin, Some(3));

        // The propose to the unreachable peer fails, which is exactly the
        // observable sign the pre-push flush fired, and the pending record
        // must be retained for the retry with its original origin.
        let err = task
            .handle(Some((add_block(30, vec![b1.clone()], Vec::new()), Some(4))))
            .expect_err("the epoch change must flush the pending batch first");
        assert!(!err.to_string().is_empty());
        assert_eq!(
            task.batch.len(),
            1,
            "the failed flush must keep the pending batch"
        );
        assert_eq!(task.batch_origin, Some(3));
    }

    #[test]
    fn byte_gate_keeps_entries_that_fit_in_one_batch() {
        let a = sized_add_block(1, 200);
        let b = sized_add_block(2, 200);
        let size_a = a.encode_size().expect("encode_size") as u64;
        let size_b = b.encode_size().expect("encode_size") as u64;
        // Both fit comfortably under the budget: no pre-push flush may fire,
        // so the two entries must stay in one batch.
        let budget = (size_a + size_b) * 2;
        let mut task = idle_sender(true, budget);
        task.handle(rec(a)).expect("handle");
        task.handle(rec(b)).expect("handle");
        assert_eq!(
            task.batch.len(),
            2,
            "two entries under the budget must share one batch"
        );
        assert_eq!(task.batch_bytes, size_a + size_b);
    }

    #[test]
    fn byte_gate_flushes_before_an_entry_that_would_exceed_the_budget() {
        let a = sized_add_block(1, 200);
        let b = sized_add_block(2, 200);
        let size_a = a.encode_size().expect("encode_size") as u64;
        let size_b = b.encode_size().expect("encode_size") as u64;
        // Each alone fits, the two together exceed the budget: pushing the
        // second must flush the first first. The propose to the unreachable
        // peer fails, which is exactly the observable sign the pre-push flush
        // fired, and the first entry must be retained for the retry.
        let budget = (size_a + size_b) - 1;
        let mut task = idle_sender(true, budget);
        task.handle(rec(a)).expect("handle");
        assert_eq!(task.batch.len(), 1);
        assert_eq!(task.batch_bytes, size_a);
        let err = task
            .handle(rec(b))
            .expect_err("the pre-push flush must fire");
        assert!(
            err.to_string().contains("propose") || !err.to_string().is_empty(),
            "the unreachable peer must surface an error: {err}"
        );
        assert_eq!(
            task.batch.len(),
            1,
            "the failed flush must keep the first entry in the batch"
        );
        assert_eq!(task.batch_bytes, size_a);
    }

    #[test]
    fn byte_gate_disabled_by_zero_budget_never_flushes_on_bytes() {
        let a = sized_add_block(1, 200);
        let b = sized_add_block(2, 200);
        // `writer_flush_batch_bytes = 0` is the documented off-switch: the
        // sender must not flush on bytes at all (op-count and time only).
        let mut task = idle_sender(true, 0);
        task.handle(rec(a)).expect("handle");
        task.handle(rec(b)).expect("handle");
        assert_eq!(task.batch.len(), 2, "a zero budget disables byte flushes");
        assert_eq!(task.batch_bytes, 0);
    }

    /// The journal shape this optimisation targets: writing one file block by
    /// block, so op k carries all k blocks of the file (O(N²) journal bytes).
    /// One `CompleteFile` splits the run, to prove folding stops there.
    fn single_file_workload(ops: i64) -> Vec<JournalEntry> {
        use crate::master::journal::{CompleteFileEntry, CreateFileEntry, MkdirEntry};
        use crate::master::meta::inode::{InodeDir, InodeFile};

        let mut entries = vec![
            JournalEntry::Mkdir(MkdirEntry {
                op_id: 1,
                rpc_id: 1,
                path: "/coalesce".to_string(),
                dir: InodeDir::new(FILE_ID + 1, 100),
            }),
            JournalEntry::CreateFile(CreateFileEntry {
                op_id: 2,
                rpc_id: 2,
                path: FILE_PATH.to_string(),
                file: InodeFile::new(FILE_ID, 100),
            }),
        ];
        let mut blocks: Vec<BlockMeta> = Vec::new();
        let mut op_id = 3u64;

        for seq in 0..ops {
            // `acquire_new_block`: the block that just filled is committed.
            if !blocks.is_empty() {
                let last = blocks.last_mut().expect("a full block");
                let commit = commit_of(last);
                last.commit(&commit);
                let mut commit_block = vec![commit.clone()];
                // A retried commit from another replica: the store keeps both
                // location records, so the fold must union them.
                commit_block.push(CommitBlock {
                    locations: vec![BlockLocation::with_id(1002)],
                    ..commit
                });
                entries.push(add_block(op_id, blocks.clone(), commit_block));
                op_id += 1;
            }

            // `assign_worker_inode`: the new block gains its locations.
            blocks.push(prealloc(seq));
            entries.push(add_block(op_id, blocks.clone(), Vec::new()));
            op_id += 1;
        }

        let mut file = InodeFile::new(FILE_ID, 100);
        file.len = blocks.len() as i64 * (4 << 20);
        file.blocks = blocks.clone();
        entries.push(JournalEntry::CompleteFile(CompleteFileEntry {
            op_id,
            rpc_id: op_id as i64,
            path: FILE_PATH.to_string(),
            file,
            commit_blocks: Vec::new(),
        }));
        op_id += 1;

        // Writes after the close must not fold into the record before it.
        blocks.push(prealloc(ops));
        entries.push(add_block(op_id, blocks, Vec::new()));

        entries
    }

    /// Fold a workload through the real `SenderTask` and return the batch it
    /// would propose.
    fn fold(entries: &[JournalEntry], coalesce: bool, flush_batch_bytes: u64) -> Vec<JournalEntry> {
        let mut task = idle_sender(coalesce, flush_batch_bytes);
        for entry in entries {
            task.handle(rec(entry.clone())).expect("handle");
        }
        assert_eq!(task.batch_bytes, accounted_bytes(&task.batch));
        task.batch.batch.clone()
    }

    /// Replay `entries` into a throwaway journal system and describe the
    /// resulting inode plus the committed locations the store ended up with.
    fn replay_report(name: &str, entries: &[JournalEntry]) -> FsResult<(String, String)> {
        use crate::master::journal::JournalSystem;
        use crate::master::meta::inode::InodePath;
        use curvine_config::{ClusterConf, MasterConf};
        use curvine_runtime::common::Utils;

        let root =
            std::env::temp_dir().join(format!("curvine-coalesce-{}-{}", name, Utils::rand_id()));
        let conf = ClusterConf {
            testing: true,
            format_master: true,
            master: MasterConf {
                meta_dir: root.join("meta").to_str().unwrap().into(),
                ..Default::default()
            },
            journal: JournalConf {
                journal_dir: root.join("journal").to_str().unwrap().into(),
                io_threads: 1,
                worker_threads: 2,
                ..JournalConf::with_test()
            },
            ..Default::default()
        };
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        for entry in entries {
            loader.apply_entry(entry.clone())?;
        }

        let fs_dir = js.fs().fs_dir();
        let fs_dir = fs_dir.read();
        let inp = InodePath::resolve(fs_dir.root_ptr(), FILE_PATH, &fs_dir.store)?;
        let inode = inp.get_last_inode().expect("the replayed file must exist");
        let file = inode.as_file_ref()?.clone();
        let described = format!("{:?}", file);
        let mut locations = String::new();
        for block in &file.blocks {
            let mut locs: Vec<u32> = fs_dir
                .store
                .get_block_locations(block.id)?
                .iter()
                .map(|v| v.worker_id)
                .collect();
            locs.sort_unstable();
            locations.push_str(&format!("{}:{:?} ", block.id, locs));
        }
        drop(fs_dir);
        drop(loader);
        js.shutdown();
        let _ = std::fs::remove_dir_all(&root);
        Ok((described, locations))
    }

    /// The whole point of the fold: replaying the merged batch must leave the
    /// same inode and the same committed locations as replaying every op.
    #[test]
    fn coalesced_replay_equals_uncoalesced_replay() -> FsResult<()> {
        let raw = single_file_workload(40);
        let folded = fold(&raw, true, 4 << 20);
        assert!(
            folded.len() < raw.len() / 2,
            "the workload must fold: raw={} folded={}",
            raw.len(),
            folded.len()
        );
        assert_eq!(
            folded
                .iter()
                .filter(|v| matches!(v, JournalEntry::AddBlock(_)))
                .count(),
            2,
            "one folded record per side of the CompleteFile"
        );

        let (raw_inode, raw_locations) = replay_report("raw", &raw)?;
        let (folded_inode, folded_locations) = replay_report("folded", &folded)?;

        assert!(raw_inode.contains("len:"), "the workload must write length");
        assert!(
            raw_locations.matches(':').count() >= 40,
            "the replayed file must carry the workload's blocks: {}",
            raw_locations
        );
        assert_eq!(raw_inode, folded_inode, "replayed inode must be identical");
        assert_eq!(
            raw_locations, folded_locations,
            "committed block locations must be identical"
        );
        Ok(())
    }
}
