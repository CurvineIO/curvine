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

use crate::master::journal::{JournalBatch, JournalEntry};
use crate::master::{Master, MasterMetrics};
use curvine_config::JournalConf;
use curvine_core_error::CommonResult;
use curvine_error::FsResult;
use curvine_raft::raft::RaftClient;
use curvine_runtime::common::SerdeUtils;
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::sync::channel::BlockingReceiver;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

pub struct SenderTask {
    pub(crate) client: RaftClient,
    pub(crate) batch: JournalBatch,
    pub(crate) flush_batch_ms: u64,
    pub(crate) flush_batch_size: u64,
    pub(crate) flush_batch_bytes: u64,
    pub(crate) batch_bytes: u64,
    pub(crate) last_flush_ms: u64,
    pub(crate) metrics: &'static MasterMetrics,
}

impl SenderTask {
    pub fn new(client: RaftClient, conf: &JournalConf, batch_seq_id: u64) -> CommonResult<Self> {
        let sender = Self {
            client,
            batch: JournalBatch::new(batch_seq_id),
            flush_batch_ms: conf.writer_flush_batch_ms,
            flush_batch_size: conf.writer_flush_batch_size,
            flush_batch_bytes: conf.writer_flush_batch_bytes,
            batch_bytes: 0,
            last_flush_ms: LocalTime::mills(),
            metrics: Master::get_metrics()?,
        };

        Ok(sender)
    }

    // Start a thread to execute sender task
    pub fn spawn(self, receiver: BlockingReceiver<JournalEntry>) -> FsResult<()> {
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
        receiver: BlockingReceiver<JournalEntry>,
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

    pub fn handle(&mut self, entry: Option<JournalEntry>) -> FsResult<()> {
        let force = if let Some(v) = entry {
            let force = matches!(v, JournalEntry::Snapshot(_));
            // Accumulate the encoded byte size of the batch so `loop0` can
            // flush before the next entry when the ceiling is reached. Snapshot
            // entries force their own flush and are not budgeted.
            if !force && self.flush_batch_bytes > 0 {
                self.batch_bytes = self.batch_bytes.saturating_add(v.encode_size()? as u64);
            }
            self.batch.push(v);
            self.metrics.journal_queue_len.dec();
            force
        } else {
            false
        };

        let len = self.batch.len() as u64;
        if len == 0 {
            return Ok(());
        }

        if force || len >= self.flush_batch_size || self.flush_interval_elapsed() {
            self.flush_pending()?;
        }

        Ok(())
    }

    fn flush_interval_elapsed(&self) -> bool {
        LocalTime::mills() - self.last_flush_ms >= self.flush_batch_ms
    }

    fn flush_pending(&mut self) -> FsResult<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let spend = TimeSpent::new();

        let bytes = SerdeUtils::serialize(&self.batch)?;
        self.client.block_on_send_propose(bytes)?;
        self.batch_bytes = 0;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::AddBlockEntry;
    use crate::master::meta::BlockMeta;

    #[test]
    fn byte_budget_accumulates_encoded_size() {
        Master::init_test_metrics();
        let mut conf = JournalConf::default();
        conf.hostname = "127.0.0.1".to_string();
        conf.rpc_port = 1;
        conf.journal_addrs = vec![curvine_config::RaftPeer::new(1, "127.0.0.1", 1)];
        conf.writer_flush_batch_size = 1000;
        conf.writer_flush_batch_ms = 60_000;
        conf.writer_flush_batch_bytes = 1024;

        let rt = conf.create_runtime();
        let client = RaftClient::from_conf(rt, &conf);
        let mut task = SenderTask::new(client, &conf, 0).expect("sender");

        let entry = JournalEntry::AddBlock(AddBlockEntry {
            op_id: 1,
            rpc_id: 0,
            path: "/f".to_string(),
            blocks: (0..100).map(|b| BlockMeta::new(1000 + b, 1024)).collect(),
            commit_block: vec![],
        });
        let size = entry.encode_size().expect("encode_size");
        assert!(size > 0, "an AddBlock entry always serializes to bytes");

        task.handle(Some(entry)).expect("handle");
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
}
