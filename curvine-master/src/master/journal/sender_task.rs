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

use crate::master::journal::recovery::{classify_apply_error, ApplyDisposition, RecoveryGate};
use crate::master::journal::{JournalBatch, JournalEntry};
use crate::master::{Master, MasterMetrics};
use curvine_config::JournalConf;
use curvine_core_error::CommonResult;
use curvine_error::FsResult;
use curvine_raft::raft::RaftClient;
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::sync::channel::BlockingReceiver;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

pub struct SenderTask {
    pub(crate) client: RaftClient,
    pub(crate) recovery: RecoveryGate,
    pub(crate) batch: JournalBatch,
    pub(crate) flush_batch_ms: u64,
    pub(crate) flush_batch_size: u64,
    pub(crate) last_flush_ms: u64,
    pub(crate) metrics: &'static MasterMetrics,
}

impl SenderTask {
    pub(crate) fn new(
        client: RaftClient,
        conf: &JournalConf,
        batch_seq_id: u64,
        recovery: RecoveryGate,
    ) -> CommonResult<Self> {
        let sender = Self {
            client,
            recovery,
            batch: JournalBatch::new(batch_seq_id),
            flush_batch_ms: conf.writer_flush_batch_ms,
            flush_batch_size: conf.writer_flush_batch_size,
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
            Self::loop0(receiver, poll, task);
        })?;
        Ok(())
    }

    fn loop0(receiver: BlockingReceiver<JournalEntry>, poll: Duration, mut task: SenderTask) {
        // The thread stays up. Transient Raft errors keep the current batch and
        // retry. Serialization or metadata damage enters safe mode instead of
        // aborting the process.
        let mut backoff = Duration::from_millis(200);
        loop {
            if task.recovery.rejects_writes() {
                discard_unproposed(&mut task.batch, &receiver);
                thread::sleep(Duration::from_secs(1));
                continue;
            }
            let event = match receiver.recv_timeout(poll) {
                Ok(v) => Some(v),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => {
                    if let Err(e) = task.flush_pending() {
                        log::warn!("journal-writer final flush failed, batch retained: {e}");
                    }
                    return;
                }
            };
            match task.handle(event) {
                Ok(()) => backoff = Duration::from_millis(200),
                Err(e) => {
                    let message = e.to_string();
                    match classify_apply_error(&message) {
                        ApplyDisposition::Retry => {
                            log::warn!(
                                "journal-writer flush failed, batch retained, retry in {:?}: {}",
                                backoff,
                                message
                            );
                            thread::sleep(backoff);
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                        }
                        ApplyDisposition::EnterSafeMode => {
                            log::error!(
                                "journal-writer entered safe mode, batch retained: {}",
                                message
                            );
                            task.recovery.enter_safe_mode(&message);
                            task.recovery.mark_writer_failed();
                        }
                    }
                }
            }
        }
    }

    pub fn handle(&mut self, entry: Option<JournalEntry>) -> FsResult<()> {
        let force = if let Some(v) = entry {
            let force = matches!(v, JournalEntry::Snapshot(_));
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

        // encode drains its input. Propose from a copy so a refused Raft
        // call can retry the same entries.
        let mut staging = self.batch.clone();
        let bytes = JournalBatch::encode(&mut staging)?;
        self.client.block_on_send_propose(bytes)?;
        self.batch.batch.clear();

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

/// Entries still in this process have not been proposed. Safe mode must not
/// flush them later and must not apply them to local metadata.
fn discard_unproposed(batch: &mut JournalBatch, receiver: &BlockingReceiver<JournalEntry>) {
    if !batch.is_empty() {
        log::warn!(
            "journal-writer dropping unproposed batch in safe mode, seq={}",
            batch.seq_id
        );
        batch.batch.clear();
        batch.next();
    }
    while receiver.try_recv().is_ok() {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::{CacheInvalidationEntry, JournalEntry};
    use crate::master::Master;
    use curvine_config::RaftPeer;

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn transient_propose_failure_keeps_the_batch() {
        Master::init_test_metrics();
        let mut conf = JournalConf::default();
        conf.hostname = "127.0.0.1".to_string();
        conf.rpc_port = 1;
        conf.journal_addrs = vec![RaftPeer::new(1, "127.0.0.1", 1)];
        conf.io_threads = 1;
        conf.worker_threads = 1;
        conf.conn_timeout_ms = 200;
        conf.io_timeout_ms = 200;
        conf.conn_retry_max_duration_ms = 0;
        conf.rpc_retry_max_duration_ms = 0;
        conf.conn_retry_min_sleep_ms = 1;
        conf.conn_retry_max_sleep_ms = 1;
        conf.rpc_retry_min_sleep_ms = 1;
        conf.rpc_retry_max_sleep_ms = 1;
        conf.writer_flush_batch_size = 1;
        conf.writer_flush_batch_ms = 60_000;

        let rt = conf.create_runtime();
        let client = RaftClient::from_conf(rt, &conf);
        let recovery = RecoveryGate::new();
        let mut task = SenderTask::new(client, &conf, 7, recovery).expect("sender");
        let error = task
            .handle(Some(JournalEntry::CacheInvalidation(
                CacheInvalidationEntry {
                    op_id: 3,
                    rpc_id: 1,
                    inodes: vec![],
                },
            )))
            .expect_err("propose to a closed port must fail");

        assert!(
            classify_apply_error(&error.to_string()) == ApplyDisposition::Retry,
            "connection failure must stay retryable: {error}"
        );
        assert_eq!(task.batch.len(), 1, "failed flush must keep the batch");
        assert_eq!(task.batch.seq_id, 7, "failed flush must not roll the batch");
        assert!(!task.recovery.is_safe_mode());
    }

    #[test]
    fn safe_mode_discards_unproposed_entries() {
        use curvine_runtime::sync::channel::BlockingChannel;

        let (sender, receiver) = BlockingChannel::new(0).split();
        let mut batch = JournalBatch::new(4);
        batch.push(JournalEntry::CacheInvalidation(CacheInvalidationEntry {
            op_id: 1,
            rpc_id: 1,
            inodes: vec![],
        }));
        sender
            .send(JournalEntry::CacheInvalidation(CacheInvalidationEntry {
                op_id: 2,
                rpc_id: 2,
                inodes: vec![],
            }))
            .expect("enqueue");

        discard_unproposed(&mut batch, &receiver);

        assert!(batch.is_empty(), "the unproposed batch must be dropped");
        assert_eq!(batch.seq_id, 5);
        assert!(
            receiver.try_recv().is_err(),
            "queued entries must not remain to be flushed later"
        );
    }
}
