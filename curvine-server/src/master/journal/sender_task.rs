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
use curvine_common::conf::JournalConf;
use curvine_common::raft::RaftClient;
use curvine_common::utils::SerdeUtils;
use curvine_common::FsResult;
use orpc::common::{LocalTime, TimeSpent};
use orpc::err_box;
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

pub struct SenderTask {
    pub(crate) client: RaftClient,
    pub(crate) batch: JournalBatch,
    pub(crate) flush_batch_ms: u64,
    pub(crate) flush_batch_size: u64,
    pub(crate) last_flush_ms: u64,
    pub(crate) metrics: &'static MasterMetrics,
}

impl SenderTask {
    pub fn new(client: RaftClient, conf: &JournalConf, batch_seq_id: u64) -> Self {
        let sender = Self {
            client,
            batch: JournalBatch::new(batch_seq_id),
            flush_batch_ms: conf.writer_flush_batch_ms,
            flush_batch_size: conf.writer_flush_batch_size,
            last_flush_ms: LocalTime::mills(),
            metrics: Master::get_metrics(),
        };

        sender
    }

    // Start a thread to execute sender task
    pub fn spawn(self, receiver: mpsc::Receiver<JournalEntry>) -> FsResult<()> {
        let poll = Duration::from_millis(self.flush_batch_ms);
        let name = "journal-writer".to_string();
        let task = self;
        thread::Builder::new().name(name.clone()).spawn(move || {
            if let Err(e) = Self::loop0(receiver, poll, task) {
                panic!("thread {} stop: {:?}", name, e);
            }
        })?;
        Ok(())
    }

    fn loop0(
        receiver: mpsc::Receiver<JournalEntry>,
        poll: Duration,
        mut task: SenderTask,
    ) -> FsResult<()> {
        // The thread should not end.
        loop {
            let event = match receiver.recv_timeout(poll) {
                Ok(v) => Some(v),
                Err(RecvTimeoutError::Timeout) => None,
                Err(e) => return err_box!("event loop: {}", e),
            };
            task.handle(event)?;
        }
    }

    pub fn handle(&mut self, entry: Option<JournalEntry>) -> FsResult<()> {
        if let Some(v) = entry {
            self.batch.push(v);
            self.metrics.journal_queue_len.dec();
        }

        let len = self.batch.len() as u64;
        if len == 0 {
            return Ok(());
        }

        if len >= self.flush_batch_size
            || LocalTime::mills() - self.last_flush_ms >= self.flush_batch_ms
        {
            let spend = TimeSpent::new();

            let bytes = SerdeUtils::serialize(&self.batch)?;
            self.client.block_on_send_propose(bytes)?;

            self.metrics.journal_flush_count.inc();
            self.metrics
                .journal_flush_time
                .inc_by(spend.used_us() as i64);

            // Scroll to the next batch.
            self.batch.next();
            self.last_flush_ms = LocalTime::mills();
        }

        Ok(())
    }
}
