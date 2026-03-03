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

use crate::conf::JournalConf;
use crate::proto::raft::SnapshotData;
use crate::raft::snapshot::{DownloadJob, SnapshotState};
use crate::raft::storage::{AppStorage, LogStorage};
use crate::raft::{LibRaftResult, RaftClient, RaftError, RaftResult};
use log::{error, info};
use orpc::common::{TimeSpent, Utils};
use orpc::err_box;
use orpc::runtime::{GroupExecutor, JobCtl, JobState};
use prost::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{GetEntriesContext, RaftState, Storage};
use std::sync::{Arc, Mutex};

// raft log packaging class
// Unify the access interfaces of app_store and log_store. Convenient to code use.
#[derive(Clone)]
pub struct PeerStorage<A, B> {
    log_store: A,
    app_store: B,
    conf: JournalConf,
    executor: Arc<GroupExecutor>,
    snap_state: Arc<Mutex<SnapshotState>>,
    client: RaftClient,
}

impl<A, B> PeerStorage<A, B>
where
    A: LogStorage,
    B: AppStorage,
{
    pub fn new(log_store: A, app_store: B, client: RaftClient, conf: &JournalConf) -> Self {
        let executor = GroupExecutor::new("raft-job-executor", conf.worker_threads, 10);

        Self {
            log_store,
            app_store,
            conf: conf.clone(),
            executor: Arc::new(executor),
            snap_state: Arc::new(Mutex::new(SnapshotState::Relax)),
            client,
        }
    }

    pub fn append(&self, entries: &[Entry]) -> RaftResult<()> {
        self.log_store.append(entries)
    }

    pub fn set_entries(&self, entries: &[Entry]) -> RaftResult<()> {
        self.log_store.set_entries(entries)
    }

    pub fn set_hard_state(&self, hard_state: &HardState) -> RaftResult<()> {
        self.log_store.set_hard_state(hard_state)
    }

    pub fn set_hard_state_commit(&self, commit: u64) -> RaftResult<()> {
        self.log_store.set_hard_state_commit(commit)
    }

    pub fn set_conf_state(&self, conf_state: &ConfState) -> RaftResult<()> {
        self.log_store.set_conf_state(conf_state)
    }

    pub async fn apply_propose(&self, is_leader: bool, data: &[u8]) -> RaftResult<()> {
        self.app_store.apply(is_leader, data).await
    }

    fn check_snapshot_state(&self) -> SnapshotState {
        let mut snap_state = self.snap_state.lock().unwrap();

        match &*snap_state {
            SnapshotState::Relax => {
                // pass
            }

            SnapshotState::Generating(st) => {
                let finish = match st.state() {
                    JobState::Cancelled => true,
                    JobState::Finished => true,
                    JobState::Failed => panic!("snap generating"),
                    _ => false,
                };

                if finish {
                    *snap_state = SnapshotState::Relax;
                }
            }

            SnapshotState::Applying(st) => {
                let finish = match st.state() {
                    JobState::Cancelled => true,
                    JobState::Finished => true,
                    JobState::Failed => panic!("snap applying"),
                    _ => false,
                };

                if finish {
                    *snap_state = SnapshotState::Relax;
                }
            }
        }

        snap_state.clone()
    }

    // Is it possible to create a snapshot currently?
    // The previous snapshot is being created and the storage is applying the snapshot, so the snapshot cannot be created at present.
    pub fn can_generate_snapshot(&self) -> bool {
        // Snapshot cannot be created during application.
        match self.check_snapshot_state() {
            SnapshotState::Relax => true,
            SnapshotState::Generating(_) => false,
            SnapshotState::Applying(_) => false,
        }
    }

    pub fn is_snapshot_applying(&self) -> bool {
        match self.check_snapshot_state() {
            SnapshotState::Relax => false,
            SnapshotState::Generating(_) => false,
            SnapshotState::Applying(_) => true,
        }
    }

    pub fn gen_apply_snapshot_job(&self, snapshot: Snapshot) -> RaftResult<()> {
        if self.is_snapshot_applying() {
            return err_box!("Currently applying snapshot");
        }

        let log_store = self.log_store.clone();
        let app_store = self.app_store.clone();

        let job_ctl = JobCtl::new();
        let mut snap_state = self.snap_state.lock().unwrap();
        *snap_state = SnapshotState::Applying(job_ctl.clone());

        let client = self.client.clone();
        let conf = self.conf.clone();
        let executor = self.executor.clone();
        let job = move || {
            let mut data = SnapshotData::decode(snapshot.get_data())?;
            let mut spend = TimeSpent::new();

            // Start downloading the snapshot.
            match data.files_data {
                None => panic!("Not found snapshot data"),
                Some(ref mut files) => {
                    let dir = app_store.snapshot_dir(Utils::rand_id())?;
                    let mut download_job =
                        DownloadJob::new(executor, data.node_id, dir, files.clone(), &conf, client);

                    // Modify the data in the local directory.
                    files.dir = download_job.run()?;
                }
            }
            let download_ms = spend.used_ms();
            spend.reset();

            // Install snapshot.
            let snapshot_meta = snapshot.metadata.clone();
            app_store.apply_snapshot(&data)?;
            log_store.apply_snapshot(snapshot)?;
            let apply_ms = spend.used_ms();

            info!(
                "Apply snapshot, meta: {:?}, download used {} ms, apply used {} ms",
                snapshot_meta, download_ms, apply_ms
            );

            Ok::<(), RaftError>(())
        };

        self.spawn_job(job, job_ctl)
    }

    pub fn gen_create_snapshot_job(
        &self,
        node_id: u64,
        last_applied: u64,
        compact_id: u64,
    ) -> RaftResult<()> {
        if !self.can_generate_snapshot() {
            return err_box!("Currently creating snapshot");
        }

        let log_store = self.log_store.clone();
        let app_store = self.app_store.clone();

        let job_ctl = JobCtl::new();
        let mut snap_state = self.snap_state.lock().unwrap();
        *snap_state = SnapshotState::Generating(job_ctl.clone());

        let job = move || {
            let cost = TimeSpent::new();

            let snapshot = app_store.create_snapshot(node_id, last_applied)?;
            let snapshot_id = snapshot.snapshot_id;
            log_store.create_snapshot(snapshot, last_applied)?;
            log_store.compact(compact_id)?;

            info!(
                "Create new snapshot, snapshot_id {}, last_applied {}, compact_id {}, used {} ms",
                snapshot_id,
                last_applied,
                compact_id,
                cost.used_ms()
            );
            Ok::<(), RaftError>(())
        };

        self.spawn_job(job, job_ctl)
    }

    fn spawn_job<F>(&self, job: F, job_ctl: JobCtl) -> RaftResult<()>
    where
        F: FnOnce() -> RaftResult<()> + Send + 'static,
    {
        self.executor.spawn(move || {
            job_ctl.advance_state(JobState::Running);
            match job() {
                Ok(_) => {
                    job_ctl.advance_state(JobState::Finished);
                }

                Err(e) => {
                    job_ctl.advance_state(JobState::Failed);
                    error!("create snap {}", e)
                }
            };
        })?;

        Ok(())
    }
}

impl<A, B> Storage for PeerStorage<A, B>
where
    A: LogStorage,
    B: AppStorage,
{
    fn initial_state(&self) -> LibRaftResult<RaftState> {
        self.log_store.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> LibRaftResult<Vec<Entry>> {
        self.log_store.entries(low, high, max_size, context)
    }

    fn term(&self, idx: u64) -> LibRaftResult<u64> {
        self.log_store.term(idx)
    }

    fn first_index(&self) -> LibRaftResult<u64> {
        self.log_store.first_index()
    }

    fn last_index(&self) -> LibRaftResult<u64> {
        self.log_store.last_index()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> LibRaftResult<Snapshot> {
        if !self.can_generate_snapshot() {
            return Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ));
        }

        self.log_store.snapshot(request_index, to)
    }
}
