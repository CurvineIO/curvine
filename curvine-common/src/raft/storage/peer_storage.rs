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
use crate::proto::raft::{FsmState, SnapshotData};
use crate::raft::snapshot::{DownloadJob, SnapshotState};
use crate::raft::storage::{AppStorage, ApplyMsg, LogStorage};
use crate::raft::{LibRaftResult, RaftClient, RaftError, RaftResult};
use log::{error, info, warn};
use orpc::common::{TimeSpent, Utils};
use orpc::err_box;
use orpc::runtime::{GroupExecutor, JobCtl, JobState, RpcRuntime, Runtime};
use prost::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{GetEntriesContext, RaftState, StateRole, Storage};
use std::sync::{Arc, Mutex};

// raft log packaging class
// Unify the access interfaces of app_store and log_store. Convenient to code use.
#[derive(Clone)]
pub struct PeerStorage<A, B> {
    rt: Arc<Runtime>,
    pub(crate) log_store: A,
    pub(crate) app_store: B,
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
    pub fn new(
        rt: Arc<Runtime>,
        log_store: A,
        app_store: B,
        client: RaftClient,
        conf: &JournalConf,
    ) -> Self {
        let executor = GroupExecutor::new("raft-job-executor", conf.worker_threads, 10);

        Self {
            rt,
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

    pub fn get_fsm_state(&self) -> FsmState {
        self.app_store.get_fsm_state()
    }

    pub fn get_applied(&self) -> u64 {
        self.get_fsm_state().applied.index
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

    pub async fn apply_propose(&self, wait: bool, apply_msg: ApplyMsg) -> RaftResult<()> {
        self.app_store.apply(wait, apply_msg).await
    }

    /// Scan log entries in [low, high). Used to get committed-but-not-applied entries for replay.
    pub fn scan_entries(&self, low: u64, high: u64) -> RaftResult<Vec<Entry>> {
        self.log_store.scan_entries(low, high)
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

    pub async fn role_change(&self, role: StateRole) -> RaftResult<()> {
        self.app_store.role_change(role).await
    }

    pub fn gen_apply_snapshot_job(&self, snapshot: Snapshot) -> RaftResult<()> {
        if self.is_snapshot_applying() {
            return err_box!("Currently applying snapshot");
        }

        let log_store = self.log_store.clone();
        let app_store = self.app_store.clone();
        let rt = self.rt.clone();

        let job_ctl = JobCtl::new();
        let mut snap_state = self.snap_state.lock().unwrap();
        *snap_state = SnapshotState::Applying(job_ctl.clone());

        let client = self.client.clone();
        let conf = self.conf.clone();
        let executor = self.executor.clone();
        let job = move || {
            let mut snap_data = SnapshotData::decode(snapshot.get_data())?;
            let mut spend = TimeSpent::new();

            // Start downloading the snapshot.
            match snap_data.files_data {
                None => panic!("Not found snapshot data"),
                Some(ref mut files) => {
                    let dir = app_store.snapshot_dir(Utils::rand_id())?;
                    let mut download_job = DownloadJob::new(
                        executor,
                        snap_data.node_id,
                        dir,
                        files.clone(),
                        &conf,
                        client,
                    );

                    // Modify the data in the local directory.
                    files.dir = download_job.run()?;
                }
            }
            let download_ms = spend.used_ms();
            spend.reset();

            if let Err(e) = log_store.apply_snapshot(snapshot) {
                return if e.is_snapshot_out_of_date() {
                    warn!("{}, skip apply; local state is already newer", e);
                    Ok(())
                } else {
                    Err(e)
                };
            }

            rt.block_on(app_store.apply_snapshot(snap_data.clone()))?;
            let apply_ms = spend.used_ms();

            info!(
                "Apply snapshot, snap_data: {:?}, download used {} ms, apply used {} ms",
                snap_data, download_ms, apply_ms
            );

            Ok::<(), RaftError>(())
        };

        self.spawn_job(job, job_ctl)
    }

    pub fn gen_create_snapshot_job(&self) -> RaftResult<()> {
        if !self.can_generate_snapshot() {
            return err_box!("Currently creating snapshot");
        }

        let log_store = self.log_store.clone();
        let app_store = self.app_store.clone();
        let rt = self.rt.clone();

        let job_ctl = JobCtl::new();
        let mut snap_state = self.snap_state.lock().unwrap();
        *snap_state = SnapshotState::Generating(job_ctl.clone());

        let job = move || {
            let cost = TimeSpent::new();

            let snap_data = rt.block_on(app_store.create_snapshot())?;
            log_store.create_snapshot(snap_data.clone())?;
            log_store.compact(snap_data.fsm_state.compact())?;

            info!(
                "create new snapshot, snap_data {:?}, used {} ms",
                snap_data,
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
