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
use crate::raft::snapshot::{SnapshotFetcher, SnapshotState};
use crate::raft::storage::{AppStorage, ApplyMsg, LogStorage};
use crate::raft::{LibRaftResult, RaftClient, RaftError, RaftResult};
use curvine_core_error::err_box;
use curvine_runtime::common::TimeSpent;
use curvine_runtime::runtime::{GroupExecutor, JobCtl, JobState, RpcRuntime, Runtime};
use log::{error, info, warn};
use prost::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{GetEntriesContext, RaftState, StateRole, Storage};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

// raft log packaging class
// Unify the access interfaces of app_store and log_store. Convenient to code use.
#[derive(Clone)]
pub struct PeerStorage<A, B> {
    pub(crate) rt: Arc<Runtime>,
    pub id: u64,
    pub(crate) log_store: A,
    pub(crate) app_store: B,
    pub(crate) conf: JournalConf,
    pub(crate) executor: Arc<GroupExecutor>,
    snap_state: Arc<Mutex<SnapshotState>>,
    pub(crate) client: RaftClient,
}

impl<A, B> PeerStorage<A, B>
where
    A: LogStorage,
    B: AppStorage,
{
    pub fn new(
        rt: Arc<Runtime>,
        id: u64,
        log_store: A,
        app_store: B,
        client: RaftClient,
        conf: &JournalConf,
    ) -> Self {
        let executor = GroupExecutor::new("raft-job-executor", conf.worker_threads, 10);

        Self {
            rt,
            id,
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
        self.log_store.set_hard_state(hard_state)?;
        self.app_store.hard_state_changed(hard_state);
        Ok(())
    }

    pub fn set_hard_state_commit(&self, commit: u64) -> RaftResult<()> {
        let hard_state = self.log_store.set_hard_state_commit(commit)?;
        self.app_store.hard_state_changed(&hard_state);
        Ok(())
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

    /// Wait for both log snapshot persistence and application restore before
    /// advancing Ready or acknowledging replication. The job persists the log
    /// snapshot first; a restart between the two stages reinstalls it locally.
    pub async fn wait_snapshot_applied(&self) -> RaftResult<()> {
        loop {
            let state = self.snap_state.lock().unwrap().clone();
            match state {
                SnapshotState::Applying(job) => match job.state() {
                    JobState::Finished => return Ok(()),
                    JobState::Failed | JobState::Cancelled => {
                        return err_box!(
                            "snapshot application did not complete; refusing to advance Ready"
                        );
                    }
                    _ => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
                },
                _ => return Ok(()),
            }
        }
    }

    pub async fn role_change(&self, role: StateRole) -> RaftResult<()> {
        self.app_store.role_change(role).await
    }

    pub fn gen_apply_snapshot_job(
        &self,
        mut snapshot: Snapshot,
    ) -> RaftResult<oneshot::Receiver<RaftResult<()>>> {
        if self.is_snapshot_applying() {
            return err_box!("Currently applying snapshot");
        }

        let job_ctl = JobCtl::new();
        let mut snap_state = self.snap_state.lock().unwrap();
        *snap_state = SnapshotState::Applying(job_ctl.clone());

        let ps = self.clone();
        let job = move || {
            let data = SnapshotData::decode(snapshot.get_data())?;
            let mut spend = TimeSpent::new();

            let fetcher = ps.create_fetcher(data)?;

            let mut data = ps.rt.block_on(fetcher.fetch())?;
            data.node_id = ps.id;
            let download_ms = spend.used_ms();
            spend.reset();

            snapshot.data = data.encode_to_vec();
            if let Err(e) = ps.log_store.apply_snapshot(snapshot) {
                return if e.is_snapshot_out_of_date() {
                    warn!("{}, skip apply; local state is already newer", e);
                    Ok(())
                } else {
                    Err(e)
                };
            }

            ps.rt.block_on(ps.app_store.apply_snapshot(data.clone()))?;
            let apply_ms = spend.used_ms();

            info!(
                "Apply snapshot, snap_data: {:?}, download used {} ms, apply used {} ms",
                data, download_ms, apply_ms
            );

            Ok::<(), RaftError>(())
        };

        self.spawn_job(job, job_ctl)
    }

    pub fn gen_create_snapshot_job(&self) -> RaftResult<()> {
        if !self.can_generate_snapshot() {
            return err_box!("Currently creating snapshot");
        }

        let id = self.id;
        let log_store = self.log_store.clone();
        let app_store = self.app_store.clone();
        let rt = self.rt.clone();
        let client = self.client.clone();

        let job_ctl = JobCtl::new();
        let mut snap_state = self.snap_state.lock().unwrap();
        *snap_state = SnapshotState::Generating(job_ctl.clone());

        let job = move || {
            let cost = TimeSpent::new();

            let mut snap_data = rt.block_on(app_store.create_snapshot())?;
            snap_data.node_id = id;
            log_store.create_snapshot(snap_data.clone())?;
            log_store.compact(snap_data.fsm_state.compact())?;

            info!(
                "create new snapshot, dir {}, fsm_state: {:?}, used {} ms",
                snap_data.data_dir(),
                snap_data.fsm_state,
                cost.used_ms()
            );

            let snapshot_dir = snap_data.data_dir();
            match client.report_snapshot(snap_data) {
                Ok(accepted) => info!("report snapshot {} accepted: {}", snapshot_dir, accepted),
                Err(e) => {
                    warn!("report snapshot {} failed: {}", snapshot_dir, e)
                }
            }

            Ok::<(), RaftError>(())
        };

        drop(self.spawn_job(job, job_ctl)?);
        Ok(())
    }

    pub fn register_snapshot(&self, snapshot: SnapshotData) -> RaftResult<bool> {
        let spent = TimeSpent::new();

        let store = self.log_store.clone();
        let accepted = store.register_snapshot(snapshot.clone())?;
        info!(
            "register snapshot from node id {}, accepted {}, node addr {}, dir {}, fsm_state {:?} cost {}ms",
            snapshot.node_id,
            accepted,
            self.client.get_addr_string(snapshot.node_id),
            snapshot.data_dir(),
            snapshot.fsm_state,
            spent.used_ms()
        );

        if accepted {
            self.executor.spawn(move || {
                if let Err(e) = store.compact(snapshot.fsm_state.compact()) {
                    warn!("log store compact failed: {}", e);
                }
            })?;
        }

        Ok(accepted)
    }

    pub fn create_fetcher(&self, data: SnapshotData) -> RaftResult<SnapshotFetcher> {
        SnapshotFetcher::new(
            self.id,
            &self.app_store,
            &self.conf,
            self.client.clone(),
            data,
        )
    }

    fn spawn_job<F>(&self, job: F, job_ctl: JobCtl) -> RaftResult<oneshot::Receiver<RaftResult<()>>>
    where
        F: FnOnce() -> RaftResult<()> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        self.executor.spawn(move || {
            job_ctl.advance_state(JobState::Running);
            let result = job();
            match &result {
                Ok(()) => {
                    job_ctl.advance_state(JobState::Finished);
                }

                Err(e) => {
                    job_ctl.advance_state(JobState::Failed);
                    error!("snapshot job failed: {}", e)
                }
            }
            let _ = sender.send(result);
        })?;

        Ok(receiver)
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
