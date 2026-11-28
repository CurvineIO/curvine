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
        crate::fault_point! {
            sync,
            name: "raft.storage.before_append",
            description: "Before raft log entries are persisted",
            context: {"node" => self.id as i64, "entries" => entries.len() as i64},
        }
        let result = self.log_store.append(entries);
        crate::fault_point! {
            sync,
            name: "raft.storage.after_append",
            description: "After raft log entries are persisted",
            context: {"node" => self.id as i64, "entries" => entries.len() as i64},
        }
        result
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
        crate::fault_point! {
            sync,
            name: "raft.storage.before_hard_state",
            description: "Before term/vote/commit are persisted",
            context: {
                "node" => self.id as i64,
                "term" => hard_state.term as i64,
                "commit" => hard_state.commit as i64,
            },
        }
        self.log_store.set_hard_state(hard_state)?;
        crate::fault_point! {
            sync,
            name: "raft.storage.after_hard_state",
            description: "After term/vote/commit are persisted",
            context: {
                "node" => self.id as i64,
                "term" => hard_state.term as i64,
                "commit" => hard_state.commit as i64,
            },
        }
        self.app_store.hard_state_changed(hard_state);
        Ok(())
    }

    pub fn set_hard_state_commit(&self, commit: u64) -> RaftResult<()> {
        crate::fault_point! {
            sync,
            name: "raft.storage.before_commit",
            description: "Before an advanced commit index is persisted",
            context: {"node" => self.id as i64, "commit" => commit as i64},
        }
        let hard_state = self.log_store.set_hard_state_commit(commit)?;
        crate::fault_point! {
            sync,
            name: "raft.storage.after_commit",
            description: "After an advanced commit index is persisted",
            context: {"node" => self.id as i64, "commit" => commit as i64},
        }
        self.app_store.hard_state_changed(&hard_state);
        Ok(())
    }

    pub fn set_conf_state(&self, conf_state: &ConfState) -> RaftResult<()> {
        self.log_store.set_conf_state(conf_state)
    }

    pub fn recovery_state(&self) -> RaftResult<Option<u64>> {
        self.log_store.recovery_state()
    }

    pub fn set_recovery_state(&self, state: Option<u64>) -> RaftResult<()> {
        self.log_store.set_recovery_state(state)
    }

    pub async fn apply_propose(&self, wait: bool, apply_msg: ApplyMsg) -> RaftResult<()> {
        crate::fault_point! {
            async,
            name: "raft.storage.before_apply_dispatch",
            description: "Before a committed entry is handed to the apply worker",
            context: {"node" => self.id as i64, "wait" => wait},
        }
        let result = self.app_store.apply(wait, apply_msg).await;
        crate::fault_point! {
            async,
            name: "raft.storage.after_apply_dispatch",
            description: "After a committed entry was handed to the apply worker",
            context: {"node" => self.id as i64, "wait" => wait},
        }
        result
    }

    /// Scan log entries in [low, high). Used to get committed-but-not-applied entries for replay.
    pub fn scan_entries(&self, low: u64, high: u64) -> RaftResult<Vec<Entry>> {
        self.log_store.scan_entries(low, high)
    }

    fn check_snapshot_state(&self) -> SnapshotState {
        let (snap_state, failed) = {
            let mut state = self.snap_state.lock().unwrap();
            let mut failed = None;

            match &*state {
                SnapshotState::Relax => {
                    // pass
                }

                SnapshotState::Generating(st) => match st.state() {
                    JobState::Cancelled | JobState::Finished => {
                        *state = SnapshotState::Relax;
                    }

                    // The generation job reports nothing back to the driver, so a
                    // failure is terminal. It is raised after the lock is released
                    // to keep the snapshot state usable while the node unwinds.
                    JobState::Failed => failed = Some("snap generating"),
                    _ => {}
                },

                SnapshotState::Applying(st) => {
                    let finish = matches!(
                        st.state(),
                        JobState::Cancelled | JobState::Finished | JobState::Failed
                    );

                    if finish {
                        *state = SnapshotState::Relax;
                    }
                }
            }

            (state.clone(), failed)
        };

        if let Some(reason) = failed {
            panic!("{}", reason);
        }

        snap_state
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
            let snapshot_id = data.snapshot_id;
            let _ = snapshot_id; // read only by fault-injection contexts
            let mut spend = TimeSpent::new();

            crate::fault_point! {
                sync,
                name: "raft.snapshot.install.before_fetch",
                description: "Before a follower downloads a snapshot",
                context: {"node" => ps.id as i64, "snapshot_id" => snapshot_id as i64},
            }
            let fetcher = ps.create_fetcher(data)?;

            let mut data = ps.rt.block_on(fetcher.fetch())?;
            data.node_id = ps.id;
            let download_ms = spend.used_ms();
            spend.reset();
            crate::fault_point! {
                sync,
                name: "raft.snapshot.install.after_fetch",
                description: "After a follower downloaded a snapshot",
                context: {"node" => ps.id as i64, "snapshot_id" => snapshot_id as i64},
            }

            snapshot.data = data.encode_to_vec();
            crate::fault_point! {
                sync,
                name: "raft.snapshot.install.before_log_apply",
                description: "Before the raft log store installs a snapshot",
                context: {"node" => ps.id as i64, "snapshot_id" => snapshot_id as i64},
            }
            if let Err(e) = ps.log_store.apply_snapshot(snapshot) {
                return if e.is_snapshot_out_of_date() {
                    warn!("{}, skip apply; local state is already newer", e);
                    Ok(())
                } else {
                    Err(e)
                };
            }
            crate::fault_point! {
                sync,
                name: "raft.snapshot.install.after_log_apply",
                description: "After the raft log store installed a snapshot",
                context: {"node" => ps.id as i64, "snapshot_id" => snapshot_id as i64},
            }

            crate::fault_point! {
                sync,
                name: "raft.snapshot.install.before_app_apply",
                description: "Before the metadata store applies a snapshot",
                context: {"node" => ps.id as i64, "snapshot_id" => snapshot_id as i64},
            }
            ps.rt.block_on(ps.app_store.apply_snapshot(data.clone()))?;
            crate::fault_point! {
                sync,
                name: "raft.snapshot.install.after_app_apply",
                description: "After the metadata store applied a snapshot",
                context: {"node" => ps.id as i64, "snapshot_id" => snapshot_id as i64},
            }
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

            crate::fault_point! {
                sync,
                name: "raft.snapshot.create.before",
                description: "Before a follower creates a metadata snapshot",
                context: {"node" => id as i64},
            }
            let mut snap_data = rt.block_on(app_store.create_snapshot())?;
            snap_data.node_id = id;
            crate::fault_point! {
                sync,
                name: "raft.snapshot.create.after_app_snapshot",
                description: "After the metadata store produced a snapshot",
                context: {"node" => id as i64, "snapshot_id" => snap_data.snapshot_id as i64},
            }
            log_store.create_snapshot(snap_data.clone())?;
            crate::fault_point! {
                sync,
                name: "raft.snapshot.create.after_log_snapshot",
                description: "After the log store registered a snapshot",
                context: {"node" => id as i64, "snapshot_id" => snap_data.snapshot_id as i64},
            }
            crate::fault_point! {
                sync,
                name: "raft.snapshot.create.before_compact",
                description: "Before the log is compacted behind a snapshot",
                context: {"node" => id as i64, "snapshot_id" => snap_data.snapshot_id as i64},
            }
            log_store.compact(snap_data.fsm_state.compact_upto(None))?;
            crate::fault_point! {
                sync,
                name: "raft.snapshot.create.after_compact",
                description: "After the log is compacted behind a snapshot",
                context: {"node" => id as i64, "snapshot_id" => snap_data.snapshot_id as i64},
            }

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

    pub fn register_snapshot(
        &self,
        snapshot: SnapshotData,
        min_commit: Option<u64>,
    ) -> RaftResult<bool> {
        let spent = TimeSpent::new();

        let store = self.log_store.clone();
        crate::fault_point! {
            sync,
            name: "raft.snapshot.register.before",
            description: "Before the leader registers a reported snapshot",
            context: {"node" => self.id as i64, "snapshot_id" => snapshot.snapshot_id as i64},
        }
        let accepted = store.register_snapshot(snapshot.clone())?;
        crate::fault_point! {
            sync,
            name: "raft.snapshot.register.after",
            description: "After the leader registers a reported snapshot",
            context: {
                "node" => self.id as i64,
                "snapshot_id" => snapshot.snapshot_id as i64,
                "accepted" => accepted,
            },
        }
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
            let compact_index = snapshot.fsm_state.compact_upto(min_commit);
            self.executor.spawn(move || {
                crate::fault_point! {
                    sync,
                    name: "raft.snapshot.register.before_compact",
                    description: "Before the leader compacts behind a reported snapshot",
                    context: {"compact_index" => compact_index as i64},
                }
                if let Err(e) = store.compact(compact_index) {
                    warn!("log store compact failed: {}", e);
                }
                crate::fault_point! {
                    sync,
                    name: "raft.snapshot.register.after_compact",
                    description: "After the leader compacts behind a reported snapshot",
                    context: {"compact_index" => compact_index as i64},
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
