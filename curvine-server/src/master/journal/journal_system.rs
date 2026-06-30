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

use crate::master::fs::{MasterFilesystem, WorkerManager};
use crate::master::journal::{JournalLoader, JournalWriter};
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::meta::FsDir;
use crate::master::quota::eviction::evictor::{Evictor, LFUEvictor, LRUEvictor};
use crate::master::quota::eviction::types::EvictionPolicy;
use crate::master::quota::eviction::EvictionConf;
use crate::master::{
    JobManager, MasterMonitor, MetaRaftJournal, MountManager, QuotaManager, SyncFsDir,
    SyncWorkerManager,
};
use curvine_common::conf::ClusterConf;
use curvine_common::proto::raft::SnapshotData;
use curvine_common::raft::storage::{AppStorage, LogStorage, RocksLogStorage};
use curvine_common::raft::{RaftClient, RaftResult, RoleMonitor, RoleStateListener};
use curvine_common::FsResult;
use orpc::common::FileUtils;
use orpc::err_box;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::StateCtl;
use prost::Message;
use raft::eraftpb::Entry;
use raft::Storage;
use std::fs;
use std::path::Path;
use std::sync::Arc;

// Send and replay metadata operation logs based on raft.
pub struct JournalSystem {
    rt: Arc<Runtime>,
    fs: MasterFilesystem,
    worker_manager: SyncWorkerManager,
    raft_journal: MetaRaftJournal,
    master_monitor: MasterMonitor,
    mount_manager: Arc<MountManager>,
    quota_manager: Arc<QuotaManager>,
    job_manager: Arc<JobManager>,
}

struct FsInitParts {
    worker_manager: SyncWorkerManager,
    journal_writer: Arc<JournalWriter>,
    fs_dir: SyncFsDir,
    fs: MasterFilesystem,
    mount_manager: Arc<MountManager>,
    quota_manager: Arc<QuotaManager>,
    job_manager: Arc<JobManager>,
}

#[derive(Debug, Eq, PartialEq)]
enum MasterDataDirState {
    RocksDb,
    CleanEmpty,
    Invalid,
}

impl JournalSystem {
    #[allow(clippy::too_many_arguments)]
    fn new(
        rt: Arc<Runtime>,
        fs: MasterFilesystem,
        worker_manager: SyncWorkerManager,
        raft_journal: MetaRaftJournal,
        master_monitor: MasterMonitor,
        mount_manager: Arc<MountManager>,
        quota_manager: Arc<QuotaManager>,
        job_manager: Arc<JobManager>,
    ) -> Self {
        Self {
            rt,
            fs,
            worker_manager,
            raft_journal,
            master_monitor,
            mount_manager,
            quota_manager,
            job_manager,
        }
    }

    fn build_fs_parts(
        conf: &ClusterConf,
        rt: Arc<Runtime>,
        master_monitor: MasterMonitor,
    ) -> FsResult<FsInitParts> {
        let worker_manager = SyncWorkerManager::new(WorkerManager::new(conf));
        let client = RaftClient::from_conf(rt.clone(), &conf.journal);
        let journal_writer = Arc::new(JournalWriter::new(conf.testing, client, &conf.journal));

        let ttl_bucket_list = Arc::new(TtlBucketList::new(
            conf.master.ttl_bucket_interval_ms() as i64
        ));
        let eviction_conf = EvictionConf::from_conf(conf);
        let evictor: Arc<dyn Evictor> = match eviction_conf.policy {
            EvictionPolicy::Lru => Arc::new(LRUEvictor::new(eviction_conf.clone())),
            EvictionPolicy::Lfu => Arc::new(LFUEvictor::new(eviction_conf.clone())),
            //to-do: EvictionPolicy::Arc
        };

        let fs_dir = SyncFsDir::new(FsDir::new(
            conf,
            journal_writer.clone(),
            ttl_bucket_list,
            evictor.clone(),
        )?);

        let fs = MasterFilesystem::new(
            conf,
            fs_dir.clone(),
            worker_manager.clone(),
            master_monitor.clone(),
        );
        let mount_manager = Arc::new(MountManager::new(fs.clone()));
        let quota_manager = QuotaManager::new(
            eviction_conf,
            fs.clone(),
            evictor.clone(),
            rt.clone(),
            conf.testing,
        );

        let job_manager = Arc::new(JobManager::from_cluster_conf(
            fs.clone(),
            mount_manager.clone(),
            rt,
            conf,
        ));

        Ok(FsInitParts {
            worker_manager,
            journal_writer,
            fs_dir,
            fs,
            mount_manager,
            quota_manager,
            job_manager,
        })
    }

    fn require_existing_master_data(conf: &ClusterConf) -> FsResult<()> {
        if conf.format_master {
            return Ok(());
        }

        let meta_db_conf = conf.db_conf();
        let journal_db_conf = conf.journal.db_conf();
        let meta_state =
            Self::classify_master_data_dir(&meta_db_conf.base_dir, &meta_db_conf.data_dir);
        let journal_state =
            Self::classify_master_data_dir(&journal_db_conf.base_dir, &journal_db_conf.data_dir);

        if meta_state == MasterDataDirState::RocksDb && journal_state == MasterDataDirState::RocksDb
        {
            return Ok(());
        }

        if meta_state == MasterDataDirState::CleanEmpty
            && journal_state == MasterDataDirState::CleanEmpty
        {
            FileUtils::create_dir(&meta_db_conf.base_dir, true)?;
            FileUtils::create_dir(&journal_db_conf.base_dir, true)?;
            return Ok(());
        }

        let mut invalid_dirs = Vec::new();

        if meta_state != MasterDataDirState::RocksDb {
            invalid_dirs.push(format!("meta={}", meta_db_conf.data_dir));
        }
        if journal_state != MasterDataDirState::RocksDb {
            invalid_dirs.push(format!("journal={}", journal_db_conf.data_dir));
        }

        err_box!(
            "format_master=false found inconsistent or invalid master data directories: {}. Meta and journal must either both be valid RocksDB stores or both be missing/clean empty; non-empty non-RocksDB directories are refused. For replacing an HA master, preseed a consistent meta and journal copy before starting the pod",
            invalid_dirs.join(", ")
        )
    }

    fn classify_master_data_dir(base_dir: &str, data_dir: &str) -> MasterDataDirState {
        if Self::looks_like_rocksdb_data_dir(data_dir) {
            return MasterDataDirState::RocksDb;
        }

        let base_path = Path::new(base_dir);
        if !base_path.exists() {
            return MasterDataDirState::CleanEmpty;
        }
        if !base_path.is_dir() {
            return MasterDataDirState::Invalid;
        }

        let data_path = Path::new(data_dir);
        if Self::is_clean_empty_master_base(base_path, data_path) {
            MasterDataDirState::CleanEmpty
        } else {
            MasterDataDirState::Invalid
        }
    }

    fn is_clean_empty_master_base(base_path: &Path, data_path: &Path) -> bool {
        let Ok(entries) = fs::read_dir(base_path) else {
            return false;
        };

        for entry in entries {
            let Ok(entry) = entry else {
                return false;
            };

            let path = entry.path();
            if path != data_path || !Self::is_empty_dir(&path) {
                return false;
            }
        }

        true
    }

    fn is_empty_dir(path: &Path) -> bool {
        path.is_dir()
            && fs::read_dir(path)
                .map(|mut entries| entries.next().is_none())
                .unwrap_or(false)
    }

    fn looks_like_rocksdb_data_dir(path: &str) -> bool {
        let path = Path::new(path);
        if !path.is_dir() || !path.join("CURRENT").is_file() {
            return false;
        }

        fs::read_dir(path)
            .map(|entries| {
                entries.filter_map(Result::ok).any(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.starts_with("MANIFEST-"))
                })
            })
            .unwrap_or(false)
    }

    pub fn from_conf(conf: &ClusterConf) -> FsResult<Self> {
        // When the journal system is used, please note that it is separate from the fs system.
        Self::require_existing_master_data(conf)?;

        let rt = conf.journal.create_runtime();

        let log_store = RocksLogStorage::from_conf(&conf.journal, conf.format_master);

        let db_conf = conf.db_conf();
        if conf.format_master && FileUtils::exists(&db_conf.data_dir) {
            FileUtils::delete_path(&db_conf.data_dir, true)?;
        }

        let role_monitor = RoleMonitor::new();
        let master_monitor = MasterMonitor::new(role_monitor.read_ctl(), StateCtl::new(0));
        let parts = Self::build_fs_parts(conf, rt.clone(), master_monitor.clone())?;

        let raft_journal = MetaRaftJournal::new(
            rt.clone(),
            log_store.clone(),
            JournalLoader::new(
                rt.clone(),
                parts.fs_dir.clone(),
                parts.mount_manager.clone(),
                &conf.journal,
                parts.job_manager.clone(),
                log_store,
                parts.journal_writer.clone(),
            ),
            conf.journal.clone(),
            role_monitor,
        );

        let js = Self::new(
            rt,
            parts.fs,
            parts.worker_manager,
            raft_journal,
            master_monitor,
            parts.mount_manager,
            parts.quota_manager,
            parts.job_manager,
        );

        Ok(js)
    }

    #[doc(hidden)]
    pub fn fs_only_for_test(conf: &ClusterConf) -> FsResult<MasterFilesystem> {
        let rt = conf.journal.create_runtime();
        let master_monitor = MasterMonitor::new(StateCtl::new(0), StateCtl::new(0));
        Ok(Self::build_fs_parts(conf, rt, master_monitor)?.fs)
    }

    pub async fn start(self) -> FsResult<RoleStateListener> {
        let listener = self.raft_journal.run().await?;
        Ok(listener)
    }

    pub fn start_blocking(self) -> FsResult<RoleStateListener> {
        let rt = self.rt.clone();
        let js = self;
        rt.block_on(async move { js.start().await })
    }

    pub fn shutdown(self) {
        let Self {
            rt,
            fs,
            worker_manager,
            raft_journal,
            master_monitor,
            mount_manager,
            quota_manager,
            job_manager,
        } = self;

        let _ = rt.block_on(raft_journal.app_store().shutdown());
        drop(fs);
        drop(worker_manager);
        drop(raft_journal);
        drop(master_monitor);
        drop(mount_manager);
        drop(quota_manager);
        drop(job_manager);
        match Arc::try_unwrap(rt) {
            Ok(rt) => rt.shutdown_background(),
            Err(rt) => drop(rt),
        }
    }

    pub fn fs(&self) -> MasterFilesystem {
        self.fs.clone()
    }

    pub fn worker_manager(&self) -> SyncWorkerManager {
        self.worker_manager.clone()
    }

    #[doc(hidden)]
    pub fn journal_loader(&self) -> JournalLoader {
        self.raft_journal.app_store().clone()
    }

    pub fn state_listener(&self) -> RoleStateListener {
        self.raft_journal.new_state_listener()
    }

    pub fn master_monitor(&self) -> MasterMonitor {
        self.master_monitor.clone()
    }

    pub fn mount_manager(&self) -> Arc<MountManager> {
        self.mount_manager.clone()
    }

    pub fn quota_manager(&self) -> Arc<QuotaManager> {
        self.quota_manager.clone()
    }

    pub fn job_manager(&self) -> Arc<JobManager> {
        self.job_manager.clone()
    }

    // Create a snapshot manually, dedicated for testing.
    pub fn create_snapshot(&self) -> RaftResult<()> {
        let data = self
            .rt
            .block_on(self.raft_journal.app_store().create_snapshot())?;

        // The test will not generate a raft log. Please modify the status here.
        let entry = Entry {
            index: 1,
            ..Default::default()
        };

        self.raft_journal.log_store().append(&[entry])?;
        self.raft_journal.log_store().set_hard_state_commit(1)?;

        self.raft_journal.log_store().create_snapshot(data)
    }

    // Manually apply a snapshot, dedicated for testing.
    pub fn apply_snapshot(&self) -> RaftResult<()> {
        let snapshot = self.raft_journal.log_store().snapshot(0, 0)?;
        let data = SnapshotData::decode(snapshot.get_data())?;
        self.rt
            .block_on(self.raft_journal.app_store().apply_snapshot(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::conf::{JournalConf, MasterConf};
    use curvine_common::raft::RaftPeer;
    use orpc::common::Utils;

    fn non_format_master_conf(name: &str, multi_master: bool) -> ClusterConf {
        let mut journal = JournalConf::with_test();
        journal.enable = false;
        journal.journal_dir = Utils::test_sub_dir(format!("master-journal-test/journal-{}", name));
        if multi_master {
            journal
                .journal_addrs
                .push(RaftPeer::new(2, "localhost", journal.rpc_port + 1));
        }

        ClusterConf {
            format_master: false,
            testing: true,
            master: MasterConf {
                meta_dir: Utils::test_sub_dir(format!("master-journal-test/meta-{}", name)),
                ..Default::default()
            },
            journal,
            ..Default::default()
        }
    }

    #[test]
    fn require_existing_master_data_allows_clean_empty_non_format_dirs() -> FsResult<()> {
        for multi_master in [false, true] {
            let name = format!(
                "clean-empty-non-format-{}-{}",
                if multi_master { "ha" } else { "single" },
                Utils::rand_str(6)
            );
            let conf = non_format_master_conf(&name, multi_master);
            let _ = fs::remove_dir_all(&conf.master.meta_dir);
            let _ = fs::remove_dir_all(&conf.journal.journal_dir);

            JournalSystem::require_existing_master_data(&conf)?;

            assert!(Path::new(&conf.master.meta_dir).is_dir());
            assert!(Path::new(&conf.journal.journal_dir).is_dir());
        }

        Ok(())
    }

    #[test]
    fn require_existing_master_data_refuses_dirty_non_format_dirs() -> FsResult<()> {
        for multi_master in [false, true] {
            let name = format!(
                "dirty-non-format-{}-{}",
                if multi_master { "ha" } else { "single" },
                Utils::rand_str(6)
            );
            let conf = non_format_master_conf(&name, multi_master);
            let _ = fs::remove_dir_all(&conf.master.meta_dir);
            let _ = fs::remove_dir_all(&conf.journal.journal_dir);
            fs::create_dir_all(&conf.master.meta_dir)?;
            fs::create_dir_all(&conf.journal.journal_dir)?;
            fs::write(
                Path::new(&conf.journal.journal_dir).join("orphaned-file"),
                "not rocksdb",
            )?;

            let err = JournalSystem::require_existing_master_data(&conf)
                .expect_err("dirty master data directory must be refused");
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("format_master=false")
                    && err_msg.contains("inconsistent or invalid master data directories"),
                "unexpected error: {}",
                err_msg
            );
        }

        Ok(())
    }
}
