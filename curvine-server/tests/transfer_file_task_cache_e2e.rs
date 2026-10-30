// Copyright 2026 OPPO.
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

//! End-to-end: many small files transferred with the file-level task cache.
//!
//! Real MiniCluster, real local UFS, real planner, real worker I/O, real
//! `TransferService` report path, real SQLite store. The claim under test is
//! that a job made only of file-level tasks transfers completely while writing
//! no `transfer_tasks` rows at all, and still converges with correct counts.

use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_data_transfer::common::UfsFactory;
use curvine_data_transfer::transfer::{
    ClusterMetadataCache, SqliteTransferStore, TaskCache, TransferPlanner, TransferService,
    TransferStore,
};
use curvine_data_transfer::worker::task::{LoadTaskRunner, TaskContext};
use curvine_fs_api::Path;
use curvine_model::{
    classify_transfer_plan, JobSourceType, LoadJobInfo, LoadTaskInfo, MountInfo, StorageType,
    TransferJobRecord, TransferKind, TransferPlanKind, TransferProgress, TransferState,
    TransferTaskReportInfo, TtlAction, WorkerAddress,
};
use curvine_proto::TransferTaskReportRequest;
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use std::sync::Arc;
use std::time::Duration;

const FILES: usize = 100;
const FILE_BYTES: usize = 1024;

fn cluster() -> (Arc<MiniCluster>, String) {
    let test_id = Utils::rand_str(8);
    let base_dir = Utils::test_sub_dir(format!("transfer-file-cache-{test_id}"));
    std::fs::create_dir_all(&base_dir).unwrap();
    let mut conf = ClusterConf::default();
    conf.master.meta_dir = format!("{base_dir}/meta");
    conf.journal.journal_dir = format!("{base_dir}/journal");
    // Minimum CV block size is 1 MiB and a small file holds one block, so the
    // worker needs at least FILES MiB. It is RAM-backed, disk stays untouched.
    conf.worker.data_dir = vec![format!("[MEM:128MB]{base_dir}/worker")];
    conf.journal.raft_tick_interval_ms = 100;
    conf.client.short_circuit = false;
    let cluster = Arc::new(MiniCluster::with_num(&conf, 1, 1));
    cluster.start_cluster();
    (cluster, base_dir)
}

fn job(job_id: &str, source: &str, target: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("key-{job_id}"),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: source.to_string(),
        target_path: target.to_string(),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        mount_snapshot_attempts: 0,
        planning_attempts: 0,
        terminal_summary: None,
        terminal_counts: None,
        cv_metadata_epoch: None,
        state: TransferState::Planning,
        owner: "e2e-owner".to_string(),
        lease_epoch: 1,
        lease_expire_at: i64::MAX,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("req-{job_id}"),
        submitter: "e2e".to_string(),
        tenant: "e2e".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

#[test]
fn small_file_job_transfers_without_task_rows() -> CommonResult<()> {
    let (cluster, base_dir) = cluster();
    let rt = cluster.clone_client_rt();
    rt.block_on(async move {
        let fs = cluster.new_fs();
        let ufs_dir = format!("{base_dir}/ufs");
        std::fs::create_dir_all(&ufs_dir)?;
        // FILES x 1 KiB of source data: tiny on disk, one CV block each.
        for index in 0..FILES {
            std::fs::write(format!("{ufs_dir}/f{index:04}.bin"), vec![b'x'; FILE_BYTES])?;
        }

        let mount_path = "/e2e-file-cache";
        fs.mount(
            &Path::from_str(format!("file://{ufs_dir}"))?,
            &Path::from_str(mount_path)?,
            curvine_model::MountOptions::builder()
                .block_size(1024 * 1024)
                .replicas(1)
                .auto_cache(false)
                .build(),
        )
        .await?;
        let mount: MountInfo = fs
            .get_mount_table()
            .await?
            .into_iter()
            .find(|mount| mount.cv_path == mount_path)
            .unwrap();

        let factory = Arc::new(UfsFactory::with_rt(
            &cluster.cluster_conf.client,
            cluster.clone_client_rt(),
        ));
        let cache =
            ClusterMetadataCache::with_snapshot_policy(fs.clone(), Duration::from_secs(60), true);
        cache.refresh().await?;
        let planner = TransferPlanner::new(
            fs.clone(),
            factory.clone(),
            cache,
            cluster.cluster_conf.client.clone(),
            Arc::new(
                curvine_data_transfer::transfer::TransferRuntimeConfig::from_conf(
                    &cluster.cluster_conf,
                ),
            ),
            32,
        );

        let source = format!("file://{ufs_dir}");
        // Targets must live under the mount so the load resolves a mount.
        let target = "/e2e-file-cache/out";
        let job = job("e2e-file-cache", &source, target);
        let planned = planner.plan(&job).await?;
        // The planner packs a directory of small files into batch tasks, so
        // the task count is well below the file count. Either way every task
        // is file-level (Batch or Plain), never a segment.
        assert!(
            !planned.tasks.is_empty() && planned.tasks.len() <= FILES,
            "unexpected task count {} for {FILES} files",
            planned.tasks.len()
        );
        for task in &planned.tasks {
            println!(
                "planned task {} kind={:?}",
                task.task_id,
                classify_transfer_plan(&task.source_read_plan_json)
            );
        }

        // The scheduler's split: a plan without segment markers is file-level
        // and therefore cache-only; segment tasks keep their rows.
        let (cached, persisted): (Vec<_>, Vec<_>) =
            planned.tasks.iter().cloned().partition(|task| {
                !matches!(
                    classify_transfer_plan(&task.source_read_plan_json),
                    TransferPlanKind::Segment | TransferPlanKind::SegmentGroup
                )
            });
        assert_eq!(
            cached.len(),
            planned.tasks.len(),
            "all tasks must be file-level"
        );
        assert!(persisted.is_empty(), "no segment task is expected here");

        let store_path = format!("{base_dir}/transfer.db");
        let store = Arc::new(SqliteTransferStore::open(&store_path)?);
        store.create_or_get_by_request_id(job.clone())?;
        let task_cache = Arc::new(TaskCache::new());
        task_cache.seed(&job.job_id, job.run_id, cached.clone());
        let service = TransferService::new(store.clone()).with_task_cache(task_cache.clone());

        // Every task copies for real through the worker runner, then reports
        // through the real service path (the in-process equivalent of the RPC).
        let mut completed = 0usize;
        for (index, task) in cached.iter().enumerate() {
            let attempt_id = 1;
            let worker_session = "e2e-session";
            assert!(task_cache.start_attempt(&curvine_model::TaskAttemptStart {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                owner: job.owner.clone(),
                lease_epoch: job.lease_epoch,
                task_id: task.task_id.clone(),
                attempt_id,
                worker_id: 1,
                worker_session_id: worker_session.to_string(),
                report_target_json: "[]".to_string(),
                now_ms: 10,
                stale_deadline_at: i64::MAX,
                resume_loaded_size: 0,
            }));

            let job_info = LoadJobInfo {
                job_id: job.job_id.clone(),
                source_path: source.clone(),
                target_path: target.to_string(),
                block_size: 1024 * 1024,
                replicas: 1,
                storage_type: StorageType::default(),
                ttl_ms: 0,
                ttl_action: TtlAction::default(),
                mount_info: mount.clone(),
                create_time: 0,
                overwrite: Some(true),
                source_type: JobSourceType::default(),
                transfer_job_id: None,
            };
            let context = Arc::new(TaskContext::new(LoadTaskInfo {
                job: job_info,
                task_id: task.task_id.clone(),
                worker: WorkerAddress::default(),
                source_path: task.source_path.clone(),
                target_path: task.target_path.clone(),
                create_time: 0,
                source_read_plan_json: task.source_read_plan_json.clone(),
                estimated_size: FILE_BYTES as i64,
                transfer_report: Some(TransferTaskReportInfo {
                    run_id: job.run_id,
                    attempt_id,
                    worker_id: 1,
                    worker_session_id: worker_session.to_string(),
                    report_target: String::new(),
                    report_endpoints: Vec::new(),
                    attempt_started_at_ms: 0,
                    resume_loaded_size: 0,
                }),
            }));
            // No reporter is installed: the runner performs the real copy and
            // then returns false because it cannot deliver a terminal report.
            let runner =
                LoadTaskRunner::new(context, fs.clone(), factory.clone(), None, 10_000, 120_000);
            let ran = runner.run().await;
            println!("task {} run returned {}", task.task_id, ran);

            let _ = index;

            // Deliver the terminal report through the real service path.
            service.report_task(TransferTaskReportRequest {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                task_id: task.task_id.clone(),
                attempt_id,
                worker_id: 1,
                worker_session_id: worker_session.to_string(),
                state: curvine_model::TransferTaskState::Completed as i32,
                progress: curvine_proto::TransferProgressProto {
                    loaded_size: task.progress.total_size.max(FILE_BYTES as i64),
                    total_size: task.progress.total_size.max(FILE_BYTES as i64),
                    update_time: 50,
                    message: String::new(),
                },
                accepted_at_ms: Some(10),
                first_byte_at_ms: Some(11),
                attempt_started_at_ms: Some(10),
                block_locations: Vec::new(),
                protocol_version: Some(1),
            })?;
            completed += 1;
        }
        assert_eq!(completed, planned.tasks.len());

        let listing = fs.list_status(&Path::from_str(target)?).await?;
        println!("target listing ({} entries):", listing.len());
        for entry in listing.iter().take(8) {
            println!("  {} dir={} len={}", entry.path, entry.is_dir, entry.len);
        }
        // Real bytes on the target: stat each expected file rather than
        // relying on a paged directory listing.
        for index in 0..FILES {
            let path = Path::from_str(format!("{target}/f{index:04}.bin"))?;
            let status = fs.get_status(&path).await.unwrap_or_else(|err| {
                panic!("target file {index} missing: {err}");
            });
            assert_eq!(status.len, FILE_BYTES as i64, "target {index} size");
        }

        // No task row was ever written for this job.
        assert!(
            store
                .list_transfer_tasks(&job.job_id, job.run_id)?
                .is_empty(),
            "file-level tasks must not be persisted"
        );

        // Progress and counts converge from the cache alone.
        let (_, counts, _, _, _) = service.get_transfer_status(&job.job_id, None, None)?;
        assert_eq!(counts.completed, planned.tasks.len() as u64);
        assert_eq!(counts.pending + counts.running + counts.failed, 0);
        assert!(
            task_cache
                .summary(&job.job_id, job.run_id, 100)
                .unwrap()
                .all_completed
        );

        // The job terminal snapshot carries the cache-derived totals.
        let summary = task_cache.summary(&job.job_id, job.run_id, 100).unwrap();
        store.update_terminal_snapshot(
            &job.job_id,
            job.run_id,
            summary.progress,
            summary.counts,
            100,
        )?;
        let stored = store.get_transfer(&job.job_id)?.unwrap();
        let snapshot = stored.terminal_summary.expect("terminal snapshot written");
        // A batch task's total is the sum of the files it carries.
        assert_eq!(
            snapshot.loaded_size,
            (FILES * FILE_BYTES) as i64,
            "terminal snapshot must reflect the cache-derived totals"
        );
        assert_eq!(
            stored.terminal_counts.unwrap().completed,
            planned.tasks.len() as u64
        );

        Ok(())
    })
}
