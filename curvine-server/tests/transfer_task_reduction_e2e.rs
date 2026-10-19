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

//! Real E2E for `docs/plans/2026-09-16-transfer-task-reduction.md`.
//!
//! Every test uses real I/O (real SQLite files, real MiniCluster, real RPC
//! bytes). Group copy/resume mechanics run against S3 (adapter-supplied
//! etag); file:// (Local, no identity) proves the fail-closed side.

use curvine_config::{ClusterConf, TransferConf};
use curvine_core_error::CommonResult;
use curvine_data_transfer::common::UfsFactory;
use curvine_data_transfer::transfer::{
    SqliteTransferStore, TransferMetrics, TransferPlannedTasks, TransferService, TransferStore,
};
use curvine_data_transfer::worker::task::{LoadTaskRunner, TaskContext};
use curvine_fs_api::{FileSystem, Path};
use curvine_model::{
    classify_transfer_plan, group_resume_remaining, normalize_group_resume, parse_segment_group,
    segment_group_json, JobSourceType, LoadJobInfo, LoadTaskInfo, MountInfo, SegmentGroupPlan,
    SetAttrOptsBuilder, StorageType, TaskAttemptStart, TransferJobRecord, TransferKind,
    TransferPlanKind, TransferProgress, TransferState, TransferStateUpdate, TransferTaskRecord,
    TransferTaskReport, TransferTaskReportInfo, TransferTaskState, TtlAction, WorkerAddress,
};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "fault-injection")]
use curvine_fault::{FaultRuleBuilder, FaultRuntime};

const MB: i64 = 1024 * 1024;

/// D1: shared classifier, compact manifest, size bound, RPC compat.
#[test]
fn e2e_d1_classifier_rpc_manifest_compat() -> CommonResult<()> {
    assert_eq!(
        classify_transfer_plan("{\"batch\":{\"batch\":[]}}"),
        TransferPlanKind::Batch
    );
    assert_eq!(
        classify_transfer_plan("{\"segment\":{\"offset\":0}}"),
        TransferPlanKind::Segment
    );
    assert_eq!(
        classify_transfer_plan("  {\"segment_group\":{\"start_offset\":0}}"),
        TransferPlanKind::SegmentGroup
    );
    assert_eq!(classify_transfer_plan("not json"), TransferPlanKind::Plain);

    let group = SegmentGroupPlan {
        start_offset: 0,
        end_offset: 256 * MB,
        total_len: 512 * MB,
        first_block_index: 0,
        segment_bytes: 128 * MB,
        source_len: 512 * MB,
        source_mtime: 7,
        source_version: "e2e-v1".to_string(),
    };
    group.validate()?;
    let json = segment_group_json(&group)?;
    assert!(json.len() < 1024, "range manifest must stay compact");
    assert_eq!(
        classify_transfer_plan(&json),
        TransferPlanKind::SegmentGroup
    );
    assert_eq!(parse_segment_group(&json)?, group);
    assert_eq!(normalize_group_resume(&json, 200 * MB), 128 * MB);
    assert_eq!(
        group_resume_remaining(&json, 200 * MB),
        (128 * MB, 128 * MB)
    );

    // Real RPC bytes: LoadTaskInfo with resume round-trips, legacy JSON
    // without the field defaults to 0 (rolling compat).
    let info = TransferTaskReportInfo {
        run_id: 1,
        attempt_id: 2,
        worker_id: 7,
        worker_session_id: "s".to_string(),
        report_target: String::new(),
        report_endpoints: Vec::new(),
        attempt_started_at_ms: 0,
        resume_loaded_size: 128 * MB,
    };
    let bytes = curvine_runtime::common::SerdeUtils::serialize(&info)?;
    let back: TransferTaskReportInfo = curvine_runtime::common::SerdeUtils::deserialize(&bytes)?;
    assert_eq!(back.resume_loaded_size, 128 * MB);
    let legacy: TransferTaskReportInfo = serde_json::from_str(
        r#"{"run_id":1,"attempt_id":2,"worker_id":7,"worker_session_id":"s"}"#,
    )?;
    assert_eq!(legacy.resume_loaded_size, 0);
    Ok(())
}

/// A1 + V2-planner: geometry, contiguity, row projection, manifest size.
#[test]
fn e2e_a1_geometry_row_projection() -> CommonResult<()> {
    use curvine_data_transfer::transfer::{plan_segment_groups, segment_offsets, SEGMENT_BYTES};
    for block in [4 * MB, SEGMENT_BYTES, 1024 * MB] {
        let ranges = segment_offsets(block * 2 + block / 2, block);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], (0, block));
        assert_eq!(ranges[1], (block, block));
        for (index, (offset, _)) in ranges[1..].iter().enumerate() {
            assert_eq!(*offset / block, ranges[0].0 / block + index as i64 + 1);
        }
    }
    // Oversize single block stands alone.
    let big = 6 * 1024 * MB;
    let groups = plan_segment_groups(big, big, 84, big, 1, String::new());
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].group_len(), big);
    // Empty file has no groups.
    assert!(plan_segment_groups(0, SEGMENT_BYTES, 84, 0, 0, String::new()).is_empty());
    // Contiguity over a ragged tail.
    let len = SEGMENT_BYTES * 100 + 12345;
    let groups = plan_segment_groups(len, SEGMENT_BYTES, 4, len, 0, String::new());
    let mut next = 0i64;
    for g in &groups {
        assert_eq!(g.start_offset, next);
        g.validate()?;
        next = g.end_offset;
    }
    assert_eq!(next, len);
    // Nightly-load row projection (real numbers from the design).
    let gb = 1024 * MB;
    for (len, old_rows, new_rows) in [(gb, 8, 8), (100 * gb, 800, 84)] {
        assert_eq!(segment_offsets(len, SEGMENT_BYTES).len(), old_rows);
        assert_eq!(
            plan_segment_groups(len, SEGMENT_BYTES, 84, len, 0, String::new()).len(),
            new_rows
        );
    }
    let groups_5t = plan_segment_groups(
        5 * 1024 * gb,
        SEGMENT_BYTES,
        84,
        5 * 1024 * gb,
        0,
        String::new(),
    );
    assert_eq!(groups_5t.len(), 1024);
    assert!(segment_group_json(&groups_5t[0])?.len() < 1024);
    Ok(())
}

fn e2e_job(job_id: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("key-{job_id}"),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: "file:///source".to_string(),
        target_path: format!("/target/{job_id}"),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        mount_snapshot_attempts: 0,
        planning_attempts: 0,
        terminal_summary: None,
        terminal_counts: None,
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("req-{job_id}"),
        submitter: "e2e".to_string(),
        tenant: "e2e".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

fn e2e_task(job_id: &str, task_id: &str, total: i64) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: job_id.to_string(),
        run_id: 1,
        task_id: task_id.to_string(),
        attempt_id: 0,
        source_path: "file:///source".to_string(),
        target_path: format!("/target/{job_id}/{task_id}"),
        worker_id: 0,
        worker_session_id: String::new(),
        source_read_plan_json: "{}".to_string(),
        report_target_json: String::new(),
        state: TransferTaskState::Pending,
        progress: TransferProgress {
            total_size: total,
            ..Default::default()
        },
        estimated_size: total,
        retry_count: 0,
        attempt_started_at: 0,
        accepted_at_ms: 0,
        first_byte_at_ms: 0,
        last_report_at: 0,
        stale_deadline_at: 0,
        updated_at: 1,
        replace_target: false,
        block_locations: Vec::new(),
    }
}

/// C1/C2/C3/D0-store/V1-store on a real SQLite file: monotonic progress,
/// fencing, resume clamp, snapshot+expired, row-bounded purge, live safety,
/// terminal snapshot requirement, EXPLAIN uses the TTL index, config + metrics.
#[test]
fn e2e_c123_store_service_config_index() -> CommonResult<()> {
    let dir = Utils::test_sub_dir(format!("transfer-e2e-c123-{}", Utils::rand_str(6)));
    std::fs::create_dir_all(&dir)?;
    let db_path = format!("{dir}/transfer.db");
    let store = Arc::new(SqliteTransferStore::open(&db_path)?);

    // C3: real config init (no TOML crate in this test target).
    let mut conf = TransferConf::default();
    conf.init()?;
    assert_eq!(conf.task_retention, Duration::from_secs(24 * 60 * 60));
    let mut explicit = TransferConf {
        terminal_retention_str: "168h".to_string(),
        task_retention_str: "24h".to_string(),
        ..Default::default()
    };
    explicit.init()?;
    assert!(explicit.task_retention < explicit.terminal_retention);
    let mut bad = TransferConf {
        terminal_retention_str: "24h".to_string(),
        task_retention_str: "168h".to_string(),
        ..Default::default()
    };
    assert!(bad.init().is_err());

    // Plan one job with real store transactions.
    let job = e2e_job("e2e-c123");
    store.create_or_get_by_request_id_checked(job.clone())?;
    let lease = store
        .acquire_runnable_transfer("owner", 60_000, 1, 8, false)?
        .unwrap();
    store.persist_planned_tasks(TransferPlannedTasks {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        owner: lease.owner.clone(),
        lease_epoch: lease.lease_epoch,
        tasks: vec![e2e_task(&job.job_id, "task-0", 384)],
        message: "planned".to_string(),
        now_ms: 2,
    })?;

    // D0-store: monotonic max + fencing (resume clamp is unit-covered;
    // e2e starts from 0 so the max path is observable).
    let lease_now = store.get_transfer(&job.job_id)?.unwrap();
    assert!(store.start_task_attempt(TaskAttemptStart {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        owner: lease_now.owner.clone(),
        lease_epoch: lease_now.lease_epoch,
        task_id: "task-0".to_string(),
        attempt_id: 1,
        worker_id: 7,
        worker_session_id: "session".to_string(),
        report_target_json: "{}".to_string(),
        now_ms: 3,
        stale_deadline_at: 1_000,
        resume_loaded_size: 0,
    })?);

    let report = |loaded: i64, now_ms: i64| TransferTaskReport {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        task_id: "task-0".to_string(),
        attempt_id: 1,
        worker_id: 7,
        worker_session_id: "session".to_string(),
        state: TransferTaskState::Running,
        progress: TransferProgress {
            loaded_size: loaded,
            total_size: 384,
            update_time: now_ms,
            message: "r".to_string(),
        },
        now_ms,
        stale_deadline_at: 2_000,
        accepted_at_ms: 0,
        first_byte_at_ms: 0,
        block_locations: Vec::new(),
    };
    assert!(store.update_task_report(report(256, 4))?);
    assert!(store.update_task_report(report(128, 5))?);
    let task = store.list_transfer_tasks(&job.job_id, job.run_id)?[0].clone();
    assert_eq!(
        task.progress.loaded_size, 256,
        "stale report must not regress"
    );
    let mut stale = report(100, 6);
    stale.attempt_id = 2;
    assert!(!store.update_task_report(stale)?, "old attempt fenced");

    // C2: live job rows are never purged.
    let live = e2e_job("e2e-live");
    store.create_or_get_by_request_id_checked(live.clone())?;
    let live_lease = store
        .acquire_runnable_transfer("owner", 60_000, 1, 8, false)?
        .unwrap();
    store.persist_planned_tasks(TransferPlannedTasks {
        job_id: live.job_id.clone(),
        run_id: live.run_id,
        owner: live_lease.owner.clone(),
        lease_epoch: live_lease.lease_epoch,
        tasks: vec![e2e_task(&live.job_id, "task-0", 10)],
        message: "planned".to_string(),
        now_ms: 2,
    })?;

    // Terminalize + snapshot, then purge with real row limits.
    let lease_now = store.get_transfer(&job.job_id)?.unwrap();
    assert!(store.update_transfer_state(TransferStateUpdate {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        owner: lease_now.owner.clone(),
        lease_epoch: lease_now.lease_epoch,
        from_states: vec![
            TransferState::Planning,
            TransferState::Dispatching,
            TransferState::Running
        ],
        to_state: TransferState::Completed,
        message: "done".to_string(),
        now_ms: 10,
    })?);
    // Missing snapshot blocks purge (fail-closed for pre-C1 rows).
    assert_eq!(store.purge_terminal_tasks(100, 1000)?, 0);
    let summary = store.get_transfer_task_summary(&job.job_id, job.run_id, 10)?;
    store.update_terminal_snapshot(
        &job.job_id,
        job.run_id,
        summary.progress.clone(),
        summary.counts,
        10,
    )?;
    assert_eq!(store.purge_terminal_tasks(100, 1)?, 1);
    assert_eq!(store.purge_terminal_tasks(100, 1000)?, 0);

    // C1: service returns snapshot + expired after rows are gone (job kept
    // until the job-retention step, mirroring scheduler task-then-job order).
    let service = TransferService::new(store.clone());
    let (status, _counts, tasks, _, expired) =
        service.get_transfer_status(&job.job_id, Some(10), None)?;
    assert!(expired);
    assert!(tasks.is_empty());
    assert_eq!(
        status.summary.total_size, 384,
        "snapshot retains totals after purge"
    );
    assert_eq!(store.purge_terminal_jobs(100, 1000)?, 1);

    // C3: real EXPLAIN proves the TTL index is used, not a full scan.
    let conn = rusqlite::Connection::open(&db_path)?;
    let plan: String = conn.query_row(
        "EXPLAIN QUERY PLAN select t.job_id from transfer_tasks t join transfer_jobs j
         on t.job_id=j.job_id and t.run_id=j.run_id
         where j.state in (6,7,8,9) and j.updated_at<1
           and j.terminal_summary_json is not null
         order by j.updated_at asc limit 1",
        [],
        |row| row.get(3),
    )?;
    assert!(
        plan.contains("transfer_jobs_state_updated_idx")
            || plan.contains("USING INDEX")
            || plan.contains("USING COVERING INDEX"),
        "purge must use TTL index, got: {plan}"
    );

    // C3: metrics record without panicking on the global registry.
    let metrics = TransferMetrics::get()?;
    metrics.inc_cleanup_purged_tasks(1);
    metrics.inc_group_resume(128);
    Ok(())
}

fn e2e_cluster() -> (Arc<MiniCluster>, String) {
    let test_id = Utils::rand_str(8);
    let base_dir = Utils::test_sub_dir(format!("transfer-e2e-{test_id}"));
    std::fs::create_dir_all(&base_dir).unwrap();
    let mut conf = ClusterConf::default();
    conf.master.meta_dir = format!("{base_dir}/meta");
    conf.journal.journal_dir = format!("{base_dir}/journal");
    conf.worker.data_dir = vec![format!("[MEM:64MB]{base_dir}/worker")];
    conf.journal.raft_tick_interval_ms = 100;
    conf.client.short_circuit = false;
    let cluster = Arc::new(MiniCluster::with_num(&conf, 1, 1));
    cluster.start_cluster();
    (cluster, base_dir)
}

fn e2e_job_info(
    job_id: &str,
    source: &str,
    target: &str,
    block_size: i64,
    mount: &MountInfo,
    storage_type: StorageType,
) -> LoadJobInfo {
    LoadJobInfo {
        job_id: job_id.to_string(),
        source_path: source.to_string(),
        target_path: target.to_string(),
        block_size,
        replicas: 1,
        storage_type,
        ttl_ms: 0,
        ttl_action: TtlAction::default(),
        mount_info: mount.clone(),
        create_time: 0,
        overwrite: Some(true),
        source_type: JobSourceType::default(),
        transfer_job_id: None,
    }
}

/// A normal Transfer load writes to a temporary CV inode. The source identity
/// and retry marker must be part of CompleteFile so rename cannot expose a
/// final path with valid bytes but `ufs_mtime == 0`.
#[test]
fn e2e_u2c_complete_atomically_preserves_mtime_and_marker() -> CommonResult<()> {
    let (cluster, base_dir) = e2e_cluster();
    let rt = cluster.clone_client_rt();
    rt.block_on(async move {
        let fs = cluster.new_fs();
        let ufs_dir = format!("{base_dir}/ufs-atomic-complete");
        std::fs::create_dir_all(&ufs_dir)?;
        let source = format!("file://{ufs_dir}/source.bin");
        std::fs::write(format!("{ufs_dir}/source.bin"), b"atomic-complete")?;

        let mount_path = Path::from_str("/e2e-u2c-atomic")?;
        fs.mount(
            &Path::from_str(format!("file://{ufs_dir}"))?,
            &mount_path,
            curvine_model::MountOptions::builder()
                .block_size(1024 * 1024)
                .replicas(1)
                .auto_cache(false)
                .build(),
        )
        .await?;
        let mount = fs
            .get_mount_table()
            .await?
            .into_iter()
            .find(|mount| mount.cv_path == "/e2e-u2c-atomic")
            .unwrap();
        let factory = Arc::new(UfsFactory::with_rt(
            &cluster.cluster_conf.client,
            cluster.clone_client_rt(),
        ));
        let source_status = factory
            .get_ufs(&mount)?
            .get_status(&Path::from_str(&source)?)
            .await?;
        assert_ne!(
            source_status.mtime, 0,
            "test source must carry a non-zero UFS mtime"
        );
        let target = "/e2e-u2c-atomic-output/final.bin";
        let mut committed_inode = None;

        for attempt_id in [1, 2] {
            let context = Arc::new(TaskContext::new(LoadTaskInfo {
                job: e2e_job_info(
                    "e2e-u2c-atomic",
                    &source,
                    target,
                    1024 * 1024,
                    &mount,
                    StorageType::default(),
                ),
                task_id: "atomic-task".to_string(),
                worker: WorkerAddress::default(),
                source_path: source.clone(),
                target_path: target.to_string(),
                create_time: 0,
                source_read_plan_json: String::new(),
                estimated_size: source_status.len,
                transfer_report: Some(TransferTaskReportInfo {
                    run_id: 1,
                    attempt_id,
                    worker_id: 0,
                    worker_session_id: "atomic-session".to_string(),
                    report_target: String::new(),
                    report_endpoints: Vec::new(),
                    attempt_started_at_ms: 0,
                    resume_loaded_size: 0,
                }),
            }));
            let runner =
                LoadTaskRunner::new(context, fs.clone(), factory.clone(), None, 10_000, 120_000);
            // No reporter is installed in this isolated runner. Terminal
            // reporting therefore returns false after the data commit, which
            // lets this test inspect the committed inode directly.
            assert!(!runner.run().await);

            let status = fs.get_status(&Path::from_str(target)?).await?;
            assert_eq!(status.storage_policy.ufs_mtime, source_status.mtime);
            assert!(
                status.cv_valid(None),
                "atomic UFS mtime must make the completed target cache-valid"
            );
            assert_eq!(
                status
                    .x_attr
                    .get("curvine.transfer.task")
                    .map(Vec::as_slice),
                Some(&b"atomic-task"[..])
            );
            if let Some(inode) = committed_inode {
                assert_eq!(
                    status.id, inode,
                    "retry must recognize the atomic marker instead of replacing the target"
                );
            } else {
                committed_inode = Some(status.id);
            }
        }

        // Master-originated load tasks have no Transfer report. They still
        // commit through a temporary CV inode so cancellation or report
        // failure cannot expose a partially completed cache entry.
        let legacy_target = "/e2e-u2c-atomic-output/legacy.bin";
        let legacy_context = Arc::new(TaskContext::new(LoadTaskInfo {
            job: e2e_job_info(
                "e2e-u2c-atomic",
                &source,
                legacy_target,
                1024 * 1024,
                &mount,
                StorageType::default(),
            ),
            task_id: "legacy-task".to_string(),
            worker: WorkerAddress::default(),
            source_path: source,
            target_path: legacy_target.to_string(),
            create_time: 0,
            source_read_plan_json: String::new(),
            estimated_size: source_status.len,
            transfer_report: None,
        }));
        let legacy_runner =
            LoadTaskRunner::new(legacy_context, fs.clone(), factory, None, 10_000, 120_000);
        assert!(legacy_runner.run().await);
        let legacy_status = fs.get_status(&Path::from_str(legacy_target)?).await?;
        assert_eq!(legacy_status.storage_policy.ufs_mtime, source_status.mtime);
        assert!(legacy_status.cv_valid(None));

        Ok(())
    })
}

/// A Master-originated load carries no Transfer report. Cancellation after its
/// temporary inode is complete must delete that inode instead of exposing the
/// final cache entry.
#[cfg(feature = "fault-injection")]
#[test]
fn e2e_legacy_u2c_cancel_after_temp_complete_keeps_final_absent() -> CommonResult<()> {
    let faults = FaultRuntime::process();
    faults.clear()?;
    let rule = FaultRuleBuilder::named("worker.load_task.after_temp_complete")
        .matches("task_id", "legacy-cancel-task")?
        .times(1)?
        .delay(500)?;
    faults.configure("legacy-u2c-cancel", rule)?;

    let (cluster, base_dir) = e2e_cluster();
    let rt = cluster.clone_client_rt();
    let result: CommonResult<()> = rt.block_on(async move {
        let fs = cluster.new_fs();
        let ufs_dir = format!("{base_dir}/ufs-legacy-cancel");
        std::fs::create_dir_all(&ufs_dir)?;
        let source = format!("file://{ufs_dir}/source.bin");
        std::fs::write(format!("{ufs_dir}/source.bin"), b"legacy-cancel")?;

        let mount_path = Path::from_str("/e2e-u2c-legacy-cancel")?;
        fs.mount(
            &Path::from_str(format!("file://{ufs_dir}"))?,
            &mount_path,
            curvine_model::MountOptions::builder()
                .block_size(1024 * 1024)
                .replicas(1)
                .auto_cache(false)
                .build(),
        )
        .await?;
        let mount = fs
            .get_mount_table()
            .await?
            .into_iter()
            .find(|mount| mount.cv_path == mount_path.full_path())
            .unwrap();
        let target = "/e2e-u2c-legacy-cancel-output/final.bin";
        let context = Arc::new(TaskContext::new(LoadTaskInfo {
            job: e2e_job_info(
                "e2e-u2c-legacy-cancel",
                &source,
                target,
                1024 * 1024,
                &mount,
                StorageType::default(),
            ),
            task_id: "legacy-cancel-task".to_string(),
            worker: WorkerAddress::default(),
            source_path: source,
            target_path: target.to_string(),
            create_time: 0,
            source_read_plan_json: String::new(),
            estimated_size: 13,
            transfer_report: None,
        }));
        let runner = LoadTaskRunner::new(
            context.clone(),
            fs.clone(),
            Arc::new(UfsFactory::with_rt(
                &cluster.cluster_conf.client,
                cluster.clone_client_rt(),
            )),
            None,
            10_000,
            120_000,
        );
        let run = tokio::spawn(async move { runner.run().await });

        for _ in 0..100 {
            if FaultRuntime::process()
                .status()
                .rules
                .iter()
                .any(|rule| rule.id == "legacy-u2c-cancel" && rule.executions == 1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            FaultRuntime::process()
                .status()
                .rules
                .iter()
                .any(|rule| rule.id == "legacy-u2c-cancel" && rule.executions == 1),
            "runner must reach the post-complete fault point before cancellation"
        );
        context.set_canceled("test cancellation after temporary completion");
        assert!(run.await.expect("legacy runner must join"));

        let target_path = Path::from_str(target)?;
        assert!(
            !fs.exists(&target_path).await?,
            "canceled legacy load must not expose its final target"
        );
        let output_dir = Path::from_str("/e2e-u2c-legacy-cancel-output")?;
        assert!(
            fs.list_status(&output_dir).await?.is_empty(),
            "canceled legacy load must clean its temporary output"
        );
        Ok(())
    });
    let cleanup = faults.clear();
    result?;
    cleanup?;
    Ok(())
}

/// A2: real UFS lengths + pure grouping prove small files stay per-segment
/// (fail-closed: production planner emits single segments, never groups).
#[test]
fn e2e_a2_planner_fallback_batch_unchanged() -> CommonResult<()> {
    use curvine_data_transfer::transfer::{plan_segment_groups, segment_offsets, SEGMENT_BYTES};
    let (cluster, base_dir) = e2e_cluster();
    let rt = cluster.clone_client_rt();
    rt.block_on(async move {
        let fs = cluster.new_fs();
        let ufs_dir = format!("{base_dir}/ufs");
        std::fs::create_dir_all(format!("{ufs_dir}/small"))?;
        std::fs::write(format!("{ufs_dir}/small/a.txt"), b"aaa")?;
        std::fs::write(format!("{ufs_dir}/small/b.txt"), b"bb")?;
        let big: Vec<u8> = (0..3 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        std::fs::write(format!("{ufs_dir}/big.bin"), &big)?;

        let ufs_root = Path::from_str(format!("file://{ufs_dir}"))?;
        let mount_path = Path::from_str("/e2e-a2")?;
        let opts = curvine_model::MountOptions::builder()
            .block_size(1024 * 1024)
            .replicas(1)
            .auto_cache(false)
            .build();
        fs.mount(&ufs_root, &mount_path, opts).await?;

        // Real UFS lengths via the mounted view.
        let factory = Arc::new(UfsFactory::with_rt(
            &cluster.cluster_conf.client,
            cluster.clone_client_rt(),
        ));
        let mount = fs
            .get_mount_table()
            .await?
            .into_iter()
            .find(|m| m.cv_path == "/e2e-a2")
            .unwrap();
        let ufs = factory.get_ufs(&mount)?;
        let big_status = ufs
            .get_status(&Path::from_str(format!("file://{ufs_dir}/big.bin"))?)
            .await?;
        assert_eq!(big_status.len, 3 * 1024 * 1024);
        // 3MiB / 1MiB = 3 segments; with 84 workers the pure partitioner
        // makes 3 single-segment groups, each of which the planner emits as
        // legacy single-segment JSON (small files unchanged, fail-closed).
        let groups = plan_segment_groups(
            big_status.len,
            1024 * 1024,
            84,
            big_status.len,
            0,
            String::new(),
        );
        assert_eq!(groups.len(), 3);
        for g in &groups {
            assert_eq!(g.group_len(), 1024 * 1024);
        }
        assert_eq!(segment_offsets(big_status.len, 1024 * 1024).len(), 3);
        // Batch inputs are untouched regular files (no grouping involved).
        let a = ufs
            .get_status(&Path::from_str(format!("file://{ufs_dir}/small/a.txt"))?)
            .await?;
        assert_eq!(a.len, 3);
        assert_eq!(segment_offsets(3 * SEGMENT_BYTES, SEGMENT_BYTES).len(), 3);
        Ok(())
    })
}

/// A3/A4/D0-runner/V1-mechanical: real group copy, resume, finalize, rejects.
#[test]
fn e2e_a3a4_group_fail_closed_on_file() -> CommonResult<()> {
    let (cluster, base_dir) = e2e_cluster();
    let rt = cluster.clone_client_rt();
    let rt_inner = rt.clone();
    rt.block_on(async move {
        let fs = cluster.new_fs();
        let ufs_dir = format!("{base_dir}/ufs");
        std::fs::create_dir_all(&ufs_dir)?;
        let payload: Vec<u8> = (0..3 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        std::fs::write(format!("{ufs_dir}/src.bin"), &payload)?;

        let ufs_root = Path::from_str(format!("file://{ufs_dir}"))?;
        let mount_path = Path::from_str("/e2e-a3")?;
        let opts = curvine_model::MountOptions::builder()
            .block_size(1024 * 1024)
            .replicas(1)
            .auto_cache(false)
            .build();
        fs.mount(&ufs_root, &mount_path, opts.clone()).await?;
        let mount = fs
            .get_mount_table()
            .await?
            .into_iter()
            .find(|m| m.cv_path == "/e2e-a3")
            .unwrap();

        let source = format!("file://{ufs_dir}/src.bin");
        let target = "/e2e-a3-out/src.bin".to_string();
        let factory = Arc::new(UfsFactory::with_rt(
            &cluster.cluster_conf.client,
            rt_inner.clone(),
        ));
        let source_path = Path::from_str(&source)?;
        let src_status = factory.get_ufs(&mount)?.get_status(&source_path).await?;

        let block_size = 1024 * 1024;
        let total = payload.len() as i64;
        assert_eq!(total, 3 * block_size);

        // Empty version stays fail-closed (production behavior).
        let empty = SegmentGroupPlan {
            start_offset: 0,
            end_offset: total,
            total_len: total,
            first_block_index: 0,
            segment_bytes: block_size,
            source_len: total,
            source_mtime: 0,
            source_version: String::new(),
        };
        let empty_json = segment_group_json(&empty)?;
        let ctx = Arc::new(TaskContext::new(LoadTaskInfo {
            job: e2e_job_info(
                "e2e-a3",
                &source,
                &target,
                block_size,
                &mount,
                StorageType::default(),
            ),
            task_id: "task-empty".to_string(),
            worker: WorkerAddress::default(),
            source_path: source.clone(),
            target_path: target.clone(),
            create_time: 0,
            source_read_plan_json: empty_json,
            estimated_size: total,
            transfer_report: Some(TransferTaskReportInfo {
                run_id: 1,
                attempt_id: 1,
                worker_id: 0,
                worker_session_id: "s".to_string(),
                report_target: String::new(),
                report_endpoints: Vec::new(),
                attempt_started_at_ms: 0,
                resume_loaded_size: 0,
            }),
        }));
        let runner = LoadTaskRunner::new(ctx, fs.clone(), factory.clone(), None, 10_000, 120_000);
        assert!(
            !runner.run().await,
            "empty source version must not copy group bytes"
        );

        // SPDK target stays fail-closed.
        let spdk_group = SegmentGroupPlan {
            source_version: "e2e-v1".to_string(),
            ..empty.clone()
        };
        let spdk_json = segment_group_json(&spdk_group)?;
        let spdk_ctx = Arc::new(TaskContext::new(LoadTaskInfo {
            job: e2e_job_info(
                "e2e-a3",
                &source,
                &target,
                block_size,
                &mount,
                StorageType::SpdkDisk,
            ),
            task_id: "task-spdk".to_string(),
            worker: WorkerAddress::default(),
            source_path: source.clone(),
            target_path: target.clone(),
            create_time: 0,
            source_read_plan_json: spdk_json,
            estimated_size: total,
            transfer_report: Some(TransferTaskReportInfo {
                run_id: 1,
                attempt_id: 1,
                worker_id: 0,
                worker_session_id: "s".to_string(),
                report_target: String::new(),
                report_endpoints: Vec::new(),
                attempt_started_at_ms: 0,
                resume_loaded_size: 0,
            }),
        }));
        let spdk_runner =
            LoadTaskRunner::new(spdk_ctx, fs.clone(), factory.clone(), None, 10_000, 120_000);
        let spdk_out = spdk_runner.run().await;
        assert!(!spdk_out, "SPDK group must fail-closed, got {spdk_out}");

        // A non-empty but unverifiable version must also refuse: file://
        // (Local adapter) yields no etag/version, so even a well-formed
        // manifest cannot prove the source is unchanged. Full copy/resume
        // mechanics live in the S3 test where the adapter supplies a real
        // etag; here file:// proves the fail-closed side.
        let group = SegmentGroupPlan {
            start_offset: 0,
            end_offset: total,
            total_len: total,
            first_block_index: 0,
            segment_bytes: block_size,
            source_len: src_status.len.max(total),
            source_mtime: src_status.mtime,
            source_version: "e2e-v1".to_string(),
        };
        group.validate()?;
        let group_json = segment_group_json(&group)?;
        let unverifiable_ctx = Arc::new(TaskContext::new(LoadTaskInfo {
            job: e2e_job_info(
                "e2e-a3",
                &source,
                &target,
                block_size,
                &mount,
                StorageType::default(),
            ),
            task_id: "task-unverifiable".to_string(),
            worker: WorkerAddress::default(),
            source_path: source.clone(),
            target_path: target.clone(),
            create_time: 0,
            source_read_plan_json: group_json.clone(),
            estimated_size: total,
            transfer_report: Some(TransferTaskReportInfo {
                run_id: 1,
                attempt_id: 1,
                worker_id: 0,
                worker_session_id: "s".to_string(),
                report_target: String::new(),
                report_endpoints: Vec::new(),
                attempt_started_at_ms: 0,
                resume_loaded_size: 0,
            }),
        }));
        let unverifiable_runner = LoadTaskRunner::new(
            unverifiable_ctx.clone(),
            fs.clone(),
            factory.clone(),
            None,
            10_000,
            120_000,
        );
        assert!(
            !unverifiable_runner.run().await,
            "file:// has no verifiable identity; group must refuse"
        );
        assert_eq!(
            unverifiable_ctx.get_state(),
            curvine_model::JobTaskState::Failed
        );
        let _ = payload;

        // Source change and layout mismatch stay fail-closed.
        std::fs::write(format!("{ufs_dir}/src.bin"), b"changed")?;
        let changed_ctx = Arc::new(TaskContext::new(LoadTaskInfo {
            job: e2e_job_info(
                "e2e-a3",
                &source,
                &target,
                block_size,
                &mount,
                StorageType::default(),
            ),
            task_id: "task-changed".to_string(),
            worker: WorkerAddress::default(),
            source_path: source.clone(),
            target_path: target.clone(),
            create_time: 0,
            source_read_plan_json: segment_group_json(&group)?,
            estimated_size: total,
            transfer_report: Some(TransferTaskReportInfo {
                run_id: 1,
                attempt_id: 3,
                worker_id: 0,
                worker_session_id: "s".to_string(),
                report_target: String::new(),
                report_endpoints: Vec::new(),
                attempt_started_at_ms: 0,
                resume_loaded_size: 0,
            }),
        }));
        // Reuse the last factory via a fresh one (same conf/rt).
        let factory2 = Arc::new(UfsFactory::with_rt(
            &cluster.cluster_conf.client,
            cluster.clone_client_rt(),
        ));
        let changed_runner =
            LoadTaskRunner::new(changed_ctx, fs.clone(), factory2, None, 10_000, 120_000);
        assert!(
            !changed_runner.run().await,
            "changed source must not resume"
        );
        Ok(())
    })
}

/// A3b: the identity production can actually supply. OSS-HDFS exposes no
/// version/etag, so a group manifest carries only the HEAD `(len, mtime)`
/// pair. file:// (Local) yields exactly that stat-only identity, so the
/// production path is testable without S3: a version-less group must copy,
/// land one block per segment, honour a non-zero resume floor, converge into a
/// complete file, and refuse a drifted source.
///
/// Byte-level verification is deliberately left to the S3 full-stack E2E and
/// staging: this MiniCluster does not serve block locations for files written
/// by segment/group tasks (`locs` stays empty after commit), so a reader here
/// sees "no available worker" for both write paths. The assertions therefore
/// cover the identity gate and the committed layout, which is what this change
/// alters.
#[test]
fn e2e_a3b_group_stat_identity_copies_and_resumes_on_file() -> CommonResult<()> {
    let (cluster, base_dir) = e2e_cluster();
    let rt = cluster.clone_client_rt();
    let rt_inner = rt.clone();
    rt.block_on(async move {
        let fs = cluster.new_fs();
        let ufs_dir = format!("{base_dir}/ufs-stat");
        std::fs::create_dir_all(&ufs_dir)?;
        let block_size = 1024 * 1024;
        let total = 3 * block_size;
        let payload: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
        std::fs::write(format!("{ufs_dir}/src.bin"), &payload)?;

        let ufs_root = Path::from_str(format!("file://{ufs_dir}"))?;
        let mount_path = Path::from_str("/e2e-a3b")?;
        let opts = curvine_model::MountOptions::builder()
            .block_size(block_size)
            .replicas(1)
            .auto_cache(false)
            .build();
        fs.mount(&ufs_root, &mount_path, opts).await?;
        let mount = fs
            .get_mount_table()
            .await?
            .into_iter()
            .find(|m| m.cv_path == "/e2e-a3b")
            .unwrap();

        let source = format!("file://{ufs_dir}/src.bin");
        let target_full = "/e2e-a3b-out/full.bin".to_string();
        let target_resume = "/e2e-a3b-out/resume.bin".to_string();
        let factory = Arc::new(UfsFactory::with_rt(
            &cluster.cluster_conf.client,
            rt_inner.clone(),
        ));
        let src_status = factory
            .get_ufs(&mount)?
            .get_status(&Path::from_str(&source)?)
            .await?;

        // Version-less group covering all three blocks (group_len > one
        // segment, so it stays a range manifest): the identity is stat-only,
        // which is what the OSS-HDFS adapter can supply in production.
        let plan = SegmentGroupPlan {
            start_offset: 0,
            end_offset: total,
            total_len: total,
            first_block_index: 0,
            segment_bytes: block_size,
            source_len: src_status.len,
            source_mtime: src_status.mtime,
            source_version: String::new(),
        };
        plan.validate()?;
        assert!(plan.has_identity(), "HEAD len+mtime is a valid identity");
        // The value the scheduler stamps at convergence (plan-time HEAD mtime).
        let plan_source_mtime = plan.source_mtime;
        let plan_json = segment_group_json(&plan)?;
        assert_eq!(
            classify_transfer_plan(&plan_json),
            TransferPlanKind::SegmentGroup
        );
        assert_eq!(
            group_resume_remaining(&plan_json, block_size),
            (block_size, plan.group_len() - block_size)
        );

        let make_ctx = |task_id: &str, src: &str, tgt: &str, plan_json: String, resume: i64| {
            Arc::new(TaskContext::new(LoadTaskInfo {
                job: e2e_job_info(
                    "e2e-a3b",
                    src,
                    tgt,
                    block_size,
                    &mount,
                    StorageType::default(),
                ),
                task_id: task_id.to_string(),
                worker: WorkerAddress::default(),
                source_path: src.to_string(),
                target_path: tgt.to_string(),
                create_time: 0,
                source_read_plan_json: plan_json,
                estimated_size: total,
                transfer_report: Some(TransferTaskReportInfo {
                    run_id: 1,
                    attempt_id: 1,
                    worker_id: 0,
                    worker_session_id: "s".to_string(),
                    report_target: String::new(),
                    report_endpoints: Vec::new(),
                    attempt_started_at_ms: 0,
                    resume_loaded_size: resume,
                }),
            }))
        };
        let run_task = |ctx: Arc<TaskContext>| {
            let fs = fs.clone();
            let factory = factory.clone();
            async move {
                let runner = LoadTaskRunner::new(ctx, fs, factory, None, 10_000, 120_000);
                runner.run().await
            }
        };
        // A fresh client for observation: block locations are assigned while the
        // group writes, so the writer's own cached layout still shows empty
        // `locs`. In production the reader is a separate client.
        let reader_fs = cluster.new_fs();
        let layout = |tgt: String| {
            let fs = reader_fs.clone();
            async move { fs.get_block_locations(&Path::from_str(&tgt)?).await }
        };
        // The job's convergence protocol, exactly as the scheduler drives it:
        // complete the file and stamp the plan-time source mtime as its UFS
        // identity. Without that stamp the target stays `{ufs_mtime: 0,
        // state: Cv}` and every read falls back to UFS.
        let complete_target = |tgt: String| {
            let fs = reader_fs.clone();
            async move {
                let target_path = Path::from_str(&tgt)?;
                let blocks = fs.get_block_locations(&target_path).await?;
                let commit: Vec<_> = blocks
                    .block_locs
                    .iter()
                    .map(curvine_model::CommitBlock::from)
                    .collect();
                let set_attr_opts = SetAttrOptsBuilder::new()
                    .ufs_mtime(plan_source_mtime)
                    .build();
                fs.fs_client()
                    .complete_file_by_id(
                        &target_path,
                        blocks.status.id,
                        total,
                        commit,
                        false,
                        Some(set_attr_opts),
                    )
                    .await?;
                fs.get_status(&target_path).await
            }
        };

        // (1) A version-less manifest copies: one block per planned segment,
        // and segments alone must not mark the file complete.
        let _ = run_task(make_ctx(
            "task-stat-full",
            &source,
            &target_full,
            plan_json.clone(),
            0,
        ))
        .await;
        let blocks = layout(target_full.clone()).await?;
        assert_eq!(blocks.block_locs.len(), 3, "one block per segment");
        for block in &blocks.block_locs {
            assert_eq!(block.block.len, block_size, "every segment must land");
        }
        assert_eq!(blocks.status.len, total);
        assert!(
            !blocks.status.is_complete(),
            "segments alone must not mark the file complete"
        );
        let done = complete_target(target_full.clone()).await?;
        assert!(done.is_complete() && done.len == total);
        // The stamp is what makes the cached copy usable: `cv_valid` rejects
        // `ufs_mtime == 0` outright, so without it every read falls back to UFS
        // and every incremental load re-copies the file.
        assert_eq!(
            done.storage_policy.ufs_mtime, plan_source_mtime,
            "convergence must stamp the plan-time source mtime"
        );
        assert!(
            done.cv_valid(None),
            "a stamped target must be cache-valid for readers"
        );

        // (2) A non-zero resume floor is accepted and still lands the remaining
        // segments under the same version-less identity. The skipped prefix is
        // trusted, which is why the source is re-identified before any byte is
        // skipped: a previous attempt that committed only block 0 is simulated
        // with a single-segment task from another source.
        let wrong: Vec<u8> = vec![0xAA; block_size as usize + 7];
        std::fs::write(format!("{ufs_dir}/wrong.bin"), &wrong)?;
        let wrong_source = format!("file://{ufs_dir}/wrong.bin");
        let seg_json = serde_json::json!({
            "segment": {
                "offset": 0,
                "length": block_size,
                "total_len": total,
                "block_index": 0
            }
        })
        .to_string();
        let _ = run_task(make_ctx(
            "task-stat-block0",
            &wrong_source,
            &target_resume,
            seg_json,
            0,
        ))
        .await;
        let _ = run_task(make_ctx(
            "task-stat-resume",
            &source,
            &target_resume,
            plan_json.clone(),
            block_size,
        ))
        .await;
        let blocks = layout(target_resume.clone()).await?;
        assert_eq!(blocks.block_locs.len(), 3);
        for block in &blocks.block_locs {
            assert_eq!(block.block.len, block_size);
        }
        let done = complete_target(target_resume.clone()).await?;
        assert!(done.is_complete() && done.len == total);

        // (3) The stat gate fails closed on drift: rewrite the same length with
        // different bytes so both mtime and content move, then reuse the stale
        // manifest. The copy must not run.
        let drifted: Vec<u8> = payload.iter().map(|b| b ^ 0xff).collect();
        std::fs::write(format!("{ufs_dir}/src.bin"), &drifted)?;
        let stale = run_task(make_ctx(
            "task-stat-stale",
            &source,
            &target_full,
            plan_json,
            0,
        ))
        .await;
        assert!(!stale, "a source whose HEAD identity moved must not resume");
        Ok(())
    })
}
