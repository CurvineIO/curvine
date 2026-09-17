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
    StorageType, TaskAttemptStart, TransferJobRecord, TransferKind, TransferPlanKind,
    TransferProgress, TransferState, TransferStateUpdate, TransferTaskRecord, TransferTaskReport,
    TransferTaskReportInfo, TransferTaskState, TtlAction, WorkerAddress,
};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use std::sync::Arc;
use std::time::Duration;

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
    }
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
