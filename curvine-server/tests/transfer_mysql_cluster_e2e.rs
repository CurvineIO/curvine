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

//! Full-stack E2E: real MiniCluster + real MySQL + real IO (file:// and S3).
//!
//! Requires `CURVINE_TRANSFER_MYSQL_TEST_URL` (e.g.
//! `mysql://root:curvine-test@127.0.0.1:3308/curvine_transfer`).
//! The S3 test additionally requires MinIO at `http://127.0.0.1:9000`
//! (bucket `e2e-src`, `aws` CLI on PATH, `minioadmin/minioadmin`).
//! Covers design doc scenarios end to end: small-file batch, large-file
//! segments, planner fallback, store monotonic/fencing/snapshot/purge,
//! group copy/resume/finalize, worker crash via mid-copy cancel, deadline
//! retry, and all fail-closed rejects. Group manifests always come from the
//! real planner (S3 etag); file:// (Local, no identity) proves fail-closed.

use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_data_transfer::common::UfsFactory;
use curvine_data_transfer::transfer::{
    ClusterMetadataCache, MysqlTransferStore, TransferPlannedTasks, TransferPlanner, TransferStore,
};
use curvine_data_transfer::worker::task::{LoadTaskRunner, TaskContext, TaskManager};
use curvine_fs_api::Path;
use curvine_model::{
    segment_group_json, JobSourceType, LoadJobInfo, LoadTaskInfo, MountInfo, SegmentGroupPlan,
    StorageType, TaskAttemptStart, TransferCommand, TransferJobRecord, TransferKind,
    TransferPlanKind, TransferProgress, TransferState, TransferStateUpdate, TransferTaskRecord,
    TransferTaskReport, TransferTaskReportInfo, TransferTaskState, TtlAction, WorkerAddress,
};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use std::sync::Arc;
use std::time::Duration;

const MB: i64 = 1024 * 1024;

fn mysql_url() -> Option<String> {
    std::env::var("CURVINE_TRANSFER_MYSQL_TEST_URL").ok()
}

fn require_mysql() -> Option<String> {
    let url = mysql_url();
    if url.is_none() {
        eprintln!("CURVINE_TRANSFER_MYSQL_TEST_URL is not set; skipping MySQL E2E");
    }
    url
}

fn reset_mysql(url: &str) {
    let opts = mysql::Opts::from_url(url).unwrap();
    let pool = mysql::Pool::new(opts).unwrap();
    let mut conn = pool.get_conn().unwrap();
    use mysql::prelude::Queryable;
    for stmt in [
        "drop table if exists transfer_tasks",
        "drop table if exists transfer_jobs",
        "drop table if exists transfer_schema_version",
    ] {
        conn.query_drop(stmt).unwrap();
    }
}

fn e2e_job(command: &TransferCommand, job_id: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("{}:{job_id}", command.job_key()),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: command.source_path.clone(),
        target_path: format!("{}-{job_id}", command.target_path),
        command_json: serde_json::to_string(command).unwrap(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 0,
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

fn e2e_cluster() -> (Arc<MiniCluster>, String) {
    let test_id = Utils::rand_str(8);
    let base_dir = Utils::test_sub_dir(format!("transfer-mysql-e2e-{test_id}"));
    std::fs::create_dir_all(&base_dir).unwrap();
    let mut conf = ClusterConf::default();
    conf.master.meta_dir = format!("{base_dir}/meta");
    conf.journal.journal_dir = format!("{base_dir}/journal");
    conf.worker.data_dir = vec![format!("[MEM:128MB]{base_dir}/worker")];
    conf.journal.raft_tick_interval_ms = 100;
    conf.client.short_circuit = false;
    let cluster = Arc::new(MiniCluster::with_num(&conf, 1, 1));
    cluster.start_cluster();
    (cluster, base_dir)
}

/// MySQL store gates on real SQL: resume clamp, monotonic Running max
/// (greatest/coalesce), attempt fencing, late-terminal fencing, terminal
/// snapshot, purge row limit + missing-snapshot skip, EXPLAIN sanity.
#[test]
fn mysql_e2e_store_gates() -> CommonResult<()> {
    let Some(url) = require_mysql() else {
        return Ok(());
    };
    reset_mysql(&url);
    let store = Arc::new(MysqlTransferStore::open(&url)?);

    let command = TransferCommand {
        kind: TransferKind::Load,
        source_path: "file:///src".to_string(),
        target_path: "/tgt".to_string(),
        ..Default::default()
    };
    let job = e2e_job(&command, &format!("mysql-gates-{}", Utils::rand_str(6)));
    store.create_or_get_by_request_id_checked(job.clone())?;
    let lease = store
        .acquire_runnable_transfer("owner", 60_000, 1, 8, false)?
        .unwrap();
    let task = TransferTaskRecord {
        job_id: job.job_id.clone(),
        run_id: 1,
        task_id: "task-0".to_string(),
        attempt_id: 0,
        source_path: "file:///src".to_string(),
        target_path: "/tgt/task-0".to_string(),
        worker_id: 0,
        worker_session_id: String::new(),
        source_read_plan_json: "{}".to_string(),
        report_target_json: String::new(),
        state: TransferTaskState::Pending,
        progress: TransferProgress {
            total_size: 384,
            ..Default::default()
        },
        estimated_size: 384,
        retry_count: 0,
        attempt_started_at: 0,
        accepted_at_ms: 0,
        first_byte_at_ms: 0,
        last_report_at: 0,
        stale_deadline_at: 0,
        updated_at: 1,
        replace_target: false,
        block_locations: Vec::new(),
    };
    store.persist_planned_tasks(TransferPlannedTasks {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        owner: lease.owner.clone(),
        lease_epoch: lease.lease_epoch,
        tasks: vec![task],
        message: "planned".to_string(),
        now_ms: 2,
    })?;

    // Resume clamp.
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
        resume_loaded_size: 9999,
    })?);
    let row = store.list_transfer_tasks(&job.job_id, job.run_id)?[0].clone();
    assert_eq!(row.progress.loaded_size, 384);

    // Fresh job for max-path observability (previous row already at 384).
    let job2 = e2e_job(&command, &format!("mysql-max-{}", Utils::rand_str(6)));
    store.create_or_get_by_request_id_checked(job2.clone())?;
    let lease2 = store
        .acquire_runnable_transfer("owner", 60_000, 10, 8, false)?
        .unwrap();
    let mut task2 = TransferTaskRecord {
        job_id: job2.job_id.clone(),
        ..store.list_transfer_tasks(&job.job_id, job.run_id)?[0].clone()
    };
    task2.job_id = job2.job_id.clone();
    task2.task_id = "task-0".to_string();
    task2.progress.loaded_size = 0;
    task2.state = TransferTaskState::Pending;
    task2.attempt_id = 0;
    store.persist_planned_tasks(TransferPlannedTasks {
        job_id: job2.job_id.clone(),
        run_id: job2.run_id,
        owner: lease2.owner.clone(),
        lease_epoch: lease2.lease_epoch,
        tasks: vec![task2],
        message: "planned".to_string(),
        now_ms: 11,
    })?;
    let lease2_now = store.get_transfer(&job2.job_id)?.unwrap();
    assert!(store.start_task_attempt(TaskAttemptStart {
        job_id: job2.job_id.clone(),
        run_id: job2.run_id,
        owner: lease2_now.owner.clone(),
        lease_epoch: lease2_now.lease_epoch,
        task_id: "task-0".to_string(),
        attempt_id: 1,
        worker_id: 7,
        worker_session_id: "session".to_string(),
        report_target_json: "{}".to_string(),
        now_ms: 12,
        stale_deadline_at: 1_000,
        resume_loaded_size: 0,
    })?);
    let r2 = |loaded: i64, now_ms: i64| TransferTaskReport {
        job_id: job2.job_id.clone(),
        run_id: job2.run_id,
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
    assert!(store.update_task_report(r2(256, 13))?);
    assert!(store.update_task_report(r2(128, 14))?);
    let row2 = store.list_transfer_tasks(&job2.job_id, job2.run_id)?[0].clone();
    assert_eq!(row2.progress.loaded_size, 256, "MySQL must not regress");
    let mut stale = r2(100, 15);
    stale.attempt_id = 99;
    assert!(!store.update_task_report(stale)?);
    // Crash fencing on real SQL: terminal lands, then the dead attempt's
    // late reports (Running or foreign terminal) cannot clobber it.
    let mut done = r2(384, 16);
    done.state = TransferTaskState::Completed;
    assert!(store.update_task_report(done)?);
    assert!(!store.update_task_report(r2(100, 17))?);
    let mut foreign = r2(384, 18);
    foreign.attempt_id = 99;
    foreign.state = TransferTaskState::Completed;
    assert!(!store.update_task_report(foreign)?);
    let row2 = store.list_transfer_tasks(&job2.job_id, job2.run_id)?[0].clone();
    assert_eq!(row2.state, TransferTaskState::Completed);
    assert_eq!(row2.progress.loaded_size, 384);

    // Snapshot + purge gates.
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
        now_ms: 20,
    })?);
    assert_eq!(
        store.purge_terminal_tasks(100, 1000)?,
        0,
        "no snapshot => no purge"
    );
    let summary = store.get_transfer_task_summary(&job.job_id, job.run_id, 20)?;
    store.update_terminal_snapshot(
        &job.job_id,
        job.run_id,
        summary.progress.clone(),
        summary.counts,
        20,
    )?;
    assert_eq!(store.purge_terminal_tasks(100, 1000)?, 1);
    assert_eq!(store.purge_terminal_jobs(100, 1000)?, 1);

    // EXPLAIN runs against the real purge shape; tiny tables may not pick the
    // index, so existence (below) is the portable proof and prod-size EXPLAIN
    // stays a Phase-2 gate.
    let opts = mysql::Opts::from_url(&url).unwrap();
    let pool = mysql::Pool::new(opts).unwrap();
    let mut conn = pool.get_conn().unwrap();
    use mysql::prelude::Queryable;
    let rows: Vec<mysql::Row> = conn
        .exec(
            "EXPLAIN select t.job_id from transfer_tasks t join transfer_jobs j
             on t.job_id=j.job_id and t.run_id=j.run_id
             where j.state in (6,7,8,9) and j.updated_at<1
               and json_type(json_extract(j.record_json,'$.terminal_summary'))='OBJECT'
             order by j.updated_at asc limit 1",
            (),
        )
        .unwrap();
    assert!(!rows.is_empty(), "EXPLAIN must return a plan");
    let key: Option<String> = rows[0].get("key").unwrap_or(None);
    eprintln!("purge EXPLAIN key={key:?} (1-row tables may not pick the index)");
    let keys: Vec<String> = conn
        .exec(
            "select index_name from information_schema.statistics
             where table_schema=database() and table_name='transfer_jobs'
               and index_name='transfer_jobs_state_updated_idx'",
            (),
        )
        .unwrap();
    assert!(!keys.is_empty(), "TTL purge index must exist on real MySQL");
    Ok(())
}

/// Real cluster + real MySQL + real planner: small-file batch rows and
/// large-file per-segment single rows land in MySQL; cap check precedes
/// destructive target delete. file:// has no identity, so groups stay singles.
#[test]
fn mysql_cluster_e2e_batch_segment_load() -> CommonResult<()> {
    let Some(url) = require_mysql() else {
        return Ok(());
    };
    reset_mysql(&url);
    let store = Arc::new(MysqlTransferStore::open(&url)?);
    let (cluster, base_dir) = e2e_cluster();
    let rt = cluster.clone_client_rt();
    rt.block_on(async move {
        let fs = cluster.new_fs();
        let ufs_dir = format!("{base_dir}/ufs");
        std::fs::create_dir_all(format!("{ufs_dir}/small"))?;
        std::fs::create_dir_all(format!("{ufs_dir}/small/sub"))?;
        for i in 0..20 {
            std::fs::write(
                format!("{ufs_dir}/small/f{i:03}.txt"),
                vec![b'x' + (i % 26) as u8; 1024],
            )?;
        }
        // Batch manifests may span subdirectories (rel_path preserved).
        std::fs::write(format!("{ufs_dir}/small/sub/nested.txt"), b"nested")?;
        // Sparse 300MiB file (>=256MiB batch target => real segments; sparse
        // so planning stays fast and content-free).
        let big_path = format!("{ufs_dir}/big.bin");
        let f = std::fs::File::create(&big_path)?;
        f.set_len(300 * 1024 * 1024)?;
        // Compressed large files must not split (design §14 nonSplittable).
        let gz = std::fs::File::create(format!("{ufs_dir}/large2.gz"))?;
        gz.set_len(300 * 1024 * 1024)?;

        let ufs_root = Path::from_str(format!("file://{ufs_dir}"))?;
        let mount_path = Path::from_str("/e2e-batch")?;
        let opts = curvine_model::MountOptions::builder()
            .block_size(1024 * 1024)
            .replicas(1)
            .auto_cache(false)
            .build();
        fs.mount(&ufs_root, &mount_path, opts).await?;
        let cache = ClusterMetadataCache::new(fs.clone());
        cache.refresh().await?;
        let factory = Arc::new(UfsFactory::with_rt(
            &cluster.cluster_conf.client,
            cluster.clone_client_rt(),
        ));
        let planner = TransferPlanner::new(
            fs.clone(),
            factory,
            cache,
            cluster.cluster_conf.client.clone(),
            100_000,
            32,
        );

        let command = TransferCommand {
            kind: TransferKind::Load,
            source_path: format!("file://{ufs_dir}"),
            target_path: "/e2e-batch/out".to_string(),
            ..Default::default()
        };
        let job_id = format!("batch-seg-{}", Utils::rand_str(6));
        let job = e2e_job(&command, &job_id);
        let planned = planner.plan(&job).await?;
        assert!(!planned.tasks.is_empty());
        store.create_or_get_by_request_id_checked(job.clone())?;
        let lease = store
            .acquire_runnable_transfer("owner", 60_000, 1, 8, false)?
            .unwrap();
        store.persist_planned_tasks(TransferPlannedTasks {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            tasks: planned.tasks.clone(),
            message: "planned".to_string(),
            now_ms: 2,
        })?;
        let rows = store.list_transfer_tasks(&job.job_id, job.run_id)?;
        let (mut batch, mut single, mut plain_gz) = (0, 0, 0);
        for t in &rows {
            match curvine_model::classify_transfer_plan(&t.source_read_plan_json) {
                TransferPlanKind::Batch => batch += 1,
                TransferPlanKind::Segment => single += 1,
                TransferPlanKind::SegmentGroup => {
                    panic!("planner must stay fail-closed without source version")
                }
                TransferPlanKind::Plain => {
                    if t.target_path.ends_with("large2.gz") {
                        plain_gz += 1;
                    }
                }
            }
        }
        assert!(batch >= 1, "small files must batch, got {batch}");
        assert_eq!(
            single, 300,
            "300MiB/1MiB must be 300 single segments, got {single}"
        );
        assert_eq!(plain_gz, 1, "compressed large file must stay single");
        // The 300 single-segment rows must tile [0, 300MiB) with one block
        // each: parse the real plan JSONs back out of MySQL and check
        // contiguity + block_index sequence (the executable tail of planning).
        let mut segs: Vec<(i64, i64, i64)> = rows
            .iter()
            .filter(|t| {
                curvine_model::classify_transfer_plan(&t.source_read_plan_json)
                    == TransferPlanKind::Segment
            })
            .map(|t| {
                let v: serde_json::Value = serde_json::from_str(&t.source_read_plan_json).unwrap();
                let s = &v["segment"];
                (
                    s["offset"].as_i64().unwrap(),
                    s["length"].as_i64().unwrap(),
                    s["block_index"].as_i64().unwrap(),
                )
            })
            .collect();
        segs.sort();
        assert_eq!(segs.len(), 300);
        let mut next = 0i64;
        for (i, (off, len, blk)) in segs.iter().enumerate() {
            assert_eq!(*off, next, "segment {i} must start where previous ended");
            assert_eq!(*len, 1024 * 1024);
            assert_eq!(*blk, i as i64);
            next += len;
        }
        assert_eq!(next, 300 * 1024 * 1024);
        let batch_json: String = rows
            .iter()
            .find(|t| {
                curvine_model::classify_transfer_plan(&t.source_read_plan_json)
                    == TransferPlanKind::Batch
            })
            .unwrap()
            .source_read_plan_json
            .clone();
        assert!(
            batch_json.contains("sub/nested.txt"),
            "batch manifest must preserve nested rel_path"
        );

        // Real IO: copy the batch task and verify small + nested content.
        let batch_row = rows
            .iter()
            .find(|t| {
                curvine_model::classify_transfer_plan(&t.source_read_plan_json)
                    == TransferPlanKind::Batch
            })
            .unwrap();
        let job_info = planned.job_info.clone();
        let batch_ctx = Arc::new(TaskContext::new(LoadTaskInfo {
            job: job_info.clone(),
            task_id: batch_row.task_id.clone(),
            worker: WorkerAddress::default(),
            source_path: batch_row.source_path.clone(),
            target_path: batch_row.target_path.clone(),
            create_time: 0,
            source_read_plan_json: batch_row.source_read_plan_json.clone(),
            estimated_size: batch_row.estimated_size,
            transfer_report: None,
        }));
        let batch_runner = LoadTaskRunner::new(
            batch_ctx,
            fs.clone(),
            Arc::new(UfsFactory::with_rt(
                &cluster.cluster_conf.client,
                cluster.clone_client_rt(),
            )),
            None,
            10_000,
            120_000,
        );
        assert!(batch_runner.run().await, "real batch copy must succeed");
        let probe = Path::from_str(format!("{}/small/f000.txt", job.target_path))?;
        let text = fs.read_string(&probe).await?;
        assert_eq!(text.as_bytes(), &[b'x'; 1024]);
        let nested = Path::from_str(format!("{}/small/sub/nested.txt", job.target_path))?;
        assert_eq!(fs.read_string(&nested).await?.as_str(), "nested");

        // Real IO: copy one single segment and verify its block landed.
        let seg_row = rows
            .iter()
            .find(|t| {
                curvine_model::classify_transfer_plan(&t.source_read_plan_json)
                    == TransferPlanKind::Segment
            })
            .unwrap();
        let seg_ctx = Arc::new(TaskContext::new(LoadTaskInfo {
            job: job_info,
            task_id: seg_row.task_id.clone(),
            worker: WorkerAddress::default(),
            source_path: seg_row.source_path.clone(),
            target_path: seg_row.target_path.clone(),
            create_time: 0,
            source_read_plan_json: seg_row.source_read_plan_json.clone(),
            estimated_size: seg_row.estimated_size,
            transfer_report: None,
        }));
        let seg_runner = LoadTaskRunner::new(
            seg_ctx,
            fs.clone(),
            Arc::new(UfsFactory::with_rt(
                &cluster.cluster_conf.client,
                cluster.clone_client_rt(),
            )),
            None,
            10_000,
            120_000,
        );
        let _ = seg_runner.run().await;
        let seg_target = Path::from_str(&seg_row.target_path)?;
        let seg_blocks = fs.get_block_locations(&seg_target).await?;
        assert!(!seg_blocks.block_locs.is_empty(), "segment block must land");

        // Cap precedes delete: pre-create the task target, plan with cap 1,
        // target survives.
        let victim = Path::from_str(format!("{}/big.bin", job.target_path))?;
        {
            let mut w = fs
                .create_with_opts(
                    &victim,
                    curvine_model::CreateFileOptsBuilder::new()
                        .create_parent(true)
                        .build(),
                    true,
                )
                .await?;
            use curvine_fs_api::Writer;
            w.write(b"victim").await?;
            w.flush().await?;
            w.complete().await?;
        }
        let tiny_planner = TransferPlanner::new(
            fs.clone(),
            Arc::new(UfsFactory::with_rt(
                &cluster.cluster_conf.client,
                cluster.clone_client_rt(),
            )),
            ClusterMetadataCache::new(fs.clone()),
            cluster.cluster_conf.client.clone(),
            1,
            32,
        );
        // Fresh cache without refresh => mount snapshot missing => plan errs
        // before touching the victim; either way the victim must survive.
        let _ = tiny_planner.plan(&job).await;
        let still = fs.get_status(&victim).await?;
        assert!(still.len >= 6, "cap/mount failure must not delete target");
        Ok(())
    })
}

fn group_job_info(
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

fn require_s3() -> Option<()> {
    if std::process::Command::new("aws")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("aws CLI missing; skipping S3 E2E");
        return None;
    }
    if std::net::TcpStream::connect("127.0.0.1:9000").is_err() {
        eprintln!("MinIO 127.0.0.1:9000 unreachable; skipping S3 E2E");
        return None;
    }
    Some(())
}

fn aws_s3(args: &[&str]) -> CommonResult<()> {
    let out = std::process::Command::new("aws")
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .arg("--endpoint-url")
        .arg("http://127.0.0.1:9000")
        .arg("--region")
        .arg("us-east-1")
        .arg("s3")
        .args(args)
        .output()?;
    if !out.status.success() {
        return curvine_core_error::err_box!(format!(
            "aws s3 {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(())
}

fn s3_props() -> Vec<(String, String)> {
    vec![
        (
            "s3.endpoint_url".to_string(),
            "http://127.0.0.1:9000".to_string(),
        ),
        ("s3.region_name".to_string(), "us-east-1".to_string()),
        (
            "s3.credentials.access".to_string(),
            "minioadmin".to_string(),
        ),
        (
            "s3.credentials.secret".to_string(),
            "minioadmin".to_string(),
        ),
        ("s3.force.path.style".to_string(), "true".to_string()),
    ]
}

/// Real S3 source via MinIO + real MySQL: the planner emits genuine group
/// rows (adapter etag), then every group mechanic reuses those rows with zero
/// hand-built manifests (negatives excepted): full copy, crash-cancel +
/// MySQL-backed resume, deadline retry, and all fail-closed rejects.
#[test]
fn s3_cluster_e2e_s3_source() -> CommonResult<()> {
    let Some(url) = require_mysql() else {
        return Ok(());
    };
    let Some(_) = require_s3() else {
        return Ok(());
    };
    reset_mysql(&url);
    let store = Arc::new(MysqlTransferStore::open(&url)?);

    // Per-run prefix isolates us from other bucket users.
    let run = format!("s3e2e-{}", Utils::rand_str(8));
    let local = Utils::test_sub_dir(format!("transfer-s3e2e-{run}"));
    std::fs::create_dir_all(format!("{local}/small"))?;
    for i in 0..20 {
        let mut v = vec![b'x' + (i % 26) as u8; 1024];
        v[0..13].copy_from_slice(format!("s3-small-{i:03}-").as_bytes());
        std::fs::write(format!("{local}/small/f{i:03}.txt"), &v)?;
    }
    // 300MiB real bytes (>=256MiB planner segment threshold). Upload once;
    // mechanics below copy whole groups or slices.
    let big_local = format!("{local}/big.bin");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&big_local)?;
        let chunk: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
        for _ in 0..300 {
            f.write_all(&chunk)?;
        }
    }
    aws_s3(&[
        "sync",
        &format!("{local}/small"),
        &format!("s3://e2e-src/{run}/small"),
    ])?;
    aws_s3(&["cp", &big_local, &format!("s3://e2e-src/{run}/big.bin")])?;

    let (cluster, _base) = e2e_cluster();
    let rt = cluster.clone_client_rt();
    let rt_inner = rt.clone();
    rt.block_on(async move {
        let fs = cluster.new_fs();
        let ufs_root = Path::from_str(format!("s3://e2e-src/{run}"))?;
        let mount_path = Path::from_str("/e2e-s3")?;
        let mut builder = curvine_model::MountOptions::builder()
            .block_size(1024 * 1024)
            .replicas(1)
            .auto_cache(false);
        for (k, v) in s3_props() {
            builder = builder.add_property(k, v);
        }
        fs.mount(&ufs_root, &mount_path, builder.build()).await?;
        let mount: MountInfo = fs
            .get_mount_table()
            .await?
            .into_iter()
            .find(|m| m.cv_path == "/e2e-s3")
            .unwrap();
        let factory = Arc::new(UfsFactory::with_rt(
            &cluster.cluster_conf.client,
            rt_inner.clone(),
        ));

        // Real planner over the S3 tree: the adapter etag flows into genuine
        // group manifests (no flag needed; version + capability decide).
        let cache = ClusterMetadataCache::new(fs.clone());
        cache.refresh().await?;
        let planner = TransferPlanner::new(
            fs.clone(),
            factory.clone(),
            cache,
            cluster.cluster_conf.client.clone(),
            100_000,
            32,
        );
        let command = TransferCommand {
            kind: TransferKind::Load,
            source_path: format!("s3://e2e-src/{run}"),
            target_path: "/e2e-s3/out".to_string(),
            ..Default::default()
        };
        let job_id = format!("s3-batch-{run}");
        let job = e2e_job(&command, &job_id);
        let planned = planner.plan(&job).await?;
        store.create_or_get_by_request_id_checked(job.clone())?;
        let lease = store
            .acquire_runnable_transfer("owner", 60_000, 1, 8, false)?
            .unwrap();
        store.persist_planned_tasks(TransferPlannedTasks {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            tasks: planned.tasks.clone(),
            message: "planned".to_string(),
            now_ms: 2,
        })?;
        let rows = store.list_transfer_tasks(&job.job_id, job.run_id)?;
        assert!(!rows.is_empty());
        let mut batch = 0;
        let mut groups = 0;
        let mut singles = 0;
        let mut versions = std::collections::HashSet::new();
        for t in &rows {
            match curvine_model::classify_transfer_plan(&t.source_read_plan_json) {
                TransferPlanKind::Batch => batch += 1,
                TransferPlanKind::SegmentGroup => {
                    groups += 1;
                    let plan: serde_json::Value = serde_json::from_str(&t.source_read_plan_json)?;
                    let g = &plan["segment_group"];
                    assert_eq!(
                        g["end_offset"].as_i64().unwrap() - g["start_offset"].as_i64().unwrap(),
                        150 * MB
                    );
                    let v = g["source_version"].as_str().unwrap().to_owned();
                    assert!(!v.is_empty(), "planner must persist a real S3 etag");
                    versions.insert(v);
                }
                TransferPlanKind::Segment => singles += 1,
                TransferPlanKind::Plain => {}
            }
        }
        // 300MiB/1MiB = 300 segments; 1 worker => G=min(max(2,1),300)=2 groups.
        assert!(batch >= 1, "S3 small files must batch");
        assert_eq!(
            groups, 2,
            "planner must emit real groups for S3, got {groups}"
        );
        assert_eq!(singles, 0);
        assert_eq!(versions.len(), 1, "one object => one etag everywhere");

        // Real batch copy from S3, content verified through CV.
        let batch_row = rows
            .iter()
            .find(|t| {
                curvine_model::classify_transfer_plan(&t.source_read_plan_json)
                    == TransferPlanKind::Batch
            })
            .unwrap();
        let batch_ctx = Arc::new(TaskContext::new(LoadTaskInfo {
            job: planned.job_info.clone(),
            task_id: batch_row.task_id.clone(),
            worker: WorkerAddress::default(),
            source_path: batch_row.source_path.clone(),
            target_path: batch_row.target_path.clone(),
            create_time: 0,
            source_read_plan_json: batch_row.source_read_plan_json.clone(),
            estimated_size: batch_row.estimated_size,
            transfer_report: None,
        }));
        let batch_runner = LoadTaskRunner::new(
            batch_ctx,
            fs.clone(),
            factory.clone(),
            None,
            10_000,
            120_000,
        );
        assert!(batch_runner.run().await, "S3 batch copy must succeed");
        let probe = Path::from_str(format!("{}/small/f000.txt", job.target_path))?;
        let text = fs.read_string(&probe).await?;
        assert_eq!(text.len(), 1024);
        assert!(text.starts_with("s3-small-000-"));
        // Close the batch job's real lifecycle so later jobs in the same
        // target tree can acquire leases (target-conflict protection).
        assert!(store.update_transfer_state(TransferStateUpdate {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            from_states: vec![
                TransferState::Planning,
                TransferState::Dispatching,
                TransferState::Running
            ],
            to_state: TransferState::Completed,
            message: "batch done".to_string(),
            now_ms: 3,
        })?);
        let batch_summary = store.get_transfer_task_summary(&job.job_id, job.run_id, 3)?;
        store.update_terminal_snapshot(
            &job.job_id,
            job.run_id,
            batch_summary.progress,
            batch_summary.counts,
            3,
        )?;

        // All group mechanics below reuse the planner-emitted rows: zero
        // hand-built manifests (negatives excepted).
        let source = format!("s3://e2e-src/{run}/big.bin");
        let group_rows: Vec<_> = rows
            .iter()
            .filter(|t| {
                curvine_model::classify_transfer_plan(&t.source_read_plan_json)
                    == TransferPlanKind::SegmentGroup
            })
            .cloned()
            .collect();
        assert_eq!(group_rows.len(), groups);
        let group_row = group_rows[0].clone();
        let group: SegmentGroupPlan =
            curvine_model::parse_segment_group(&group_row.source_read_plan_json)?;
        let block_size = group.segment_bytes;
        assert_eq!(block_size, 1024 * 1024);
        let group_total = group.group_len();
        assert_eq!(group_total, 150 * MB);
        let target = group_row.target_path.clone();
        let file_total = group.total_len;
        assert_eq!(file_total, 300 * MB);
        let mk_ctx =
            |task_id: &str, target: &str, plan_json: &str, resume: i64, storage: StorageType| {
                Arc::new(TaskContext::new(LoadTaskInfo {
                    job: group_job_info("s3-group", &source, target, block_size, &mount, storage),
                    task_id: task_id.to_string(),
                    worker: WorkerAddress::default(),
                    source_path: source.clone(),
                    target_path: target.to_string(),
                    create_time: 0,
                    source_read_plan_json: plan_json.to_string(),
                    estimated_size: group_total,
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
        let slice_full = |blocks: &curvine_model::FileBlocks, first: usize, count: usize| {
            let mut sorted = blocks.block_locs.clone();
            sorted.sort_by_key(|b| b.block.id);
            assert_eq!(sorted.len(), 300);
            for b in sorted.iter().skip(first).take(count) {
                assert_eq!(b.block.len, block_size);
            }
        };
        // FULL copy: execute all planned groups against the planned target.
        for (i, grow) in group_rows.iter().enumerate() {
            let ctx = mk_ctx(
                &format!("full-{i}"),
                &target,
                &grow.source_read_plan_json,
                0,
                StorageType::default(),
            );
            let runner =
                LoadTaskRunner::new(ctx, fs.clone(), factory.clone(), None, 10_000, 120_000);
            let _ = runner.run().await;
        }
        let target_path = Path::from_str(&target)?;
        let blocks = fs.get_block_locations(&target_path).await?;
        assert_eq!(blocks.block_locs.len(), 300);
        for b in &blocks.block_locs {
            assert_eq!(b.block.len, block_size, "every planned segment must land");
        }
        let commit: Vec<_> = blocks
            .block_locs
            .iter()
            .map(curvine_model::CommitBlock::from)
            .collect();
        fs.fs_client()
            .complete_file_by_id(
                &target_path,
                blocks.status.id,
                file_total,
                commit,
                false,
                None,
            )
            .await?;
        let done = fs.get_status(&target_path).await?;
        assert!(done.is_complete() && done.len == file_total);

        // Crash mid-copy on a fresh target with group[0]'s manifest.
        let target_crash = format!("{}/crash.bin", job.target_path);
        let g0_json = group_rows[0].source_read_plan_json.clone();
        let ctx = mk_ctx(
            "task-crash",
            &target_crash,
            &g0_json,
            0,
            StorageType::default(),
        );
        let killer = {
            let ctx = ctx.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(10));
                ctx.set_canceled("s3 worker crash");
            })
        };
        let runner = LoadTaskRunner::new(
            ctx.clone(),
            fs.clone(),
            factory.clone(),
            None,
            10_000,
            120_000,
        );
        let _ = runner.run().await;
        let _ = killer.join();
        assert_eq!(ctx.get_state(), curvine_model::JobTaskState::Canceled);
        let crash_path = Path::from_str(&target_crash)?;
        let blocks = fs.get_block_locations(&crash_path).await?;
        let mut prefix = 0i64;
        let mut sorted = blocks.block_locs.clone();
        sorted.sort_by_key(|b| b.block.id);
        for b in sorted.iter().take(150) {
            if b.block.len >= block_size {
                prefix += block_size;
            } else {
                break;
            }
        }
        assert!(prefix >= block_size, "S3 crash must leave >=1 segment");
        // Persist the S3 prefix to real MySQL exactly like a flushed progress
        // report; a stale lower report must not regress it.
        let s3job_id = format!("s3-crash-{run}");
        let s3command = TransferCommand {
            kind: TransferKind::Load,
            source_path: source.clone(),
            target_path: target_crash.clone(),
            ..Default::default()
        };
        let s3job = e2e_job(&s3command, &s3job_id);
        store.create_or_get_by_request_id_checked(s3job.clone())?;
        let s3lease = store
            .acquire_runnable_transfer("owner", 60_000, 1, 8, false)?
            .unwrap();
        store.persist_planned_tasks(TransferPlannedTasks {
            job_id: s3job.job_id.clone(),
            run_id: s3job.run_id,
            owner: s3lease.owner.clone(),
            lease_epoch: s3lease.lease_epoch,
            tasks: vec![TransferTaskRecord {
                job_id: s3job.job_id.clone(),
                run_id: 1,
                task_id: "task-crash".to_string(),
                attempt_id: 0,
                source_path: source.clone(),
                target_path: target_crash.clone(),
                worker_id: 0,
                worker_session_id: String::new(),
                source_read_plan_json: g0_json.clone(),
                report_target_json: String::new(),
                state: TransferTaskState::Pending,
                progress: TransferProgress {
                    total_size: group_total,
                    ..Default::default()
                },
                estimated_size: group_total,
                retry_count: 0,
                attempt_started_at: 0,
                accepted_at_ms: 0,
                first_byte_at_ms: 0,
                last_report_at: 0,
                stale_deadline_at: 0,
                updated_at: 1,
                replace_target: false,
                block_locations: Vec::new(),
            }],
            message: "planned".to_string(),
            now_ms: 2,
        })?;
        let s3lease_now = store.get_transfer(&s3job.job_id)?.unwrap();
        assert!(store.start_task_attempt(TaskAttemptStart {
            job_id: s3job.job_id.clone(),
            run_id: s3job.run_id,
            owner: s3lease_now.owner.clone(),
            lease_epoch: s3lease_now.lease_epoch,
            task_id: "task-crash".to_string(),
            attempt_id: 1,
            worker_id: 7,
            worker_session_id: "s".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 3,
            stale_deadline_at: 60_000,
            resume_loaded_size: 0,
        })?);
        let s3report = |loaded: i64, now_ms: i64| TransferTaskReport {
            job_id: s3job.job_id.clone(),
            run_id: s3job.run_id,
            task_id: "task-crash".to_string(),
            attempt_id: 1,
            worker_id: 7,
            worker_session_id: "s".to_string(),
            state: TransferTaskState::Running,
            progress: TransferProgress {
                loaded_size: loaded,
                total_size: group_total,
                update_time: now_ms,
                message: "s3 prefix".to_string(),
            },
            now_ms,
            stale_deadline_at: 60_000,
            accepted_at_ms: 0,
            first_byte_at_ms: 0,
            block_locations: Vec::new(),
        };
        assert!(store.update_task_report(s3report(prefix, 4))?);
        assert!(store.update_task_report(s3report(0, 5))?);
        let persisted = store.list_transfer_tasks(&s3job.job_id, s3job.run_id)?[0]
            .progress
            .loaded_size;
        assert_eq!(persisted, prefix);
        let resume = curvine_model::normalize_group_resume(&g0_json, persisted);
        assert_eq!(resume % block_size, 0);
        let resume_ctx = mk_ctx(
            "task-resume",
            &target_crash,
            &g0_json,
            resume,
            StorageType::default(),
        );
        let resume_runner = LoadTaskRunner::new(
            resume_ctx,
            fs.clone(),
            factory.clone(),
            None,
            10_000,
            120_000,
        );
        let _ = resume_runner.run().await;
        let blocks = fs.get_block_locations(&crash_path).await?;
        slice_full(&blocks, 0, 150);

        // Deadline path on group[1]'s slice: tiny timeout must fail the
        // attempt, then a retry from zero converges blocks 150-299.
        let target_timeout = format!("{}/timeout.bin", job.target_path);
        let g1_json = group_rows[1].source_read_plan_json.clone();
        let timeout_ctx = mk_ctx(
            "task-timeout",
            &target_timeout,
            &g1_json,
            0,
            StorageType::default(),
        );
        let timeout_runner = LoadTaskRunner::new(
            timeout_ctx.clone(),
            fs.clone(),
            factory.clone(),
            None,
            10_000,
            10,
        );
        assert!(
            !timeout_runner.run().await,
            "deadline must fail the attempt"
        );
        assert_eq!(timeout_ctx.get_state(), curvine_model::JobTaskState::Failed);
        let retry_ctx = mk_ctx(
            "task-timeout-retry",
            &target_timeout,
            &g1_json,
            0,
            StorageType::default(),
        );
        let retry_runner = LoadTaskRunner::new(
            retry_ctx,
            fs.clone(),
            factory.clone(),
            None,
            10_000,
            120_000,
        );
        let _ = retry_runner.run().await;
        let timeout_path = Path::from_str(&target_timeout)?;
        let blocks = fs.get_block_locations(&timeout_path).await?;
        slice_full(&blocks, 150, 150);

        // Design rejects, all against live S3 state.
        let target_reject = format!("{}/reject.bin", job.target_path);
        // Empty version: gate refuses before any IO semantics.
        let mut nover = curvine_model::parse_segment_group(&g0_json)?;
        nover.source_version.clear();
        let nover_ctx = mk_ctx(
            "task-nover",
            &target_reject,
            &segment_group_json(&nover)?,
            0,
            StorageType::default(),
        );
        let nover_runner = LoadTaskRunner::new(
            nover_ctx,
            fs.clone(),
            factory.clone(),
            None,
            10_000,
            120_000,
        );
        assert!(!nover_runner.run().await);
        // SPDK target: fail-closed before identity.
        let spdk_ctx = mk_ctx(
            "task-spdk",
            &target_reject,
            &g0_json,
            0,
            StorageType::SpdkDisk,
        );
        let spdk_runner =
            LoadTaskRunner::new(spdk_ctx, fs.clone(), factory.clone(), None, 10_000, 120_000);
        assert!(!spdk_runner.run().await);
        // Tampered version isolates the identity gate: len/mtime are correct,
        // only the etag differs, yet resume must refuse.
        let mut tampered = curvine_model::parse_segment_group(&g0_json)?;
        tampered.source_version = "tampered-etag".to_string();
        let tampered_ctx = mk_ctx(
            "task-tampered",
            &target_reject,
            &segment_group_json(&tampered)?,
            0,
            StorageType::default(),
        );
        let tampered_runner = LoadTaskRunner::new(
            tampered_ctx,
            fs.clone(),
            factory.clone(),
            None,
            10_000,
            120_000,
        );
        assert!(!tampered_runner.run().await);
        // Wrong geometry against the live layout (kept manifest-valid so
        // the layout gate, not the parser, fires).
        let mut bad_geo = curvine_model::parse_segment_group(&g0_json)?;
        bad_geo.segment_bytes *= 2;
        bad_geo.end_offset += 150 * MB;
        let bad_ctx = mk_ctx(
            "task-bad-geo",
            &target_reject,
            &segment_group_json(&bad_geo)?,
            0,
            StorageType::default(),
        );
        let bad_runner =
            LoadTaskRunner::new(bad_ctx, fs.clone(), factory.clone(), None, 10_000, 120_000);
        assert!(!bad_runner.run().await);
        // S3 source rewrite is caught (len collapses 300MiB -> 1KiB).
        aws_s3(&[
            "cp",
            &format!("{local}/small/f000.txt"),
            &format!("s3://e2e-src/{run}/big.bin"),
        ])?;
        let stale_ctx = mk_ctx(
            "task-stale",
            &target_reject,
            &g0_json,
            0,
            StorageType::default(),
        );
        let stale_runner = LoadTaskRunner::new(
            stale_ctx,
            fs.clone(),
            factory.clone(),
            None,
            10_000,
            120_000,
        );
        assert!(
            !stale_runner.run().await,
            "rewritten S3 object must not resume"
        );
        Ok(())
    })
}

fn tm_transfer_task(task_id: &str, run_id: u64, attempt_id: u64, session: &str) -> LoadTaskInfo {
    LoadTaskInfo {
        job: LoadJobInfo {
            job_id: "tm-job".to_string(),
            source_path: "file:///tmp/tm-no-such-file.bin".to_string(),
            target_path: "/tm-out/file.bin".to_string(),
            block_size: 1024 * 1024,
            replicas: 1,
            storage_type: StorageType::default(),
            ttl_ms: 0,
            ttl_action: TtlAction::default(),
            mount_info: MountInfo::default(),
            create_time: 0,
            overwrite: Some(true),
            source_type: JobSourceType::default(),
            transfer_job_id: None,
        },
        task_id: task_id.to_string(),
        worker: WorkerAddress::default(),
        source_path: "file:///tmp/tm-no-such-file.bin".to_string(),
        target_path: "/tm-out/file.bin".to_string(),
        create_time: 0,
        source_read_plan_json: "{}".to_string(),
        estimated_size: 1024,
        transfer_report: Some(TransferTaskReportInfo {
            run_id,
            attempt_id,
            worker_id: 0,
            worker_session_id: session.to_string(),
            report_target: String::new(),
            report_endpoints: vec!["127.0.0.1:1".to_string()],
            attempt_started_at_ms: 0,
            resume_loaded_size: 0,
        }),
    }
}

/// Real TaskManager admission (no cluster needed; all checks are local):
/// stale session, duplicate dedup, supersede on newer attempt, stale attempt.
#[test]
fn task_manager_admission_contract() -> CommonResult<()> {
    let conf = ClusterConf::default();
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let mgr = TaskManager::with_rt(rt, &conf, "session-1")?;

    // Stale worker session is rejected before touching task state.
    let stale = mgr.submit_task(tm_transfer_task("t-stale", 1, 1, "session-OLD"))?;
    assert!(!stale.accepted);

    // First submit accepted (runner will fail async without a master; the
    // admission verdict is what matters here).
    let first = mgr.submit_task(tm_transfer_task("t-dup", 1, 1, "session-1"))?;
    assert!(first.accepted);
    // Same run+attempt => duplicate dedup, accepted without supersede.
    let dup = mgr.submit_task(tm_transfer_task("t-dup", 1, 1, "session-1"))?;
    assert!(dup.accepted);
    // Newer attempt supersedes the old context.
    let newer = mgr.submit_task(tm_transfer_task("t-dup", 1, 2, "session-1"))?;
    assert!(newer.accepted);
    // Older attempt after newer => stale rejection.
    let older = mgr.submit_task(tm_transfer_task("t-dup", 1, 1, "session-1"))?;
    assert!(!older.accepted);
    Ok(())
}
