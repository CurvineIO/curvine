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

#![cfg(feature = "fault-injection")]

use curvine_client::rpc::JobMasterClient;
use curvine_common_core::conf::ClusterConf;
use curvine_common_core::fs::Path;
use curvine_common_core::state::{JobTaskState, LoadJobCommand, MountOptions};
use curvine_fault::{FaultHttpController, FaultRuleBuilder, FaultRuntime, FaultTestSession};
use curvine_server::test::MiniCluster;
use orpc_rpc::common::Utils;
use orpc_rpc::runtime::RpcRuntime;
use orpc_rpc::CommonResult;
use std::sync::Arc;
use std::time::Duration;

const TOKEN_ENV: &str = "CURVINE_LOAD_TASK_FAULT_TEST_TOKEN";
const BLOCK_SIZE: i64 = 1024 * 1024;
const SOURCE_LEN: usize = 4 * 1024 * 1024;
const FINAL_STREAM: i64 = 1;

/// Reproduces cancellation after the parent's final pre-await check:
///
/// - stream 0 completes normally;
/// - stream 1 is paused before copying;
/// - the parent is paused after checking cancellation and before awaiting stream 1;
/// - cancellation is delivered, so stream 1 later returns the soft `Ok(0)` exit.
///
/// The target has already been resized to the source length, so stamping its
/// UFS mtime would turn this partial file into a false cache hit.
#[test]
fn canceled_final_parallel_stream_does_not_mark_cache_valid() -> CommonResult<()> {
    std::env::set_var(TOKEN_ENV, "load-task-fault-test-secret");
    FaultRuntime::process().clear()?;

    let test_id = Utils::rand_str(8);
    let base_dir = Utils::test_sub_dir(format!("load-task-fault-{test_id}"));
    let ufs_dir = format!("{base_dir}/ufs");
    std::fs::create_dir_all(&ufs_dir)?;
    let source_file = format!("{ufs_dir}/source.bin");
    std::fs::write(&source_file, vec![0x5a; SOURCE_LEN])?;

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = format!("{base_dir}/meta");
    conf.journal.journal_dir = format!("{base_dir}/journal");
    conf.worker.data_dir = vec![format!("[MEM:64MB]{base_dir}/worker")];
    conf.journal.raft_tick_interval_ms = 100;
    conf.client.short_circuit = false;
    conf.fault_injection.enabled = true;
    conf.fault_injection.auth_token_env = TOKEN_ENV.to_string();

    let cluster = Arc::new(MiniCluster::with_num(&conf, 1, 1));
    cluster.start_cluster();

    let worker_conf = &cluster.worker_conf[0];
    let fault_base = format!(
        "http://{}:{}",
        worker_conf.worker.hostname, worker_conf.worker.web_port
    );
    let controller = Arc::new(FaultHttpController::new(
        fault_base,
        std::env::var(TOKEN_ENV)?,
    )?);
    let mut faults = FaultTestSession::new();
    faults.add_target("worker", controller);

    cluster.clone_client_rt().block_on(async move {
        faults.preflight().await?;

        let final_segment_delay =
            FaultRuleBuilder::named("worker.load_task.parallel.before_segment_copy")
                .matches("stream_index", FINAL_STREAM)?
                .times(1)?
                .delay(8_000)?;
        faults
            .configure("worker", "delay-final-segment", final_segment_delay)
            .await?;

        let final_join_delay =
            FaultRuleBuilder::named("worker.load_task.parallel.before_join_await")
                .matches("stream_index", FINAL_STREAM)?
                .times(1)?
                .delay(2_000)?;
        faults
            .configure("worker", "delay-final-join", final_join_delay)
            .await?;

        let runner_finished = FaultRuleBuilder::named("worker.load_task.after_run")
            .times(1)?
            .record()?;
        faults
            .configure("worker", "runner-finished", runner_finished)
            .await?;

        let fs = cluster.new_fs();
        let mount_path = Path::from_str("/cache")?;
        let ufs_root = Path::from_str(format!("file://{ufs_dir}"))?;
        let mount_opts = MountOptions::builder()
            .add_property("load_task.parallel_streams", "2")
            .add_property("load_task.min_bytes_per_stream", "1")
            .block_size(BLOCK_SIZE)
            .replicas(1)
            .auto_cache(false)
            .build();
        fs.mount(&ufs_root, &mount_path, mount_opts).await?;

        let job_client = JobMasterClient::new(fs.fs_client());
        let source_path = format!("file://{source_file}");
        let load = job_client
            .submit_load_job(
                LoadJobCommand::builder(&source_path)
                    .block_size(BLOCK_SIZE)
                    .replicas(1)
                    .build(),
            )
            .await?;

        faults
            .wait_for_executions("worker", "delay-final-segment", 1, Duration::from_secs(20))
            .await?;
        faults
            .wait_for_executions("worker", "delay-final-join", 1, Duration::from_secs(20))
            .await?;

        job_client.cancel_job(&load.job_id).await?;

        faults
            .wait_for_executions("worker", "runner-finished", 1, Duration::from_secs(20))
            .await?;

        let status = job_client.get_job_status(&load.job_id).await?;
        assert_eq!(status.state, JobTaskState::Canceled);

        let target_path = Path::from_str(&load.target_path)?;
        let target_status = fs.get_status(&target_path).await?;
        assert_eq!(
            target_status.storage_policy.ufs_mtime, 0,
            "a canceled parallel load must not stamp UFS cache validity"
        );
        assert!(
            !target_status.cv_valid(None),
            "a canceled parallel load must not become a valid cache entry"
        );

        faults.cleanup().await?;
        CommonResult::Ok(())
    })
}
