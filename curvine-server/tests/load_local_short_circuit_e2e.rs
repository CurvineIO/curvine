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

//! E2E regression: the worker-embedded transfer client must keep the node
//! hostname so load writes take the local short-circuit path.
//!
//! `TaskManager` used to rewrite `client.hostname` to "localhost". On any node
//! whose `worker.hostname` is not literally "localhost" (production: the node
//! IP) that broke `FsContext::is_local_worker`, so every chunk looped through
//! the worker RPC port instead of writing straight to the local disk.
//!
//! The cluster below runs with a node hostname deliberately different from the
//! client default, and the load goes through the real TaskManager -> RPC ->
//! disk machinery; the assertion reads the worker's real write counters.

use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_data_transfer::worker::task::TaskManager;
use curvine_fs_api::Path;
use curvine_model::{
    JobSourceType, LoadJobInfo, LoadTaskInfo, MountInfo, StorageType, TtlAction, WorkerAddress,
};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use curvine_worker::Worker;
use std::time::{Duration, Instant};

const MB: i64 = 1024 * 1024;

/// Node hostname for both the worker registration and the transfer client.
/// Anything that is not the client default ("localhost") reproduces the
/// production mismatch; `127.0.0.1` needs no DNS to resolve or bind.
const NODE_HOSTNAME: &str = "127.0.0.1";

/// Value of the `write_blocks` sample for `label`, or 0 when absent.
///
/// Matches the metric name plus that one label: samples can carry extra
/// process-wide labels (e.g. `cluster_id`), and label order is not part of
/// the line format.
fn write_blocks(metrics: &str, label: &str) -> i64 {
    let want = format!("type=\"{}\"", label);
    metrics
        .lines()
        .find_map(|line| {
            let (series, value) = line.trim().rsplit_once(' ')?;
            let (name, labels) = series.split_once('{')?;
            if name != "write_blocks" {
                return None;
            }
            let labels = labels.trim_end_matches('}');
            if labels.split(',').any(|pair| pair == want) {
                value.trim().parse::<f64>().ok().map(|v| v as i64)
            } else {
                None
            }
        })
        .unwrap_or(0)
}

fn write_block_lines(metrics: &str) -> String {
    metrics
        .lines()
        .filter(|line| line.starts_with("write_blocks{"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn job_info(source: &str, target: &str, mount: &MountInfo) -> LoadJobInfo {
    LoadJobInfo {
        job_id: "load-local-sc".to_string(),
        source_path: source.to_string(),
        target_path: target.to_string(),
        block_size: MB,
        replicas: 1,
        storage_type: StorageType::default(),
        ttl_ms: 0,
        ttl_action: TtlAction::default(),
        mount_info: mount.clone(),
        create_time: 0,
        overwrite: Some(true),
        source_type: JobSourceType::default(),
        transfer_job_id: None,
    }
}

#[test]
fn load_write_takes_local_short_circuit_path() -> CommonResult<()> {
    let test_id = Utils::rand_str(8);
    let base_dir = Utils::test_sub_dir(format!("load-local-sc-{test_id}"));

    let mut conf = ClusterConf::default();
    conf.master.meta_dir = format!("{base_dir}/meta");
    conf.journal.journal_dir = format!("{base_dir}/journal");
    conf.worker.data_dir = vec![format!("[MEM:64MB]{base_dir}/worker")];
    conf.journal.raft_tick_interval_ms = 100;
    conf.client.short_circuit = true;
    conf.worker.hostname = NODE_HOSTNAME.to_string();
    conf.client.hostname = NODE_HOSTNAME.to_string();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    cluster.start_cluster();
    let rt = cluster.clone_client_rt();
    let rt_inner = rt.clone();

    rt.block_on(async move {
        let fs = cluster.new_fs();
        let ufs_dir = format!("{base_dir}/ufs");
        std::fs::create_dir_all(&ufs_dir)?;
        let payload: Vec<u8> = (0..3 * MB).map(|i| (i % 251) as u8).collect();
        std::fs::write(format!("{ufs_dir}/src.bin"), &payload)?;

        let mount_path = Path::from_str("/load-local-sc")?;
        let opts = curvine_model::MountOptions::builder()
            .block_size(MB)
            .replicas(1)
            .auto_cache(false)
            .build();
        fs.mount(
            &Path::from_str(format!("file://{ufs_dir}"))?,
            &mount_path,
            opts,
        )
        .await?;
        let mount = fs
            .get_mount_table()
            .await?
            .into_iter()
            .find(|m| m.cv_path == "/load-local-sc")
            .unwrap();

        let manager = TaskManager::with_rt(rt_inner, cluster.master_conf(), "load-local-sc")?;
        let before = Worker::get_metrics()?.text_output()?;
        let (local_before, remote_before) = (
            write_blocks(&before, "local"),
            write_blocks(&before, "remote"),
        );

        let source = format!("file://{ufs_dir}/src.bin");
        let target = "/load-local-sc-out/plain.bin".to_string();
        // No transfer job is registered in this MiniCluster, so the runner's
        // final scheduler report (not the copy) logs "Job not found" and the
        // task ends as failed; the file is already committed by then and the
        // counters below are what this test is about.
        let result = manager.submit_task(LoadTaskInfo {
            job: job_info(&source, &target, &mount),
            task_id: "task-local-sc".to_string(),
            worker: WorkerAddress::default(),
            source_path: source,
            target_path: target.clone(),
            create_time: 0,
            source_read_plan_json: "{}".to_string(),
            estimated_size: 3 * MB,
            transfer_report: None,
        })?;
        assert!(
            result.accepted,
            "load task rejected: {}",
            result.reject_reason
        );

        let target_path = Path::from_str(&target)?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Ok(status) = fs.get_status(&target_path).await {
                if status.is_complete() && status.len == 3 * MB {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "load task did not commit the target file in time"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let after = Worker::get_metrics()?.text_output()?;
        let local_delta = write_blocks(&after, "local") - local_before;
        let remote_delta = write_blocks(&after, "remote") - remote_before;
        assert!(
            local_delta > 0,
            "load write must short-circuit to local disk (local +{local_delta}, remote +{remote_delta}): {}",
            write_block_lines(&after)
        );
        assert_eq!(
            remote_delta,
            0,
            "no chunk may loop through the worker RPC port (local +{local_delta}, remote +{remote_delta}): {}",
            write_block_lines(&after)
        );
        Ok(())
    })
}
