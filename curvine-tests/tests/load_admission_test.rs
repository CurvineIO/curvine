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

//! A load job must survive workers that refuse tasks because their queue is
//! full: the worker's queue limit is what keeps a huge job out of its memory,
//! and the master has to pace itself on that answer instead of failing the job.

use curvine_client::file::{CurvineFileSystem, FsClient};
use curvine_client::rpc::JobMasterClient;
use curvine_core_error::CommonResult;
use curvine_fs_api::Path;
use curvine_model::{JobTaskState, MountOptions};
use curvine_runtime::common::Logger;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use log::info;
use std::sync::Arc;
use std::time::Duration;

const FILES: usize = 60;
/// Small enough that a 60-file job cannot fit in one worker's queue, so the
/// dispatch has to wait out queue-full answers for most of the run.
const QUEUE_LIMIT: usize = 2;
const CONCURRENCY: usize = 1;

#[test]
fn load_completes_while_workers_reject_full_queues() -> CommonResult<()> {
    Logger::default();

    let testing = Testing::builder()
        .workers(2)
        .mutate_conf(|conf| {
            conf.job.worker_max_concurrent_tasks = CONCURRENCY;
            conf.job.worker_max_queued_tasks = QUEUE_LIMIT;
        })
        .build()?;
    testing.start_cluster()?;

    // Lay the files out under the cluster's local UFS mount.
    let source_dir = format!("{}/data", testing.ufs_path);
    let source_path = Path::from_str(&source_dir)?;
    std::fs::create_dir_all(source_path.path())?;
    for index in 0..FILES {
        std::fs::write(
            format!("{}/file-{index:03}.bin", source_path.path()),
            vec![b'x'; 4096],
        )?;
    }

    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let client_rt = rt.clone();

    rt.block_on(async move {
        let fs_context = Arc::new(curvine_client::file::FsContext::with_rt(
            conf,
            client_rt.clone(),
        )?);
        let client = FsClient::new(fs_context);

        let mount_path = Path::from_str("/admission")?;
        let _ = client.umount(&mount_path).await;
        let mount = client
            .mount(&source_path, &mount_path, MountOptions::builder().build())
            .await;
        assert!(mount.is_ok(), "mount should succeed: {:?}", mount);

        let fs = CurvineFileSystem::with_rt(testing.get_active_cluster_conf()?, client_rt.clone())?;
        let job_client = JobMasterClient::new(fs.fs_client());

        let load = job_client.submit_load(source_path.full_path()).await?;
        assert!(!load.job_id.is_empty(), "load must return a job id");
        info!("load job {} submitted", load.job_id);

        let mut completed = false;
        for _ in 0..600 {
            let status = job_client.get_job_status(&load.job_id).await?;
            match status.state {
                JobTaskState::Completed => {
                    completed = true;
                    break;
                }
                JobTaskState::Failed | JobTaskState::Canceled => {
                    return Err(format!(
                        "load job ended in {:?}: {}",
                        status.state, status.progress.message
                    )
                    .into());
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
        assert!(
            completed,
            "load job did not finish; queue-full answers must be retried, not failed"
        );

        // Every file has to be in Curvine, not just reported as loaded.
        for index in 0..FILES {
            let cv_path = Path::from_str(format!("{}/file-{index:03}.bin", load.target_path))?;
            assert!(
                fs.exists(&cv_path).await?,
                "{} missing after the load completed",
                cv_path.full_path()
            );
        }

        Ok(())
    })
}
