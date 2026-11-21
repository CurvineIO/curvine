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

//! Planning a load walks the source tree; the walk lists directories
//! concurrently, so completeness of the walk is what these tests pin.

use curvine_client::file::{CurvineFileSystem, FsClient, FsContext};
use curvine_client::rpc::JobMasterClient;
use curvine_core_error::CommonResult;
use curvine_fs_api::Path;
use curvine_model::{JobTaskState, LoadJobCommand, MountOptions};
use curvine_runtime::common::Logger;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;
use std::time::Duration;

fn write_file(dir: &str, name: &str, len: usize) -> CommonResult<()> {
    std::fs::write(format!("{dir}/{name}"), vec![b'x'; len])?;
    Ok(())
}

/// The plan walk lists directories concurrently, so the completeness of the
/// walk is what has to be proven: every file of a wide, deep tree must become a
/// task, in every branch, no matter which listing finished first.
#[test]
fn load_plans_a_wide_and_deep_tree() -> CommonResult<()> {
    Logger::default();

    const DIRS: usize = 6;
    const SUBDIRS: usize = 5;
    const FILES: usize = 4;
    const LEN: usize = 1024;

    let testing = Testing::builder().workers(2).build()?;
    testing.start_cluster()?;

    let ufs_root = Path::from_str(&testing.ufs_path)?;
    let local_root = format!("{}/walk-src", ufs_root.path());
    let mut expected = Vec::new();
    for d in 0..DIRS {
        for s in 0..SUBDIRS {
            let dir = format!("{local_root}/d{d}/s{s}");
            std::fs::create_dir_all(&dir)?;
            for f in 0..FILES {
                let name = format!("f{f}.bin");
                write_file(&dir, &name, LEN)?;
                expected.push((format!("d{d}/s{s}/{name}"), LEN));
            }
        }
        // A file directly under the top level, so the walk has to plan files it
        // meets before their sibling directories are listed.
        write_file(&local_root, &format!("top-{d}.bin"), LEN)?;
        expected.push((format!("top-{d}.bin"), LEN));
    }

    let source = Path::from_str(&format!("{}/walk-src", testing.ufs_path))?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let client_rt = rt.clone();

    rt.block_on(async move {
        let fs_context = Arc::new(FsContext::with_rt(conf, client_rt.clone())?);
        let client = FsClient::new(fs_context);
        let mount_path = Path::from_str("/walk")?;
        let _ = client.umount(&mount_path).await;
        client
            .mount(&source, &mount_path, MountOptions::builder().build())
            .await?;

        let fs = CurvineFileSystem::with_rt(testing.get_active_cluster_conf()?, client_rt)?;
        let job_client = JobMasterClient::new(fs.fs_client());

        let command = LoadJobCommand::builder(&format!("{}/walk-src", testing.ufs_path)).build();
        let submitted = job_client.submit_load_job(command).await?;
        let job_id = submitted.job_id.clone();
        let mut finished = false;
        for _ in 0..1200 {
            let status = job_client.get_job_status(&job_id).await?;
            match status.state {
                JobTaskState::Completed => {
                    finished = true;
                    break;
                }
                JobTaskState::Failed | JobTaskState::Canceled => {
                    return Err(curvine_error::FsError::common(format!(
                        "load ended {:?}: {}",
                        status.state, status.progress.message
                    )));
                }
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        assert!(
            finished,
            "the walk of a {DIRS}x{SUBDIRS} tree did not finish"
        );

        for (name, len) in expected {
            let path = Path::from_str(&format!("/walk/{name}"))?;
            let status = fs
                .get_status(&path)
                .await
                .unwrap_or_else(|err| panic!("{name} was never planned by the walk: {err}"));
            assert_eq!(status.len, len as i64, "{name} has the wrong length");
        }
        Ok::<(), curvine_error::FsError>(())
    })?;
    Ok(())
}
