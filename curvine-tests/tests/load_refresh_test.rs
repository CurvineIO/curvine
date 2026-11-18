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

//! `cv load --refresh` decides what happens to files that are already cached:
//! `always` copies everything again, `if-newer` only re-copies what changed
//! (size or UFS mtime), `skip-cached` trusts the cache and only loads new files.

use curvine_client::file::{CurvineFileSystem, FsClient, FsContext};
use curvine_client::rpc::JobMasterClient;
use curvine_core_error::CommonResult;
use curvine_fs_api::Path;
use curvine_model::{JobTaskState, LoadJobCommand, LoadRefresh, MountOptions};
use curvine_runtime::common::Logger;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;
use std::time::Duration;

const FILE_COUNT: usize = 4;

fn write_file(dir: &str, name: &str, len: usize) -> CommonResult<()> {
    std::fs::write(format!("{dir}/{name}"), vec![b'x'; len])?;
    Ok(())
}

#[test]
fn refresh_modes_decide_what_is_copied_again() -> CommonResult<()> {
    Logger::default();

    let testing = Testing::builder().workers(2).build()?;
    testing.start_cluster()?;

    // `ufs_path` is a `file://` URL; the fixture lives at its local path.
    let ufs_root = Path::from_str(&testing.ufs_path)?;
    let source_local = format!("{}/refresh-src", ufs_root.path());
    std::fs::create_dir_all(&source_local)?;
    let source_dir = format!("{}/refresh-src", testing.ufs_path);
    for i in 0..FILE_COUNT {
        write_file(&source_local, &format!("file-{i}.bin"), 4096)?;
    }

    let source = Path::from_str(&source_dir)?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let client_rt = rt.clone();

    rt.block_on(async move {
        let fs_context = Arc::new(FsContext::with_rt(conf, client_rt.clone())?);
        let client = FsClient::new(fs_context);
        let mount_path = Path::from_str("/refresh")?;
        let _ = client.umount(&mount_path).await;
        client
            .mount(&source, &mount_path, MountOptions::builder().build())
            .await?;

        let fs = CurvineFileSystem::with_rt(testing.get_active_cluster_conf()?, client_rt)?;
        let job_client = JobMasterClient::new(fs.fs_client());

        // Returns (bytes copied, files skipped as already current, bytes of those
        // files). A run that skipped everything is still recorded, so its skips
        // are visible; a run that copied everything reports no skips.
        let load = |refresh: LoadRefresh| {
            let command = LoadJobCommand::builder(&source_dir)
                .refresh(refresh)
                .build();
            let job_client = job_client.clone();
            async move {
                let submitted = job_client.submit_load_job(command).await?;
                let job_id = submitted.job_id.clone();
                let mut status = job_client.get_job_status(&job_id).await?;
                for _ in 0..600 {
                    match status.state {
                        JobTaskState::Completed => {
                            let progress = &status.progress;
                            return Ok((
                                progress.loaded_size - progress.skipped_size,
                                progress.skipped_files,
                                progress.skipped_size,
                            ));
                        }
                        JobTaskState::Failed | JobTaskState::Canceled => {
                            return Err(curvine_error::FsError::common(format!(
                                "load ended {:?}: {}",
                                status.state, status.progress.message
                            )));
                        }
                        _ => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            status = job_client.get_job_status(&job_id).await?;
                        }
                    }
                }
                Err(curvine_error::FsError::common("load did not finish"))
            }
        };

        let cv_len = |name: &str| {
            let fs = &fs;
            let path = Path::from_str(&format!("/refresh/{name}"));
            async move { Ok::<i64, curvine_error::FsError>(fs.get_status(&path?).await?.len) }
        };

        let dataset = (FILE_COUNT * 4096) as i64;

        // Mode 1: everything is copied, so the cache ends up matching the source.
        let (copied, skipped, skipped_size) = load(LoadRefresh::Always).await?;
        assert_eq!(copied, dataset, "always copies every byte of the dataset");
        assert_eq!((skipped, skipped_size), (0, 0), "always skips nothing");
        for i in 0..FILE_COUNT {
            assert_eq!(
                cv_len(&format!("file-{i}.bin")).await?,
                4096,
                "file-{i} should be cached after an unconditional load"
            );
        }

        // `always` again, on an unchanged dataset: it must plan and copy the
        // whole thing again, which is the behaviour the other modes exist to
        // avoid (a length check alone cannot tell the two apart).
        let (copied, skipped, _) = load(LoadRefresh::Always).await?;
        assert_eq!(copied, dataset, "always re-copies an unchanged dataset");
        assert_eq!(skipped, 0, "always still skips nothing");

        // Both skipping modes copy nothing for an unchanged dataset, and the
        // files they skipped are counted in the job's own statistics.
        for mode in [LoadRefresh::SkipCached, LoadRefresh::IfNewer] {
            let (copied, skipped, skipped_size) = load(mode).await?;
            assert_eq!(copied, 0, "{mode} must not copy anything");
            assert_eq!(
                skipped, FILE_COUNT as i64,
                "{mode} must report the files it skipped"
            );
            assert_eq!(skipped_size, dataset, "{mode} reports their bytes too");
        }

        // Change one existing file (size) and add a new one.
        std::thread::sleep(Duration::from_millis(1_100));
        write_file(&source_local, "file-0.bin", 8192)?;
        write_file(&source_local, "file-new.bin", 1024)?;

        // Mode 3: skip whatever is cached, load only the new file. The changed
        // file keeps its stale cached copy.
        let (copied, skipped, skipped_size) = load(LoadRefresh::SkipCached).await?;
        assert_eq!(copied, 1024, "skip-cached copies only the uncached file");
        assert_eq!(skipped, 4, "skip-cached reports the four cached files");
        assert_eq!(
            skipped_size,
            8192 + 3 * 4096,
            "with their source sizes (file-0 already grew to 8192)"
        );
        assert_eq!(
            cv_len("file-0.bin").await?,
            4096,
            "skip-cached must not refresh a changed file"
        );
        assert_eq!(
            cv_len("file-new.bin").await?,
            1024,
            "skip-cached still loads files missing from the cache"
        );

        // Mode 2: the size changed, so the stale copy is replaced.
        let (copied, skipped, _) = load(LoadRefresh::IfNewer).await?;
        assert_eq!(copied, 8192, "if-newer copies exactly the stale file");
        assert_eq!(skipped, 4, "if-newer reports the four current files");
        assert_eq!(
            cv_len("file-0.bin").await?,
            8192,
            "if-newer must re-copy the file whose size changed"
        );
        assert_eq!(
            cv_len("file-1.bin").await?,
            4096,
            "if-newer must leave unchanged files alone"
        );

        // A source that only looks newer (same size, new mtime) is refreshed too,
        // and then stops being planned once the cache matches it again.
        std::thread::sleep(Duration::from_millis(1_100));
        let touched = format!("{source_local}/file-2.bin");
        let file = std::fs::File::options().write(true).open(&touched)?;
        file.set_modified(std::time::SystemTime::now() + Duration::from_secs(60))?;
        drop(file);

        let (copied, _, _) = load(LoadRefresh::IfNewer).await?;
        assert_eq!(copied, 4096, "an mtime-only change is re-copied");
        let (copied, skipped, skipped_size) = load(LoadRefresh::IfNewer).await?;
        assert_eq!(copied, 0, "the refreshed file is current afterwards");
        assert_eq!(skipped, 5, "every file is now counted as already cached");
        assert_eq!(skipped_size, 8192 + 3 * 4096 + 1024, "with their sizes");

        Ok(())
    })
}

async fn run_single_load(
    job_client: &JobMasterClient,
    single: &str,
) -> Result<(i64, JobTaskState), curvine_error::FsError> {
    let command = LoadJobCommand::builder(single).build();
    let submitted = job_client.submit_load_job(command).await?;
    let job_id = submitted.job_id.clone();
    for _ in 0..600 {
        match job_client.get_job_status(&job_id).await {
            Ok(status) if status.state == JobTaskState::Completed => {
                return Ok((status.progress.total_size, status.state));
            }
            Ok(status) if matches!(status.state, JobTaskState::Failed | JobTaskState::Canceled) => {
                return Err(curvine_error::FsError::common(format!(
                    "load ended {:?}: {}",
                    status.state, status.progress.message
                )))
            }
            // A fully-fast-pathed load is answered without a job record.
            Err(_) => return Ok((0, JobTaskState::Completed)),
            Ok(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    Err(curvine_error::FsError::common("load did not finish"))
}

/// The default mode must keep the historical whole-source fast path: loading one
/// file that is already cached completes immediately instead of re-copying it.
/// `--refresh` changes the per-file policy, not what a plain `cv load` does.
#[test]
fn the_default_mode_keeps_the_already_loaded_fast_path() -> CommonResult<()> {
    Logger::default();

    let testing = Testing::builder().workers(2).build()?;
    testing.start_cluster()?;

    let ufs_root = Path::from_str(&testing.ufs_path)?;
    let source_local = format!("{}/fast-path", ufs_root.path());
    std::fs::create_dir_all(&source_local)?;
    write_file(&source_local, "one.bin", 4096)?;

    let source_dir = format!("{}/fast-path", testing.ufs_path);
    let single = format!("{source_dir}/one.bin");
    let source = Path::from_str(&source_dir)?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let client_rt = rt.clone();

    rt.block_on(async move {
        let fs_context = Arc::new(FsContext::with_rt(conf, client_rt.clone())?);
        let client = FsClient::new(fs_context);
        let mount_path = Path::from_str("/fast")?;
        let _ = client.umount(&mount_path).await;
        client
            .mount(&source, &mount_path, MountOptions::builder().build())
            .await?;

        let fs = CurvineFileSystem::with_rt(testing.get_active_cluster_conf()?, client_rt)?;
        let job_client = JobMasterClient::new(fs.fs_client());

        // First load copies the file.
        let (copied, state) = run_single_load(&job_client, &single).await?;
        assert_eq!(state, JobTaskState::Completed);
        assert_eq!(copied, 4096, "the first load must copy the file");
        let path = Path::from_str("/fast/one.bin")?;
        assert_eq!(fs.get_status(&path).await?.len, 4096);

        // Second load, default mode: the already-loaded fast path answers without
        // planning (and therefore without copying) anything.
        let (copied, state) = run_single_load(&job_client, &single).await?;
        assert_eq!(state, JobTaskState::Completed);
        assert_eq!(
            copied, 0,
            "the default mode must not re-copy an already-loaded file"
        );

        Ok::<(), curvine_error::FsError>(())
    })?;
    Ok(())
}
