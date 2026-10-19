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

use curvine_client::file::{FsClient, FsContext};
use curvine_client::unified::UnifiedFileSystem;
use curvine_core_error::CommonResult;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{CurvineURI, FileSystem, Path};
use curvine_model::{ContentSummary, MountOptions, MountOptionsBuilder, WriteType};
use curvine_runtime::common::Logger;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_server::common::ufs_manager::UfsManager;
use curvine_tests::Testing;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

// Test the load function of curvine-client
#[test]
fn test_mount_umount_s3_hdfs_and_path_mapping() -> CommonResult<()> {
    Logger::default();
    // Create a test cluster configuration
    let testing = Testing::builder().default().build()?;
    testing.start_cluster()?;
    let cluster_conf = testing.get_active_cluster_conf()?;
    println!("cluster_conf: {:?}", cluster_conf);

    // Create a test cluster configuration
    let client_rt = Arc::new(cluster_conf.client_rpc_conf().create_runtime());
    let rt = client_rt.clone();

    client_rt.block_on(async move {
        let uri = CurvineURI::new("s3://flink").unwrap();
        info!("norm path is {}", uri.normalize_uri().unwrap());

        let s3_mnt_path = "/a/b".into();
        let s3_path = "s3://flink/savepoints/ab/".into();

        let s3_path2 = "s3://flink/savepoints".into();
        let s3_mnt_path2 = "/c/d".into();

        let s3_path3 = "s3://flink/savepoints/ab/cde".into();
        let s3_mnt_path3 = "/c/d/e".into();

        let s3_path4 = "s3://flink/savepoints/ab/cde".into();
        let s3_mnt_path4 = "/a".into();

        let s3_path5 = "s3://flink/savepoints/ab/cde".into();
        let s3_mnt_path5 = "/a/b/c".into();

        let s3_path6 = "s3://flink/savepoi".into();
        let s3_mnt_path6 = "/c/d/e".into();

        let s3_path7 = "s3://flink2".into();
        let s3_mnt_path7 = "/s3".into();

        let hdfs_mnt_path = "/xyz".into();
        let hdfs_path = "hdfs://test-cluster:1234/x/y//z//".into();

        let hdfs_mnt_path2 = "/".into();
        let hdfs_path2 = "hdfs://test-cluster:1234/x/y//z//".into();
        // Create a MasterClient for submitting a load request
        let fs_context = Arc::new(FsContext::with_rt(cluster_conf.clone(), rt.clone())?);
        let client = FsClient::new(fs_context);
        let mut ufs_manager = UfsManager::new(Arc::new(client.clone()));

        //umount all exits mount points
        let exists_mnts = client.get_mount_table().await?;
        for mnt in exists_mnts.mount_table {
            info!("Unmounting existing mount point: {}", mnt.mount_id);
            let path = mnt.cv_path.into();
            let umount_resp = client.umount(&path).await;
            assert!(umount_resp.is_ok(), "{}", umount_resp.unwrap_err());
        }

        let configs = get_s3_test_config().await;
        let s3_mnt_opt = MountOptions::builder().set_properties(configs).build();
        let mount_resp = client
            .mount(&s3_path, &s3_mnt_path, s3_mnt_opt.clone())
            .await;
        info!("S3 MountResp: {:?}", mount_resp);
        assert!(mount_resp.is_ok(), "mount should success");

        let mount_resp = client
            .mount(&s3_path7, &s3_mnt_path7, s3_mnt_opt.clone())
            .await;
        info!("S3 MountResp: {:?}", mount_resp);
        assert!(mount_resp.is_ok(), "mount should success",);

        let configs = get_hdfs_test_config().await;
        let hdfs_mnt_opt = MountOptions::builder().set_properties(configs).build();
        let mount_resp = client
            .mount(&hdfs_path, &hdfs_mnt_path, hdfs_mnt_opt.clone())
            .await;
        info!("Hdfs MountResp: {:?}", mount_resp);
        assert!(mount_resp.is_ok(), "mount should success");

        // forbidden mount to root
        let mount_resp = client
            .mount(&hdfs_path2, &hdfs_mnt_path2, hdfs_mnt_opt.clone())
            .await;
        info!("Hdfs MountResp: {:?}", mount_resp);
        assert!(mount_resp.is_err(), "mount should failed");

        //test get curvine path from ufs after mount
        let src_path = CurvineURI::new("s3://flink/savepoints/ab/cde")?;
        let target_path = ufs_manager.get_curvine_path(&src_path).await;
        assert!(target_path.is_ok());
        let target_path = target_path.unwrap();
        assert!(target_path == "/a/b/cde");
        info!("target_path: {:?}", target_path);

        let src_path = CurvineURI::new("s3://flink2/a.txt")?;
        let target_path = ufs_manager.get_curvine_path(&src_path).await;
        let target_path = target_path.unwrap();
        assert!(
            target_path == "/s3/a.txt",
            "actual target_path is {}",
            target_path
        );
        info!("target_path: {:?}", target_path);

        let src_path = CurvineURI::new("hdfs://test-cluster:1234/x/y/z")?;
        let target_path = ufs_manager.get_curvine_path(&src_path).await;
        let target_path = target_path.unwrap();
        assert!(target_path == "/xyz", "target_path is {}", target_path);
        info!("target_path: {:?}", target_path);

        // monut s3 again
        let mount_resp = client
            .mount(&s3_path, &s3_mnt_path, s3_mnt_opt.clone())
            .await;
        info!("MountResp: {:?}", mount_resp);
        assert!(mount_resp.is_err(), "{}", mount_resp.unwrap_err());

        let mount_resp = client
            .mount(&s3_path2, &s3_mnt_path2, s3_mnt_opt.clone())
            .await;
        info!("MountResp: {:?}", mount_resp);
        assert!(mount_resp.is_err(), "{}", mount_resp.unwrap_err());

        let mount_resp = client
            .mount(&s3_path3, &s3_mnt_path3, s3_mnt_opt.clone())
            .await;
        info!("MountResp: {:?}", mount_resp);
        assert!(mount_resp.is_err(), "{}", mount_resp.unwrap_err());

        let mount_resp = client
            .mount(&s3_path4, &s3_mnt_path4, s3_mnt_opt.clone())
            .await;
        info!("MountResp: {:?}", mount_resp);
        assert!(mount_resp.is_err(), "{}", mount_resp.unwrap_err());

        let mount_resp = client
            .mount(&s3_path5, &s3_mnt_path5, s3_mnt_opt.clone())
            .await;
        info!("MountResp: {:?}", mount_resp);
        assert!(mount_resp.is_err(), "{}", mount_resp.unwrap_err());

        let mount_resp = client
            .mount(&s3_path6, &s3_mnt_path6, s3_mnt_opt.clone())
            .await;
        info!("MountResp: {:?}", mount_resp);
        assert!(mount_resp.is_ok(), "{}", "mount should success");
        client.umount(&s3_mnt_path6).await?;

        // get mountpoint
        let mountpoint_resp = client.get_mount_info(&s3_path).await?;
        info!("MountPoint: {:?}", mountpoint_resp);
        let mountpoint = mountpoint_resp.unwrap();
        assert_eq!(
            mountpoint.cv_path,
            s3_mnt_path.path(),
            "mnt path should be {}, actual {}",
            s3_mnt_path.path(),
            mountpoint.cv_path
        );

        let s3_path = "s3://flink/savepoints/ab".into();
        let mountpoint_resp = client.get_mount_info(&s3_path).await?;
        info!("MountPoint: {:?}", mountpoint_resp);
        let mountpoint = mountpoint_resp.unwrap();
        assert_eq!(
            mountpoint.cv_path,
            s3_mnt_path.path(),
            "mnt path should be {}, actual {}",
            s3_mnt_path.path(),
            mountpoint.cv_path
        );

        let resp = client.get_mount_table().await?;
        info!("MountTable: {:?}", resp);
        let mount_table = resp.mount_table;
        assert_eq!(mount_table.len(), 3);

        let umount_resp = client.umount(&s3_mnt_path).await;
        info!("UmountResp: {:?}", umount_resp);
        assert!(umount_resp.is_ok(), "umount should be success");

        //umount again
        let umount_resp = client.umount(&s3_mnt_path).await;
        info!("UmountResp: {:?}", umount_resp);
        assert!(umount_resp.is_err(), "{}", umount_resp.unwrap_err());

        let invalid_mnt_path = "/b/c".into();
        let umount_resp = client.umount(&invalid_mnt_path).await;
        info!("UmountResp: {:?}", umount_resp);
        assert!(umount_resp.is_err(), "{}", umount_resp.unwrap_err());

        Ok(())
    })
}

// Mount table scale + version negotiation: build 1000+ mount points, then
// compare a full pull against a negotiated no-change pull. Self-contained
// (file:// UFS paths) so it runs without external S3/HDFS. Timings are logged
// for the perf record and only sanity-asserted (negotiated must not be slower
// than full) to stay robust on loaded CI machines; correctness is asserted
// strictly.
#[test]
fn test_mount_table_scale_version_negotiation_perf() -> CommonResult<()> {
    Logger::default();
    let testing = Testing::builder().default().build()?;
    testing.start_cluster()?;
    let cluster_conf = testing.get_active_cluster_conf()?;

    let client_rt = Arc::new(cluster_conf.client_rpc_conf().create_runtime());
    let rt = client_rt.clone();

    client_rt.block_on(async move {
        let fs_context = Arc::new(FsContext::with_rt(cluster_conf.clone(), rt.clone())?);
        let client = FsClient::new(fs_context);

        // Clean leftovers so the mount counts below stay deterministic.
        let exists = client.get_mount_table().await?;
        for mnt in exists.mount_table {
            let path = mnt.cv_path.into();
            let _ = client.umount(&path).await;
        }

        let scale: i64 = 1000;
        let mut props = HashMap::new();
        props.insert(
            "s3.endpoint_url".to_string(),
            "http://127.0.0.1:9000".to_string(),
        );
        props.insert(
            "s3.credentials.access".to_string(),
            "test-access-key".to_string(),
        );
        props.insert(
            "s3.credentials.secret".to_string(),
            "test-secret-key".to_string(),
        );
        let mnt_opt = MountOptions::builder().set_properties(props).build();

        for i in 0..scale {
            let ufs_path = format!("file:///tmp/curvine-scale-mount-{}", i).into();
            let cv_path = format!("/scale/dir-{}/mnt-{}", i / 100, i).into();
            client
                .mount(&ufs_path, &cv_path, mnt_opt.clone())
                .await
                .unwrap_or_else(|e| panic!("mount #{} failed: {}", i, e));
        }
        info!("mounted {} mount points", scale);

        const ROUNDS: u32 = 5;

        // Prime pull (no known version): captures the current version and
        // asserts the master reports one.
        let rep = client.get_mount_table_versioned(None).await?;
        assert_eq!(rep.mount_table.len() as i64, scale);
        let last_version: Option<u64> = rep.version.map(|v| v as u64);
        assert!(
            last_version.is_some(),
            "master must report the mount table version"
        );

        // 1) Full pulls without a known version — the legacy/baseline shape.
        let mut full_ms: u128 = 0;
        for _ in 0..ROUNDS {
            let start = Instant::now();
            let rep = client.get_mount_table_versioned(None).await?;
            full_ms += start.elapsed().as_millis();
            assert_eq!(rep.mount_table.len() as i64, scale);
        }
        info!(
            "GetMountTable full pull x{} avg {} ms ({} mounts)",
            ROUNDS,
            full_ms / ROUNDS as u128,
            scale
        );

        // 2) Legacy request (no known_version field set) still gets the table.
        let rep = client.get_mount_table().await?;
        assert_eq!(rep.mount_table.len() as i64, scale);

        // 3) Negotiated pulls with the current version — no change since the
        // last pull, so the master replies with an empty payload.
        let mut nego_ms: u128 = 0;
        for _ in 0..ROUNDS {
            let start = Instant::now();
            let rep = client.get_mount_table_versioned(last_version).await?;
            nego_ms += start.elapsed().as_millis();
            assert!(
                rep.mount_table.is_empty(),
                "unchanged table must return an empty payload"
            );
            assert_eq!(rep.version.map(|v| v as u64), last_version);
        }
        info!(
            "GetMountTable negotiated pull x{} avg {} ms",
            ROUNDS,
            nego_ms / ROUNDS as u128
        );
        assert!(
            nego_ms <= full_ms,
            "negotiated pull ({}ms) should not be slower than full pull ({}ms)",
            nego_ms,
            full_ms
        );

        // 4) A stale known version (mount happened elsewhere) → full table and
        // a bumped version in the same response.
        let ufs_path = format!("file:///tmp/curvine-scale-mount-{}", scale).into();
        let cv_path = format!("/scale/dir-{}/mnt-{}", scale / 100, scale).into();
        client.mount(&ufs_path, &cv_path, mnt_opt.clone()).await?;
        let rep = client.get_mount_table_versioned(last_version).await?;
        assert_eq!(rep.mount_table.len() as i64, scale + 1);
        assert!(rep.version.unwrap() as u64 > last_version.unwrap());

        // 5) A version the master never issued (restart gap / HA mismatch)
        // must fall through to a full table — no false "no change".
        let rep = client.get_mount_table_versioned(Some(987654321)).await?;
        assert_eq!(rep.mount_table.len() as i64, scale + 1);

        // Cleanup so other tests start from a clean table.
        let exists = client.get_mount_table().await?;
        for mnt in exists.mount_table {
            let path = mnt.cv_path.into();
            let _ = client.umount(&path).await;
        }

        Ok(())
    })
}

async fn get_s3_test_config() -> HashMap<String, String> {
    let mut config = HashMap::new();

    // Configuration of MinIO as S3 compatible service
    config.insert(
        "s3.endpoint_url".to_string(),
        "http://s3v2.dg-access-test.wanyol.com".to_string(),
    );
    config.insert("s3.region_name".to_string(), "cn-south-1".to_string());
    config.insert(
        "s3.credentials.access".to_string(),
        "T6A4jOFA9TssTrn2K1A6pFDT-xwFiFQbfS2JxZ5D".to_string(),
    );
    config.insert(
        "s3.credentials.secret".to_string(),
        "4ewVYsv5MFn2KPd6oYmKRQgMCz22LSlqF0Zl2KZz".to_string(),
    );
    config.insert("s3.path_style".to_string(), "true".to_string());

    config
}

async fn get_hdfs_test_config() -> HashMap<String, String> {
    let mut config = HashMap::new();
    config.insert("nameserver".to_string(), "test-cluster".to_string());
    config
}

fn enable_dir_space_with_mount_tracking(conf: &mut curvine_config::ClusterConf) {
    conf.master.dir_space.enabled = true;
    conf.master.dir_space.global_scan_interval = "500ms".to_string();
    conf.master.dir_space.global_scan_timeout = "500ms".to_string();
    conf.master.dir_space.tracked_scan_interval = "500ms".to_string();
    conf.master.dir_space.tracked_scan_timeout = "500ms".to_string();
    conf.master.dir_space.auto_track_mounts = false;
    conf.master.dir_space.tracked_paths = vec!["/dir_space_mnt_it".to_string()];
    conf.master.init().expect("dir-space mount test config");
}

async fn wait_for_dir_space_snapshot(
    fs: &UnifiedFileSystem,
    path: &Path,
) -> FsResult<ContentSummary> {
    for _ in 0..160 {
        match fs.try_get_content_summary_snapshot(path).await {
            Ok(Some(summary)) if summary.snapshot.is_some() => return Ok(summary),
            Ok(Some(_)) | Ok(None) | Err(FsError::SnapshotNotReady(_)) => {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(err) => return Err(err),
        }
    }
    Err(FsError::snapshot_not_ready(path.to_string()))
}

#[test]
fn dir_space_tracked_fs_mode_mount_snapshot() -> FsResult<()> {
    let testing = Testing::builder()
        .default()
        .mutate_conf(enable_dir_space_with_mount_tracking)
        .build()?;
    testing.start_cluster()?;
    let rt = Arc::new(AsyncRuntime::single());
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;
    let ufs_base = testing.ufs_path.clone();

    rt.block_on(async move {
        let ufs = Path::from_str(format!("{}/dir_space_mount", ufs_base))?;
        let cv_mount = Path::from_str("/dir_space_mnt_it")?;
        let opts = MountOptionsBuilder::new()
            .write_type(WriteType::FsMode)
            .build();
        fs.mount(&ufs, &cv_mount, opts).await?;
        fs.mkdir(&Path::from_str("/dir_space_mnt_it/sub")?, true)
            .await?;
        fs.create(&Path::from_str("/dir_space_mnt_it/sub/file")?, true)
            .await?;

        let summary = wait_for_dir_space_snapshot(&fs, &cv_mount).await?;
        assert!(summary.file_count >= 1);
        assert!(summary.directory_count >= 2);
        Ok(())
    })
}
