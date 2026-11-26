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
use curvine_core_error::CommonResult;
use curvine_fs_api::CurvineURI;
use curvine_model::MountOptions;
use curvine_runtime::common::Logger;
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::common::ufs_manager::UfsManager;
use curvine_tests::Testing;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;

// Test the load function of curvine-client
#[test]
fn test_mount_umount_s3_hdfs_and_path_mapping() -> CommonResult<()> {
    Logger::default();

    // Credentials are never committed to the repository, see get_s3_test_config.
    // Without them there is nothing to mount, so skip instead of failing.
    let s3_config = match get_s3_test_config() {
        Ok(config) => config,
        Err(missing) => {
            println!("skipping test_mount_umount_s3_hdfs_and_path_mapping: {missing} not set");
            return Ok(());
        }
    };

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

        let s3_mnt_opt = MountOptions::builder().set_properties(s3_config).build();
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

const S3_ENV_ENDPOINT: &str = "MOUNT_TEST_S3_ENDPOINT";
const S3_ENV_REGION: &str = "MOUNT_TEST_S3_REGION";
const S3_ENV_ACCESS_KEY: &str = "MOUNT_TEST_S3_ACCESS_KEY";
const S3_ENV_SECRET_KEY: &str = "MOUNT_TEST_S3_SECRET_KEY";

/// Build the S3 compatible (MinIO or object storage) test configuration from the
/// environment. Credentials must never be committed to the repository:
///
/// ```bash
/// export MOUNT_TEST_S3_ENDPOINT=http://127.0.0.1:9000
/// export MOUNT_TEST_S3_REGION=cn-south-1
/// export MOUNT_TEST_S3_ACCESS_KEY=...
/// export MOUNT_TEST_S3_SECRET_KEY=...
/// cargo test -p curvine-tests --test mount_test
/// ```
///
/// Returns the missing variable names when the credential pair is incomplete, so the
/// skip notice points at the variable the caller actually has to export.
fn get_s3_test_config() -> Result<HashMap<String, String>, String> {
    match (env_value(S3_ENV_ACCESS_KEY), env_value(S3_ENV_SECRET_KEY)) {
        (Some(access), Some(secret)) => {
            let mut config = HashMap::new();
            config.insert(
                "s3.endpoint_url".to_string(),
                std::env::var(S3_ENV_ENDPOINT)
                    .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string()),
            );
            config.insert(
                "s3.region_name".to_string(),
                std::env::var(S3_ENV_REGION).unwrap_or_else(|_| "us-east-1".to_string()),
            );
            config.insert("s3.credentials.access".to_string(), access);
            config.insert("s3.credentials.secret".to_string(), secret);
            config.insert("s3.path_style".to_string(), "true".to_string());

            Ok(config)
        }
        (access, secret) => {
            let missing: Vec<&str> = [
                (S3_ENV_ACCESS_KEY, access.is_none()),
                (S3_ENV_SECRET_KEY, secret.is_none()),
            ]
            .into_iter()
            .filter(|(_, is_missing)| *is_missing)
            .map(|(name, _)| name)
            .collect();

            Err(missing.join(" and "))
        }
    }
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

async fn get_hdfs_test_config() -> HashMap<String, String> {
    let mut config = HashMap::new();
    config.insert("nameserver".to_string(), "test-cluster".to_string());
    config
}
