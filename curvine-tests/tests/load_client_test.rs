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

#![allow(unused)]

use bytes::BytesMut;
use curvine_client::file::{CurvineFileSystem, FsClient, FsContext};
use curvine_client::LoadClient;
use curvine_common::fs::{Path, Reader};
use curvine_common::proto::LoadState;
use curvine_common::proto::MountOptions;
use curvine_tests::Testing;
use log::info;
use orpc::common::Logger;
use orpc::runtime::RpcRuntime;
use orpc::CommonResult;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const UFS_PATH: &str =
    "s3://flink/savepoints/006810e8385c1eade44cfe618fb3ef72/savepoint-006810-6d218be90e9f";
const TEST_FILE: &str = "s3://flink/savepoints/006810e8385c1eade44cfe618fb3ef72/savepoint-006810-6d218be90e9f/03754607-a9bd-4fe9-8938-214066c79525";
// Test the load function of curvine-client
#[test]
fn load_client_test() -> CommonResult<()> {
    Logger::default();

    // Create a test cluster configuration
    let conf = Testing::get_cluster_conf()?;
    println!("cluster_conf: {:?}", conf);

    // Create a test cluster configuration
    let client_rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let rt_clone = client_rt.clone();
    let mount_path = "/flink";
    let mut curvine_path = String::new();

    client_rt.block_on(async move {
        // Create MasterClient for submitting load request
        let fs_context = Arc::new(FsContext::with_rt(conf.clone(), rt_clone.clone())?);
        let client = FsClient::new(fs_context);
        let configs = get_s3_test_config().await;
        let mnt_opt = MountOptions {
            update: false,
            properties: configs,
            auto_cache: false,
            cache_ttl_secs: None,
            consistency_config: None,
        };
        let ret = client.umount(mount_path).await;
        let mount_resp = client.mount(UFS_PATH, mount_path, mnt_opt.clone()).await;
        info!("S3 MountResp: {:?}", mount_resp);
        assert!(mount_resp.is_ok(), "mount should success");

        // Create MasterClient for submitting load request
        let client = LoadClient::new(Arc::new(client))?;
        let fs = CurvineFileSystem::with_rt(conf, rt_clone.clone())?;
        // Submit a load request
        info!("Submit a load request: {}", TEST_FILE);

        let load_response = client
            .submit_load(TEST_FILE, Some("300s".to_string()), Some(true))
            .await?;
        curvine_path = load_response.target_path.clone();

        info!("Load response: {}", load_response);
        assert_ne!(load_response.job_id, "", "The Load request should succeed");
        assert!(!load_response.job_id.is_empty(), "A valid job_id should be returned");


        // Query the load status
        let job_id = load_response.job_id;
        let mut loaded = false;
        let mut retry_count = 0;
        let max_retries = 100;

        while !loaded && retry_count < max_retries {
            let status = client.get_load_status(&job_id).await?;
            info!("Loading status: {}", status);
            if status.state == LoadState::Completed as i32 {
                loaded = true;
                info!("The file load is complete");

                let read_len = read(&fs, curvine_path.as_str()).await?;
                let expected_size = status.metrics.unwrap().total_size.unwrap_or_default();
                assert_eq!(
                    read_len, expected_size,
                    "Read length {} is not equal to the expected size {}",
                    read_len, expected_size
                );
                break;
            } else if status.state == LoadState::Failed as i32 {
                return Err(format!("The loading task failed: {}", status.message.unwrap_or_default()).into());
            } else {
                // Wait for a while before trying again
                tokio::time::sleep(Duration::from_secs(5)).await;
                retry_count += 1;
                // Cancel the task immediately
                // client.cancel_load(&job_id).await?;
                info!("A cancellation request has been sent, waiting for the task status to be updated");
            }
        }

        if !loaded {
            return Err("The loading task timed out".into());
        }

        let path = Path::from_str(curvine_path.as_str())?;
        if fs.exists(&path).await? {
            fs.delete(&path, true).await?;
        }
        Ok(())
    })
}

async fn read(fs: &CurvineFileSystem, path_str: &str) -> CommonResult<i64> {
    let path = Path::from_str(path_str)?;
    let mut reader = fs.open(&path).await?;

    let mut len: usize = 0;
    let mut buf = BytesMut::zeroed(1024);
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        len += n;
    }
    reader.complete().await?;

    Ok(len as i64)
}
async fn test_cancel_load(client: &LoadClient, file_path: &str) -> CommonResult<()> {
    info!("Start testing the unload task: {}", file_path);

    // Submit a loading task
    let load_response = client
        .submit_load(file_path, Some("30s".to_string()), Some(true))
        .await?;

    info!(
        "The upload task is submitted successfully, and you are ready to cancel it: {}",
        load_response
    );
    assert_ne!(load_response.job_id, "", "The Load request should succeed");

    // Cancel the task immediately
    let job_id = load_response.job_id;
    client.cancel_load(&job_id).await?;
    info!("A cancellation request has been sent, waiting for the task status to be updated");

    // Verify that the task status changes to Canceled
    let mut canceled = false;
    let mut retry_count = 0;
    let max_retries = 20;

    while !canceled && retry_count < max_retries {
        let status = client.get_load_status(&job_id).await?;
        info!("The status of the load after the cancellation: {}", status);

        if status.state == LoadState::Canceled as i32 {
            canceled = true;
            info!("The task was successfully canceled");
            break;
        } else if status.state == LoadState::Failed as i32 {
            return Err(format!(
                "The loading task failed: {}",
                status.message.unwrap_or_default()
            )
            .into());
        } else if status.state == LoadState::Completed as i32 {
            info!("The task was completed before it was canceled");
            break;
        } else {
            // Wait for a while and try again
            tokio::time::sleep(Duration::from_secs(1)).await;
            retry_count += 1;
        }
    }

    if !canceled {
        info!("The task was not successfully canceled and may have been completed or in progress");
    }

    Ok(())
}

async fn get_s3_test_config() -> HashMap<String, String> {
    let mut config = HashMap::new();

    // MinIO is configured as an S3 compliant service
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
