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

use curvine_client::file::CurvineFileSystem;
use curvine_common::fs::{Path, Writer};
use curvine_tests::Testing;
use log::info;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn get_fs() -> CurvineFileSystem {
    let testing = Testing::builder().default().build().unwrap();
    testing.start_cluster().unwrap();
    let mut conf = testing.get_active_cluster_conf().unwrap();
    conf.client.rpc_timeout_ms = 3 * 1000;
    conf.client.master_conn_pool_size = 1;
    conf.master.io_timeout = "3s".to_string();
    conf.worker.io_timeout = "3s".to_string();
    conf.client.master_conn_pool_size = 1;
    conf.client.short_circuit = false;

    let rt = Arc::new(AsyncRuntime::single());
    testing.get_fs(Some(rt), Some(conf)).unwrap()
}

// Test the master request timeout.
// The client service recycles the connection in 3 seconds, and retrys the request after 5 seconds to correctly create a new connection.
#[test]
fn client_rpc_timeout() -> CommonResult<()> {
    let fs = get_fs();
    let rt = fs.clone_runtime();

    let path = Path::from_str("/client_rpc_timeout")?;
    rt.block_on(async move {
        fs.mkdir(&path, true).await?;
        tokio::time::sleep(Duration::from_millis(5 * 1000)).await;

        let status = fs.get_status(&path).await?;
        info!("status {:?}", status);
        Ok(())
    })
}

#[test]
fn worker_data_timeout() -> CommonResult<()> {
    let fs = get_fs();
    let rt = fs.clone_runtime();

    thread::sleep(Duration::from_secs(3));

    let path = Path::from_str("/worker_data_timeout.data")?;
    rt.block_on(async move {
        let mut writer = fs.create(&path, true).await?;

        writer.write("123".as_bytes()).await?;
        writer.flush().await?;
        tokio::time::sleep(Duration::from_millis(5 * 1000)).await;

        writer.write("abc".as_bytes()).await?;
        writer.complete().await?;
        Ok(())
    })
}
