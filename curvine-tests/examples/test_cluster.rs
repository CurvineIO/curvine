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

use curvine_common::conf::ClusterConf;
use curvine_tests::Testing;
use orpc::common::Logger;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;

// Start a test cluster and use it for local development and testing.
fn main() -> CommonResult<()> {
    Logger::default();
    let mut conf = ClusterConf::from("etc/curvine-cluster.toml")?;
    conf.format_master = true;
    conf.format_worker = true;
    conf.journal.snapshot_interval = "10s".to_owned();
    conf.master.min_block_size = 1024 * 1024;
    conf.master.ttl_bucket_interval = "1m".to_string();
    conf.master.ttl_checker_interval = "1m".to_string();
    conf.master.init()?;

    let testing = Testing {
        worker_num: 3,
        ..Default::default()
    };
    testing.start_cluster_with_conf(&conf)?;

    let rt = AsyncRuntime::single();

    // Wait for the program to exit
    rt.block_on(async move {
        let ctrl_c = tokio::signal::ctrl_c();

        #[cfg(target_os = "linux")]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut unix_sig = signal(SignalKind::terminate()).unwrap();
            tokio::select! {
                _ = ctrl_c => {
                    println!("Receive ctrl_c signal, test cluster");
                }

               _ = unix_sig.recv() => {
                    println!("Received SIGTERM, shutting test cluster");
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            ctrl_c.await.unwrap();
            println!("Receive ctrl_c signal, test cluster");
        }
    });

    Ok(())
}
