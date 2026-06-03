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

use clap::Parser;
use curvine_fuse::cli::FuseMountArgs;
use curvine_fuse::fs::CurvineFileSystem;
use curvine_fuse::session::FuseSession;
use curvine_fuse::web_server::WebServer;
use orpc::common::Logger;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::sync::Arc;

// fuse mount.
// Debugging, after starting the cluster, execute the following naming, mount fuse
// umount -f /curvine-fuse; cargo run --bin curvine-fuse -- --conf /server/conf/curvine-cluster.toml
fn main() -> CommonResult<()> {
    let args = FuseMountArgs::parse();
    println!("fuse args {:?}", args);

    // Ignore SIGPIPE to prevent unexpected termination when peer closes
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }

    let cluster_conf = args.get_conf()?;
    Logger::init(cluster_conf.fuse.log.clone());
    cluster_conf.print();

    let rt = Arc::new(AsyncRuntime::new(
        "curvine-fuse",
        cluster_conf.fuse.io_threads,
        cluster_conf.fuse.worker_threads,
    ));

    let fuse_rt = rt.clone();

    rt.block_on(async move {
        let fs = CurvineFileSystem::new(cluster_conf, fuse_rt.clone()).unwrap();
        let conf = fs.conf().clone();

        let node_state = fs.state().clone();
        let web_port = conf.web_port;
        fuse_rt.spawn(async move {
            if let Err(e) = WebServer::start(web_port, node_state).await {
                log::error!("Failed to start metrics server: {}", e);
            }
        });

        let mut session = FuseSession::new(fuse_rt.clone(), fs, conf).await.unwrap();
        session.run().await
    })?;

    Ok(())
}
