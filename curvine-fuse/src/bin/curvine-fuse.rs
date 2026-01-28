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
use curvine_common::conf::ClusterConf;
use curvine_common::version;
use curvine_fuse::fs::CurvineFileSystem;
use curvine_fuse::session::FuseSession;
use curvine_fuse::web_server::WebServer;
use orpc::common::Logger;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use serde::Serialize;
use serde_json::json;
use serde_with::skip_serializing_none;
use std::sync::Arc;

// fuse mount.
// Debugging, after starting the cluster, execute the following naming, mount fuse
// umount -f /curvine-fuse; cargo run --bin curvine-fuse -- --conf /server/conf/curvine-cluster.toml
fn main() -> CommonResult<()> {
    let args = FuseArgs::parse();
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

    let web_port = cluster_conf.fuse.web_port;
    rt.spawn(async move {
        if let Err(e) = WebServer::start(web_port).await {
            tracing::error!("Failed to start metrics server: {}", e);
        }
    });

    rt.block_on(async move {
        let fs = CurvineFileSystem::new(cluster_conf, fuse_rt.clone()).unwrap();
        let conf = fs.conf().clone();
        let mut session = FuseSession::new(fuse_rt.clone(), fs, conf).await.unwrap();
        session.run().await
    })?;

    Ok(())
}

// Mount command function parameters
#[skip_serializing_none]
#[derive(Debug, Parser, Clone, Serialize)]
#[command(version = version::VERSION)]
pub struct FuseArgs {
    // Mount the mount point, mount the file system to a directory of the machine.
    #[arg(long, help = "Mount point path (default: /curvine-fuse)")]
    pub mnt_path: Option<String>,

    // Specify the root path of the mount point to access the file system, default "/"
    #[arg(long, help = "Remote filesystem path (default: /)")]
    pub(crate) fs_path: Option<String>,

    // Number of mount points
    #[arg(long, help = "Number of mount points (default: 1)")]
    pub mnt_number: Option<usize>,

    // Debug mode
    #[arg(short, long, action = clap::ArgAction::SetTrue, help = "Enable debug mode")]
    pub(crate) debug: bool,

    // Configuration file path (optional)
    #[arg(
        short,
        long,
        help = "Configuration file path (optional)",
        default_value = "conf/curvine-cluster.toml"
    )]
    pub(crate) conf: String,

    // IO threads (optional)
    #[arg(long, help = "IO threads (optional)")]
    pub io_threads: Option<usize>,

    // Worker threads (optional)
    #[arg(long, help = "Worker threads (optional)")]
    pub worker_threads: Option<usize>,

    // How many tasks can read and write data at each mount point
    #[arg(long, help = "Tasks per mount point (optional)")]
    pub mnt_per_task: Option<usize>,

    // Whether to enable the clone fd feature
    #[arg(long, help = "Enable clone fd feature (optional)")]
    pub clone_fd: Option<bool>,

    // Fuse request queue size
    #[arg(long, help = "FUSE channel size (optional)")]
    pub fuse_channel_size: Option<usize>,

    // Read and write file request queue size
    #[arg(long, help = "Stream channel size (optional)")]
    pub stream_channel_size: Option<usize>,

    #[arg(long, help = "Enable direct IO (optional)")]
    pub direct_io: Option<bool>,

    #[arg(long, help = "Cache readdir results (optional)")]
    pub cache_readdir: Option<bool>,

    // Timeout settings
    #[arg(long, help = "Entry timeout in seconds (optional)")]
    pub entry_timeout: Option<f64>,

    #[arg(long, help = "Attribute timeout in seconds (optional)")]
    pub attr_timeout: Option<f64>,

    #[arg(long, help = "Negative timeout in seconds (optional)")]
    pub negative_timeout: Option<f64>,

    // Performance settings
    #[arg(long, help = "Max background operations (optional)")]
    pub max_background: Option<u16>,

    #[arg(long, help = "Congestion threshold (optional)")]
    pub congestion_threshold: Option<u16>,

    // Node cache settings
    #[arg(long, help = "Node cache size (optional)")]
    pub node_cache_size: Option<u64>,

    #[arg(long, help = "Node cache timeout (e.g., '1h', '30m') (optional)")]
    pub node_cache_timeout: Option<String>,

    // Fuse web port
    #[arg(long, help = "Web server port (optional)")]
    pub web_port: Option<u16>,

    #[arg(long, help = "Master address (e.g., 'm1:8995,m2:8995'")]
    pub master_addrs: Option<String>,

    // FUSE options
    #[arg(short, long)]
    pub(crate) options: Vec<String>,
}

impl FuseArgs {
    // parse the cluster configuration file.
    pub fn get_conf(&self) -> CommonResult<ClusterConf> {
        let args_value = serde_json::to_value(self).unwrap();

        // Change the format of master_addrs from "m1:8995,..." to [{"hostname": "m1", "port": 8995}, ...]
        let master_addrs = if let Some(master_addrs_str) =
            args_value.get("master_addrs").and_then(|v| v.as_str())
        {
            master_addrs_str
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|addr| {
                    let addr = addr.trim();
                    let parts: Vec<&str> = addr.split(':').collect();

                    if parts.len() == 2 {
                        let hostname = parts[0];
                        let port = parts[1]
                            .parse::<u16>()
                            .map_err(|_| format!("Invalid port: {}", parts[1]))?;

                        Ok(json!({
                            "hostname": hostname,
                            "port": port
                        }))
                    } else {
                        Err(format!("Invalid address format: {}", addr))
                    }
                })
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        } else {
            Vec::new()
        };

        let args_json = json!({
            "fuse": args_value,
            "client": {
                "master_addrs": master_addrs
            }
        })
        .to_string();

        let conf = ClusterConf::from(&self.conf, Some(&args_json));
        println!("Loaded configuration from {}", &self.conf);
        conf
    }

    pub fn default_mnt_opts() -> Vec<String> {
        if cfg!(feature = "fuse3") {
            vec![
                "allow_other".to_string(),
                "async".to_string(),
                "auto_unmount".to_string(),
            ]
        } else {
            vec![
                "allow_other".to_string(),
                "async".to_string(),
                "direct_io".to_string(),
                "big_write".to_string(),
                "max_write=131072".to_string(),
            ]
        }
    }
}
