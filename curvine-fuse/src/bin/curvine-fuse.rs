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
use curvine_fuse::fs::CurvineFileSystem;
use curvine_fuse::session::FuseSession;
use orpc::common::Logger;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::{err_box, CommonResult};
use std::sync::Arc;

// fuse mount.
// Debugging, after starting the cluster, execute the following naming, mount fuse
// umount -f /curvine-fuse; cargo run --bin curvine-fuse -- --master-hostname localhost --master-rpc-port 8995 --mnt-path /curvine-fuse -d
fn main() -> CommonResult<()> {
    let args = FuseArgs::parse();
    println!("fuse args {:?}", args);

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
        let mut session = FuseSession::new(fuse_rt.clone(), fs, conf).await.unwrap();
        session.run().await
    })?;

    Ok(())
}

// Mount command function parameters
#[derive(Debug, Parser, Clone)]
pub struct FuseArgs {
    // Master hostname (optional)
    #[arg(long, help = "Master hostname (optional)", default_value = "localhost")]
    pub master_hostname: String,

    // Master RPC port (optional)
    #[arg(long, help = "Master RPC port (optional)", default_value = "8995")]
    pub master_rpc_port: u16,

    // Mount the mount point, mount the file system to a directory of the machine.
    #[arg(long, default_value = "/curvine-fuse")]
    pub mnt_path: String,

    // Specify the root path of the mount point to access the file system, default "/"
    #[arg(long, default_value = "/")]
    pub(crate) fs_path: String,

    // Number of mount points
    #[arg(long, default_value = "1")]
    pub mnt_number: usize,

    // Debug mode
    #[arg(short, long, action = clap::ArgAction::SetTrue, default_value = "false")]
    pub(crate) debug: bool,

    // Configuration file path (optional)
    #[arg(
        short,
        long,
        help = "Configuration file path (optional)",
        default_value = "conf/curvine-cluster.toml"
    )]
    pub(crate) conf: String,

    // Master web port (optional)
    #[arg(long, help = "Master web port (optional)")]
    pub master_web_port: Option<u16>,

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

    // Cache settings
    #[arg(long, help = "Enable auto cache (optional)")]
    pub auto_cache: Option<bool>,

    #[arg(long, help = "Enable direct IO (optional)")]
    pub direct_io: Option<bool>,

    #[arg(long, help = "Enable kernel cache (optional)")]
    pub kernel_cache: Option<bool>,

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

    // FUSE options
    #[arg(short, long)]
    pub(crate) options: Vec<String>,
}

impl FuseArgs {
    // parse the cluster configuration file.
    pub fn get_conf(&self) -> CommonResult<ClusterConf> {
        let mut conf = match ClusterConf::from(&self.conf) {
            Ok(c) => {
                println!("Loaded configuration from {}", self.conf);
                c
            }
            Err(e) => {
                eprintln!("Warning: Failed to load config file '{}': {}", self.conf, e);
                eprintln!("Using default configuration");
                Self::create_default_conf()
            }
        };

        // Master configuration - only override if different from defaults
        if self.master_hostname != "localhost" {
            conf.master.hostname = self.master_hostname.clone();
        }

        if self.master_rpc_port != 8995 {
            conf.master.rpc_port = self.master_rpc_port;
        }

        // Optional master web port - only override if specified
        if let Some(web_port) = self.master_web_port {
            conf.master.web_port = web_port;
        }

        // Client configuration (for connecting to master) - use current master config
        conf.client.master_addrs = vec![orpc::io::net::InetAddr::new(
            &conf.master.hostname,
            conf.master.rpc_port,
        )];

        // FUSE configuration - override with command line values
        // These have default values from clap, so always override
        conf.fuse.mnt_path = self.mnt_path.clone();
        conf.fuse.fs_path = self.fs_path.clone();
        conf.fuse.mnt_number = self.mnt_number;
        conf.fuse.debug = self.debug;

        // Optional FUSE parameters - only override if specified
        if let Some(io_threads) = self.io_threads {
            conf.fuse.io_threads = io_threads;
        }

        if let Some(worker_threads) = self.worker_threads {
            conf.fuse.worker_threads = worker_threads;
        }

        if let Some(mnt_per_task) = self.mnt_per_task {
            conf.fuse.mnt_per_task = mnt_per_task;
        }

        if let Some(clone_fd) = self.clone_fd {
            conf.fuse.clone_fd = clone_fd;
        }

        if let Some(fuse_channel_size) = self.fuse_channel_size {
            conf.fuse.fuse_channel_size = fuse_channel_size;
        }

        if let Some(stream_channel_size) = self.stream_channel_size {
            conf.fuse.stream_channel_size = stream_channel_size;
        }

        if let Some(auto_cache) = self.auto_cache {
            conf.fuse.auto_cache = auto_cache;
        }

        if let Some(direct_io) = self.direct_io {
            conf.fuse.direct_io = direct_io;
        }

        if let Some(kernel_cache) = self.kernel_cache {
            conf.fuse.kernel_cache = kernel_cache;
        }

        if let Some(cache_readdir) = self.cache_readdir {
            conf.fuse.cache_readdir = cache_readdir;
        }

        if let Some(entry_timeout) = self.entry_timeout {
            conf.fuse.entry_timeout = entry_timeout;
        }

        if let Some(attr_timeout) = self.attr_timeout {
            conf.fuse.attr_timeout = attr_timeout;
        }

        if let Some(negative_timeout) = self.negative_timeout {
            conf.fuse.negative_timeout = negative_timeout;
        }

        if let Some(max_background) = self.max_background {
            conf.fuse.max_background = max_background;
        }

        if let Some(congestion_threshold) = self.congestion_threshold {
            conf.fuse.congestion_threshold = congestion_threshold;
        }

        if let Some(node_cache_size) = self.node_cache_size {
            conf.fuse.node_cache_size = node_cache_size;
        }

        if let Some(node_cache_timeout) = &self.node_cache_timeout {
            conf.fuse.node_cache_timeout = node_cache_timeout.clone();
        }

        // FUSE options - override if provided
        if !self.options.is_empty() {
            conf.fuse.fuse_opts = self.options.clone()
        } else {
            conf.fuse.fuse_opts = Self::default_mnt_opts();
        }

        // Validate configuration
        if !conf.fuse.direct_io {
            return err_box!("Currently only supports direct_io");
        }

        if conf.fuse.kernel_cache {
            return err_box!("Kernel cache is not currently supported");
        }

        Ok(conf)
    }

    fn create_default_conf() -> ClusterConf {
        ClusterConf::default()
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
