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
use orpc::io::net::InetAddr;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::{err_box, CommonResult};
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

    rt.spawn(async move {
        if let Err(e) = WebServer::start(cluster_conf.fuse.web_port).await {
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
#[derive(Debug, Parser, Clone)]
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

        // FUSE configuration - only override if command line values are specified
        if let Some(mnt_path) = &self.mnt_path {
            conf.fuse.mnt_path = mnt_path.clone();
        }
        if let Some(fs_path) = &self.fs_path {
            conf.fuse.fs_path = fs_path.clone();
        }
        if let Some(mnt_number) = self.mnt_number {
            conf.fuse.mnt_number = mnt_number;
        }
        if self.debug {
            conf.fuse.debug = true;
        }

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

        if let Some(direct_io) = self.direct_io {
            conf.fuse.direct_io = direct_io;
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

        if let Some(web_port) = self.web_port {
            conf.fuse.web_port = web_port;
        }

        if let Some(master_addrs) = &self.master_addrs {
            let mut vec = vec![];
            for node in master_addrs.split(",") {
                let tmp: Vec<&str> = node.split(":").collect();
                if tmp.len() != 2 {
                    return err_box!("wrong format master_addrs {}", master_addrs);
                }
                let hostname = tmp[0].to_string();
                let port: u16 = tmp[1].parse()?;
                vec.push(InetAddr::new(hostname, port));
            }
            conf.client.master_addrs = vec;
        }

        // FUSE options - override if provided
        if !self.options.is_empty() {
            conf.fuse.fuse_opts = self.options.clone()
        } else {
            conf.fuse.fuse_opts = Self::default_mnt_opts();
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
