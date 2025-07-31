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
// umount -f /curvine-fuse; cargo run --bin curvine-fuse -- --conf /server/conf/curvine-cluster.toml -d
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
    // Mount the mount point, mount the file system to a directory of the machine.
    #[arg(long, default_value = "/curvine-fuse")]
    pub mnt_path: String,

    // Specify the root path of the mount point to access the file system, default "/"
    #[arg(long, default_value = "/")]
    pub(crate) fs_path: String,

    // Number of mount points
    #[arg(long, default_value = "1")]
    pub mnt_number: usize,

    // Specify the root path of the mount point to access the file system, default "/"
    #[arg(short, long, action = clap::ArgAction::SetTrue, default_value = "false")]
    pub(crate) debug: bool,

    // Configuration file path.
    #[arg(short, long, default_value = "conf/curvine-cluster.toml")]
    pub(crate) conf: String,

    #[arg(short, long)]
    pub(crate) options: Vec<String>,
}

impl FuseArgs {
    // parse the cluster configuration file.
    pub fn get_conf(&self) -> CommonResult<ClusterConf> {
        let mut conf = ClusterConf::from(&self.conf)?;

        // Command line parameters override configuration file parameters.
        conf.fuse.mnt_path = self.mnt_path.clone();
        conf.fuse.fs_path = self.fs_path.clone();
        conf.fuse.mnt_number = self.mnt_number;
        conf.fuse.debug = self.debug;

        if !self.options.is_empty() {
            conf.fuse.fuse_opts = self.options.clone()
        } else {
            conf.fuse.fuse_opts = Self::default_mnt_opts();
        }

        if !conf.fuse.direct_io {
            return err_box!("Currently only supports direct_io");
        }

        if conf.fuse.kernel_cache {
            return err_box!("Kernel cache is not currently supported");
        }

        Ok(conf)
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
