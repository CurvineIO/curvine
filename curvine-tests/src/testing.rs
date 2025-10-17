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
use curvine_client::file::CurvineFileSystem;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_server::test::MiniCluster;
use orpc::common::{FileUtils, Logger};
use orpc::io::LocalFile;
use orpc::runtime::Runtime;
use orpc::{err_box, CommonResult};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Parser, Clone)]
pub struct Testing {
    #[arg(long, default_value = "testing/curvine-cluster.toml")]
    pub conf: String,

    #[arg(long, default_value = "testing/s3.toml")]
    pub s3_conf: String,

    #[arg(long, default_value = "1")]
    pub master_num: u16,

    #[arg(long, default_value = "1")]
    pub worker_num: u16,
}

impl Testing {
    pub const SAVE_CONF_PATH: &'static str = "testing/curvine-cluster.toml";
    pub const S3_CONF_PATH: &'static str = "testing/s3.toml";

    /// Get the workspace root directory by finding the workspace Cargo.toml
    fn get_workspace_root() -> CommonResult<PathBuf> {
        // Start from current directory or CARGO_MANIFEST_DIR
        let start_dir = if let Ok(manifest_dir) = env::var("CARGO_MANIFEST_DIR") {
            PathBuf::from(manifest_dir)
        } else {
            env::current_dir()?
        };

        let mut current = start_dir.as_path();

        // Walk up the directory tree to find workspace root
        loop {
            let cargo_toml = current.join("Cargo.toml");
            if cargo_toml.exists() {
                // Check if this is the workspace root by looking for [workspace] section
                if let Ok(content) = std::fs::read_to_string(&cargo_toml) {
                    if content.contains("[workspace]") {
                        return Ok(current.to_path_buf());
                    }
                }
            }

            // Move to parent directory
            match current.parent() {
                Some(parent) => current = parent,
                None => break,
            }
        }

        err_box!("Could not find workspace root directory")
    }

    /// Get the absolute path for configuration file in workspace root
    fn get_workspace_conf_path(relative_path: &str) -> CommonResult<String> {
        let workspace_root = Self::get_workspace_root()?;
        let conf_path = workspace_root.join(relative_path);
        Ok(conf_path.to_string_lossy().to_string())
    }

    pub fn start_cluster(&self) -> CommonResult<()> {
        let conf = ClusterConf::from(&self.conf)?;
        self.start_cluster_with_conf(&conf)
    }

    fn get_conf_path() -> CommonResult<String> {
        // First check environment variable
        if let Ok(path) = env::var(ClusterConf::ENV_CONF_FILE) {
            if FileUtils::exists(&path) {
                return Ok(path);
            } else {
                return err_box!("Not found conf path {}", path);
            }
        }

        // Use workspace root path
        let workspace_conf_path = Self::get_workspace_conf_path(Self::SAVE_CONF_PATH)?;

        if FileUtils::exists(&workspace_conf_path) {
            Ok(workspace_conf_path)
        } else {
            err_box!("Not found conf {}", workspace_conf_path)
        }
    }

    // Start the cluster.
    pub fn start_cluster_with_conf(&self, conf: &ClusterConf) -> CommonResult<()> {
        // Use workspace root path for saving configuration
        let save_path = Self::get_workspace_conf_path(Self::SAVE_CONF_PATH)?;
        FileUtils::create_parent_dir(&save_path, true)?;

        let cluster = MiniCluster::with_num(conf, self.master_num, self.worker_num);

        // Save configuration file to workspace root
        let mut file = LocalFile::with_write(&save_path, true)?;
        let str = cluster.cluster_conf.to_pretty_toml()?;
        file.write_all(str.as_bytes())?;
        drop(file);

        cluster.start_cluster();
        Ok(())
    }

    pub fn get_cluster_conf() -> CommonResult<ClusterConf> {
        let save_path = Self::get_conf_path()?;
        ClusterConf::from(save_path)
    }

    pub fn default_fs() -> CommonResult<CurvineFileSystem> {
        Self::get_fs(None, None)
    }

    pub fn get_fs_with_rt(rt: Arc<Runtime>) -> CommonResult<CurvineFileSystem> {
        Self::get_fs(Some(rt), None)
    }

    pub fn get_fs_with_conf(conf: ClusterConf) -> CommonResult<CurvineFileSystem> {
        Self::get_fs(None, Some(conf))
    }

    pub fn get_unified_fs_with_rt(rt: Arc<Runtime>) -> CommonResult<UnifiedFileSystem> {
        Ok(UnifiedFileSystem::with_rt(Self::get_cluster_conf()?, rt)?)
    }

    pub fn get_fs(
        rt: Option<Arc<Runtime>>,
        conf: Option<ClusterConf>,
    ) -> CommonResult<CurvineFileSystem> {
        let conf = match conf {
            Some(c) => c,
            None => Self::get_cluster_conf()?,
        };

        let rt = match rt {
            Some(r) => r,
            None => Arc::new(conf.client_rpc_conf().create_runtime()),
        };

        Logger::init(conf.log.clone());
        let fs = CurvineFileSystem::with_rt(conf, rt)?;
        Ok(fs)
    }

    pub fn get_s3_conf() -> Option<HashMap<String, String>> {
        // Use workspace root path for S3 configuration
        let path = match Self::get_workspace_conf_path(Self::S3_CONF_PATH) {
            Ok(p) => p,
            Err(_) => return None,
        };

        if !FileUtils::exists(&path) {
            return None;
        }

        Some(FileUtils::read_toml_as_map(&path).unwrap())
    }
}

impl Default for Testing {
    fn default() -> Self {
        Self {
            conf: "testing/curvine-cluster.toml".to_string(),
            s3_conf: "testing/s3.toml".to_string(),
            master_num: 1,
            worker_num: 1,
        }
    }
}
