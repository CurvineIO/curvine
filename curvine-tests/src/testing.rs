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
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_server::test::MiniCluster;
use once_cell::sync::OnceCell;
use orpc::common::{FileUtils, Logger, Utils};
use orpc::io::LocalFile;
use orpc::runtime::Runtime;
use orpc::CommonResult;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

type ConfMutator = Arc<dyn Fn(&mut ClusterConf) + Send + Sync>;

#[derive(Clone)]
pub struct Testing {
    pub s3_conf_path: String,
    pub master_num: u16,
    pub worker_num: u16,
    // Absolute path to the active cluster configuration file for this Testing instance
    active_cluster_conf_path: String,
    cluster_conf: ClusterConf,
    #[allow(clippy::type_complexity)]
    options: Option<ConfMutator>,
}

impl Testing {
    /// Create a unique temp config file path under /tmp for this Testing instance.
    fn new_tmp_path() -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let pid = std::process::id();
        let rand = Utils::rand_str(6);
        format!("../testing/curvine-tests/{}-{}-{}", ts, pid, rand)
    }

    /// Start the cluster using the prepared configuration, and persist to active_cluster_conf.
    pub fn start_cluster(&self) -> CommonResult<Arc<MiniCluster>> {
        // Ensure parent dir exists
        FileUtils::create_parent_dir(&self.active_cluster_conf_path, true)?;

        // Prepare configuration (apply mutation if provided)
        let mut conf = self.cluster_conf.clone();
        if let Some(f) = &self.options {
            (f)(&mut conf);
        }

        // Clean master metadata and journal directories for fresh start
        let meta_dir = conf.master.meta_dir.clone();
        if !meta_dir.is_empty() {
            let _ = FileUtils::delete_path(&meta_dir, true);
        }

        let journal_dir = conf.journal.journal_dir.clone();
        if !journal_dir.is_empty() {
            let _ = FileUtils::delete_path(&journal_dir, true);
        }

        //clean testing/data directory
        let data_dir = "testing/data".to_string();
        if FileUtils::exists(&data_dir) {
            let _ = FileUtils::delete_path(&data_dir, true);
        }

        let cluster = MiniCluster::with_num(&conf, self.master_num, self.worker_num);

        // Save configuration file to active path
        let mut file = LocalFile::with_write(&self.active_cluster_conf_path, true)?;
        let str = cluster.cluster_conf.to_pretty_toml()?;
        file.write_all(str.as_bytes())?;
        drop(file);

        cluster.start_cluster();
        Ok(Arc::new(cluster))
    }

    /// Read the active cluster configuration from the persisted path.
    pub fn get_active_cluster_conf(&self) -> CommonResult<ClusterConf> {
        ClusterConf::from(self.active_cluster_conf_path.clone())
    }

    /// Expose the active config file path.
    pub fn active_conf_path(&self) -> &str {
        &self.active_cluster_conf_path
    }

    pub fn get_unified_fs_with_rt(&self, rt: Arc<Runtime>) -> CommonResult<UnifiedFileSystem> {
        Ok(UnifiedFileSystem::with_rt(
            self.get_active_cluster_conf()?,
            rt,
        )?)
    }

    pub fn get_fs(
        &self,
        rt: Option<Arc<Runtime>>,
        conf: Option<ClusterConf>,
    ) -> CommonResult<CurvineFileSystem> {
        let conf = match conf {
            Some(c) => c,
            None => self.get_active_cluster_conf()?,
        };

        let rt = match rt {
            Some(r) => r,
            None => Arc::new(conf.client_rpc_conf().create_runtime()),
        };

        Logger::init(conf.log.clone());
        let fs = CurvineFileSystem::with_rt(conf, rt)?;
        Ok(fs)
    }

    pub fn get_s3_conf(&self) -> Option<HashMap<String, String>> {
        let path = &self.s3_conf_path;
        if !FileUtils::exists(path) {
            return None;
        }
        Some(FileUtils::read_toml_as_map(path).unwrap())
    }
}

static DEFAULT: OnceCell<Testing> = OnceCell::new();

impl Default for Testing {
    fn default() -> Self {
        DEFAULT
            .get_or_init(|| {
                let testing = Testing::builder().workers(3).build().unwrap();
                testing.start_cluster().unwrap();
                testing
            })
            .clone()
    }
}

impl Drop for Testing {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.active_cluster_conf_path);
    }
}

pub struct TestingBuilder {
    master_num: u16,
    worker_num: u16,
    base_conf: Option<ClusterConf>,
    base_conf_path: Option<String>,
    #[allow(clippy::type_complexity)]
    options: Option<ConfMutator>,
    s3_conf_path: Option<String>,
}

impl Default for TestingBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestingBuilder {
    pub fn new() -> Self {
        Self {
            master_num: 1,
            worker_num: 1,
            base_conf: None,
            base_conf_path: None,
            options: None,
            s3_conf_path: None,
        }
    }

    /// Set a sensible default base config path for tests if none provided.
    /// After calling this, `build()` will attempt to load "../etc/curvine-cluster.toml".
    pub fn default(mut self) -> Self {
        if self.base_conf.is_none() && self.base_conf_path.is_none() {
            self.base_conf_path = Some("../etc/curvine-cluster.toml".to_string());
        }
        if self.s3_conf_path.is_none() {
            self.s3_conf_path = Some("../testing/s3.toml".to_string());
        }
        self
    }

    pub fn masters(mut self, n: u16) -> Self {
        self.master_num = n;
        self
    }
    pub fn workers(mut self, n: u16) -> Self {
        self.worker_num = n;
        self
    }
    pub fn with_base_conf_path<S: Into<String>>(mut self, path: S) -> Self {
        self.base_conf_path = Some(path.into());
        self
    }
    pub fn mutate_conf<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut ClusterConf) + Send + Sync + 'static,
    {
        self.options = Some(Arc::new(f));
        self
    }
    pub fn with_s3_conf_path<S: Into<String>>(mut self, path: S) -> Self {
        self.s3_conf_path = Some(path.into());
        self
    }

    pub fn build(self) -> CommonResult<Testing> {
        let mut conf = if let Some(c) = self.base_conf {
            c
        } else if let Some(p) = self.base_conf_path {
            ClusterConf::from(p)?
        } else {
            ClusterConf::default()
        };

        let base_path = Testing::new_tmp_path();
        let active_cluster_conf_path = format!("{}/curvine-cluster.toml", base_path);
        conf.master.meta_dir = format!("{}/meta", base_path);
        conf.journal.journal_dir = format!("{}/journal", base_path);
        conf.worker.data_dir = vec![format!("{}/data", base_path)];
        conf.master.min_block_size = 1024;
        conf.journal.raft_tick_interval_ms = 100;
        let s3_conf_path = self
            .s3_conf_path
            .unwrap_or_else(|| "../testing/s3.toml".to_string());
        Ok(Testing {
            s3_conf_path,
            master_num: self.master_num,
            worker_num: self.worker_num,
            active_cluster_conf_path,
            cluster_conf: conf,
            options: self.options,
        })
    }
}

impl Testing {
    pub fn builder() -> TestingBuilder {
        TestingBuilder::new()
    }
}
