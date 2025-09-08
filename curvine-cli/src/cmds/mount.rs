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

use crate::util::*;
use clap::Parser;
use curvine_client::file::FsClient;
use curvine_client::unified::UfsFileSystem;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{ConsistencyStrategy, MountOptions, MountType, StorageType, TtlAction};
use orpc::common::{ByteUnit, DurationUnit};
use orpc::{err_box, CommonResult};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Parser, Debug)]
pub struct MountCommand {
    /// UFS path to mount
    #[arg(default_value = "")]
    ufs_path: String,

    /// Curvine path to mount to
    #[arg(default_value = "")]
    cv_path: String,

    #[arg(short, long)]
    config: Vec<String>,

    #[arg(long, default_value = "${CURVINE_CONF_FILE}")]
    conf: String,

    /// Update the mount point config if it already exists
    #[arg(long, default_value_t = false)]
    update: bool,

    #[arg(long, default_value = "cst")]
    mnt_type: String,

    #[arg(long, default_value = "always")]
    consistency_strategy: String,

    #[arg(long, default_value = "0")]
    ttl_ms: String,

    #[arg(long, default_value = "none")]
    ttl_action: String,

    #[arg(long)]
    replicas: Option<i32>,

    #[arg(long)]
    block_size: Option<String>,

    #[arg(short, long)]
    storage_type: Option<String>,
}

impl MountCommand {
    pub async fn execute(&self, client: Arc<FsClient>) -> CommonResult<()> {
        // If no path argument is given, all mount points are listed.
        if self.ufs_path.trim().is_empty() && self.cv_path.trim().is_empty() {
            let rep = handle_rpc_result(client.get_mount_table()).await;
            println!("{}", rep);
            return Ok(());
        }

        if self.cv_path.trim().is_empty() {
            eprintln!("Error: Curvine Path cannot be empty");
            std::process::exit(1);
        }

        if self.ufs_path.trim().is_empty() {
            eprintln!("Error: UFS Path cannot be empty");
            std::process::exit(1);
        }

        println!("Ufs path: {}", self.ufs_path);
        println!("Curvine path: {}", self.cv_path);

        let configs = match self.get_config_map() {
            Ok(configs) => configs,
            Err(e) => {
                eprintln!("Error: Invalid config format: {}", e);
                std::process::exit(1);
            }
        };

        let validation_result = validate_path_and_configs(&self.ufs_path, &configs);
        if let Err(error_msg) = validation_result {
            eprintln!("Error: {}", error_msg);
            std::process::exit(1);
        }

        if !configs.is_empty() {
            println!("Configuration:");
            for (key, value) in &configs {
                println!("  {} = {}", key, value);
            }
            println!("\n");
        }

        let ufs_path = Path::from_str(&self.ufs_path)?;
        let cv_path = Path::from_str(&self.cv_path)?;

        // Creating a MountOptions Object
        let mnt_opts = self.to_mnt_opts()?;

        // try to create ufsFileSystem
        if !mnt_opts.update {
            let ufs = UfsFileSystem::new(&ufs_path, mnt_opts.add_properties.clone())?;
            if let Err(e) = ufs.list_status(&ufs_path).await {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }

        let rep = handle_rpc_result(client.mount(&ufs_path, &cv_path, mnt_opts)).await;
        println!("{}", rep);
        Ok(())
    }

    pub fn get_config_map(&self) -> CommonResult<HashMap<String, String>> {
        let mut configs = HashMap::new();
        for pair in &self.config {
            let parts: Vec<&str> = pair.split('=').collect();
            if parts.len() == 2 {
                configs.insert(parts[0].trim().to_string(), parts[1].trim().to_string());
            } else {
                return err_box!("Invalid config format: {}", pair);
            }
        }

        Ok(configs)
    }

    pub fn to_mnt_opts(&self) -> CommonResult<MountOptions> {
        let mnt_type = MountType::try_from(self.mnt_type.as_str())?;
        let consistency_strategy =
            ConsistencyStrategy::try_from(self.consistency_strategy.as_str())?;
        let ttl_ms = DurationUnit::from_str(self.ttl_ms.as_str())?.as_millis() as i64;
        let ttl_action = TtlAction::try_from(self.ttl_action.as_str())?;
        let conf_map = self.get_config_map()?;

        let mut opts = MountOptions::builder()
            .update(self.update)
            .set_properties(conf_map)
            .mount_type(mnt_type)
            .consistency_strategy(consistency_strategy)
            .ttl_ms(ttl_ms)
            .ttl_action(ttl_action);

        if let Some(replicas) = self.replicas {
            opts = opts.replicas(replicas);
        }
        if let Some(block_size) = self.block_size.as_ref() {
            opts = opts.block_size(ByteUnit::from_str(block_size.as_str())?.as_byte() as i64);
        }
        if let Some(storage_type) = self.storage_type.as_ref() {
            opts = opts.storage_type(StorageType::try_from(storage_type.as_str())?);
        }

        Ok(opts.build())
    }
}
