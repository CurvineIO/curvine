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
use curvine_common::state::StorageType;
use orpc::common::ByteUnit;
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
    curvine_path: String,

    #[arg(short, long)]
    config: Vec<String>,

    #[arg(long, default_value = "${CURVINE_CONF_FILE}")]
    conf: String,

    /// Update the mount point config if it already exists
    #[arg(long, default_value_t = false)]
    update: bool,

    #[arg(short, long)]
    replicas: Option<usize>,

    #[arg(short, long)]
    block_size: Option<String>,

    #[arg(short, long)]
    storage_type: Option<StorageType>,
}

impl MountCommand {
    pub async fn execute(&self, client: Arc<FsClient>) -> CommonResult<()> {
        // If no path argument is given, all mount points are listed.
        if self.ufs_path.trim().is_empty() && self.curvine_path.trim().is_empty() {
            let rep = handle_rpc_result(client.get_mount_table()).await;
            println!("{}", rep);
            return Ok(());
        }

        if self.curvine_path.trim().is_empty() {
            eprintln!("Error: Curvine Path cannot be empty");
            std::process::exit(1);
        }

        if self.ufs_path.trim().is_empty() {
            eprintln!("Error: UFS Path cannot be empty");
            std::process::exit(1);
        }

        println!("Ufs path: {}", self.ufs_path);
        println!("Curvine path: {}", self.curvine_path);

        let mut configs = match self.get_config_map() {
            Ok(configs) => configs,
            Err(e) => {
                eprintln!("Error: Invalid config format: {}", e);
                std::process::exit(1);
            }
        };

        if let Some(scheme) = extract_scheme(&self.ufs_path) {
            if scheme == "s3" {
                enrich_s3_configs(&self.ufs_path, &mut configs);
            }
        }

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

        // Creating a MountOptions Object
        let mount_options = curvine_common::proto::MountOptions {
            update: self.update,
            properties: configs,
            auto_cache: false,
            cache_ttl_secs: None,
            consistency_config: None,
            storage_type: self.storage_type.map(|stype| stype.into()),
            block_size: self
                .block_size
                .as_ref()
                .map(|s| ByteUnit::from_str(s))
                .transpose()?
                .map(|u| u.as_byte()),
            replicas: self.replicas.map(|r| r as u32),
        };

        let rep =
            handle_rpc_result(client.mount(&self.ufs_path, &self.curvine_path, mount_options))
                .await;
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
}
