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
use clap::{Parser, Subcommand};
use curvine_client_core::file::FsClient;
use curvine_core_error::CommonResult;
use curvine_model::RuntimeConfigEntry;
use std::sync::Arc;

#[derive(Parser, Debug)]
pub struct ConfigCommand {
    #[command(subcommand)]
    pub action: ConfigSubCommand,
}

#[derive(Subcommand, Debug)]
pub enum ConfigSubCommand {
    /// Set a master runtime override (non-persistent)
    Set { key: String, value: String },
    /// Get one key or list all registered keys when KEY is omitted
    Get { key: Option<String> },
    /// Clear runtime override and restore file-backed default
    Reset { key: String },
}

impl ConfigCommand {
    pub async fn execute(&self, client: Arc<FsClient>) -> CommonResult<()> {
        match &self.action {
            ConfigSubCommand::Set { key, value } => {
                let result = handle_rpc_result(client.update_runtime_config(key, value)).await;
                println!("key: {}", result.key);
                println!("previous: {}", result.previous_value_json);
                println!("effective: {}", result.value_json);
                if !result.persistent {
                    println!("note: runtime override (非持久)");
                }
            }
            ConfigSubCommand::Get { key } => {
                let entries = handle_rpc_result(client.get_runtime_config(key.as_deref())).await;
                if entries.is_empty() {
                    println!("(no runtime config entries)");
                    return Ok(());
                }
                for entry in entries {
                    print_entry(&entry);
                    println!();
                }
            }
            ConfigSubCommand::Reset { key } => {
                let result = handle_rpc_result(client.reset_runtime_config(key)).await;
                println!("key: {}", result.key);
                println!("effective: {}", result.effective_value_json);
                println!("note: runtime override cleared (非持久)");
            }
        }
        Ok(())
    }
}

fn print_entry(entry: &RuntimeConfigEntry) {
    println!("key: {}", entry.key);
    println!("file: {}", entry.file_value_json);
    if let Some(raw) = &entry.override_value {
        println!("override: {} (runtime override, 非持久)", raw);
    }
    println!("effective: {}", entry.effective_value_json);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmds::admin::AdminSubCommand;
    use crate::commands::Commands;
    use crate::CurvineArgs;
    use clap::Parser;

    #[test]
    fn admin_config_set_parses() {
        let args = CurvineArgs::try_parse_from(["curvine", "admin", "config", "set", "k", "v"])
            .expect("parse");
        match args.command {
            Some(Commands::Admin(admin)) => match admin.action {
                AdminSubCommand::Config(cfg) => {
                    assert!(matches!(cfg.action, ConfigSubCommand::Set { .. }));
                }
            },
            _ => panic!("expected admin command"),
        }
    }
}
