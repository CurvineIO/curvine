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
use curvine_config::{
    DirSpaceConf, DIR_SPACE_ENABLED_RUNTIME_KEY, DIR_SPACE_TRACKED_PATHS_RUNTIME_KEY,
};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::RuntimeConfigEntry;
use std::sync::Arc;

#[derive(Parser, Debug)]
pub struct DirSpaceCommand {
    #[command(subcommand)]
    pub action: DirSpaceSubCommand,
}

#[derive(Subcommand, Debug)]
pub enum DirSpaceSubCommand {
    /// Show the scan switch and the explicit path list
    Status,
    /// Turn directory statistics on (non-persistent)
    Enable,
    /// Turn directory statistics off; no path is scanned
    Disable,
    /// Change the explicit statistics path list
    Path {
        #[command(subcommand)]
        action: DirSpacePathCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum DirSpacePathCommand {
    /// Add one absolute path
    Add { path: String },
    /// Remove one absolute path
    Remove { path: String },
}

struct DirSpaceView {
    enabled: bool,
    paths: Vec<String>,
}

impl DirSpaceCommand {
    pub async fn execute(&self, client: Arc<FsClient>) -> CommonResult<()> {
        match &self.action {
            DirSpaceSubCommand::Status => {
                let view = load_view(&client).await?;
                print_view(&view);
            }
            DirSpaceSubCommand::Enable => {
                set_enabled(&client, true).await?;
            }
            DirSpaceSubCommand::Disable => {
                set_enabled(&client, false).await?;
            }
            DirSpaceSubCommand::Path { action } => match action {
                DirSpacePathCommand::Add { path } => {
                    set_path(&client, path, true).await?;
                }
                DirSpacePathCommand::Remove { path } => {
                    set_path(&client, path, false).await?;
                }
            },
        }
        Ok(())
    }
}

async fn load_view(client: &FsClient) -> CommonResult<DirSpaceView> {
    let entries = handle_rpc_result(client.get_runtime_config(None)).await;
    Ok(DirSpaceView {
        enabled: entry_bool(&entries, DIR_SPACE_ENABLED_RUNTIME_KEY)?,
        paths: entry_paths(&entries, DIR_SPACE_TRACKED_PATHS_RUNTIME_KEY)?,
    })
}

async fn set_enabled(client: &FsClient, enabled: bool) -> CommonResult<()> {
    let value = if enabled { "true" } else { "false" };
    let result =
        handle_rpc_result(client.update_runtime_config(DIR_SPACE_ENABLED_RUNTIME_KEY, value)).await;
    println!("enabled: {}", result.value_json);
    println!("note: runtime override (非持久)");
    Ok(())
}

async fn set_path(client: &FsClient, path: &str, tracked: bool) -> CommonResult<()> {
    let view = load_view(client).await?;
    let next = DirSpaceConf::with_tracked_path(&view.paths, path, tracked)?;
    let value = match serde_json::to_string(&next) {
        Ok(value) => value,
        Err(err) => return err_box!("dir-space paths: {}", err),
    };
    let result = handle_rpc_result(
        client.update_runtime_config(DIR_SPACE_TRACKED_PATHS_RUNTIME_KEY, &value),
    )
    .await;
    println!("paths: {}", result.value_json);
    println!("note: runtime override (非持久)");
    Ok(())
}

fn print_view(view: &DirSpaceView) {
    println!("enabled: {}", view.enabled);
    if view.paths.is_empty() {
        println!("paths: []");
        return;
    }
    println!("paths:");
    for path in &view.paths {
        println!("  {path}");
    }
}

fn entry_bool(entries: &[RuntimeConfigEntry], key: &str) -> CommonResult<bool> {
    let raw = entry_effective(entries, key)?;
    match raw {
        "true" => Ok(true),
        "false" => Ok(false),
        other => err_box!("{} has unexpected value {}", key, other),
    }
}

fn entry_paths(entries: &[RuntimeConfigEntry], key: &str) -> CommonResult<Vec<String>> {
    let raw = entry_effective(entries, key)?;
    match serde_json::from_str(raw) {
        Ok(paths) => Ok(paths),
        Err(err) => err_box!("{} is not a JSON string array: {}", key, err),
    }
}

fn entry_effective<'a>(entries: &'a [RuntimeConfigEntry], key: &str) -> CommonResult<&'a str> {
    match entries.iter().find(|entry| entry.key == key) {
        Some(entry) => Ok(entry.effective_value_json.as_str()),
        None => err_box!("master runtime config has no key {}", key),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmds::admin::AdminSubCommand;
    use crate::cmds::AdminCommand;
    use clap::Parser;

    #[test]
    fn dir_space_path_add_parses() {
        let args = AdminCommand::try_parse_from(["curvine", "dir-space", "path", "add", "/stats"])
            .expect("parse");
        match args.action {
            AdminSubCommand::DirSpace(cmd) => match cmd.action {
                DirSpaceSubCommand::Path { action } => match action {
                    DirSpacePathCommand::Add { path } => assert_eq!(path, "/stats"),
                    other => panic!("unexpected path command: {other:?}"),
                },
                other => panic!("unexpected dir-space command: {other:?}"),
            },
            other => panic!("unexpected admin command: {other:?}"),
        }
    }

    #[test]
    fn entry_paths_reads_effective_json() {
        let entries = vec![RuntimeConfigEntry {
            key: DIR_SPACE_TRACKED_PATHS_RUNTIME_KEY.to_string(),
            file_value_json: "[]".to_string(),
            override_value: Some("[\"/stats\"]".to_string()),
            effective_value_json: "[\"/stats\"]".to_string(),
        }];
        assert_eq!(
            entry_paths(&entries, DIR_SPACE_TRACKED_PATHS_RUNTIME_KEY).unwrap(),
            vec!["/stats".to_string()]
        );
    }
}
