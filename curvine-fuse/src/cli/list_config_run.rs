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

use crate::cli::fuse_cli::{FuseCli, ListConfigFlagsArgs, ListConfigFormat};
use clap::{Arg, CommandFactory};
use orpc::CommonResult;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Serialize, PartialEq, Eq)]
struct CliFlagRecord {
    pub long: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    pub required: bool,
    pub takes_value: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct CliFlagsDocument {
    pub version: String,
    pub flags: Vec<CliFlagRecord>,
}

/// Exports curvine-fuse CLI flags as JSON for docs/CI (CSI does not embed this at runtime).
pub fn run_list_config_flags(args: ListConfigFlagsArgs) -> CommonResult<()> {
    match args.format {
        ListConfigFormat::Json => {
            let document = export_cli_flags_json()?;
            println!("{}", serde_json::to_string_pretty(&document)?);
            Ok(())
        }
    }
}

fn export_cli_flags_json() -> CommonResult<CliFlagsDocument> {
    let cmd = FuseCli::command();
    let mut by_long = BTreeMap::new();
    collect_flags(&cmd, &mut by_long);

    Ok(CliFlagsDocument {
        version: cmd.get_version().unwrap_or_default().to_string(),
        flags: by_long.into_values().collect(),
    })
}

fn collect_flags(cmd: &clap::Command, out: &mut BTreeMap<String, CliFlagRecord>) {
    for arg in cmd.get_arguments() {
        if should_skip_arg(arg) {
            continue;
        }
        if let Some(record) = flag_record_from_arg(arg) {
            out.entry(record.long.clone()).or_insert(record);
        }
    }
    for sub in cmd.get_subcommands() {
        if should_skip_subcommand(sub.get_name()) {
            continue;
        }
        collect_flags(sub, out);
    }
}

fn should_skip_subcommand(name: &str) -> bool {
    matches!(name, "list-config-flags" | "help")
}

fn should_skip_arg(arg: &Arg) -> bool {
    let id = arg.get_id().as_str();
    matches!(id, "help" | "version")
}

fn flag_record_from_arg(arg: &Arg) -> Option<CliFlagRecord> {
    let long = arg.get_long()?.to_string();
    Some(CliFlagRecord {
        long,
        help: arg.get_help().map(|h| h.to_string()),
        required: arg.is_required_set(),
        takes_value: arg.get_action().takes_values(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_includes_fuse_mount_flags() {
        let doc = export_cli_flags_json().unwrap();
        let longs: Vec<_> = doc.flags.iter().map(|f| f.long.as_str()).collect();
        assert!(longs.contains(&"io-threads"));
        assert!(longs.contains(&"conf"));
        assert!(longs.contains(&"master-addrs"));
    }

    #[test]
    fn export_deduplicates_shared_mount_flags() {
        let doc = export_cli_flags_json().unwrap();
        let io_threads = doc.flags.iter().filter(|f| f.long == "io-threads").count();
        assert_eq!(io_threads, 1);
    }

    #[test]
    fn export_is_sorted_by_long_name() {
        let doc = export_cli_flags_json().unwrap();
        let longs: Vec<_> = doc.flags.iter().map(|f| f.long.as_str()).collect();
        let mut sorted = longs.clone();
        sorted.sort_unstable();
        assert_eq!(longs, sorted);
    }

    #[test]
    fn export_includes_client_cli_flags() {
        let doc = export_cli_flags_json().unwrap();
        let longs: Vec<_> = doc.flags.iter().map(|f| f.long.as_str()).collect();
        assert!(longs.iter().any(|long| long.starts_with("client.")));
    }

    #[test]
    fn export_omits_list_config_flags_subcommand_args() {
        let doc = export_cli_flags_json().unwrap();
        let longs: Vec<_> = doc.flags.iter().map(|f| f.long.as_str()).collect();
        assert!(!longs.contains(&"format"));
    }
}
