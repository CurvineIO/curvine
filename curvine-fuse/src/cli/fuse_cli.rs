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

use clap::{Parser, Subcommand, ValueEnum};
use curvine_common::version;

use crate::cli::mount_args::FuseMountArgs;

/// Output format for `list-config-flags`.
#[derive(Debug, Clone, Copy, Default, ValueEnum, PartialEq, Eq)]
pub enum ListConfigFormat {
    #[default]
    Json,
}

/// Arguments for the `list-config-flags` subcommand.
#[derive(Debug, Parser, Clone)]
pub struct ListConfigFlagsArgs {
    #[arg(long, value_enum, default_value_t = ListConfigFormat::Json)]
    pub format: ListConfigFormat,
}

/// Top-level curvine-fuse CLI. Mount is the default when no subcommand is given.
#[derive(Debug, Parser, Clone)]
#[command(
    name = "curvine-fuse",
    version = version::VERSION,
    subcommand_required = false,
    args_conflicts_with_subcommands = true
)]
pub struct FuseCli {
    #[command(subcommand)]
    pub cmd: Option<FuseSubcommand>,

    #[command(flatten)]
    pub mount: FuseMountArgs,
}

#[derive(Debug, Clone, Subcommand)]
pub enum FuseSubcommand {
    /// Mount the curvine filesystem (also the default when omitted)
    Mount(FuseMountArgs),
    /// Validate configuration without mounting
    ValidateConfig(FuseMountArgs),
    /// List mount-related CLI flags as JSON for docs and CI
    ListConfigFlags(ListConfigFlagsArgs),
}

impl FuseCli {
    /// Returns true when the parsed invocation should run the mount flow.
    pub fn runs_mount(&self) -> bool {
        matches!(self.cmd, None | Some(FuseSubcommand::Mount(_)))
    }

    /// Returns mount args from the subcommand when present, otherwise top-level flags.
    pub fn resolve_mount_args(&self) -> FuseMountArgs {
        match &self.cmd {
            Some(FuseSubcommand::Mount(args)) => args.clone(),
            None => self.mount.clone(),
            Some(FuseSubcommand::ValidateConfig(_)) => {
                unreachable!("resolve_mount_args called for validate-config")
            }
            Some(FuseSubcommand::ListConfigFlags(_)) => {
                unreachable!("resolve_mount_args called for list-config-flags")
            }
        }
    }

    /// Returns args for validate-config, from the subcommand or top-level flags.
    pub fn resolve_validate_args(&self) -> FuseMountArgs {
        match &self.cmd {
            Some(FuseSubcommand::ValidateConfig(args)) => args.clone(),
            None => self.mount.clone(),
            Some(FuseSubcommand::Mount(_)) => {
                unreachable!("resolve_validate_args called for mount")
            }
            Some(FuseSubcommand::ListConfigFlags(_)) => {
                unreachable!("resolve_validate_args called for list-config-flags")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_invocation_preserves_top_level_flags() {
        let cli = FuseCli::try_parse_from(["curvine-fuse", "--io-threads", "4"]).unwrap();
        assert!(cli.cmd.is_none());
        let args = cli.resolve_mount_args();
        assert_eq!(args.io_threads, Some(4));
    }

    #[test]
    fn mount_subcommand_preserves_flags() {
        let cli = FuseCli::try_parse_from(["curvine-fuse", "mount", "--io-threads", "8"]).unwrap();
        let args = cli.resolve_mount_args();
        assert_eq!(args.io_threads, Some(8));
    }

    #[test]
    fn mixed_top_level_flags_and_subcommand_is_rejected() {
        let err =
            FuseCli::try_parse_from(["curvine-fuse", "--io-threads", "4", "mount"]).unwrap_err();
        assert!(err.to_string().contains("cannot be used with"));
    }

    #[test]
    fn unknown_subcommand_is_rejected() {
        let err = FuseCli::try_parse_from(["curvine-fuse", "unknown-cmd"]).unwrap_err();
        assert!(err.to_string().contains("unrecognized subcommand"));
    }

    #[test]
    fn validate_config_subcommand_parses() {
        let cli = FuseCli::try_parse_from([
            "curvine-fuse",
            "validate-config",
            "--conf",
            "conf/curvine-cluster.toml",
        ])
        .unwrap();
        match cli.cmd {
            Some(FuseSubcommand::ValidateConfig(_)) => {}
            _ => panic!("expected validate-config subcommand"),
        }
    }

    #[test]
    fn list_config_flags_subcommand_parses() {
        let cli = FuseCli::try_parse_from(["curvine-fuse", "list-config-flags"]).unwrap();
        match cli.cmd {
            Some(FuseSubcommand::ListConfigFlags(args)) => {
                assert_eq!(args.format, ListConfigFormat::Json);
            }
            _ => panic!("expected list-config-flags subcommand"),
        }
    }

    #[test]
    fn list_config_flags_accepts_format_json() {
        let cli =
            FuseCli::try_parse_from(["curvine-fuse", "list-config-flags", "--format", "json"])
                .unwrap();
        match cli.cmd {
            Some(FuseSubcommand::ListConfigFlags(args)) => {
                assert_eq!(args.format, ListConfigFormat::Json);
            }
            _ => panic!("expected list-config-flags subcommand"),
        }
    }
}
