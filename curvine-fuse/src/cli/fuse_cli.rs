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
use curvine_common::conf::ClientConfCliOverrides;
use curvine_common::version;

use crate::cli::mount_args::{FuseMountArgs, FuseRuntimeArgs};

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

    #[command(flatten)]
    pub client: ClientConfCliOverrides,
}

#[derive(Debug, Clone, Subcommand)]
pub enum FuseSubcommand {
    /// Mount the curvine filesystem (also the default when omitted)
    Mount(FuseRuntimeArgs),
    /// Validate configuration without mounting
    ValidateConfig(FuseRuntimeArgs),
    /// List mount-related CLI flags as JSON for docs and CI
    ListConfigFlags(ListConfigFlagsArgs),
}

impl FuseCli {
    /// Returns true when the parsed invocation should run the mount flow.
    pub fn runs_mount(&self) -> bool {
        matches!(self.cmd, None | Some(FuseSubcommand::Mount(_)))
    }

    /// Returns runtime args from the subcommand when present, otherwise top-level flags.
    pub fn resolve_runtime_args(&self) -> FuseRuntimeArgs {
        match &self.cmd {
            Some(FuseSubcommand::Mount(args)) | Some(FuseSubcommand::ValidateConfig(args)) => {
                args.clone()
            }
            None => FuseRuntimeArgs {
                mount: self.mount.clone(),
                client: self.client.clone(),
            },
            Some(FuseSubcommand::ListConfigFlags(_)) => {
                unreachable!("resolve_runtime_args called for list-config-flags")
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
        let args = cli.resolve_runtime_args();
        assert_eq!(args.mount.io_threads, Some(4));
    }

    #[test]
    fn client_overrides_parse_in_isolation() {
        #[derive(Parser)]
        struct Harness {
            #[command(flatten)]
            client: ClientConfCliOverrides,
        }
        let parsed = Harness::try_parse_from(["curvine-fuse", "--client.io-threads", "9"]).unwrap();
        assert_eq!(parsed.client.io_threads, Some(9));
    }

    #[test]
    fn bare_invocation_parses_client_cli_flags() {
        let cli = FuseCli::try_parse_from([
            "curvine-fuse",
            "--client.io-threads=9",
            "--client.block-size=64KB",
        ])
        .unwrap();
        assert_eq!(cli.client.io_threads, Some(9));
        assert_eq!(cli.client.block_size_str.as_deref(), Some("64KB"));
        let args = cli.resolve_runtime_args();
        assert_eq!(args.client.io_threads, Some(9));
        assert_eq!(args.client.block_size_str.as_deref(), Some("64KB"));
    }

    #[test]
    fn validate_config_subcommand_parses_client_flags() {
        let cli = FuseCli::try_parse_from([
            "curvine-fuse",
            "validate-config",
            "--conf",
            "conf/curvine-cluster.toml",
            "--client.read-parallel",
            "3",
        ])
        .unwrap();
        match cli.cmd {
            Some(FuseSubcommand::ValidateConfig(args)) => {
                assert_eq!(args.client.read_parallel, Some(3));
            }
            _ => panic!("expected validate-config subcommand"),
        }
    }

    #[test]
    fn runtime_args_apply_client_overrides_to_conf() {
        let args = FuseCli::try_parse_from(["curvine-fuse", "--client.io-threads", "12"])
            .unwrap()
            .resolve_runtime_args();
        let conf = args.get_conf().unwrap();
        assert_eq!(conf.client.io_threads, 12);
    }

    #[test]
    fn mount_subcommand_preserves_flags() {
        let cli = FuseCli::try_parse_from(["curvine-fuse", "mount", "--io-threads", "8"]).unwrap();
        let args = cli.resolve_runtime_args();
        assert_eq!(args.mount.io_threads, Some(8));
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
