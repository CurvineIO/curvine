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

mod alloc;
mod cmds;
mod commands;
mod util;

use clap::{error::ErrorKind, CommandFactory, Parser};
use commands::Commands;
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_job_client::JobMasterClient;
use curvine_job_client::TransferClient;
use curvine_net::net::InetAddr;
use curvine_runtime::common::{Logger, Utils};
use curvine_runtime::runtime::RpcRuntime;
use curvine_sys::version;
use curvine_unified_fs::UnifiedFileSystem;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(
    author,
    version = env!("CARGO_PKG_VERSION"),
    about,
    long_about = None
)]
pub struct CurvineArgs {
    /// Print the component version in JSON format and exit
    #[arg(long, global = true)]
    pub version_json: bool,

    /// Configuration file path (optional)
    #[arg(long, help = "Configuration file path (optional)", global = true)]
    pub conf: Option<String>,

    /// Master address list (e.g., 'm1:8995,m2:8995')
    #[arg(
        long,
        help = "Master address list (e.g., 'm1:8995,m2:8995')",
        global = true
    )]
    pub master_addrs: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

pub struct ConfLoadResult {
    pub conf: ClusterConf,
    pub source: String,
}

impl CurvineArgs {
    /// Get cluster configuration with priority: CLI args > config file > env vars > defaults
    pub fn get_conf(&self) -> CommonResult<ClusterConf> {
        Ok(self.get_conf_with_source(false)?.conf)
    }

    pub fn get_conf_with_source(
        &self,
        enable_default_discovery: bool,
    ) -> CommonResult<ConfLoadResult> {
        let conf_path = self.resolve_conf_path(enable_default_discovery);

        let (mut conf, mut source) = if let Some((path, source)) = conf_path {
            match ClusterConf::from(&path) {
                Ok(c) => (c, format!("{} ({})", source, path)),
                Err(e) => {
                    eprintln!("Warning: Failed to load config file '{}': {}", path, e);
                    eprintln!("Using default configuration");
                    (
                        Self::create_default_conf(),
                        format!("default configuration (failed to load {})", path),
                    )
                }
            }
        } else {
            (
                Self::create_default_conf(),
                "default configuration".to_string(),
            )
        };

        // Priority 2: Override with CLI master_addrs if provided
        if let Some(master_addrs) = &self.master_addrs {
            let vec = match InetAddr::parse_list(master_addrs) {
                Ok(vec) => vec,
                Err(e) => {
                    return err_box!(
                        "Invalid master_addrs format: '{}'. Expected format: 'host1:port1,host2:port2': {}",
                        master_addrs,
                        e
                    );
                }
            };
            conf.client.master_addrs = vec;
            source = format!("{} + --master_addrs override", source);
        }

        // Initialize configuration (parse string values to actual types)
        conf.client.init()?;

        Ok(ConfLoadResult { conf, source })
    }

    fn create_default_conf() -> ClusterConf {
        ClusterConf::default()
    }

    fn resolve_conf_path(&self, enable_default_discovery: bool) -> Option<(String, String)> {
        if let Some(path) = &self.conf {
            return Some((path.clone(), "--conf".to_string()));
        }

        if let Ok(path) = std::env::var(ClusterConf::ENV_CONF_FILE) {
            return Some((path, ClusterConf::ENV_CONF_FILE.to_string()));
        }

        if !enable_default_discovery {
            return None;
        }

        let mut candidates = vec![
            (PathBuf::from("curvine-cluster.toml"), "current directory"),
            (
                PathBuf::from("conf/curvine-cluster.toml"),
                "current conf directory",
            ),
        ];

        if let Some(home) = std::env::var_os("HOME") {
            candidates.push((
                PathBuf::from(home).join(".curvine/curvine-cluster.toml"),
                "user config directory",
            ));
        }

        if let Ok(curvine_home) = std::env::var("CURVINE_HOME") {
            candidates.push((
                Path::new(&curvine_home).join("conf/curvine-cluster.toml"),
                "CURVINE_HOME",
            ));
        }

        candidates
            .into_iter()
            .find(|(path, _)| path.exists())
            .map(|(path, source)| (path.to_string_lossy().to_string(), source.to_string()))
    }
}

fn main() -> CommonResult<()> {
    let args = CurvineArgs::parse();
    if args.version_json {
        let json = match version::component_version_json("cli") {
            Ok(json) => json,
            Err(e) => return err_box!("Failed to serialize component version: {}", e),
        };
        println!("{}", json);
        return Ok(());
    }
    if args.command.is_none() {
        CurvineArgs::command()
            .error(
                ErrorKind::MissingSubcommand,
                "a subcommand is required unless --version-json is provided",
            )
            .exit();
    }

    Utils::set_panic_exit_hook();

    if matches!(args.command, Some(Commands::Version)) {
        println!("curvine-cli {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let enable_default_discovery = matches!(args.command, Some(Commands::Bench(_)));
    let conf_load = args.get_conf_with_source(enable_default_discovery)?;
    let conf = conf_load.conf;
    let conf_source = conf_load.source;
    Logger::init(conf.cli.log.clone());

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let curvine_fs = UnifiedFileSystem::with_rt(conf.clone(), rt.clone())?;
    let fs_client = curvine_fs.fs_client();
    let load_client = JobMasterClient::new(fs_client.clone());
    let transfer_client = if conf.transfer.enabled {
        Some(TransferClient::with_rt(&conf, rt.clone())?)
    } else {
        None
    };

    rt.block_on(async move {
        let result = match args.command {
            Some(Commands::Bench(cmd)) => cmd.execute(curvine_fs, conf_source.clone()).await,
            Some(Commands::Fs(cmd)) => cmd.execute(curvine_fs).await,
            Some(Commands::Report(cmd)) => cmd.execute(curvine_fs).await,
            Some(Commands::Load(cmd)) => match transfer_client.clone() {
                Some(transfer_client) => cmd.execute_transfer(curvine_fs.clone(), transfer_client).await,
                None => cmd.execute_legacy(load_client.clone()).await,
            },
            Some(Commands::Export(cmd)) => match transfer_client.clone() {
                Some(transfer_client) => cmd.execute_transfer(curvine_fs.clone(), transfer_client).await,
                None => cmd.execute_legacy(load_client.clone()).await,
            },
            Some(Commands::LoadStatus(cmd)) => match transfer_client.clone() {
                Some(transfer_client) => cmd.execute_transfer_only(transfer_client).await,
                None => cmd.execute(load_client.clone()).await,
            },
            Some(Commands::TransferStatus(cmd)) => {
                let Some(transfer_client) = transfer_client.clone() else {
                    return err_box!(
                        "transfer-status requires transfer.enabled=true and a running transfer service"
                    );
                };
                cmd.execute_transfer_only(transfer_client).await
            }
            Some(Commands::CancelLoad(cmd)) => match transfer_client.clone() {
                Some(transfer_client) => cmd.execute_transfer_only(transfer_client).await,
                None => cmd.execute(load_client.clone()).await,
            },
            Some(Commands::CancelTransfer(cmd)) => {
                let Some(transfer_client) = transfer_client.clone() else {
                    return err_box!(
                        "cancel-transfer requires transfer.enabled=true and a running transfer service"
                    );
                };
                cmd.execute_transfer_only(transfer_client).await
            }
            Some(Commands::Transfer(cmd)) => {
                let Some(transfer_client) = transfer_client.clone() else {
                    return err_box!(
                        "transfer requires transfer.enabled=true and a running transfer service"
                    );
                };
                cmd.execute(transfer_client).await
            }
            Some(Commands::Mount(cmd)) => cmd.execute(curvine_fs).await,
            Some(Commands::UnMount(cmd)) => cmd.execute(fs_client).await,
            Some(Commands::Node(cmd)) => cmd.execute(fs_client, conf.clone()).await,
            Some(Commands::Version) | None => Ok(()),
        };

        if let Err(e) = &result {
            eprintln!("Error: {}", e);
        }

        result
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn export_subcommand_is_available() {
        let args = CurvineArgs::try_parse_from(["curvine", "export", "/mnt/file", "--watch"])
            .expect("export command should parse");

        assert!(matches!(args.command, Some(Commands::Export(_))));
    }

    #[test]
    fn version_json_does_not_require_subcommand() {
        let args = CurvineArgs::try_parse_from(["curvine", "--version-json"])
            .expect("version json should parse without subcommand");

        assert!(args.version_json);
        assert!(args.command.is_none());
    }

    #[test]
    fn bare_invocation_parses_so_custom_missing_subcommand_path_runs() {
        let args = CurvineArgs::try_parse_from(["curvine"])
            .expect("bare invocation should parse so main can emit MissingSubcommand");

        assert!(args.command.is_none());
    }

    #[test]
    fn mount_accepts_config_without_conf_short_option_collision() {
        CurvineArgs::command().debug_assert();
        let args = CurvineArgs::try_parse_from([
            "curvine",
            "--conf",
            "curvine-cluster.toml",
            "mount",
            "s3://bucket/path",
            "/bucket/path",
            "-c",
            "s3.endpoint_url=http://127.0.0.1:9000",
        ])
        .expect("mount config should use -c while cluster config uses --conf");

        assert!(matches!(args.command, Some(Commands::Mount(_))));
    }
}
