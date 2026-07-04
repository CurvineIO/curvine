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

mod cmds;
mod commands;
mod util;

use clap::Parser;
use commands::Commands;
use curvine_client::rpc::JobMasterClient;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::version;
use orpc::common::{Logger, Utils};
use orpc::io::net::InetAddr;
use orpc::runtime::RpcRuntime;
use orpc::{err_box, CommonResult};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version = version::VERSION, about, long_about = None)]
pub struct CurvineArgs {
    /// Configuration file path (optional)
    #[arg(
        short,
        long,
        help = "Configuration file path (optional)",
        global = true
    )]
    pub conf: Option<String>,

    /// Master address list (e.g., 'm1:8995,m2:8995')
    #[arg(
        long,
        help = "Master address list (e.g., 'm1:8995,m2:8995')",
        global = true
    )]
    pub master_addrs: Option<String>,

    #[command(subcommand)]
    command: Commands,
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
            let mut vec = vec![];
            for node in master_addrs.split(',') {
                let tmp: Vec<&str> = node.split(':').collect();
                if tmp.len() != 2 {
                    return err_box!("Invalid master_addrs format: '{}'. Expected format: 'host1:port1,host2:port2'", master_addrs);
                }
                let hostname = tmp[0].to_string();
                let port: u16 = tmp[1]
                    .parse()
                    .map_err(|_| format!("Invalid port number in master_addrs: '{}'", tmp[1]))?;
                vec.push(InetAddr::new(hostname, port));
            }
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
    Utils::set_panic_exit_hook();

    let enable_default_discovery = matches!(args.command, Commands::Bench(_));
    let conf_load = args.get_conf_with_source(enable_default_discovery)?;
    let conf = conf_load.conf;
    let conf_source = conf_load.source;
    Logger::init(conf.cli.log.clone());

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let curvine_fs = UnifiedFileSystem::with_rt(conf.clone(), rt.clone())?;
    let fs_client = curvine_fs.fs_client();
    let load_client = JobMasterClient::new(fs_client.clone());

    rt.block_on(async move {
        let result = match args.command {
            Commands::Bench(cmd) => cmd.execute(curvine_fs, conf_source.clone()).await,
            Commands::Fs(cmd) => cmd.execute(curvine_fs).await,
            Commands::Report(cmd) => cmd.execute(curvine_fs).await,
            Commands::Load(cmd) => cmd.execute(load_client).await,
            Commands::LoadStatus(cmd) => cmd.execute(load_client).await,
            Commands::CancelLoad(cmd) => cmd.execute(load_client).await,
            Commands::Mount(cmd) => cmd.execute(curvine_fs).await,
            Commands::UnMount(cmd) => cmd.execute(fs_client).await,
            Commands::Node(cmd) => cmd.execute(fs_client, conf.clone()).await,
            Commands::Version => {
                println!("curvine-cli {}", version::VERSION);
                Ok(())
            }
        };

        if let Err(e) = &result {
            eprintln!("Error: {}", e);
        }

        result
    })
}
