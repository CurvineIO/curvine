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
use orpc::runtime::RpcRuntime;
use orpc::CommonResult;
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;

#[derive(Parser, Debug, Serialize)]
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

    #[serde(skip_serializing)]
    #[command(subcommand)]
    command: Commands,
}

impl CurvineArgs {
    pub fn get_conf(&self) -> CommonResult<ClusterConf> {
        let conf_path = self
            .conf
            .clone()
            .or_else(|| std::env::var(ClusterConf::ENV_CONF_FILE).ok());

        let args_value = serde_json::to_value(self).unwrap();

        // Change the format of master_addrs from "m1:8995,..." to [{"hostname": "m1", "port": 8995}, ...]
        let master_addrs = if let Some(master_addrs_str) =
            args_value.get("master_addrs").and_then(|v| v.as_str())
        {
            master_addrs_str
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|addr| {
                    let addr = addr.trim();
                    let parts: Vec<&str> = addr.split(':').collect();

                    if parts.len() == 2 {
                        let hostname = parts[0];
                        let port = parts[1]
                            .parse::<u16>()
                            .map_err(|_| format!("Invalid port: {}", parts[1]))?;

                        Ok(json!({
                            "hostname": hostname,
                            "port": port
                        }))
                    } else {
                        Err(format!("Invalid address format: {}", addr))
                    }
                })
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        } else {
            Vec::new()
        };

        let args_json = json!({
            "cli": args_value,
            "client": {
                "master_addrs": master_addrs
            }
        })
        .to_string();

        ClusterConf::from(conf_path.unwrap(), Some(&args_json))
    }
}

fn main() -> CommonResult<()> {
    let args = CurvineArgs::parse();
    Utils::set_panic_exit_hook();

    let conf = args.get_conf()?;
    Logger::init(conf.cli.log.clone());

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let curvine_fs = UnifiedFileSystem::with_rt(conf.clone(), rt.clone())?;
    let fs_client = curvine_fs.fs_client();
    let load_client = JobMasterClient::new(fs_client.clone());

    rt.block_on(async move {
        let result = match args.command {
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
