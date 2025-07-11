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
use curvine_client::file::CurvineFileSystem;
use curvine_client::LoadClient;
use curvine_common::conf::ClusterConf;
use curvine_common::version;
use orpc::common::Utils;
use orpc::runtime::RpcRuntime;
use orpc::CommonResult;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct CurvineArgs {
    #[command(subcommand)]
    command: Commands,
}

fn main() -> CommonResult<()> {
    let args = CurvineArgs::parse();
    Utils::set_panic_exit_hook();

    let actual_conf = std::env::var("CURVINE_CONF_FILE").unwrap_or_else(|_| {
        eprintln!("CURVINE_CONF_FILE not set, Usage:");
        eprintln!("export CURVINE_CONF_FILE=/path/to/your/curvine-cluster.toml");
        std::process::exit(1);
    });

    let conf = ClusterConf::from(&actual_conf)
        .map_err(|e| format!("Failed to load configuration from {}: {}", actual_conf, e))?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let curvine_fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone())?;
    let fs_client = curvine_fs.fs_client();
    let load_client = LoadClient::new(fs_client.clone())?;

    rt.block_on(async move {
        let result = match args.command {
            Commands::Fs(cmd) => cmd.execute(curvine_fs).await,
            Commands::Report(cmd) => cmd.execute(curvine_fs).await,
            Commands::Load(cmd) => cmd.execute(load_client).await,
            Commands::LoadStatus(cmd) => cmd.execute(load_client).await,
            Commands::CancelLoad(cmd) => cmd.execute(load_client).await,
            Commands::Mount(cmd) => cmd.execute(fs_client).await,
            Commands::UnMount(cmd) => cmd.execute(fs_client).await,
            Commands::Node(cmd) => cmd.execute(fs_client, conf.clone()).await,
            Commands::Version => {
                println!("Curvine version: {}", version::GIT_VERSION);
                Ok(())
            }
        };

        if let Err(e) = &result {
            eprintln!("Error: {}", e);
        }

        result
    })
}
