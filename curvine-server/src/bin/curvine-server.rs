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

use clap::Parser;
use curvine_alloc as _;
use curvine_config::{ClusterConf, ConfigLoader};
use curvine_core_error::{err_box, CommonResult};
use curvine_data_transfer::transfer::TransferServer;
use curvine_master::master::Master;
use curvine_runtime::common::{LocalTime, Utils};
use curvine_sys::version;
use curvine_worker::Worker;

fn main() -> CommonResult<()> {
    let args: ServerArgs = ServerArgs::parse();
    if args.version_json {
        let json = match version::component_version_json(args.component_name()) {
            Ok(json) => json,
            Err(e) => return err_box!("Failed to serialize component version: {}", e),
        };
        println!("{}", json);
        return Ok(());
    }

    println!(
        "datetime: {}, git version: {}, args: {:#?}",
        LocalTime::now_datetime(),
        version::GIT_VERSION,
        args
    );

    let service = args.get_service()?;
    let mut conf = args.get_conf(&service)?;

    Utils::set_panic_exit_hook();

    match service {
        ServiceType::Master => {
            conf.check_master_hostname()?;
            let master = Master::with_conf(conf)?;
            master.block_on_start()?;
        }

        ServiceType::Worker => {
            let worker = Worker::with_conf(conf)?;
            worker.block_on_start()?;
        }

        ServiceType::Transfer => {
            let transfer = TransferServer::with_conf(conf)?;
            transfer.block_on_start()?;
        }
    }

    Ok(())
}

#[derive(Debug, Parser, Clone)]
#[command(version = version::VERSION)]
pub struct ServerArgs {
    #[arg(long, help = "Print the component version in JSON format and exit")]
    version_json: bool,

    // Start the worker or the master
    #[arg(long, default_value = "")]
    service: String,

    // Configuration file path
    #[arg(long, default_value = "")]
    conf: String,
}

impl ServerArgs {
    pub fn component_name(&self) -> &'static str {
        match self.service.to_lowercase().as_str() {
            "master" => "master",
            "worker" => "worker",
            "transfer" => "data-transfer",
            _ => "server",
        }
    }

    pub fn get_service(&self) -> CommonResult<ServiceType> {
        let service = self.service.to_lowercase();
        match service.as_str() {
            "master" => Ok(ServiceType::Master),
            "worker" => Ok(ServiceType::Worker),
            "transfer" => Ok(ServiceType::Transfer),
            v => err_box!("Unsupported service type: {}", v),
        }
    }

    pub fn get_conf(&self, service: &ServiceType) -> CommonResult<ClusterConf> {
        // Unified discovery: `--conf` > CURVINE_CONF_FILE > well-known
        // locations (launch scripts export the env var; bare local runs fall
        // back to ./conf/curvine-cluster.toml).
        let found = ConfigLoader::discover(Some(&self.conf)).ok_or_else(
            || -> curvine_core_error::CommonError {
                format!(
                    "no configuration file found: pass --conf or set {}, or place \
                     curvine-cluster.toml in a well-known location",
                    ClusterConf::ENV_CONF_FILE
                )
                .into()
            },
        )?;
        println!("Loading config from {} ({})", found.as_str(), found.source);

        let load = match service {
            ServiceType::Transfer => ClusterConf::from_transfer,
            ServiceType::Master | ServiceType::Worker => ClusterConf::from,
        };
        load(found.as_str())
    }
}

pub enum ServiceType {
    Master,
    Worker,
    Transfer,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_json_accepts_service_component() {
        let args =
            ServerArgs::try_parse_from(["curvine-server", "--service", "master", "--version-json"])
                .expect("server version json should parse");

        assert!(args.version_json);
        assert_eq!(args.component_name(), "master");
    }

    #[test]
    fn version_json_without_service_uses_server_component() {
        let args = ServerArgs::try_parse_from(["curvine-server", "--version-json"])
            .expect("server version json should parse without service");

        assert!(args.version_json);
        assert_eq!(args.component_name(), "server");
    }
}
