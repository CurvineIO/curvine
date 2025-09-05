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

use clap::{arg, Parser};
use curvine_common::conf::ClusterConf;
use curvine_common::version;
use curvine_server::master::Master;
use curvine_server::worker::Worker;
use orpc::common::{LocalTime, Utils};
use orpc::{err_box, CommonResult};

fn main() -> CommonResult<()> {
    let args: ServerArgs = ServerArgs::parse();
    println!(
        "datetime: {}, git version: {}, args: {:#?}",
        LocalTime::now_datetime(),
        version::GIT_VERSION,
        args
    );

    let service = args.get_service()?;
    let conf = args.get_conf()?;
    conf.print();
    
    Utils::set_panic_exit_hook();

    match service {
        ServiceType::Master => {
            let master = Master::with_conf(conf)?;
            master.block_on_start();
        }

        ServiceType::Worker => {
            let worker = Worker::with_conf(conf)?;
            worker.block_on_start();
        }
    }

    Ok(())
}

#[derive(Debug, Parser, Clone)]
pub struct ServerArgs {
    // Start the worker or the master
    #[arg(long, default_value = "")]
    service: String,

    // Configuration file path
    #[arg(long, default_value = "")]
    conf: String,

    // Enable S3 object gateway alongside master
    #[arg(long, default_value_t = false)]
    enable_s3_gateway: bool,

    // Gateway listen address
    #[arg(long, default_value = "0.0.0.0:9900")]
    object_listen: String,

    // Gateway region
    #[arg(long, default_value = "us-east-1")]
    object_region: String,
}

impl ServerArgs {
    pub fn get_service(&self) -> CommonResult<ServiceType> {
        let service = self.service.to_lowercase();
        match service.as_str() {
            "master" => Ok(ServiceType::Master),
            "worker" => Ok(ServiceType::Worker),
            v => err_box!("Unsupported service type: {}", v),
        }
    }

    pub fn get_conf(&self) -> CommonResult<ClusterConf> {
        ClusterConf::from(&self.conf)
    }
}

pub enum ServiceType {
    Master,
    Worker,
}
