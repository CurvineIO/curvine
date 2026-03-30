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
use curvine_common::conf::ClusterConf;
use curvine_common::version;
use curvine_server::master::Master;
use curvine_server::worker::Worker;
#[cfg(feature = "heap-trace")]
use log::warn;
use orpc::common::{LocalTime, Utils};
use orpc::{err_box, CommonResult};

#[cfg(feature = "heap-trace")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> CommonResult<()> {
    let args: ServerArgs = ServerArgs::parse();
    println!(
        "datetime: {}, git version: {}, args: {:#?}",
        LocalTime::now_datetime(),
        version::GIT_VERSION,
        args
    );

    let service = args.get_service()?;
    let mut conf = args.get_conf()?;

    Utils::set_panic_exit_hook();

    match service {
        ServiceType::Master => {
            conf.check_master_hostname()?;
            init_heap_trace(&conf)?;
            let master = Master::with_conf(conf)?;
            master.block_on_start();
        }

        ServiceType::Worker => {
            init_heap_trace(&conf)?;
            let worker = Worker::with_conf(conf)?;
            worker.block_on_start();
        }
    }

    Ok(())
}

#[cfg(feature = "heap-trace")]
fn init_heap_trace(conf: &ClusterConf) -> CommonResult<()> {
    if !conf.heap_trace.runtime_enabled {
        return Ok(());
    }

    const REQUIRED_JEMALLOC_SETTINGS: &[&str] = &["prof:true", "prof_active:true"];
    let malloc_conf = std::env::var("MALLOC_CONF").unwrap_or_default();
    let profiling_ready = REQUIRED_JEMALLOC_SETTINGS
        .iter()
        .all(|setting| malloc_conf.contains(setting));

    if !profiling_ready {
        warn!(
            "Heap trace requested but jemalloc profiling is not enabled at process start; disabling heap trace runtime. Set MALLOC_CONF=prof:true,prof_active:true[,lg_prof_sample:<log2 bytes>] before launch."
        );
        return Ok(());
    }

    Ok(())
}

#[cfg(not(feature = "heap-trace"))]
fn init_heap_trace(_conf: &ClusterConf) -> CommonResult<()> {
    Ok(())
}

#[derive(Debug, Parser, Clone)]
#[command(version = version::VERSION)]
pub struct ServerArgs {
    // Start the worker or the master
    #[arg(long, default_value = "")]
    service: String,

    // Configuration file path
    #[arg(long, default_value = "")]
    conf: String,
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
