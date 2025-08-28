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
use orpc::runtime::RpcRuntime;
use orpc::CommonResult;

/// Curvine S3-compatible Object Gateway
#[derive(Debug, Parser, Clone)]
pub struct ObjectArgs {
    /// Path to curvine cluster configuration file
    #[arg(
        short,
        long,
        help = "Configuration file path (optional)",
        default_value = "etc/curvine-cluster.toml"
    )]
    pub conf: String,

    /// Listen address for S3 gateway (host:port)
    #[arg(long, default_value = "0.0.0.0:9900")]
    pub listen: String,

    /// Region to report via GetBucketLocation
    #[arg(long, default_value = "us-east-1")]
    pub region: String,
}

fn main() -> CommonResult<()> {
    let args = ObjectArgs::parse();

    let conf = match ClusterConf::from(&args.conf) {
        Ok(c) => {
            tracing::info!("Loaded configuration from {}", args.conf);
            c
        }
        Err(e) => {
            println!(
                "Warning: Failed to load config file '{}': {}. Using default configuration",
                args.conf, e
            );
            ClusterConf::default()
        }
    };

    // Initialize logging like master
    orpc::common::Logger::init(conf.master.log.clone());

    // Use configuration values with command line arguments as override
    let listen = if args.listen != "0.0.0.0:9900" {
        // Command line argument overrides config
        args.listen.clone()
    } else {
        // Use config value or default
        conf.object.listen.clone()
    };

    let region = if args.region != "us-east-1" {
        // Command line argument overrides config
        args.region.clone()
    } else {
        // Use config value or default
        conf.object.region.clone()
    };

    tracing::info!(
        "S3 Gateway configuration - Listen: {}, Region: {}",
        listen,
        region
    );

    // Create unified AsyncRuntime for all operations
    let rt = std::sync::Arc::new(orpc::runtime::AsyncRuntime::new(
        "curvine-object",
        conf.client.io_threads,
        conf.client.worker_threads,
    ));

    // Run start_gateway with the unified runtime
    rt.block_on(curvine_object::start_gateway(
        conf,
        listen,
        region,
        rt.clone(),
    ))
}
