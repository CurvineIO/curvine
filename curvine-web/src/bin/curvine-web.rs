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

use std::sync::Arc;

use clap::{Parser, Subcommand};
use curvine_config::{pipeline, ClusterConf, ConfigSource, ConfigSourceMap};
use curvine_core_error::CommonResult;
use curvine_runtime::common::{LocalTime, Logger, Utils};
use curvine_sys::version;
use curvine_web::server::{AdminWebService, WebServer};

fn main() -> CommonResult<()> {
    let args = WebArgs::parse();
    println!(
        "datetime: {}, git version: {}, args: {:#?}",
        LocalTime::now_datetime(),
        version::GIT_VERSION,
        args
    );

    match args.command {
        WebCommand::Start(start) => start_web(start),
    }
}

fn start_web(args: StartArgs) -> CommonResult<()> {
    let mut conf = ClusterConf::from(&args.conf)?;
    let mut config_sources = if args.conf.trim().is_empty() {
        ConfigSourceMap::new()
    } else {
        let text = pipeline::read_file_text(&args.conf)?;
        pipeline::build_document_with_sources(&text, &[])?.1
    };
    Logger::init(conf.log.clone());
    Utils::set_panic_exit_hook();

    if let Some(hostname) = args.hostname.as_deref().filter(|value| !value.is_empty()) {
        conf.web.hostname = hostname.to_string();
        config_sources.insert("web.hostname".to_string(), ConfigSource::Cli);
    }
    if let Some(port) = args.port {
        conf.web.port = port;
        config_sources.insert("web.port".to_string(), ConfigSource::Cli);
    }
    let web_conf = conf.standalone_web_conf(&conf.web.hostname, conf.web.port);

    let rt = Arc::new(web_conf.create_runtime());
    let service = AdminWebService::with_sources(conf, config_sources, rt.clone());
    let web = WebServer::with_rt(rt, web_conf, service);
    web.block_on_start();
    Ok(())
}

#[derive(Debug, Parser, Clone)]
#[command(version = version::VERSION)]
struct WebArgs {
    #[command(subcommand)]
    command: WebCommand,
}

#[derive(Debug, Subcommand, Clone)]
enum WebCommand {
    Start(StartArgs),
}

#[derive(Debug, Parser, Clone)]
struct StartArgs {
    #[arg(long, default_value = "")]
    conf: String,

    #[arg(long)]
    hostname: Option<String>,

    #[arg(long)]
    port: Option<u16>,
}
