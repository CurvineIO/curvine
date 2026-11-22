// Copyright 2026 OPPO.
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
use curvine_cli::cmds::FsCommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_runtime::common::Logger;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::process::{Command, Output};
use std::sync::Arc;

const CHILD_MODE: &str = "CURVINE_CLI_MUTATION_OUTPUT_CHILD";

fn child_main() -> CommonResult<()> {
    let conf_path = std::env::var("CURVINE_CONF_FILE")?;
    let conf = ClusterConf::from(conf_path)?;
    Logger::init(conf.cli.log.clone());

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = UnifiedFileSystem::with_rt(conf, rt.clone())?;
    let args: Vec<_> = std::env::args().skip(1).collect();
    let command = FsCommand::try_parse_from(args)?;
    rt.block_on(command.execute(fs))
}

fn run_child(testing: &Testing, args: &[&str]) -> std::io::Result<Output> {
    Command::new(std::env::current_exe()?)
        .env(CHILD_MODE, "1")
        .env("CURVINE_CONF_FILE", testing.active_conf_path())
        .args(args)
        .output()
}

fn assert_success_output(args: &[&str], expected_status: &str, output: Output) {
    assert!(
        output.status.success(),
        "{args:?} failed with stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.is_empty(),
        "{args:?} wrote to stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(expected_status),
        "{args:?} did not report {expected_status:?} on stderr: {stderr}"
    );
    assert!(
        !stderr.contains("Creating directory:"),
        "{args:?} wrote the removed mkdir debug message to stderr: {stderr}"
    );
}

fn test_success_output() -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(|conf| conf.cli.log.log_dir = "stderr".to_string())
        .build()?;
    let _cluster = testing.start_cluster()?;
    let base_path = format!("/cli-mutation-output-{}", std::process::id());
    let file_path = format!("{base_path}/file");

    let mkdir_args = ["fs", "mkdir", &base_path];
    assert_success_output(
        &mkdir_args,
        "Directory created successfully",
        run_child(&testing, &mkdir_args)?,
    );

    let create_args = ["fs", "touch", &file_path];
    assert_success_output(
        &create_args,
        &format!("Created empty file: {file_path}"),
        run_child(&testing, &create_args)?,
    );

    let update_args = ["fs", "touch", &file_path];
    assert_success_output(
        &update_args,
        &format!("Updated timestamp: {file_path}"),
        run_child(&testing, &update_args)?,
    );

    Ok(())
}

fn main() {
    let result = if std::env::var_os(CHILD_MODE).is_some() {
        child_main()
    } else {
        test_success_output()
    };

    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
