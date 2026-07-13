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

use crate::cmds::LoadStatusCommand;
use crate::util::*;
use clap::Parser;
use curvine_client::rpc::JobMasterClient;
use curvine_common::state::LoadJobCommand;
use orpc::CommonResult;

#[derive(Parser, Debug)]
pub struct LoadCommand {
    /// UFS source path to load into Curvine, or a UFS-backed Curvine path
    source_path: String,

    /// Optional Curvine target path for explicit UFS-to-Curvine load
    target_path: Option<String>,

    /// Watch load job status after submission
    #[arg(long, short = 'w')]
    watch: bool,
}

impl LoadCommand {
    pub async fn execute(&self, client: JobMasterClient) -> CommonResult<()> {
        if self.source_path.trim().is_empty() {
            eprintln!("Error: Path cannot be empty");
            std::process::exit(1);
        }
        if self
            .target_path
            .as_ref()
            .is_some_and(|target_path| target_path.trim().is_empty())
        {
            eprintln!("Error: Target path cannot be empty");
            std::process::exit(1);
        }

        println!("\n Loading file to Curvine");
        println!("Source path: {}", self.source_path);
        if let Some(target_path) = &self.target_path {
            println!("Target path: {}", target_path);
        }

        let command_builder = LoadJobCommand::builder(&self.source_path);
        let command = if let Some(target_path) = &self.target_path {
            command_builder.target_path(target_path).build()
        } else {
            command_builder.build()
        };
        let rep = handle_rpc_result(client.submit_load_job(command)).await;
        println!("{}", rep);

        if self.watch {
            let status_command =
                LoadStatusCommand::new(rep.job_id.clone(), false, "1s".to_string());

            status_command.execute(client).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_explicit_cv_target_path() {
        let cmd =
            LoadCommand::try_parse_from(["load", "s3://flink/user", "/flink/user", "--watch"])
                .expect("load command should accept an optional CV target path");

        assert_eq!(cmd.source_path, "s3://flink/user");
        assert_eq!(cmd.target_path.as_deref(), Some("/flink/user"));
        assert!(cmd.watch);
    }
}
