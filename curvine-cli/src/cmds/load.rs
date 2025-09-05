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
    path: String,

    #[arg(long, default_value = "${CURVINE_CONF_FILE}")]
    conf: String,

    /// Watch load job status after submission
    #[arg(long, short = 'w')]
    watch: bool,
}

impl LoadCommand {
    pub async fn execute(&self, client: JobMasterClient) -> CommonResult<()> {
        if self.path.trim().is_empty() {
            eprintln!("Error: Path cannot be empty");
            std::process::exit(1);
        }

        println!("\n Loading file to Curvine");
        println!("Source path: {}", self.path);

        let command = LoadJobCommand::builder(&self.path).build();
        let rep = handle_rpc_result(client.submit_load_job(command)).await;
        println!("{}", rep);

        if self.watch {
            let status_command = LoadStatusCommand::new(
                rep.job_id.clone(),
                false,
                "1s".to_string(),
                self.conf.clone(),
            );

            status_command.execute(client).await?;
        }

        Ok(())
    }
}
