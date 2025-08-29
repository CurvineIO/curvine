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
use curvine_client::rpc::JobMasterClient;
use curvine_common::state::WorkState;
use orpc::CommonResult;

use crate::util::*;

#[derive(Parser, Debug)]
pub struct LoadStatusCommand {
    job_id: String,

    #[arg(long, short = 'v')]
    verbose: bool,

    #[arg(long, short = 'w', default_value = "5s")]
    watch: Option<String>,

    #[arg(long, default_value = "${CURVINE_CONF_FILE}")]
    conf: String,
}

impl LoadStatusCommand {
    pub async fn execute(&self, client: JobMasterClient) -> CommonResult<()> {
        println!("\n Checking status for {}", self.job_id);

        if let Some(watch_interval) = &self.watch {
            self.watch_status(client, watch_interval).await
        } else {
            let status = handle_rpc_result(client.get_job_status(&self.job_id)).await;
            println!("{}", status);
            Ok(())
        }
    }

    /// keep watch job status
    ///
    /// # Arguments
    /// * `client` - LoadClient Instance
    /// * `interval_str` - Example：5s, 1m
    async fn watch_status(&self, client: JobMasterClient, interval_str: &str) -> CommonResult<()> {
        // Resolution refresh interval
        let duration = parse_duration(interval_str).unwrap_or_else(|_| {
            eprintln!("❌ Error: Invalid watch interval format: {}", interval_str);
            eprintln!("    Supported formats: <number>s (seconds), <number>m (minutes)");
            eprintln!("    Example: 5s, 1m");
            std::process::exit(1);
        });

        println!(
            "Watching job status (refresh every {}). Press Ctrl+C to stop.",
            format_duration(&duration)
        );

        loop {
            if cfg!(target_os = "windows") {
                let _ = std::process::Command::new("cmd")
                    .args(["/c", "cls"])
                    .status();
            } else {
                print!("\x1B[2J\x1B[1;1H");
            }

            println!(
                "\n Checking status for {} (refreshing every {})",
                self.job_id,
                format_duration(&duration)
            );
            println!("Press Ctrl+C to stop watching.");

            let status = handle_rpc_result(client.get_job_status(&self.job_id)).await;
            println!("{}", status);

            if status.state == WorkState::Completed
                || status.state == WorkState::Failed
                || status.state == WorkState::Canceled
            {
                break;
            }

            tokio::time::sleep(duration).await;
        }

        Ok(())
    }
}
