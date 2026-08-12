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

use crate::cmds::load_status::{parse_job_state, print_job_status, print_job_table};
use crate::util::handle_rpc_result;
use clap::Parser;
use curvine_core_error::CommonResult;
use curvine_job_client::{JobListOptions, JobMasterClient};

#[derive(Parser, Debug)]
pub struct LoadListCommand {
    /// Match jobs whose source or target path has this prefix.
    #[arg(long)]
    path: Option<String>,

    /// Match fs_mode jobs under this Curvine mount path.
    #[arg(long)]
    mount: Option<String>,

    /// Filter by state: pending, loading, completed, failed, canceled.
    #[arg(long)]
    state: Option<String>,

    /// Maximum number of jobs to return.
    #[arg(long, default_value_t = 50)]
    limit: usize,

    /// Skip this many jobs (most-recent first) before returning, for pagination.
    #[arg(long, default_value_t = 0)]
    offset: usize,

    /// Include completed, failed, and canceled jobs.
    #[arg(long, default_value_t = true)]
    include_finished: bool,

    /// Only show pending/loading jobs.
    #[arg(long)]
    running_only: bool,

    /// Include task-level details for each job.
    #[arg(long, short = 'v')]
    verbose: bool,

    /// Return only failed jobs; with --verbose, only failed task rows are printed.
    #[arg(long, alias = "failed_only")]
    failed_only: bool,

    #[arg(long, default_value = "${CURVINE_CONF_FILE}")]
    conf: String,
}

impl LoadListCommand {
    pub async fn execute(&self, client: JobMasterClient) -> CommonResult<()> {
        let state = if let Some(value) = &self.state {
            match parse_job_state(value) {
                Some(state) => Some(state),
                None => {
                    eprintln!(
                        "Error: unsupported state '{}'. Use pending, loading, completed, failed, or canceled.",
                        value
                    );
                    std::process::exit(1);
                }
            }
        } else {
            None
        };

        let jobs = handle_rpc_result(client.list_job_statuses_with_options(JobListOptions {
            path_prefix: self.path.clone(),
            limit: self.limit,
            include_finished: self.include_finished && !self.running_only,
            state,
            mount_path: self.mount.clone(),
            include_tasks: self.verbose,
            failed_only: self.failed_only,
            offset: self.offset,
        }))
        .await;

        if self.verbose {
            if jobs.is_empty() {
                println!("No load jobs found.");
            }
            for job in &jobs {
                print_job_status(job, true, self.failed_only);
            }
        } else {
            print_job_table(&jobs);
        }

        Ok(())
    }
}
