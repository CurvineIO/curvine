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

use crate::FsResult;
use orpc::common::DurationUnit;
use orpc::err_box;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Master load function configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JobConf {
    // job expiration time (seconds)
    #[serde(skip)]
    pub job_life_ttl: Duration,
    #[serde(alias = "job_life_ttl")]
    pub job_life_ttl_str: String,

    // Task expiration time (seconds)
    #[serde(skip)]
    pub job_cleanup_ttl: Duration,
    #[serde(alias = "job_cleanup_ttl")]
    pub job_cleanup_ttl_str: String,

    // Maximum number of files allowed to be loaded by a job
    pub job_max_files: usize,

    // Maximum concurrent master-side load job planning tasks
    pub master_max_concurrent_load_jobs: usize,

    // Maximum master-side load jobs allowed to wait in the background FIFO queue
    pub master_max_background_load_jobs: usize,

    // Maximum load job submit requests allowed to wait before background admission.
    pub master_max_pending_load_job_submits: usize,

    // Runtime threads reserved for master-side load job planning
    pub master_load_job_runtime_threads: usize,

    // Blocking threads reserved for master-side load job planning
    pub master_load_job_blocking_threads: usize,

    // Time to reuse a deterministic source-not-found load failure before retrying UFS.
    #[serde(skip)]
    pub master_failed_load_job_retry_interval: Duration,
    #[serde(alias = "master_failed_load_job_retry_interval")]
    pub master_failed_load_job_retry_interval_str: String,

    // Maximum execution time allowed for a task.
    #[serde(skip)]
    pub task_timeout: Duration,
    #[serde(alias = "task_timeout")]
    pub task_timeout_str: String,

    // Task progress reporting interval
    #[serde(skip)]
    pub task_report_interval: Duration,
    #[serde(alias = "task_report_interval")]
    pub task_report_interval_str: String,

    // Maximum concurrency allowed by worker
    pub worker_max_concurrent_tasks: usize,
}

impl JobConf {
    pub const DEFAULT_JOB_LIFE_TTL: &'static str = "24h";
    pub const DEFAULT_JOB_CLEANUP_TTL_STR: &'static str = "10m";
    pub const DEFAULT_JOB_MAX_FILES: usize = 100000;
    pub const DEFAULT_MASTER_MAX_CONCURRENT_LOAD_JOBS: usize = 16;
    pub const DEFAULT_MASTER_MAX_BACKGROUND_LOAD_JOBS: usize = 1024;
    pub const DEFAULT_MASTER_MAX_PENDING_LOAD_JOB_SUBMITS: usize = 4096;
    pub const DEFAULT_MASTER_LOAD_JOB_RUNTIME_THREADS: usize = 4;
    pub const DEFAULT_MASTER_LOAD_JOB_BLOCKING_THREADS: usize = 16;
    pub const DEFAULT_MASTER_FAILED_LOAD_JOB_RETRY_INTERVAL: &'static str = "30s";
    pub const DEFAULT_TASK_TIMEOUT: &'static str = "1h";
    pub const DEFAULT_TASK_REPORT_INTERVAL: &'static str = "10s";
    pub const DEFAULT_WORKER_MAX_CONCURRENT_TASKS: usize = 100;

    pub fn init(&mut self) -> FsResult<()> {
        self.job_life_ttl = DurationUnit::from_str(&self.job_life_ttl_str)?.as_duration();
        self.job_cleanup_ttl = DurationUnit::from_str(&self.job_cleanup_ttl_str)?.as_duration();
        self.task_timeout = DurationUnit::from_str(&self.task_timeout_str)?.as_duration();
        self.task_report_interval =
            DurationUnit::from_str(&self.task_report_interval_str)?.as_duration();
        self.master_failed_load_job_retry_interval =
            DurationUnit::from_str(&self.master_failed_load_job_retry_interval_str)?.as_duration();
        if self.master_max_concurrent_load_jobs == 0 {
            return err_box!("job.master_max_concurrent_load_jobs must be > 0");
        }
        if self.master_max_background_load_jobs == 0 {
            return err_box!("job.master_max_background_load_jobs must be > 0");
        }
        if self.master_max_pending_load_job_submits == 0 {
            return err_box!("job.master_max_pending_load_job_submits must be > 0");
        }
        if self.master_load_job_runtime_threads == 0 {
            return err_box!("job.master_load_job_runtime_threads must be > 0");
        }
        if self.master_load_job_blocking_threads == 0 {
            return err_box!("job.master_load_job_blocking_threads must be > 0");
        }

        Ok(())
    }
}
impl Default for JobConf {
    fn default() -> Self {
        Self {
            job_life_ttl: Default::default(),
            job_life_ttl_str: Self::DEFAULT_JOB_LIFE_TTL.to_string(),

            job_cleanup_ttl: Default::default(),
            job_cleanup_ttl_str: Self::DEFAULT_JOB_CLEANUP_TTL_STR.to_string(),

            job_max_files: Self::DEFAULT_JOB_MAX_FILES,
            master_max_concurrent_load_jobs: Self::DEFAULT_MASTER_MAX_CONCURRENT_LOAD_JOBS,
            master_max_background_load_jobs: Self::DEFAULT_MASTER_MAX_BACKGROUND_LOAD_JOBS,
            master_max_pending_load_job_submits: Self::DEFAULT_MASTER_MAX_PENDING_LOAD_JOB_SUBMITS,
            master_load_job_runtime_threads: Self::DEFAULT_MASTER_LOAD_JOB_RUNTIME_THREADS,
            master_load_job_blocking_threads: Self::DEFAULT_MASTER_LOAD_JOB_BLOCKING_THREADS,
            master_failed_load_job_retry_interval: Default::default(),
            master_failed_load_job_retry_interval_str:
                Self::DEFAULT_MASTER_FAILED_LOAD_JOB_RETRY_INTERVAL.to_string(),

            task_timeout: Default::default(),
            task_timeout_str: Self::DEFAULT_TASK_TIMEOUT.to_string(),

            task_report_interval: Default::default(),
            task_report_interval_str: Self::DEFAULT_TASK_REPORT_INTERVAL.to_string(),

            worker_max_concurrent_tasks: Self::DEFAULT_WORKER_MAX_CONCURRENT_TASKS,
        }
    }
}
