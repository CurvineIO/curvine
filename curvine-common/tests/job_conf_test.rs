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

use curvine_common::conf::JobConf;

#[test]
fn default_master_load_job_runtime_conf_is_valid() {
    let mut conf = JobConf::default();

    conf.init().expect("default job conf should be valid");

    assert_eq!(
        conf.master_load_job_runtime_threads,
        JobConf::DEFAULT_MASTER_LOAD_JOB_RUNTIME_THREADS
    );
    assert_eq!(
        conf.master_load_job_blocking_threads,
        JobConf::DEFAULT_MASTER_LOAD_JOB_BLOCKING_THREADS
    );
    assert_eq!(
        conf.master_max_pending_load_job_submits,
        JobConf::DEFAULT_MASTER_MAX_PENDING_LOAD_JOB_SUBMITS
    );
    assert_eq!(
        conf.master_failed_load_job_retry_interval_str,
        JobConf::DEFAULT_MASTER_FAILED_LOAD_JOB_RETRY_INTERVAL
    );
}

#[test]
fn master_load_job_runtime_conf_rejects_invalid_limits() {
    let mut conf = JobConf {
        master_max_background_load_jobs: 0,
        ..Default::default()
    };
    let err = conf
        .init()
        .expect_err("zero background queue capacity must be rejected");
    assert!(
        err.to_string()
            .contains("job.master_max_background_load_jobs must be > 0"),
        "unexpected error: {}",
        err
    );

    let mut conf = JobConf {
        master_load_job_runtime_threads: 0,
        ..Default::default()
    };
    let err = conf
        .init()
        .expect_err("zero runtime threads must be rejected");
    assert!(
        err.to_string()
            .contains("job.master_load_job_runtime_threads must be > 0"),
        "unexpected error: {}",
        err
    );

    let mut conf = JobConf {
        master_load_job_blocking_threads: 0,
        ..Default::default()
    };
    let err = conf
        .init()
        .expect_err("zero blocking threads must be rejected");
    assert!(
        err.to_string()
            .contains("job.master_load_job_blocking_threads must be > 0"),
        "unexpected error: {}",
        err
    );

    let mut conf = JobConf {
        master_max_pending_load_job_submits: 0,
        ..Default::default()
    };
    let err = conf
        .init()
        .expect_err("zero submit queue capacity must be rejected");
    assert!(
        err.to_string()
            .contains("job.master_max_pending_load_job_submits must be > 0"),
        "unexpected error: {}",
        err
    );
}
