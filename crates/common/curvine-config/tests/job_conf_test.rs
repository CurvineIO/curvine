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

use curvine_config::{JobConf, LoadTransferDefaults};
use std::time::Duration;

#[test]
fn default_job_conf_is_valid_and_keeps_transfer_tuning() {
    let mut conf = JobConf::default();

    conf.init().expect("default job conf should be valid");

    assert_eq!(
        conf.task_timeout,
        Duration::from_secs(60 * 60),
        "the default task budget is one hour"
    );
    assert_eq!(conf.task_report_interval, Duration::from_secs(10));
    assert_eq!(
        conf.worker_max_concurrent_tasks,
        JobConf::DEFAULT_WORKER_MAX_CONCURRENT_TASKS
    );
    assert_eq!(conf.load_parallel_streams, 8);
    assert_eq!(conf.load_min_bytes_per_stream, 32 * 1024 * 1024);
    assert_eq!(conf.ufs_read_chunk_size, 2 * 1024 * 1024);
    assert_eq!(conf.ufs_read_concurrent, 4);
}

#[test]
fn load_transfer_defaults_fill_the_mount_properties() {
    let mut conf = JobConf::default();
    conf.init().unwrap();

    let mut properties = std::collections::HashMap::new();
    properties.insert(
        LoadTransferDefaults::PARALLEL_STREAMS.to_string(),
        "2".to_string(),
    );
    LoadTransferDefaults::from_job(&conf).apply(&mut properties);

    // A mount property wins; the rest comes from `[job]`.
    assert_eq!(properties[LoadTransferDefaults::PARALLEL_STREAMS], "2");
    assert_eq!(
        properties[LoadTransferDefaults::MIN_BYTES_PER_STREAM],
        (32 * 1024 * 1024).to_string()
    );
    assert_eq!(
        properties[LoadTransferDefaults::READ_CHUNK_SIZE],
        (2 * 1024 * 1024).to_string()
    );
    assert_eq!(properties[LoadTransferDefaults::READ_CONCURRENT], "4");
}

#[test]
fn job_conf_rejects_invalid_limits() {
    let mut conf = JobConf {
        worker_max_concurrent_tasks: 0,
        ..Default::default()
    };
    let err = conf
        .init()
        .expect_err("zero worker concurrency must be rejected");
    assert!(
        err.to_string()
            .contains("job.worker_max_concurrent_tasks must be > 0"),
        "unexpected error: {}",
        err
    );

    let mut conf = JobConf {
        load_parallel_streams: 0,
        ..Default::default()
    };
    let err = conf
        .init()
        .expect_err("zero parallel streams must be rejected");
    assert!(
        err.to_string()
            .contains("job.load_parallel_streams must be > 0"),
        "unexpected error: {}",
        err
    );

    let mut conf = JobConf {
        ufs_read_concurrent: 0,
        ..Default::default()
    };
    let err = conf
        .init()
        .expect_err("zero read concurrency must be rejected");
    assert!(
        err.to_string()
            .contains("job.ufs_read_chunk_size and job.ufs_read_concurrent must be > 0"),
        "unexpected error: {}",
        err
    );
}
