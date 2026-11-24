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
use curvine_core_error::err_box;
use curvine_runtime::common::{ByteUnit, DurationUnit};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Transfer task configuration (`[job]`).
///
/// The legacy master load-job engine is gone; the transfer service owns load
/// and export jobs end to end. What remains here is the worker-side task
/// budget plus the per-object read tuning the transfer planner pushes into the
/// mount properties of every task (see [`LoadTransferDefaults`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JobConf {
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

    // Maximum tasks a worker holds at once: the ones waiting for a concurrency
    // permit plus the ones running. Beyond this the worker rejects new tasks
    // with a retryable "queue full" response, which paces the transfer
    // scheduler's dispatch instead of letting a huge job accumulate millions of
    // queued tasks in the worker's memory. 0 derives the limit from
    // `worker_max_concurrent_tasks` (`2 x`), which keeps one extra wave of work
    // queued.
    pub worker_max_queued_tasks: usize,

    // Per-object parallel read streams used by load/export tasks. One stream is
    // a sequential reader; each stream keeps `ufs_read_concurrent` range
    // requests in flight. Mounts may override it with the
    // `load_task.parallel_streams` property.
    pub load_parallel_streams: usize,

    // Minimum source bytes per stream: an object is split into
    // `len / load_min_bytes_per_stream` streams, capped by
    // `load_parallel_streams`. The opendal default of 256 MiB leaves every
    // sub-quarter-GiB object on a single stream, which caps throughput at the
    // per-stream rate (~2 MB/s towards a distant object store). Mounts may
    // override it with the `load_task.min_bytes_per_stream` property.
    #[serde(skip)]
    pub load_min_bytes_per_stream: i64,
    #[serde(alias = "load_min_bytes_per_stream")]
    pub load_min_bytes_per_stream_str: String,

    // Range size (bytes) of one UFS read request, and how many range requests a
    // stream keeps in flight. Mounts may override them with the
    // `opendal.read_chunk_size` / `opendal.read_concurrent` properties.
    //
    // These multiply: one object holds about
    // `load_parallel_streams * ufs_read_concurrent * ufs_read_chunk_size` bytes
    // of in-flight read buffers (8 * 4 * 2 MiB = 64 MiB with the defaults), and a
    // worker holds `worker_max_concurrent_tasks` of them (6.4 GiB at 100). Size
    // `[worker] transfer_blocking_threads` and the worker memory limit for that
    // product before raising them; the failure mode of overshooting is a worker
    // OOM, not a slow load. (The 58 TB VAE run used 4 MiB * 8 * 8 streams.)
    pub ufs_read_chunk_size: usize,
    pub ufs_read_concurrent: usize,
}

impl JobConf {
    pub const DEFAULT_TASK_TIMEOUT: &'static str = "1h";
    pub const DEFAULT_TASK_REPORT_INTERVAL: &'static str = "10s";
    pub const DEFAULT_WORKER_MAX_CONCURRENT_TASKS: usize = 100;
    /// `0` means "derive it from `worker_max_concurrent_tasks`".
    pub const DEFAULT_WORKER_MAX_QUEUED_TASKS: usize = 0;
    pub const DEFAULT_LOAD_PARALLEL_STREAMS: usize = 8;
    pub const DEFAULT_LOAD_MIN_BYTES_PER_STREAM: &'static str = "32MB";
    /// Parsed form of [`Self::DEFAULT_LOAD_MIN_BYTES_PER_STREAM`]; the worker
    /// falls back to it when a task arrives without the mount property.
    pub const DEFAULT_LOAD_MIN_BYTES_PER_STREAM_BYTES: i64 = 32 * 1024 * 1024;
    pub const DEFAULT_UFS_READ_CHUNK_SIZE: usize = 2 * 1024 * 1024;
    pub const DEFAULT_UFS_READ_CONCURRENT: usize = 4;

    pub fn init(&mut self) -> FsResult<()> {
        self.task_timeout = DurationUnit::from_str(&self.task_timeout_str)?.as_duration();
        self.task_report_interval =
            DurationUnit::from_str(&self.task_report_interval_str)?.as_duration();
        self.load_min_bytes_per_stream =
            ByteUnit::from_str(&self.load_min_bytes_per_stream_str)?.as_byte() as i64;
        if self.load_min_bytes_per_stream <= 0 {
            return err_box!("job.load_min_bytes_per_stream must be > 0");
        }
        if self.load_parallel_streams == 0 {
            return err_box!("job.load_parallel_streams must be > 0");
        }
        if self.ufs_read_chunk_size == 0 || self.ufs_read_concurrent == 0 {
            return err_box!("job.ufs_read_chunk_size and job.ufs_read_concurrent must be > 0");
        }
        if self.worker_max_concurrent_tasks == 0 {
            // A zero-permit semaphore never runs anything, and it would also
            // derive a zero queue limit (= unlimited admission), so reject it
            // instead of failing silently.
            return err_box!("job.worker_max_concurrent_tasks must be > 0");
        }
        if self.worker_max_queued_tasks == 0 {
            self.worker_max_queued_tasks = self.worker_max_concurrent_tasks.saturating_mul(2);
        }

        Ok(())
    }
}
impl Default for JobConf {
    fn default() -> Self {
        Self {
            task_timeout: Default::default(),
            task_timeout_str: Self::DEFAULT_TASK_TIMEOUT.to_string(),

            task_report_interval: Default::default(),
            task_report_interval_str: Self::DEFAULT_TASK_REPORT_INTERVAL.to_string(),

            worker_max_concurrent_tasks: Self::DEFAULT_WORKER_MAX_CONCURRENT_TASKS,
            worker_max_queued_tasks: Self::DEFAULT_WORKER_MAX_QUEUED_TASKS,

            load_parallel_streams: Self::DEFAULT_LOAD_PARALLEL_STREAMS,
            load_min_bytes_per_stream: Self::DEFAULT_LOAD_MIN_BYTES_PER_STREAM_BYTES,
            load_min_bytes_per_stream_str: Self::DEFAULT_LOAD_MIN_BYTES_PER_STREAM.to_string(),
            ufs_read_chunk_size: Self::DEFAULT_UFS_READ_CHUNK_SIZE,
            ufs_read_concurrent: Self::DEFAULT_UFS_READ_CONCURRENT,
        }
    }
}

#[cfg(test)]
mod load_transfer_conf_tests {
    use super::JobConf;

    #[test]
    fn load_transfer_defaults_split_small_objects() {
        // The size floor is parsed from its string form in init(), like the
        // other duration fields.
        let mut conf = JobConf::default();
        conf.init().unwrap();
        // 32 MiB per stream: a 128 MiB object gets 4 streams instead of one.
        assert_eq!(conf.load_min_bytes_per_stream, 32 * 1024 * 1024);
        assert_eq!(conf.load_parallel_streams, 8);
        // 1 MiB/4 range requests (the opendal defaults) cannot fill a distant
        // link; the job defaults ask for bigger ranges and more of them.
        assert_eq!(conf.ufs_read_chunk_size, 2 * 1024 * 1024);
        assert_eq!(conf.ufs_read_concurrent, 4);
    }

    #[test]
    fn load_transfer_keys_parse_and_split_floor_needs_a_size() {
        let parsed: JobConf = toml::from_str(
            "load_parallel_streams = 4\nload_min_bytes_per_stream = \"8MB\"\nufs_read_concurrent = 2",
        )
        .unwrap();
        assert_eq!(parsed.load_parallel_streams, 4);
        assert_eq!(parsed.ufs_read_concurrent, 2);

        let mut parsed = parsed;
        parsed.init().unwrap();
        assert_eq!(parsed.load_min_bytes_per_stream, 8 * 1024 * 1024);

        let mut bad = JobConf {
            load_parallel_streams: 0,
            ..Default::default()
        };
        assert!(bad.init().is_err());
    }
}

/// Worker-visible transfer tuning derived from `[job]`.
///
/// The worker splits one object into `load_task.parallel_streams` readers and each
/// reader issues `opendal.read_concurrent` range requests; opendal's own defaults
/// (1 MiB ranges, and a 256 MiB split floor enforced by the runner) leave small
/// objects on a single slow stream. The transfer planner pushes these values into
/// the mount properties carried by each task, which is what the worker and the
/// opendal adapter read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadTransferDefaults {
    parallel_streams: usize,
    min_bytes_per_stream: i64,
    read_chunk_size: usize,
    read_concurrent: usize,
}

impl LoadTransferDefaults {
    pub const PARALLEL_STREAMS: &'static str = "load_task.parallel_streams";
    pub const MIN_BYTES_PER_STREAM: &'static str = "load_task.min_bytes_per_stream";
    pub const READ_CHUNK_SIZE: &'static str = "opendal.read_chunk_size";
    pub const READ_CONCURRENT: &'static str = "opendal.read_concurrent";

    pub fn from_job(job: &JobConf) -> Self {
        Self {
            parallel_streams: job.load_parallel_streams,
            min_bytes_per_stream: job.load_min_bytes_per_stream,
            read_chunk_size: job.ufs_read_chunk_size,
            read_concurrent: job.ufs_read_concurrent,
        }
    }

    /// Fill the properties a mount did not set itself.
    pub fn apply(&self, properties: &mut std::collections::HashMap<String, String>) {
        let defaults = [
            (Self::PARALLEL_STREAMS, self.parallel_streams.to_string()),
            (
                Self::MIN_BYTES_PER_STREAM,
                self.min_bytes_per_stream.to_string(),
            ),
            (Self::READ_CHUNK_SIZE, self.read_chunk_size.to_string()),
            (Self::READ_CONCURRENT, self.read_concurrent.to_string()),
        ];
        for (key, value) in defaults {
            properties.entry(key.to_string()).or_insert(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JobConf;

    #[test]
    fn queued_task_limit_derives_from_the_concurrency_when_unset() {
        let mut conf = JobConf {
            worker_max_concurrent_tasks: 128,
            ..Default::default()
        };
        assert_eq!(JobConf::DEFAULT_WORKER_MAX_QUEUED_TASKS, 0);
        conf.init().unwrap();
        assert_eq!(
            conf.worker_max_queued_tasks, 256,
            "an unset queue limit keeps one extra wave of work per worker"
        );
    }

    #[test]
    fn queued_task_limit_keeps_an_explicit_value() {
        let mut conf = JobConf {
            worker_max_concurrent_tasks: 128,
            worker_max_queued_tasks: 4096,
            ..Default::default()
        };
        conf.init().unwrap();
        assert_eq!(conf.worker_max_queued_tasks, 4096);
    }
}
