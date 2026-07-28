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
//

use super::paths::select_list_dir;
use super::report::TaskMetric;
use super::runner::{collect, PathAction, WorkloadExecution};
use super::seed::WorkloadSeedSet;
use super::{BenchOp, CurvineBenchRunner, RENAMED_SUFFIX};
use orpc::CommonResult;
use std::sync::Arc;
use std::time::Instant;

impl CurvineBenchRunner {
    pub(crate) async fn run_single_op(
        &self,
        op: BenchOp,
        worker_id: usize,
        index: usize,
        seed_set: &WorkloadSeedSet,
        execution: WorkloadExecution,
        read_buf: &mut [u8],
    ) -> CommonResult<TaskMetric> {
        match op {
            BenchOp::Read | BenchOp::ReadSmall | BenchOp::ReadBig => {
                let path = &seed_set.files[index % seed_set.files.len()];
                self.read_one(path, read_buf).await
            }
            BenchOp::Open => {
                let path = &seed_set.files[index % seed_set.files.len()];
                self.open_one(path).await
            }
            BenchOp::Write | BenchOp::WriteSmall | BenchOp::WriteBig | BenchOp::Create => {
                let path = self.op_path("workload-file", worker_id, index);
                let size = if matches!(op, BenchOp::WriteBig) {
                    self.config.big_file_size
                } else if execution == WorkloadExecution::MetadataOnly {
                    0
                } else {
                    self.config.small_file_size.max(1)
                };
                self.write_one(&path, size).await
            }
            BenchOp::Stat => {
                let path = &seed_set.files[index % seed_set.files.len()];
                self.stat_one(path).await
            }
            BenchOp::Rename => {
                let src = self.op_path("rename-src", worker_id, index);
                let dst = self.op_path("rename-dst", worker_id, index);
                self.chained_rename(&src, &dst, self.seed_file_size(execution))
                    .await
            }
            BenchOp::Delete => {
                let path = self.op_path("delete", worker_id, index);
                self.chained_delete(&path, self.seed_file_size(execution))
                    .await
            }
            BenchOp::Mkdir => {
                let path = self.op_path("mkdir", worker_id, index);
                self.mkdir_one(&path).await
            }
            BenchOp::Rmdir => {
                let path = self.op_path("rmdir", worker_id, index);
                let start = Instant::now();
                self.mkdir_one(&path).await?;
                self.delete_one(&path).await?;
                Ok(TaskMetric {
                    files: 1,
                    operations: 1,
                    elapsed: start.elapsed(),
                    ..TaskMetric::default()
                }
                .with_latency_sample())
            }
            BenchOp::List => self.list_one(worker_id, index, &seed_set.list_dirs).await,
        }
    }

    pub(crate) async fn run_paths(
        &self,
        paths: &[String],
        action: PathAction,
    ) -> CommonResult<TaskMetric> {
        let paths: Arc<[String]> = Arc::from(paths.to_vec());
        let mut tasks = Vec::with_capacity(self.config.threads);
        for worker_id in 0..self.config.threads {
            let runner = self.clone_for_task();
            let paths = paths.clone();
            let action = action.clone();
            tasks.push(async move {
                let start = Instant::now();
                let mut metric = TaskMetric::default();
                let mut read_buf = matches!(action, PathAction::Read)
                    .then(|| vec![0u8; runner.config.block_size.max(1)]);
                for path in paths.iter().skip(worker_id).step_by(runner.config.threads) {
                    let item = runner
                        .run_path_action(path, &action, read_buf.as_deref_mut().unwrap_or(&mut []))
                        .await?;
                    metric.add(item);
                }
                metric.elapsed = start.elapsed();
                Ok(metric)
            });
        }

        collect(tasks).await
    }

    async fn run_path_action(
        &self,
        path: &str,
        action: &PathAction,
        read_buf: &mut [u8],
    ) -> CommonResult<TaskMetric> {
        match action {
            PathAction::Write { size } => self.write_one(path, *size).await,
            PathAction::Read => self.read_one(path, read_buf).await,
            PathAction::Stat => self.stat_one(path).await,
            PathAction::Open => self.open_one(path).await,
            PathAction::Rename => {
                let dst = format!("{path}{RENAMED_SUFFIX}");
                self.rename_one(path, &dst).await
            }
            PathAction::Delete => self.delete_one(path).await,
            PathAction::Mkdir => self.mkdir_one(path).await,
        }
    }

    fn seed_file_size(&self, execution: WorkloadExecution) -> usize {
        if execution == WorkloadExecution::MetadataOnly {
            0
        } else {
            self.config.small_file_size.max(1)
        }
    }

    async fn chained_rename(
        &self,
        src: &str,
        dst: &str,
        file_size: usize,
    ) -> CommonResult<TaskMetric> {
        let start = Instant::now();
        let write = self.write_one(src, file_size).await?;
        self.rename_one(src, dst).await?;
        Ok(Self::file_metric(write.bytes, start))
    }

    async fn chained_delete(&self, path: &str, file_size: usize) -> CommonResult<TaskMetric> {
        let start = Instant::now();
        let write = self.write_one(path, file_size).await?;
        self.delete_one(path).await?;
        Ok(Self::file_metric(write.bytes, start))
    }

    pub(crate) fn file_metric(bytes: u64, start: Instant) -> TaskMetric {
        TaskMetric {
            bytes,
            files: 1,
            operations: 1,
            elapsed: start.elapsed(),
            ..TaskMetric::default()
        }
        .with_latency_sample()
    }

    fn timed_void_op(start: Instant, result: CommonResult<()>) -> CommonResult<TaskMetric> {
        result?;
        Ok(Self::file_metric(0, start))
    }

    pub(crate) async fn write_one(&self, path: &str, file_size: usize) -> CommonResult<TaskMetric> {
        let start = Instant::now();
        let bytes = self
            .io()?
            .write_file(path, file_size, self.write_buf.as_ref())
            .await?;
        Ok(Self::file_metric(bytes, start))
    }

    async fn read_one(&self, path: &str, read_buf: &mut [u8]) -> CommonResult<TaskMetric> {
        let start = Instant::now();
        let mut fallback = [0u8; 1];
        let buf = if read_buf.is_empty() {
            &mut fallback[..]
        } else {
            read_buf
        };
        let bytes = self.io()?.read_file(path, buf).await?;
        Ok(Self::file_metric(bytes, start))
    }

    async fn open_one(&self, path: &str) -> CommonResult<TaskMetric> {
        Self::timed_void_op(Instant::now(), self.io()?.open_file(path).await)
    }

    async fn stat_one(&self, path: &str) -> CommonResult<TaskMetric> {
        Self::timed_void_op(Instant::now(), self.io()?.stat_file(path).await)
    }

    async fn rename_one(&self, src: &str, dst: &str) -> CommonResult<TaskMetric> {
        Self::timed_void_op(Instant::now(), self.io()?.rename_file(src, dst).await)
    }

    async fn delete_one(&self, path: &str) -> CommonResult<TaskMetric> {
        Self::timed_void_op(Instant::now(), self.io()?.delete_file(path).await)
    }

    pub(crate) async fn mkdir_one(&self, path: &str) -> CommonResult<TaskMetric> {
        Self::timed_void_op(Instant::now(), self.io()?.mkdir(path).await)
    }

    async fn list_one(
        &self,
        worker_id: usize,
        index: usize,
        list_dirs: &[String],
    ) -> CommonResult<TaskMetric> {
        let start = Instant::now();
        let list_path = select_list_dir(&self.tmp_path, worker_id, index, list_dirs);
        let operations = self.io()?.list_dir(list_path).await?;
        Ok(TaskMetric {
            operations,
            elapsed: start.elapsed(),
            ..TaskMetric::default()
        }
        .with_latency_sample())
    }
}
