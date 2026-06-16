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

use super::backend::BenchIo;
use super::paths::{workset_dirs, workset_path};
use super::report::{BenchPrefillReport, TaskMetric};
use super::runner::{collect, PathAction, WorkloadExecution};
use super::{BenchOp, BenchPrefillConfig, CurvineBenchRunner};
use crate::unified::UnifiedFileSystem;
use orpc::CommonResult;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone)]
pub(crate) struct WorkloadSeedSet {
    pub(crate) files: Vec<String>,
    pub(crate) list_dirs: Vec<String>,
}

struct WorksetSeeder {
    fs: UnifiedFileSystem,
    mode: super::BenchMode,
    target: super::BenchTarget,
    block_size: usize,
    threads: usize,
    files_per_dir: usize,
    write_buf: Arc<[u8]>,
}

impl WorksetSeeder {
    fn new(fs: UnifiedFileSystem, config: &BenchPrefillConfig) -> Self {
        Self {
            fs,
            mode: config.mode,
            target: config.target,
            block_size: config.block_size,
            threads: config.threads,
            files_per_dir: config.files_per_dir,
            write_buf: vec![b'c'; config.block_size.max(1)].into(),
        }
    }

    fn io(&self) -> CommonResult<BenchIo<'_>> {
        BenchIo::new(&self.fs, self.mode, self.target, self.block_size)
    }

    fn workset_dirs(&self, base: &str, file_count: usize) -> Vec<String> {
        workset_dirs(base, file_count, self.files_per_dir)
    }

    fn workset_paths(&self, base: &str, file_count: usize) -> Vec<String> {
        (0..file_count)
            .map(|index| workset_path(base, index, self.files_per_dir))
            .collect()
    }

    async fn mkdir_all(&self, paths: &[String]) -> CommonResult<TaskMetric> {
        let paths: Arc<[String]> = Arc::from(paths.to_vec());
        let mut tasks = Vec::with_capacity(self.threads);
        for worker_id in 0..self.threads {
            let seeder = self.clone_for_task();
            let paths = paths.clone();
            tasks.push(async move {
                let start = Instant::now();
                let mut metric = TaskMetric::default();
                for path in paths.iter().skip(worker_id).step_by(seeder.threads) {
                    let item_start = Instant::now();
                    seeder.io()?.mkdir(path).await?;
                    metric.add(CurvineBenchRunner::file_metric(0, item_start));
                }
                metric.elapsed = start.elapsed();
                Ok(metric)
            });
        }
        collect(tasks).await
    }

    async fn write_all(&self, paths: &[String], file_size: usize) -> CommonResult<TaskMetric> {
        let paths: Arc<[String]> = Arc::from(paths.to_vec());
        let mut tasks = Vec::with_capacity(self.threads);
        for worker_id in 0..self.threads {
            let seeder = self.clone_for_task();
            let paths = paths.clone();
            tasks.push(async move {
                let start = Instant::now();
                let mut metric = TaskMetric::default();
                for path in paths.iter().skip(worker_id).step_by(seeder.threads) {
                    let item_start = Instant::now();
                    let bytes = seeder
                        .io()?
                        .write_file(path, file_size, seeder.write_buf.as_ref())
                        .await?;
                    metric.add(CurvineBenchRunner::file_metric(bytes, item_start));
                }
                metric.elapsed = start.elapsed();
                Ok(metric)
            });
        }
        collect(tasks).await
    }

    fn clone_for_task(&self) -> Self {
        Self {
            fs: self.fs.clone(),
            mode: self.mode,
            target: self.target,
            block_size: self.block_size,
            threads: self.threads,
            files_per_dir: self.files_per_dir,
            write_buf: self.write_buf.clone(),
        }
    }
}

impl CurvineBenchRunner {
    pub async fn prefill(
        fs: UnifiedFileSystem,
        config: BenchPrefillConfig,
    ) -> CommonResult<BenchPrefillReport> {
        let file_count = config.file_count()?;
        let seeder = WorksetSeeder::new(fs, &config);
        let start = Instant::now();
        seeder.io()?.mkdir(&config.base_path).await?;
        let dirs = seeder.workset_dirs(&config.base_path, file_count);
        if !dirs.is_empty() {
            seeder.mkdir_all(&dirs).await?;
        }
        let files = seeder.workset_paths(&config.base_path, file_count);
        let sink = seeder.write_all(&files, config.file_size).await?;

        Ok(BenchPrefillReport {
            base_path: config.base_path,
            files: file_count,
            file_size: config.file_size,
            bytes: sink.bytes,
            operations: sink.operations,
            errors: sink.errors,
            elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
        })
    }

    pub(crate) async fn prepare_workload_seed_files(
        &self,
        workload: &super::WorkloadSpec,
        execution: WorkloadExecution,
    ) -> CommonResult<WorkloadSeedSet> {
        if let Some(count) = self.config.working_set_files {
            return Ok(WorkloadSeedSet {
                files: self.workset_paths(count),
                list_dirs: self.workset_dirs_for(&self.config.base_path, count),
            });
        }

        let uses_big_files = workload.ops.iter().any(|item| {
            matches!(
                item.op,
                BenchOp::ReadBig | BenchOp::WriteBig | BenchOp::Read | BenchOp::Write
            )
        });
        let seed_count = if uses_big_files {
            self.config.threads.max(1) * self.config.big_file_count.max(1)
        } else {
            self.config.threads.max(1) * self.config.small_file_count.max(1)
        };
        let seed_files = self.workset_paths_for(&self.tmp_path, seed_count);
        let list_dirs = self.workset_dirs_for(&self.tmp_path, seed_count);
        if !list_dirs.is_empty() {
            self.run_paths(&list_dirs, PathAction::Mkdir).await?;
        }
        let seed_file_size = if uses_big_files {
            self.config.big_file_size.max(1)
        } else if execution == WorkloadExecution::MetadataOnly {
            0
        } else {
            self.config.small_file_size.max(1)
        };
        self.run_paths(
            &seed_files,
            PathAction::Write {
                size: seed_file_size,
            },
        )
        .await?;
        Ok(WorkloadSeedSet {
            files: seed_files,
            list_dirs,
        })
    }
}
