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
use super::paths::join_path;
use super::plan::{BenchPlan, PlannedStep};
use super::report::{
    BenchOpResult, BenchReport, BenchResultGroup, CostUnit, TaskMetric, ValueUnit,
};
use super::workload::{BenchOp, WorkloadKind, WorkloadSpec};
use super::{BenchConfig, CurvineBenchRunner, BENCH_DIR_PREFIX, DEFAULT_DURATION_MS};
use crate::unified::UnifiedFileSystem;
use orpc::{err_box, CommonResult};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkloadExecution {
    Default,
    MetadataOnly,
}

#[derive(Debug, Clone)]
pub(crate) enum PathAction {
    Write {
        size: usize,
    },
    Read,
    Stat,
    Open,
    /// Rename `path` to `path.renamed`.
    Rename,
    Delete,
    Mkdir,
}

pub(crate) struct TimedLoop {
    pub next: Arc<AtomicUsize>,
    pub total_bytes: Arc<AtomicU64>,
    pub total_ops: Arc<AtomicU64>,
    pub(crate) deadline: Instant,
    pub(crate) limits: BenchLimits,
}

impl TimedLoop {
    pub(crate) fn new(limits: BenchLimits) -> Self {
        Self {
            next: Arc::new(AtomicUsize::new(0)),
            total_bytes: Arc::new(AtomicU64::new(0)),
            total_ops: Arc::new(AtomicU64::new(0)),
            deadline: Instant::now() + limits.duration,
            limits,
        }
    }

    pub(crate) fn share(&self) -> Self {
        Self {
            next: self.next.clone(),
            total_bytes: self.total_bytes.clone(),
            total_ops: self.total_ops.clone(),
            deadline: self.deadline,
            limits: self.limits,
        }
    }

    pub(crate) fn should_continue(&self) -> bool {
        Instant::now() < self.deadline
            && !self.limits.reached(
                self.total_bytes.load(Ordering::Relaxed),
                self.total_ops.load(Ordering::Relaxed),
            )
    }

    pub(crate) fn next_index(&self) -> usize {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn record_ok(&self, item: &TaskMetric) {
        self.total_bytes.fetch_add(item.bytes, Ordering::Relaxed);
        self.total_ops.fetch_add(item.operations, Ordering::Relaxed);
    }

    pub(crate) fn record_err(&self) {
        self.total_ops.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BenchLimits {
    pub(crate) duration: Duration,
    total_size: Option<u64>,
    total_ops: Option<u64>,
}

impl BenchLimits {
    pub(crate) fn from_config(config: &BenchConfig) -> Self {
        Self {
            duration: Duration::from_millis(config.duration_ms.unwrap_or(DEFAULT_DURATION_MS)),
            total_size: config.total_size,
            total_ops: config.total_ops,
        }
    }

    pub(crate) fn reached(&self, bytes: u64, ops: u64) -> bool {
        self.total_size.is_some_and(|limit| bytes >= limit)
            || self.total_ops.is_some_and(|limit| ops >= limit)
    }
}

impl CurvineBenchRunner {
    pub fn new(fs: UnifiedFileSystem, config: BenchConfig) -> Self {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let tmp_path = join_path(
            &config.base_path,
            &format!("{}{}", BENCH_DIR_PREFIX, suffix),
        );
        Self::build(fs, config, tmp_path)
    }

    pub(crate) fn build(fs: UnifiedFileSystem, config: BenchConfig, tmp_path: String) -> Self {
        let write_buf: Arc<[u8]> = vec![b'c'; config.block_size.max(1)].into();
        Self {
            fs,
            config,
            tmp_path,
            write_buf,
        }
    }

    pub async fn run(&self) -> CommonResult<BenchReport> {
        let results = self.run_once().await?;

        Ok(BenchReport {
            config: self.config.clone(),
            temp_path: self.tmp_path.clone(),
            results,
        })
    }

    async fn run_once(&self) -> CommonResult<Vec<BenchOpResult>> {
        self.prepare().await?;
        let run_result: CommonResult<Vec<BenchOpResult>> = async {
            let plan = BenchPlan::from_config(&self.config);
            let mut results = Vec::new();
            for step in plan.steps() {
                results.extend(self.run_planned_step(step).await?);
            }
            Ok(results)
        }
        .await;

        let cleanup_result = if self.config.keep_data {
            Ok(())
        } else {
            self.cleanup().await
        };

        let results = run_result?;
        if let Err(error) = cleanup_result {
            eprintln!(
                "Warning: failed to cleanup benchmark temp path {}: {}",
                self.tmp_path, error
            );
        }
        Ok(results)
    }

    async fn run_planned_step(&self, step: &PlannedStep) -> CommonResult<Vec<BenchOpResult>> {
        match step {
            PlannedStep::MetadataSuite => self.run_metadata_suite().await,
            PlannedStep::ThroughputSuite => self.run_throughput_suite().await,
            PlannedStep::Workload(kind) => self.run_workload_kind(kind).await,
        }
    }

    pub(crate) fn io(&self) -> CommonResult<BenchIo<'_>> {
        BenchIo::new(
            &self.fs,
            self.config.mode,
            self.config.target,
            self.config.block_size,
        )
    }

    pub(crate) async fn prepare(&self) -> CommonResult<()> {
        self.io()?.mkdir(&self.tmp_path).await
    }

    async fn cleanup(&self) -> CommonResult<()> {
        if !self.tmp_path.contains(BENCH_DIR_PREFIX) {
            return err_box!("Refuse to cleanup non-benchmark path {}", self.tmp_path);
        }
        self.io()?.delete_tree(&self.tmp_path).await
    }

    async fn run_workload_kind(&self, kind: &WorkloadKind) -> CommonResult<Vec<BenchOpResult>> {
        match kind {
            WorkloadKind::Metadata => self.run_metadata_suite().await,
            WorkloadKind::Throughput => self.run_throughput_suite().await,
            WorkloadKind::MixedMetadata
            | WorkloadKind::MixedThroughput
            | WorkloadKind::Custom(_) => {
                let spec = match kind.mixed_spec()? {
                    Some(spec) => spec,
                    None => return err_box!("mixed workload requires a spec"),
                };
                let execution = if spec.is_metadata_only() {
                    WorkloadExecution::MetadataOnly
                } else {
                    WorkloadExecution::Default
                };
                let group = match kind {
                    WorkloadKind::MixedMetadata => BenchResultGroup::MixedMetadata,
                    WorkloadKind::MixedThroughput => BenchResultGroup::MixedThroughput,
                    _ => BenchResultGroup::MixedWorkload,
                };
                self.run_workload_with(spec, execution, group).await
            }
        }
    }

    async fn run_workload_with(
        &self,
        workload: WorkloadSpec,
        execution: WorkloadExecution,
        group: BenchResultGroup,
    ) -> CommonResult<Vec<BenchOpResult>> {
        let seed_set = Arc::new(
            self.prepare_workload_seed_files(&workload, execution)
                .await?,
        );
        // Start the clock only after seed preparation, so slow seed writes
        // do not consume the configured duration.
        let loop_state = TimedLoop::new(self.limits());
        let wall_start = Instant::now();
        let mut tasks = Vec::with_capacity(self.config.threads);

        for worker_id in 0..self.config.threads {
            let runner = self.clone_for_task();
            let workload = workload.clone();
            let seed_set = seed_set.clone();
            let loop_state = loop_state.share();
            tasks.push(async move {
                let mut by_op = BTreeMap::<BenchOp, TaskMetric>::new();
                let mut read_buf = workload
                    .ops
                    .iter()
                    .any(|item| {
                        matches!(
                            item.op,
                            BenchOp::Read | BenchOp::ReadSmall | BenchOp::ReadBig
                        )
                    })
                    .then(|| vec![0u8; runner.config.block_size.max(1)]);
                while loop_state.should_continue() {
                    let index = loop_state.next_index();
                    let op = workload.select(index);
                    let metric = match runner
                        .run_single_op(
                            op,
                            worker_id,
                            index,
                            seed_set.as_ref(),
                            execution,
                            read_buf.as_deref_mut().unwrap_or(&mut []),
                        )
                        .await
                    {
                        Ok(item) => {
                            loop_state.record_ok(&item);
                            item
                        }
                        Err(error) => {
                            loop_state.record_err();
                            TaskMetric::from_error(error.to_string())
                        }
                    };
                    by_op.entry(op).or_default().merge(metric);
                }
                by_op
            });
        }

        let mut merged = BTreeMap::<BenchOp, TaskMetric>::new();
        for by_op in futures::future::join_all(tasks).await {
            for (op, sink) in by_op {
                let target = merged.entry(op).or_default();
                target.merge(sink);
            }
        }
        let wall_elapsed = wall_start.elapsed();

        let mut results = Vec::new();
        for (op, mut sink) in merged {
            sink.elapsed = wall_elapsed;
            results.push(sink.into_result(
                op,
                group,
                super::LatencyMode::Percentiles,
                ValueUnit::for_workload_op(op),
                CostUnit::MillisPerOp,
            ));
        }
        Ok(results)
    }

    pub(crate) fn limits(&self) -> BenchLimits {
        BenchLimits::from_config(&self.config)
    }

    pub(crate) fn clone_for_task(&self) -> Self {
        Self {
            fs: self.fs.clone(),
            config: self.config.clone(),
            tmp_path: self.tmp_path.clone(),
            write_buf: self.write_buf.clone(),
        }
    }
}

pub(crate) async fn collect<T>(tasks: Vec<T>) -> CommonResult<TaskMetric>
where
    T: std::future::Future<Output = CommonResult<TaskMetric>>,
{
    let mut merged = TaskMetric::default();
    for metric in futures::future::join_all(tasks).await {
        merged.merge(metric?);
    }
    Ok(merged)
}
