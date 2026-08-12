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

use super::{BenchConfig, BenchOp};
use serde::Serialize;
use std::time::Duration;

const LATENCY_BUCKETS: usize = 64;
const MAX_ERROR_SAMPLES: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchResultGroup {
    Metadata,
    Throughput,
    MixedMetadata,
    MixedThroughput,
    MixedWorkload,
}

impl BenchResultGroup {
    pub fn label(&self) -> &'static str {
        match self {
            BenchResultGroup::Metadata => "metadata suite (single-op)",
            BenchResultGroup::Throughput => "throughput suite (single-op)",
            BenchResultGroup::MixedMetadata => "mixed metadata workload",
            BenchResultGroup::MixedThroughput => "mixed throughput workload",
            BenchResultGroup::MixedWorkload => "mixed workload",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyMode {
    AvgOnly,
    Percentiles,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueUnit {
    MibPerSec,
    OpsPerSec,
}

impl ValueUnit {
    pub(crate) fn for_workload_op(op: BenchOp) -> Self {
        if matches!(
            op,
            BenchOp::Read | BenchOp::Write | BenchOp::ReadBig | BenchOp::WriteBig
        ) {
            Self::MibPerSec
        } else {
            Self::OpsPerSec
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::MibPerSec => "MiB/s",
            Self::OpsPerSec => "ops/s",
        }
    }

    fn value(self, metric: &TaskMetric, elapsed_secs: f64) -> f64 {
        match self {
            Self::MibPerSec => metric.bytes as f64 / 1024.0 / 1024.0 / elapsed_secs,
            Self::OpsPerSec => metric.operations as f64 / elapsed_secs,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CostUnit {
    SecondsPerFile,
    MillisPerOp,
}

impl CostUnit {
    fn label(self) -> &'static str {
        match self {
            Self::SecondsPerFile => "s/file",
            Self::MillisPerOp => "ms/op",
        }
    }

    fn value(self, avg_ms: f64) -> f64 {
        match self {
            Self::SecondsPerFile => avg_ms / 1000.0,
            Self::MillisPerOp => avg_ms,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct BenchReportUnits {
    pub latency: LatencyMode,
    pub value_unit: ValueUnit,
    pub cost_unit: CostUnit,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchOpResult {
    pub op: BenchOp,
    pub group: BenchResultGroup,
    pub latency_mode: LatencyMode,
    pub item: String,
    pub value: f64,
    pub value_unit: String,
    pub cost: f64,
    pub cost_unit: String,
    pub bytes: u64,
    pub files: u64,
    pub operations: u64,
    pub errors: u64,
    pub error_samples: Vec<String>,
    pub latency_samples: usize,
    pub elapsed_ms: f64,
    pub latency_avg_ms: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub latency_max_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchReport {
    pub config: BenchConfig,
    pub temp_path: String,
    pub results: Vec<BenchOpResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchPrefillReport {
    pub base_path: String,
    pub files: usize,
    pub file_size: usize,
    pub bytes: u64,
    pub operations: u64,
    pub errors: u64,
    pub elapsed_ms: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct LatencyStats {
    buckets: [u64; LATENCY_BUCKETS],
    count: u64,
    total_ms: f64,
    max_ms: f64,
}

impl Default for LatencyStats {
    fn default() -> Self {
        Self {
            buckets: [0; LATENCY_BUCKETS],
            count: 0,
            total_ms: 0.0,
            max_ms: 0.0,
        }
    }
}

impl LatencyStats {
    pub(crate) fn record_duration(&mut self, duration: Duration) {
        self.record_ms(duration.as_secs_f64() * 1000.0);
    }

    pub(crate) fn record_ms(&mut self, duration_ms: f64) {
        let duration_ms = duration_ms.max(0.0);
        self.count += 1;
        self.total_ms += duration_ms;
        if duration_ms > self.max_ms {
            self.max_ms = duration_ms;
        }
        let micros = (duration_ms * 1000.0).round().max(1.0) as u64;
        self.buckets[latency_bucket_index(micros)] += 1;
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.count += other.count;
        self.total_ms += other.total_ms;
        if other.max_ms > self.max_ms {
            self.max_ms = other.max_ms;
        }
        for (index, value) in other.buckets.iter().enumerate() {
            self.buckets[index] += value;
        }
    }

    pub(crate) fn samples(&self) -> usize {
        usize::try_from(self.count).unwrap_or(usize::MAX)
    }

    pub(crate) fn avg_ms(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.total_ms / self.count as f64
        }
    }

    pub(crate) fn max_ms(&self) -> f64 {
        self.max_ms
    }

    pub(crate) fn percentile_ms(&self, percentile: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }

        let percentile = percentile.clamp(0.0, 100.0);
        let target_rank =
            ((percentile / 100.0) * (self.count.saturating_sub(1)) as f64).round() as u64;
        let mut seen = 0u64;
        for (index, count) in self.buckets.iter().enumerate() {
            seen += *count;
            if seen > target_rank {
                return latency_bucket_upper_ms(index).min(self.max_ms);
            }
        }
        self.max_ms
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct TaskMetric {
    pub(crate) bytes: u64,
    pub(crate) files: u64,
    pub(crate) operations: u64,
    pub(crate) errors: u64,
    pub(crate) elapsed: Duration,
    pub(crate) latency_stats: LatencyStats,
    pub(crate) error_samples: Vec<String>,
}

impl TaskMetric {
    /// Accumulate counters from a single-op result (elapsed is NOT updated).
    pub(crate) fn add(&mut self, item: TaskMetric) {
        self.bytes += item.bytes;
        self.files += item.files;
        self.operations += item.operations;
        self.errors += item.errors;
        self.latency_stats.merge(&item.latency_stats);
        merge_error_samples(&mut self.error_samples, item.error_samples);
    }

    /// Merge another aggregate (elapsed takes max for wall-clock correctness).
    pub(crate) fn merge(&mut self, other: TaskMetric) {
        if other.elapsed > self.elapsed {
            self.elapsed = other.elapsed;
        }
        self.add(other);
    }

    pub(crate) fn add_error(&mut self, error: String) {
        push_error_sample(&mut self.error_samples, error);
    }

    pub(crate) fn from_error(error: String) -> Self {
        let mut metric = Self {
            errors: 1,
            operations: 1,
            ..Self::default()
        };
        metric.add_error(error);
        metric
    }

    pub(crate) fn with_latency_sample(mut self) -> Self {
        if self.elapsed > Duration::ZERO {
            self.latency_stats.record_duration(self.elapsed);
        }
        self
    }

    pub(crate) fn into_result(
        self,
        op: BenchOp,
        group: BenchResultGroup,
        latency_mode: LatencyMode,
        value_unit: ValueUnit,
        cost_unit: CostUnit,
    ) -> BenchOpResult {
        let elapsed_secs = self.elapsed.as_secs_f64().max(f64::EPSILON);
        let value = value_unit.value(&self, elapsed_secs);
        let avg = self.latency_stats.avg_ms();
        let cost = cost_unit.value(avg);

        BenchOpResult {
            op,
            group,
            latency_mode,
            item: op.label().to_string(),
            value,
            value_unit: value_unit.label().to_string(),
            cost,
            cost_unit: cost_unit.label().to_string(),
            bytes: self.bytes,
            files: self.files,
            operations: self.operations,
            errors: self.errors,
            error_samples: self.error_samples,
            latency_samples: self.latency_stats.samples(),
            elapsed_ms: self.elapsed.as_secs_f64() * 1000.0,
            latency_avg_ms: avg,
            latency_p50_ms: self.latency_stats.percentile_ms(50.0),
            latency_p95_ms: self.latency_stats.percentile_ms(95.0),
            latency_p99_ms: self.latency_stats.percentile_ms(99.0),
            latency_max_ms: self.latency_stats.max_ms(),
        }
    }
}

fn merge_error_samples(target: &mut Vec<String>, source: Vec<String>) {
    for sample in source {
        push_error_sample(target, sample);
    }
}

fn push_error_sample(target: &mut Vec<String>, sample: String) {
    if target.len() >= MAX_ERROR_SAMPLES {
        return;
    }
    if sample.trim().is_empty() {
        return;
    }
    target.push(sample);
}

fn latency_bucket_index(micros: u64) -> usize {
    let micros = micros.max(1);
    let idx = (u64::BITS - 1 - micros.leading_zeros()) as usize;
    idx.min(LATENCY_BUCKETS - 1)
}

fn latency_bucket_upper_ms(index: usize) -> f64 {
    let shift = (index + 1).min(63);
    let micros = if shift == 63 {
        u64::MAX
    } else {
        (1u64 << shift) - 1
    };
    micros as f64 / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_is_capped_by_observed_max() {
        let mut stats = LatencyStats::default();
        stats.record_ms(20.73);

        assert_eq!(stats.max_ms(), 20.73);
        assert!(stats.percentile_ms(50.0) <= stats.max_ms());
        assert!(stats.percentile_ms(95.0) <= stats.max_ms());
        assert!(stats.percentile_ms(99.0) <= stats.max_ms());
    }

    #[test]
    fn value_unit_labels_remain_json_compatible() {
        let metric = TaskMetric {
            bytes: 1024 * 1024,
            files: 2,
            operations: 3,
            elapsed: Duration::from_secs(1),
            ..TaskMetric::default()
        };

        assert_eq!(
            metric
                .clone()
                .into_result(
                    BenchOp::Read,
                    BenchResultGroup::MixedWorkload,
                    LatencyMode::AvgOnly,
                    ValueUnit::MibPerSec,
                    CostUnit::SecondsPerFile,
                )
                .value_unit,
            "MiB/s"
        );
        assert_eq!(
            metric
                .into_result(
                    BenchOp::Create,
                    BenchResultGroup::Metadata,
                    LatencyMode::Percentiles,
                    ValueUnit::OpsPerSec,
                    CostUnit::MillisPerOp,
                )
                .cost_unit,
            "ms/op"
        );
    }
}
