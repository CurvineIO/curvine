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

use super::report::{BenchReportUnits, CostUnit, ValueUnit};
use super::runner::PathAction;
use super::{
    BenchOp, BenchOpResult, BenchResultGroup, CurvineBenchRunner, LatencyMode, RENAMED_SUFFIX,
};
use orpc::CommonResult;

const UNITS_BIG_IO: BenchReportUnits = BenchReportUnits {
    latency: LatencyMode::AvgOnly,
    value_unit: ValueUnit::MibPerSec,
    cost_unit: CostUnit::SecondsPerFile,
};

const UNITS_META_OP: BenchReportUnits = BenchReportUnits {
    latency: LatencyMode::Percentiles,
    value_unit: ValueUnit::OpsPerSec,
    cost_unit: CostUnit::MillisPerOp,
};

#[derive(Clone, Copy)]
enum MetadataAction {
    Create,
    Stat,
    Open,
    Rename,
    DeleteRenamed,
}

#[derive(Clone, Copy)]
struct MetadataStep {
    action: MetadataAction,
    op: BenchOp,
}

const METADATA_STEPS: &[MetadataStep] = &[
    MetadataStep {
        action: MetadataAction::Create,
        op: BenchOp::Create,
    },
    MetadataStep {
        action: MetadataAction::Stat,
        op: BenchOp::Stat,
    },
    MetadataStep {
        action: MetadataAction::Open,
        op: BenchOp::Open,
    },
    MetadataStep {
        action: MetadataAction::Rename,
        op: BenchOp::Rename,
    },
    MetadataStep {
        action: MetadataAction::DeleteRenamed,
        op: BenchOp::Delete,
    },
];

#[derive(Clone, Copy)]
enum ThroughputAction {
    WriteBig,
    ReadBig,
}

#[derive(Clone, Copy)]
struct ThroughputStep {
    action: ThroughputAction,
    op: BenchOp,
}

const THROUGHPUT_STEPS: &[ThroughputStep] = &[
    ThroughputStep {
        action: ThroughputAction::WriteBig,
        op: BenchOp::WriteBig,
    },
    ThroughputStep {
        action: ThroughputAction::ReadBig,
        op: BenchOp::ReadBig,
    },
];

impl CurvineBenchRunner {
    /// NNBench-style metadata suite: each op runs alone over a shared,
    /// fixed-size file set. Rename/delete reuse create output so each phase
    /// measures exactly one metadata API.
    pub(crate) async fn run_metadata_suite(&self) -> CommonResult<Vec<BenchOpResult>> {
        let group = BenchResultGroup::Metadata;
        let count = self.config.threads * self.config.small_file_count;
        if count == 0 {
            return Ok(Vec::new());
        }

        let files = self.paths("meta", count);
        let renamed = files
            .iter()
            .map(|path| format!("{path}{RENAMED_SUFFIX}"))
            .collect::<Vec<_>>();
        let mut results = Vec::with_capacity(METADATA_STEPS.len());
        for step in METADATA_STEPS {
            let (paths, action) = match step.action {
                MetadataAction::Create => (files.as_slice(), PathAction::Write { size: 0 }),
                MetadataAction::Stat => (files.as_slice(), PathAction::Stat),
                MetadataAction::Open => (files.as_slice(), PathAction::Open),
                MetadataAction::Rename => (files.as_slice(), PathAction::Rename),
                MetadataAction::DeleteRenamed => (renamed.as_slice(), PathAction::Delete),
            };
            results.push(
                self.bench_paths(paths, action, step.op, group, UNITS_META_OP)
                    .await?,
            );
        }
        Ok(results)
    }

    /// Single-op throughput suite: write big files once, then read them back.
    pub(crate) async fn run_throughput_suite(&self) -> CommonResult<Vec<BenchOpResult>> {
        let group = BenchResultGroup::Throughput;
        if self.config.big_file_size == 0 || self.config.big_file_count == 0 {
            return Ok(Vec::new());
        }

        let big_files = self.paths("bigfile", self.config.threads * self.config.big_file_count);
        let mut results = Vec::with_capacity(THROUGHPUT_STEPS.len());
        for step in THROUGHPUT_STEPS {
            let action = match step.action {
                ThroughputAction::WriteBig => PathAction::Write {
                    size: self.config.big_file_size,
                },
                ThroughputAction::ReadBig => PathAction::Read,
            };
            results.push(
                self.bench_paths(&big_files, action, step.op, group, UNITS_BIG_IO)
                    .await?,
            );
        }
        Ok(results)
    }

    async fn bench_paths(
        &self,
        paths: &[String],
        action: PathAction,
        op: BenchOp,
        group: BenchResultGroup,
        units: BenchReportUnits,
    ) -> CommonResult<BenchOpResult> {
        Ok(self.run_paths(paths, action).await?.into_result(
            op,
            group,
            units.latency,
            units.value_unit,
            units.cost_unit,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suite_step_order_is_stable() {
        let metadata = METADATA_STEPS
            .iter()
            .map(|step| step.op)
            .collect::<Vec<_>>();
        assert_eq!(
            metadata,
            vec![
                BenchOp::Create,
                BenchOp::Stat,
                BenchOp::Open,
                BenchOp::Rename,
                BenchOp::Delete,
            ]
        );

        let throughput = THROUGHPUT_STEPS
            .iter()
            .map(|step| step.op)
            .collect::<Vec<_>>();
        assert_eq!(throughput, vec![BenchOp::WriteBig, BenchOp::ReadBig]);
    }
}
