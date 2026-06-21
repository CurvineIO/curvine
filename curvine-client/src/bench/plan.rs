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

use super::{BenchConfig, BenchProfile, WorkloadKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PlannedStep {
    MetadataSuite,
    ThroughputSuite,
    Workload(WorkloadKind),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BenchPlan {
    steps: Vec<PlannedStep>,
}

impl BenchPlan {
    pub(crate) fn from_config(config: &BenchConfig) -> Self {
        if let Some(kind) = &config.workload {
            return Self {
                steps: vec![PlannedStep::Workload(kind.clone())],
            };
        }

        let mut steps = vec![PlannedStep::MetadataSuite, PlannedStep::ThroughputSuite];
        if config.profile == BenchProfile::Deep {
            steps.extend([
                PlannedStep::Workload(WorkloadKind::MixedMetadata),
                PlannedStep::Workload(WorkloadKind::MixedThroughput),
            ]);
        }
        Self { steps }
    }

    pub(crate) fn steps(&self) -> &[PlannedStep] {
        &self.steps
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench::{BenchMode, BenchTarget};

    fn config(profile: BenchProfile, workload: Option<WorkloadKind>) -> BenchConfig {
        BenchConfig {
            base_path: "cv://default/bench".to_string(),
            mode: BenchMode::Client,
            profile,
            target: BenchTarget::Curvine,
            threads: 1,
            block_size: 1024,
            big_file_size: 1024,
            big_file_count: 1,
            small_file_size: 0,
            small_file_count: 1,
            keep_data: false,
            duration_ms: Some(1),
            total_size: None,
            total_ops: None,
            working_set_files: None,
            files_per_dir: 100,
            workload,
        }
    }

    #[test]
    fn basic_profile_runs_single_op_suites() {
        let plan = BenchPlan::from_config(&config(BenchProfile::Basic, None));

        assert_eq!(
            plan.steps(),
            &[PlannedStep::MetadataSuite, PlannedStep::ThroughputSuite]
        );
    }

    #[test]
    fn deep_profile_adds_mixed_workloads() {
        let plan = BenchPlan::from_config(&config(BenchProfile::Deep, None));

        assert_eq!(
            plan.steps(),
            &[
                PlannedStep::MetadataSuite,
                PlannedStep::ThroughputSuite,
                PlannedStep::Workload(WorkloadKind::MixedMetadata),
                PlannedStep::Workload(WorkloadKind::MixedThroughput),
            ]
        );
    }

    #[test]
    fn explicit_workload_replaces_profile_plan() {
        let plan =
            BenchPlan::from_config(&config(BenchProfile::Deep, Some(WorkloadKind::Throughput)));

        assert_eq!(
            plan.steps(),
            &[PlannedStep::Workload(WorkloadKind::Throughput)]
        );
    }
}
