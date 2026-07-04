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

use orpc::{err_box, CommonResult};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchOp {
    WriteBig,
    ReadBig,
    WriteSmall,
    ReadSmall,
    Create,
    Open,
    Stat,
    Rename,
    Delete,
    Mkdir,
    Rmdir,
    List,
    Write,
    Read,
}

impl BenchOp {
    pub fn workload_name(&self) -> &'static str {
        match self {
            BenchOp::WriteBig => "write_big",
            BenchOp::ReadBig => "read_big",
            BenchOp::WriteSmall => "write_small",
            BenchOp::ReadSmall => "read_small",
            BenchOp::Create => "create",
            BenchOp::Open => "open",
            BenchOp::Stat => "stat",
            BenchOp::Rename => "rename",
            BenchOp::Delete => "delete",
            BenchOp::Mkdir => "mkdir",
            BenchOp::Rmdir => "rmdir",
            BenchOp::List => "list",
            BenchOp::Write => "write",
            BenchOp::Read => "read",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            BenchOp::WriteBig => "Write big file",
            BenchOp::ReadBig => "Read big file",
            BenchOp::WriteSmall => "Write small file",
            BenchOp::ReadSmall => "Read small file",
            BenchOp::Create => "Create file",
            BenchOp::Open => "Open file",
            BenchOp::Stat => "Stat file",
            BenchOp::Rename => "Rename file",
            BenchOp::Delete => "Delete file",
            BenchOp::Mkdir => "Mkdir",
            BenchOp::Rmdir => "Rmdir",
            BenchOp::List => "List dir",
            BenchOp::Write => "Write",
            BenchOp::Read => "Read",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().replace(['-', '.'], "_").as_str() {
            "write_big" | "big_write" => Some(BenchOp::WriteBig),
            "read_big" | "big_read" => Some(BenchOp::ReadBig),
            "write_small" | "small_write" => Some(BenchOp::WriteSmall),
            "read_small" | "small_read" => Some(BenchOp::ReadSmall),
            "create" => Some(BenchOp::Create),
            "open" => Some(BenchOp::Open),
            "stat" | "get_status" => Some(BenchOp::Stat),
            "rename" => Some(BenchOp::Rename),
            "delete" | "unlink" => Some(BenchOp::Delete),
            "mkdir" => Some(BenchOp::Mkdir),
            "rmdir" => Some(BenchOp::Rmdir),
            "list" | "list_status" | "readdir" => Some(BenchOp::List),
            "write" => Some(BenchOp::Write),
            "read" => Some(BenchOp::Read),
            _ => None,
        }
    }

    pub(crate) fn is_metadata_op(self) -> bool {
        matches!(
            self,
            BenchOp::Create
                | BenchOp::Open
                | BenchOp::Stat
                | BenchOp::Rename
                | BenchOp::Delete
                | BenchOp::Mkdir
                | BenchOp::Rmdir
                | BenchOp::List
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WeightedOp {
    pub op: BenchOp,
    pub weight: usize,
}

pub const MIXED_METADATA_SPEC: &str = "create:30,stat:40,rename:10,delete:20";
pub const MIXED_THROUGHPUT_SPEC: &str = "read_big:50,write_big:50";

/// Mixed workloads expand weights into this many slots before shuffling.
const NORMALIZED_SLOTS: usize = 100;

/// Fixed seed for parse-time shuffle only (not runtime op pick).
const WORKLOAD_SHUFFLE_SEED: u64 = 982_451_653;

/// Workload selection. `Metadata` and `Throughput` are NNBench-style
/// single-op sequential suites; the `Mixed*` variants and `Custom` run a
/// weighted timed-loop mixed workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "spec")]
pub enum WorkloadKind {
    Metadata,
    Throughput,
    MixedMetadata,
    MixedThroughput,
    Custom(WorkloadSpec),
}

impl WorkloadKind {
    pub fn parse_or_preset(spec: &str) -> CommonResult<Self> {
        match spec.trim().to_ascii_lowercase().as_str() {
            "metadata" => Ok(Self::Metadata),
            "throughput" => Ok(Self::Throughput),
            "mixed_metadata" => Ok(Self::MixedMetadata),
            "mixed_throughput" => Ok(Self::MixedThroughput),
            "default" => Ok(Self::Custom(WorkloadSpec::parse("read:70,write:30")?)),
            _ => Ok(Self::Custom(WorkloadSpec::parse(spec)?)),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Throughput => "throughput",
            Self::MixedMetadata => "mixed_metadata",
            Self::MixedThroughput => "mixed_throughput",
            Self::Custom(_) => "custom",
        }
    }

    pub fn mixed_spec(&self) -> CommonResult<Option<WorkloadSpec>> {
        match self {
            Self::Metadata | Self::Throughput => Ok(None),
            Self::MixedMetadata => Ok(Some(WorkloadSpec::parse(MIXED_METADATA_SPEC)?)),
            Self::MixedThroughput => Ok(Some(WorkloadSpec::parse(MIXED_THROUGHPUT_SPEC)?)),
            Self::Custom(spec) => Ok(Some(spec.clone())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkloadSpec {
    pub ops: Vec<WeightedOp>,
    /// Deterministic shuffled op sequence of length [`NORMALIZED_SLOTS`].
    #[serde(skip)]
    sequence: Vec<BenchOp>,
}

impl WorkloadSpec {
    pub fn parse(spec: &str) -> CommonResult<Self> {
        let mut ops = Vec::new();
        for item in spec.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }

            let mut parts = item.split(':');
            let op_name = parts.next().unwrap_or_default();
            let weight = parts
                .next()
                .map(|v| v.parse::<usize>())
                .transpose()
                .map_err(|e| format!("Invalid workload weight in '{}': {}", item, e))?
                .unwrap_or(1);

            if weight == 0 {
                return err_box!(
                    "Invalid workload item '{}': weight must be greater than 0",
                    item
                );
            }

            let op = BenchOp::parse(op_name)
                .ok_or_else(|| format!("Unsupported workload operation '{}'", op_name))?;
            ops.push(WeightedOp { op, weight });
        }

        if ops.is_empty() {
            return err_box!("Workload must contain at least one operation");
        }

        let sequence = build_normalized_sequence(&ops);
        Ok(Self { ops, sequence })
    }

    pub(crate) fn select(&self, index: usize) -> BenchOp {
        self.sequence[index % self.sequence.len()]
    }

    pub(crate) fn is_metadata_only(&self) -> bool {
        self.ops.iter().all(|item| item.op.is_metadata_op())
    }

    pub fn validate_metadata_only(&self) -> CommonResult<()> {
        for item in &self.ops {
            if !item.op.is_metadata_op() {
                return err_box!(
                    "metadata workload preset does not support data operation {:?}",
                    item.op
                );
            }
        }
        Ok(())
    }
}

fn build_normalized_sequence(ops: &[WeightedOp]) -> Vec<BenchOp> {
    let total: usize = ops.iter().map(|item| item.weight).sum();
    debug_assert!(total > 0);

    let mut counts = vec![0usize; ops.len()];
    let mut remainders = Vec::with_capacity(ops.len());
    for (index, item) in ops.iter().enumerate() {
        let scaled = item.weight * NORMALIZED_SLOTS;
        counts[index] = scaled / total;
        remainders.push((scaled % total, index));
    }

    let slots_left = NORMALIZED_SLOTS.saturating_sub(counts.iter().sum());
    remainders.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    for (_, index) in remainders.into_iter().take(slots_left) {
        counts[index] += 1;
    }

    let mut sequence = Vec::with_capacity(NORMALIZED_SLOTS);
    for (item, count) in ops.iter().zip(counts.iter()) {
        sequence.extend(std::iter::repeat_n(item.op, *count));
    }
    debug_assert_eq!(sequence.len(), NORMALIZED_SLOTS);

    shuffle_sequence(&mut sequence);
    sequence
}

fn shuffle_sequence(sequence: &mut [BenchOp]) {
    let mut rng = StdRng::seed_from_u64(WORKLOAD_SHUFFLE_SEED);
    sequence.shuffle(&mut rng);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_workload_spec() {
        let workload = WorkloadSpec::parse("read:70,write:30,stat").unwrap();
        assert_eq!(workload.ops.len(), 3);
        assert_eq!(workload.ops[0].op, BenchOp::Read);
        assert_eq!(workload.ops[0].weight, 70);
        assert_eq!(workload.ops[2].weight, 1);
    }

    #[test]
    fn workload_selection_spreads_weighted_ops() {
        let workload = WorkloadSpec::parse("read:70,write:30").unwrap();
        let selected = (0..20)
            .map(|index| workload.select(index))
            .collect::<Vec<_>>();
        assert!(selected.contains(&BenchOp::Read));
        assert!(selected.contains(&BenchOp::Write));
    }

    #[test]
    fn workload_selection_matches_normalized_weights_per_period() {
        let workload = WorkloadSpec::parse("read:70,write:30").unwrap();
        let mut read = 0usize;
        let mut write = 0usize;
        for index in 0..NORMALIZED_SLOTS {
            match workload.select(index) {
                BenchOp::Read => read += 1,
                BenchOp::Write => write += 1,
                other => panic!("unexpected op {:?}", other),
            }
        }
        assert_eq!(read, 70);
        assert_eq!(write, 30);
    }

    #[test]
    fn workload_selection_honors_equal_small_weights() {
        let workload = WorkloadSpec::parse("create:1,open:1,delete:1").unwrap();
        let mut counts = std::collections::BTreeMap::<BenchOp, usize>::new();
        for index in 0..NORMALIZED_SLOTS {
            *counts.entry(workload.select(index)).or_default() += 1;
        }
        assert_eq!(counts.len(), 3);
        assert!(counts.values().all(|count| (33..=34).contains(count)));
        assert_eq!(counts.values().sum::<usize>(), NORMALIZED_SLOTS);
    }

    #[test]
    fn workload_shuffle_is_deterministic() {
        let left = WorkloadSpec::parse("create:50,open:30,delete:20").unwrap();
        let right = WorkloadSpec::parse("create:50,open:30,delete:20").unwrap();
        assert_eq!(
            (0..NORMALIZED_SLOTS)
                .map(|index| left.select(index))
                .collect::<Vec<_>>(),
            (0..NORMALIZED_SLOTS)
                .map(|index| right.select(index))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn metadata_classification_is_shared_by_validation() {
        let metadata = WorkloadSpec::parse("create:1,get_status:1,list_status:1").unwrap();
        assert!(metadata.is_metadata_only());
        metadata.validate_metadata_only().unwrap();

        let mixed = WorkloadSpec::parse("create:1,read:1").unwrap();
        assert!(!mixed.is_metadata_only());
        assert!(mixed.validate_metadata_only().is_err());
    }
}
