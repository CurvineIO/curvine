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

use crate::unified::UnifiedFileSystem;
use std::sync::Arc;

pub(crate) const BENCH_DIR_PREFIX: &str = "__curvine_benchmark_";
pub(crate) const RENAMED_SUFFIX: &str = ".renamed";

mod backend;
mod config;
mod ops;
mod paths;
mod plan;
mod report;
mod runner;
mod seed;
mod suites;
mod workload;

pub use config::{
    BenchConfig, BenchMode, BenchPrefillConfig, BenchProfile, BenchTarget,
    DEFAULT_BASIC_BIG_FILE_COUNT, DEFAULT_BIG_FILE_COUNT, DEFAULT_DURATION_MS,
    DEFAULT_FILES_PER_DIR,
};
pub use paths::{infer_mode, join_path, parse_duration, workset_path};
pub use report::{BenchOpResult, BenchPrefillReport, BenchReport, BenchResultGroup, LatencyMode};
pub use workload::{
    BenchOp, WeightedOp, WorkloadKind, WorkloadSpec, MIXED_METADATA_SPEC, MIXED_THROUGHPUT_SPEC,
};

pub struct CurvineBenchRunner {
    pub(crate) fs: UnifiedFileSystem,
    pub(crate) config: BenchConfig,
    pub(crate) tmp_path: String,
    pub(crate) write_buf: Arc<[u8]>,
}
