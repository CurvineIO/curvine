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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct VectorSubsystemConf {
    pub worker_private_vector_root: String,
    pub default_shard_count: u32,
    pub segment_target_size_mb: u64,
    pub active_segment_row_high_watermark: u64,
    pub active_segment_size_mb_high_watermark: u64,
}

impl Default for VectorSubsystemConf {
    fn default() -> Self {
        Self {
            worker_private_vector_root: "/var/lib/curvine/worker/vector".to_string(),
            default_shard_count: 8,
            segment_target_size_mb: 512,
            active_segment_row_high_watermark: 1_000_000,
            active_segment_size_mb_high_watermark: 4096,
        }
    }
}
