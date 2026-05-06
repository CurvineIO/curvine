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

/// RocksDB-facing identifiers for vector metadata column families.
///
/// State is derived from the Raft vector journal; phase two wires apply paths.
#[derive(Debug, Default)]
pub struct VectorMetaStore;

impl VectorMetaStore {
    pub const CF_VECTOR_BUCKET: &'static str = "vector_bucket";
    pub const CF_VECTOR_INDEX: &'static str = "vector_index";
    pub const CF_VECTOR_KEY_LATEST: &'static str = "vector_key_latest";
    pub const CF_VECTOR_KEY_HISTORY: &'static str = "vector_key_history";
    pub const CF_VECTOR_SEGMENT: &'static str = "vector_segment";
    pub const CF_VECTOR_SEGMENT_GENERATION: &'static str = "vector_segment_generation";
    pub const CF_VECTOR_SEGMENT_WORKER: &'static str = "vector_segment_worker";
    pub const CF_VECTOR_COMPACTION_JOB: &'static str = "vector_compaction_job";
    pub const CF_VECTOR_COMMON: &'static str = "vector_common";

    #[must_use]
    pub fn all_column_family_names() -> &'static [&'static str] {
        &[
            Self::CF_VECTOR_BUCKET,
            Self::CF_VECTOR_INDEX,
            Self::CF_VECTOR_KEY_LATEST,
            Self::CF_VECTOR_KEY_HISTORY,
            Self::CF_VECTOR_SEGMENT,
            Self::CF_VECTOR_SEGMENT_GENERATION,
            Self::CF_VECTOR_SEGMENT_WORKER,
            Self::CF_VECTOR_COMPACTION_JOB,
            Self::CF_VECTOR_COMMON,
        ]
    }
}
