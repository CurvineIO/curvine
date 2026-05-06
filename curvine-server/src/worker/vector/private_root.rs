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

use std::path::{Path, PathBuf};

use curvine_common::conf::VectorSubsystemConf;

#[derive(Debug, Clone)]
pub struct WorkerVectorPrivateRoot {
    root: PathBuf,
}

impl WorkerVectorPrivateRoot {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn from_conf(conf: &VectorSubsystemConf) -> Self {
        Self::new(conf.worker_private_vector_root.trim())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn segment_dataset_dir(
        &self,
        bucket: &str,
        index: &str,
        shard_id: u32,
        segment_id: &str,
    ) -> PathBuf {
        self.root
            .join("buckets")
            .join(bucket)
            .join("indexes")
            .join(index)
            .join("shards")
            .join(shard_id.to_string())
            .join("segments")
            .join(segment_id)
            .join("dataset")
    }
}
