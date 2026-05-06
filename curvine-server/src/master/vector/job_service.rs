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

use curvine_common::state::{SegmentGeneration, VectorSegmentId};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct VectorMaintenanceJobKey {
    pub segment_id: VectorSegmentId,
    pub generation: SegmentGeneration,
    pub kind: VectorMaintenanceJobKind,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum VectorMaintenanceJobKind {
    WarmCachePages,
    SealSegment,
    CompactSegment,
    RebuildFilterProjection,
    RebuildVectorIndex,
}

pub trait VectorJobService: Send + Sync {
    fn enqueue(&self, job: VectorMaintenanceJobKey) {
        let _ = job;
    }
}

#[derive(Debug, Default)]
pub struct NoopVectorJobService;

impl VectorJobService for NoopVectorJobService {}
