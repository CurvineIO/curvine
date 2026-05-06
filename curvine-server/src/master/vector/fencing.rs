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

use curvine_common::proto::VectorSegmentFenceProto;
use curvine_common::state::{OwnerEpoch, SegmentGeneration};

#[derive(Debug, Clone)]
pub struct VectorFenceSnapshot {
    pub owner_epoch: OwnerEpoch,
    pub segment_generation: SegmentGeneration,
    pub published_generation: SegmentGeneration,
}

impl VectorFenceSnapshot {
    pub fn to_proto(&self) -> VectorSegmentFenceProto {
        VectorSegmentFenceProto {
            owner_epoch: Some(self.owner_epoch.0),
            segment_generation: Some(self.segment_generation.0),
            published_generation: Some(self.published_generation.0),
        }
    }
}
