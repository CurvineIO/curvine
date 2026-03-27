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

use crate::common::heap_trace::{
    record_capture_attempt, HeapTraceArtifact, HeapTraceArtifactKind, HeapTraceConfig,
    HeapTraceFlamegraph, HeapTraceHttpResponse, HeapTraceProfile, HeapTraceSummary,
};
use crate::CommonResult;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct HeapTraceRuntime {
    conf: Arc<HeapTraceConfig>,
}

impl HeapTraceRuntime {
    pub fn new(conf: HeapTraceConfig) -> Self {
        Self {
            conf: Arc::new(conf),
        }
    }

    pub fn conf(&self) -> &HeapTraceConfig {
        self.conf.as_ref()
    }

    pub fn summary(&self) -> HeapTraceSummary {
        HeapTraceSummary::from_config(self.conf())
    }

    pub fn is_enabled(&self) -> bool {
        self.conf.runtime_enabled
    }

    pub fn capture_profile(&self) -> CommonResult<HeapTraceProfile> {
        record_capture_attempt();
        Ok(HeapTraceProfile {
            format: "pprof".to_string(),
            payload: Vec::new(),
        })
    }

    pub fn capture_flamegraph(&self) -> CommonResult<HeapTraceFlamegraph> {
        record_capture_attempt();
        Ok(HeapTraceFlamegraph {
            format: "svg".to_string(),
            svg: String::new(),
        })
    }

    pub fn profile_artifact(&self) -> CommonResult<HeapTraceArtifact> {
        let profile = self.capture_profile()?;
        Ok(HeapTraceArtifact::new(
            HeapTraceArtifactKind::Profile,
            "application/octet-stream",
            "heap.pb.gz",
            profile.payload,
        ))
    }

    pub fn flamegraph_artifact(&self) -> CommonResult<HeapTraceArtifact> {
        let flamegraph = self.capture_flamegraph()?;
        Ok(HeapTraceArtifact::new(
            HeapTraceArtifactKind::Flamegraph,
            "image/svg+xml",
            "heap.svg",
            flamegraph.svg.into_bytes(),
        ))
    }

    pub fn flamegraph_http_response(&self) -> CommonResult<HeapTraceHttpResponse> {
        let artifact = self.flamegraph_artifact()?;
        Ok(HeapTraceHttpResponse {
            content_type: artifact.media_type,
            body: artifact.payload,
        })
    }
}
