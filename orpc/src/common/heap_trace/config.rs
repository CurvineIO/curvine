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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct HeapTraceConfig {
    pub runtime_enabled: bool,
    pub sample_interval_bytes: usize,
}

impl Default for HeapTraceConfig {
    fn default() -> Self {
        Self {
            runtime_enabled: false,
            sample_interval_bytes: 8 * 1024 * 1024,
        }
    }
}

impl HeapTraceConfig {
    pub fn new(runtime_enabled: bool, sample_interval_bytes: usize) -> Self {
        Self {
            runtime_enabled,
            sample_interval_bytes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct HeapTraceSummary {
    pub runtime_enabled: bool,
    pub sample_interval_bytes: usize,
    pub capture_count: u64,
    pub last_capture_epoch_ms: Option<u64>,
}

pub type HeapProfileSummary = HeapTraceSummary;

impl HeapTraceSummary {
    pub fn from_config(conf: &HeapTraceConfig) -> Self {
        Self {
            runtime_enabled: conf.runtime_enabled,
            sample_interval_bytes: conf.sample_interval_bytes,
            capture_count: 0,
            last_capture_epoch_ms: None,
        }
    }
}
