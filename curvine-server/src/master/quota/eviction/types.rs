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

use curvine_common::conf::ClusterConf;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum EvictionPolicy {
    Lru,
    Lfu,
    Arc,
}

#[derive(Clone, Copy, Debug)]
pub enum EvictionMode {
    FreeFile,
    DeleteFile,
}

#[derive(Clone, Debug)]
pub struct EvictionConf {
    pub enable_quota_eviction: bool,
    pub eviction_mode: EvictionMode,
    pub policy: EvictionPolicy,
    pub high_watermark: f64,
    pub low_watermark: f64,
    pub candidate_scan_page: usize,
    pub dry_run: bool,
    pub capacity: usize,
}

impl EvictionConf {
    pub fn from_conf(conf: &ClusterConf) -> Self {
        let master_conf = &conf.master;

        // Parse eviction mode from string
        let eviction_mode = match master_conf.quota_eviction_mode.as_str() {
            "delete" => EvictionMode::DeleteFile,
            _ => EvictionMode::FreeFile,
        };

        // Parse eviction policy from string (case-insensitive)
        let policy = match master_conf.quota_eviction_policy.to_lowercase().as_str() {
            "lru" => EvictionPolicy::Lru,
            "lfu" => EvictionPolicy::Lfu,
            "arc" => EvictionPolicy::Arc,
            _ => EvictionPolicy::Lru,
        };

        Self {
            enable_quota_eviction: master_conf.enable_quota_eviction,
            eviction_mode,
            policy,
            high_watermark: master_conf.quota_eviction_high_rate,
            low_watermark: master_conf.quota_eviction_low_rate,
            candidate_scan_page: master_conf.quota_eviction_scan_page as usize,
            dry_run: master_conf.quota_eviction_dry_run,
            capacity: master_conf.quota_eviction_capacity,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct EvictPlan {
    pub trigger_used: i64,
    pub quota_size: i64,
    pub target_free_bytes: i64,
}

impl fmt::Display for EvictPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EvictPlan(trigger_used={}, quota_size={}, target_free_bytes={})",
            self.trigger_used, self.quota_size, self.target_free_bytes
        )
    }
}
