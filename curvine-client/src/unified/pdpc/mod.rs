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

mod hydrate_admission;
mod meta_gate;
mod route_index;
mod types;

use self::hydrate_admission::{HydrateAdmission, HydrateAdmissionGuard};
use self::meta_gate::MetaGate;
pub use self::route_index::RouteIndexSnapshot;
pub use self::types::PdpcConfig;
use curvine_common::conf::ClientConf;
use curvine_common::fs::Path;
use curvine_common::state::FileStatus;
use curvine_common::utils::CommonUtils;

#[derive(Debug, Clone)]
pub struct PdpcController {
    config: PdpcConfig,
    meta_gate: MetaGate,
    hydrate_admission: HydrateAdmission,
}

impl PdpcController {
    fn positive_status_ttl_ms(conf: &ClientConf) -> u64 {
        let sync_tick_ms = conf.sync_check_interval_min.as_millis() as u64;
        sync_tick_ms.clamp(50, 500)
    }

    fn positive_status_capacity(conf: &ClientConf) -> u64 {
        conf.pdpc_negative_cache_capacity.clamp(1, 16_384)
    }

    pub fn new(conf: &ClientConf) -> Self {
        let positive_status_ttl_ms = Self::positive_status_ttl_ms(conf);
        let positive_status_capacity = Self::positive_status_capacity(conf);
        let config = PdpcConfig {
            negative_cache_ttl_ms: conf.pdpc_negative_cache_ttl_ms,
            negative_cache_capacity: conf.pdpc_negative_cache_capacity,
            singleflight_stale_ttl_ms: conf.pdpc_singleflight_stale_ttl_ms,
            singleflight_max_entries: conf.pdpc_singleflight_max_entries,
            positive_status_ttl_ms,
            positive_status_capacity,
        };
        Self {
            config,
            meta_gate: MetaGate::new(
                conf.pdpc_negative_cache_capacity,
                conf.pdpc_negative_cache_ttl_ms,
                positive_status_capacity,
                positive_status_ttl_ms,
            ),
            hydrate_admission: HydrateAdmission::new(
                conf.pdpc_singleflight_stale_ttl_ms,
                conf.pdpc_singleflight_max_entries,
            ),
        }
    }

    pub fn config(&self) -> PdpcConfig {
        self.config
    }

    pub fn hydrate_key(&self, source_path: &Path, _source_generation: Option<&str>) -> String {
        CommonUtils::create_job_id(source_path.full_path())
    }

    pub fn try_acquire_hydrate(
        &self,
        source_path: &Path,
        source_generation: Option<&str>,
    ) -> Option<HydrateAdmissionGuard> {
        let key = self.hydrate_key(source_path, source_generation);
        self.hydrate_admission.try_acquire(key)
    }

    pub fn hydrate_in_flight_count(&self) -> usize {
        self.hydrate_admission.in_flight_count()
    }

    fn open_notfound_key(ufs_path: &Path) -> String {
        format!("open:{}", ufs_path.full_path())
    }

    pub fn should_skip_ufs_open_probe(&self, ufs_path: &Path) -> bool {
        self.meta_gate
            .should_skip_open_probe(&Self::open_notfound_key(ufs_path))
    }

    pub fn mark_ufs_open_notfound(&self, ufs_path: &Path) {
        self.meta_gate
            .mark_open_notfound(Self::open_notfound_key(ufs_path));
    }

    pub fn clear_ufs_open_notfound(&self, ufs_path: &Path) {
        self.meta_gate
            .clear_open_notfound(&Self::open_notfound_key(ufs_path));
    }

    fn status_key(ufs_path: &Path) -> String {
        format!("status:{}", ufs_path.full_path())
    }

    pub fn get_ufs_status_hint(&self, ufs_path: &Path) -> Option<FileStatus> {
        self.meta_gate.get_ufs_status(&Self::status_key(ufs_path))
    }

    pub fn put_ufs_status_hint(&self, ufs_path: &Path, status: &FileStatus) {
        self.meta_gate
            .put_ufs_status(Self::status_key(ufs_path), status.clone());
    }

    pub fn clear_ufs_status_hint(&self, ufs_path: &Path) {
        self.meta_gate.clear_ufs_status(&Self::status_key(ufs_path));
    }
}
