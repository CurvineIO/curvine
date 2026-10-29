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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use curvine_config::{parse_duration_ms, ClusterConf, ObservabilityConf};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_unified_fs::UnifiedFileSystem;
use tokio::sync::Mutex;

use super::collector::{collect_once, run_collector};
use super::model::{ObservabilitySettings, TargetHealth};
use super::provider::{MetricsProvider, ProviderStats, TargetRegistry};
use super::scraper::MetricsScraper;
use super::tsdb::InMemoryTsdb;

const DEFAULT_SCRAPE_INTERVAL_MS: u64 = 60_000;
const DEFAULT_SCRAPE_TIMEOUT_MS: u64 = 3_000;
const DEFAULT_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;
const MIN_SCRAPE_INTERVAL_MS: u64 = 30_000;
const MAX_SCRAPE_INTERVAL_MS: u64 = 5 * 60 * 1000;
const MIN_RETENTION_MS: u64 = 15 * 60 * 1000;
const MAX_RETENTION_MS: u64 = 3 * 24 * 60 * 60 * 1000;
const MAX_SAMPLES_PER_SERIES_CAP: usize = 10_000;
const MAX_TOTAL_SAMPLES: usize = 15_000_000;

pub const SCRAPE_INTERVAL_OPTIONS_MS: &[u64] = &[30_000, 60_000, 2 * 60_000, 5 * 60_000];
pub const RETENTION_OPTIONS_MS: &[u64] = &[
    60 * 60 * 1000,
    6 * 60 * 60 * 1000,
    24 * 60 * 60 * 1000,
    2 * 24 * 60 * 60 * 1000,
    3 * 24 * 60 * 60 * 1000,
];

pub struct BuiltinProvider {
    conf: ClusterConf,
    unified_fs: UnifiedFileSystem,
    tsdb: Arc<InMemoryTsdb>,
    scraper: MetricsScraper,
    targets: RwLock<HashMap<String, TargetHealth>>,
    last_discovery_ms: AtomicI64,
    last_collect_ms: AtomicI64,
    collector_started: AtomicBool,
    collector_enabled: AtomicBool,
    discovery_started: AtomicBool,
    discovery_enabled: AtomicBool,
    scrape_interval_ms: AtomicU64,
    retention_ms: AtomicU64,
    scrape_timeout_ms: u64,
    max_concurrent_scrapes: usize,
    max_samples_per_series: usize,
    collect_lock: Mutex<()>,
}

impl BuiltinProvider {
    pub fn new(
        observability: &ObservabilityConf,
        conf: ClusterConf,
        unified_fs: UnifiedFileSystem,
    ) -> Self {
        let scrape_interval_ms = parse_duration_ms(&observability.scrape_interval)
            .unwrap_or(DEFAULT_SCRAPE_INTERVAL_MS)
            .clamp(MIN_SCRAPE_INTERVAL_MS, MAX_SCRAPE_INTERVAL_MS);
        let retention_ms = parse_duration_ms(&observability.retention)
            .unwrap_or(DEFAULT_RETENTION_MS)
            .clamp(MIN_RETENTION_MS, MAX_RETENTION_MS);
        let scrape_timeout_ms = parse_duration_ms(&observability.scrape_timeout)
            .unwrap_or(DEFAULT_SCRAPE_TIMEOUT_MS)
            .max(1_000);
        let max_series = observability.max_series.max(1);
        let max_samples_per_series = observability
            .max_samples_per_series
            .clamp(64, MAX_SAMPLES_PER_SERIES_CAP);
        Self {
            conf,
            unified_fs,
            tsdb: Arc::new(InMemoryTsdb::new(
                retention_ms as i64,
                max_samples_for(retention_ms, scrape_interval_ms).min(max_samples_per_series),
                max_series,
                MAX_TOTAL_SAMPLES,
            )),
            scraper: MetricsScraper::with_max_payload_bytes(
                Duration::from_millis(scrape_timeout_ms),
                observability.max_payload_bytes,
            ),
            targets: RwLock::new(HashMap::new()),
            last_discovery_ms: AtomicI64::new(0),
            last_collect_ms: AtomicI64::new(0),
            collector_started: AtomicBool::new(false),
            collector_enabled: AtomicBool::new(false),
            discovery_started: AtomicBool::new(false),
            discovery_enabled: AtomicBool::new(false),
            scrape_interval_ms: AtomicU64::new(scrape_interval_ms),
            retention_ms: AtomicU64::new(retention_ms),
            scrape_timeout_ms,
            max_concurrent_scrapes: observability.max_concurrent_scrapes.max(1),
            max_samples_per_series,
            collect_lock: Mutex::new(()),
        }
    }

    pub fn start_collector(self: &Arc<Self>, rt: Arc<Runtime>) {
        self.collector_enabled.store(true, Ordering::Relaxed);
        if self.collector_started.swap(true, Ordering::SeqCst) {
            let provider = self.clone();
            rt.spawn(async move {
                collect_once(provider).await;
            });
            return;
        }
        let provider = self.clone();
        rt.spawn(async move {
            run_collector(provider).await;
        });
    }

    pub fn stop_collector(&self) {
        self.collector_enabled.store(false, Ordering::Relaxed);
    }

    pub fn collector_running(&self) -> bool {
        self.collector_enabled.load(Ordering::Relaxed)
    }

    pub fn start_discovery(self: &Arc<Self>, rt: Arc<Runtime>) {
        self.discovery_enabled.store(true, Ordering::Relaxed);
        if self.discovery_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let provider = self.clone();
        rt.spawn(async move {
            super::collector::run_discovery(provider).await;
        });
    }

    pub fn stop_discovery(&self) {
        self.discovery_enabled.store(false, Ordering::Relaxed);
    }

    pub fn discovery_running(&self) -> bool {
        self.discovery_enabled.load(Ordering::Relaxed)
    }

    pub fn conf(&self) -> &ClusterConf {
        &self.conf
    }

    pub fn unified_fs(&self) -> &UnifiedFileSystem {
        &self.unified_fs
    }

    pub fn tsdb(&self) -> &InMemoryTsdb {
        &self.tsdb
    }

    pub fn scraper(&self) -> &MetricsScraper {
        &self.scraper
    }

    pub fn scrape_interval_ms(&self) -> u64 {
        self.scrape_interval_ms.load(Ordering::Relaxed)
    }

    pub fn scrape_timeout_ms(&self) -> u64 {
        self.scrape_timeout_ms
    }

    pub fn max_concurrent_scrapes(&self) -> usize {
        self.max_concurrent_scrapes
    }

    pub fn collect_lock(&self) -> &Mutex<()> {
        &self.collect_lock
    }

    pub fn retention_ms(&self) -> u64 {
        self.retention_ms.load(Ordering::Relaxed)
    }

    pub fn settings(&self) -> ObservabilitySettings {
        let worst_per_series = max_samples_for(MAX_RETENTION_MS, MIN_SCRAPE_INTERVAL_MS);
        let theoretical_peak = self.tsdb().max_series().saturating_mul(worst_per_series);
        ObservabilitySettings {
            scrape_interval_ms: self.scrape_interval_ms(),
            retention_ms: self.retention_ms(),
            scrape_interval_options_ms: SCRAPE_INTERVAL_OPTIONS_MS.to_vec(),
            retention_options_ms: RETENTION_OPTIONS_MS.to_vec(),
            max_retention_ms: MAX_RETENTION_MS,
            max_samples_per_series_cap: MAX_SAMPLES_PER_SERIES_CAP,
            max_total_samples: MAX_TOTAL_SAMPLES,
            max_series: self.tsdb().max_series(),
            estimated_peak_samples: theoretical_peak.min(MAX_TOTAL_SAMPLES),
        }
    }

    pub fn update_settings(
        &self,
        scrape_interval_ms: Option<u64>,
        retention_ms: Option<u64>,
    ) -> Result<(), String> {
        let scrape = scrape_interval_ms.unwrap_or_else(|| self.scrape_interval_ms());
        let retention = retention_ms.unwrap_or_else(|| self.retention_ms());
        if !(MIN_SCRAPE_INTERVAL_MS..=MAX_SCRAPE_INTERVAL_MS).contains(&scrape) {
            return Err(format!(
                "scrape interval must be between {}ms and {}ms",
                MIN_SCRAPE_INTERVAL_MS, MAX_SCRAPE_INTERVAL_MS
            ));
        }
        if !(MIN_RETENTION_MS..=MAX_RETENTION_MS).contains(&retention) {
            return Err(format!(
                "retention must be between {}ms and {}ms",
                MIN_RETENTION_MS, MAX_RETENTION_MS
            ));
        }
        self.scrape_interval_ms.store(scrape, Ordering::Relaxed);
        self.retention_ms.store(retention, Ordering::Relaxed);
        self.tsdb.configure(
            retention as i64,
            effective_max_samples_per_series(retention, scrape, self.max_samples_per_series),
        );
        Ok(())
    }

    pub fn get_target(&self, id: &str) -> Option<TargetHealth> {
        self.targets
            .read()
            .ok()
            .and_then(|targets| targets.get(id).cloned())
    }

    pub fn update_target(&self, target: TargetHealth) {
        if let Ok(mut targets) = self.targets.write() {
            targets.insert(target.id.clone(), target);
        }
    }

    pub fn retain_targets(&self, discovered_ids: &[String]) {
        if let Ok(mut targets) = self.targets.write() {
            targets.retain(|id, _| discovered_ids.contains(id));
        }
    }

    pub fn set_last_discovery_ms(&self, value: i64) {
        self.last_discovery_ms.store(value, Ordering::Relaxed);
    }

    pub fn set_last_collect_ms(&self, value: i64) {
        self.last_collect_ms.store(value, Ordering::Relaxed);
    }
}

impl MetricsProvider for BuiltinProvider {
    fn provider_id(&self) -> &'static str {
        "builtin"
    }

    fn instant(
        &self,
        metric: Option<&str>,
        component: Option<&str>,
        instance: Option<&str>,
    ) -> Vec<super::model::InstantMetric> {
        self.tsdb().instant(metric, component, instance)
    }

    fn range(
        &self,
        metric: Option<&str>,
        component: Option<&str>,
        instance: Option<&str>,
        start_ms: i64,
        end_ms: i64,
        step_ms: Option<i64>,
    ) -> Vec<super::model::RangeSeries> {
        self.tsdb()
            .range(metric, component, instance, start_ms, end_ms, step_ms)
    }

    fn names(&self) -> Vec<String> {
        self.tsdb().names()
    }

    fn stats(&self) -> ProviderStats {
        ProviderStats {
            series_count: self.tsdb().series_count(),
            sample_count: self.tsdb().sample_count(),
            last_discovery_ms: non_zero_i64(self.last_discovery_ms.load(Ordering::Relaxed)),
            last_collect_ms: non_zero_i64(self.last_collect_ms.load(Ordering::Relaxed)),
        }
    }
}

impl TargetRegistry for BuiltinProvider {
    fn targets(&self) -> Vec<TargetHealth> {
        let Ok(targets) = self.targets.read() else {
            return Vec::new();
        };
        let mut values: Vec<_> = targets.values().cloned().collect();
        values.sort_by(|left, right| left.id.cmp(&right.id));
        values
    }
}

fn max_samples_for(retention_ms: u64, scrape_interval_ms: u64) -> usize {
    let interval = scrape_interval_ms.max(1);
    let estimated = retention_ms.div_ceil(interval) as usize + 2;
    estimated.clamp(64, MAX_SAMPLES_PER_SERIES_CAP)
}

fn effective_max_samples_per_series(
    retention_ms: u64,
    scrape_interval_ms: u64,
    configured_cap: usize,
) -> usize {
    max_samples_for(retention_ms, scrape_interval_ms).min(configured_cap)
}

fn non_zero_i64(value: i64) -> Option<i64> {
    if value > 0 {
        Some(value)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::ObservabilityConf;

    #[test]
    fn max_samples_scales_with_retention_and_interval() {
        assert_eq!(max_samples_for(3_600_000, 30_000), 122);
        assert_eq!(max_samples_for(3 * 24 * 3_600_000, 30_000), 8_642);
    }

    #[test]
    fn builtin_provider_implements_metrics_provider() {
        let conf = ObservabilityConf::default();
        let provider = BuiltinProvider::new(
            &conf,
            ClusterConf::default(),
            UnifiedFileSystem::with_rt(ClusterConf::default(), Arc::new(Runtime::single()))
                .expect("unified fs"),
        );
        assert_eq!(provider.provider_id(), "builtin");
        assert!(provider.names().is_empty());
    }

    #[test]
    fn update_settings_respects_configured_max_samples_per_series() {
        let conf = ObservabilityConf {
            max_samples_per_series: 128,
            ..Default::default()
        };
        let provider = BuiltinProvider::new(
            &conf,
            ClusterConf::default(),
            UnifiedFileSystem::with_rt(ClusterConf::default(), Arc::new(Runtime::single()))
                .expect("ufs"),
        );
        provider
            .update_settings(Some(30_000), Some(24 * 60 * 60 * 1000))
            .expect("update settings");
        assert_eq!(
            effective_max_samples_per_series(24 * 60 * 60 * 1000, 30_000, 128),
            128
        );
    }

    #[test]
    fn max_concurrent_scrapes_comes_from_config() {
        let conf = ObservabilityConf {
            max_concurrent_scrapes: 4,
            ..Default::default()
        };
        let provider = BuiltinProvider::new(
            &conf,
            ClusterConf::default(),
            UnifiedFileSystem::with_rt(ClusterConf::default(), Arc::new(Runtime::single()))
                .expect("ufs"),
        );
        assert_eq!(provider.max_concurrent_scrapes(), 4);
    }

    #[test]
    fn effective_max_samples_per_series_caps_estimated_budget() {
        assert_eq!(
            effective_max_samples_per_series(3 * 24 * 3_600_000, 30_000, 200),
            200
        );
    }
}
