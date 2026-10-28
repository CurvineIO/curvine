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

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use curvine_config::{parse_duration_ms, ClusterConf, ObservabilityConf, ObservabilityMode};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_unified_fs::UnifiedFileSystem;
use log::warn;
use tokio::time::sleep;

use super::builtin_provider::BuiltinProvider;
use super::collector::discover_once;
use super::master_filter::{
    filter_instant_metrics_to_active_master, filter_range_series_to_active_master,
    should_apply_active_master_filter, ActiveMasterMatchSet,
};
use super::model::{
    InstantMetric, ObservabilitySettings, ProviderHealth, RangeSeries, TargetHealth,
};
use super::prometheus_provider::PrometheusProvider;
use super::provider::{MetricsProvider, ProviderStats, TargetRegistry};

const AUTO_HEALTH_CHECK_INTERVAL_MS: u64 = 30_000;

fn resolve_prometheus_query_error<T>(
    fallback_builtin: bool,
    query_kind: &str,
    err: &str,
    set_fallback: impl FnOnce(String),
    builtin_available: impl FnOnce() -> Result<(), String>,
    builtin_fallback: impl FnOnce() -> T,
) -> Result<T, String> {
    warn!("prometheus {query_kind} query failed: {err}");
    if fallback_builtin {
        if let Err(reason) = builtin_available() {
            return Err(format!(
                "Prometheus {query_kind} query failed: {err}; {reason}"
            ));
        }
        set_fallback(format!(
            "Prometheus {query_kind} query failed, serving builtin data: {err}"
        ));
        Ok(builtin_fallback())
    } else {
        Err(format!("Prometheus {query_kind} query failed: {err}"))
    }
}

#[repr(u8)]
enum ActiveProviderKind {
    Builtin = 0,
    Prometheus = 1,
}

pub struct ObservabilityService {
    conf: ClusterConf,
    observability_conf: ObservabilityConf,
    mode: RwLock<ObservabilityMode>,
    runtime: RwLock<Option<Arc<Runtime>>>,
    health_loop_started: AtomicBool,
    builtin: Arc<BuiltinProvider>,
    prometheus: Option<Arc<PrometheusProvider>>,
    active_provider: AtomicU8,
    provider_fallback_reason: RwLock<Option<String>>,
    query_fallback_reason: RwLock<Option<String>>,
    prometheus_healthy: RwLock<bool>,
    /// Set by [`ObservabilityService::stop`] so the health loop can exit; the
    /// loop holds an `Arc<Self>`, so it would otherwise outlive the cluster it
    /// belongs to.
    shutdown: AtomicBool,
}

impl ObservabilityService {
    pub fn new(conf: ClusterConf, unified_fs: UnifiedFileSystem) -> Self {
        let observability_conf = conf.web.observability.clone();
        let prometheus = PrometheusProvider::new(&observability_conf.prometheus).map(Arc::new);
        let builtin = Arc::new(BuiltinProvider::new(
            &observability_conf,
            conf.clone(),
            unified_fs,
        ));
        let active_provider = initial_active_provider(&observability_conf, prometheus.is_some());
        Self {
            conf,
            mode: RwLock::new(observability_conf.mode),
            runtime: RwLock::new(None),
            health_loop_started: AtomicBool::new(false),
            observability_conf,
            builtin,
            prometheus,
            active_provider: AtomicU8::new(active_provider),
            provider_fallback_reason: RwLock::new(None),
            query_fallback_reason: RwLock::new(None),
            prometheus_healthy: RwLock::new(false),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Stops the background work [`Self::start`] began.
    ///
    /// Called when a cluster is removed or replaced: its collector, discovery
    /// loop and health loop must not keep scraping a configuration nobody
    /// serves (and must not keep its TSDB alive).
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.builtin.stop_collector();
        self.builtin.stop_discovery();
        if let Ok(mut stored) = self.runtime.write() {
            *stored = None;
        }
    }

    pub fn stopped(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    pub fn start(self: &Arc<Self>, rt: Arc<Runtime>) {
        if !self.observability_conf.enabled {
            return;
        }
        if let Ok(mut stored) = self.runtime.write() {
            *stored = Some(rt.clone());
        }
        match self.configured_mode() {
            ObservabilityMode::Builtin => {
                self.builtin.start_collector(rt);
            }
            ObservabilityMode::Prometheus => {
                if self.prometheus.is_none() {
                    warn!(
                        "observability mode=prometheus but web.observability.prometheus.url is empty; falling back to builtin collector"
                    );
                    self.set_active_provider(ActiveProviderKind::Builtin);
                    self.set_provider_fallback_reason(Some(
                        "prometheus url not configured; using builtin collector".to_string(),
                    ));
                    self.builtin.start_collector(rt);
                } else {
                    self.set_active_provider(ActiveProviderKind::Prometheus);
                    self.builtin.stop_collector();
                    self.ensure_health_loop(rt.clone());
                }
            }
            ObservabilityMode::Auto => {
                if self.prometheus.is_some() {
                    self.ensure_health_loop(rt.clone());
                } else {
                    warn!(
                        "observability mode=auto but web.observability.prometheus.url is empty; using builtin collector"
                    );
                    self.set_active_provider(ActiveProviderKind::Builtin);
                    self.set_provider_fallback_reason(Some(
                        "prometheus url not configured; using builtin collector".to_string(),
                    ));
                    self.builtin.start_collector(rt);
                }
            }
        }
    }

    pub fn configured_mode(&self) -> ObservabilityMode {
        self.mode
            .read()
            .map(|value| *value)
            .unwrap_or(self.observability_conf.mode)
    }

    pub fn prometheus_configured(&self) -> bool {
        self.prometheus.is_some()
    }

    pub async fn update_mode(
        self: &Arc<Self>,
        mode: ObservabilityMode,
    ) -> Result<ProviderHealth, String> {
        if !self.observability_conf.enabled {
            return Err("observability is disabled".to_string());
        }
        if mode == ObservabilityMode::Prometheus && self.prometheus.is_none() {
            return Err("prometheus url not configured".to_string());
        }
        if let Ok(mut value) = self.mode.write() {
            *value = mode;
        }
        let rt = self
            .runtime
            .read()
            .ok()
            .and_then(|value| value.clone())
            .ok_or_else(|| "observability runtime not initialized".to_string())?;
        self.apply_configured_mode_async(rt).await?;
        Ok(self.health())
    }

    pub fn conf(&self) -> &ClusterConf {
        &self.conf
    }

    pub fn observability_conf(&self) -> &ObservabilityConf {
        &self.observability_conf
    }

    pub fn builtin(&self) -> &Arc<BuiltinProvider> {
        &self.builtin
    }

    pub fn settings(&self) -> ObservabilitySettings {
        self.builtin.settings()
    }

    pub fn update_settings(
        &self,
        scrape_interval_ms: Option<u64>,
        retention_ms: Option<u64>,
    ) -> Result<(), String> {
        self.builtin
            .update_settings(scrape_interval_ms, retention_ms)
    }

    pub fn health(&self) -> ProviderHealth {
        let active = self.active_provider_name();
        let stats = self.active_stats();
        let (provider_health, fallback_reason, prometheus_url) = match active {
            "prometheus" => (
                Some(if self.is_prometheus_healthy() {
                    "healthy".to_string()
                } else {
                    "unhealthy".to_string()
                }),
                self.fallback_reason(),
                self.prometheus
                    .as_ref()
                    .map(|provider| provider.base_url().to_string()),
            ),
            _ => (
                Some(
                    if self.builtin.collector_running() || !self.observability_conf.enabled {
                        "healthy".to_string()
                    } else {
                        "stopped".to_string()
                    },
                ),
                self.fallback_reason(),
                None,
            ),
        };
        ProviderHealth {
            enabled: self.observability_conf.enabled,
            mode: self.configured_mode().as_str().to_string(),
            active_provider: active.to_string(),
            prometheus_configured: self.prometheus_configured(),
            provider_health,
            fallback_reason,
            prometheus_url,
            scrape_interval_ms: self.builtin.scrape_interval_ms(),
            retention_ms: self.builtin.retention_ms(),
            series_count: stats.series_count,
            sample_count: stats.sample_count,
            last_discovery_ms: stats.last_discovery_ms,
            last_collect_ms: stats.last_collect_ms,
        }
    }

    pub async fn targets(&self) -> Vec<TargetHealth> {
        let mut targets = self.builtin.targets();
        if self.active_is_prometheus() {
            if let Some(provider) = self.prometheus.as_ref() {
                let cache_ttl_ms =
                    parse_duration_ms(&self.observability_conf.prometheus.target_cache_ttl)
                        .unwrap_or(30_000);
                if let Err(err) = provider
                    .enrich_target_health(&mut targets, cache_ttl_ms)
                    .await
                {
                    warn!("prometheus targets API failed: {err}");
                }
            }
        }
        targets
    }

    pub(crate) fn prometheus_provider(&self) -> Option<&Arc<PrometheusProvider>> {
        self.prometheus.as_ref()
    }

    fn clear_query_fallback_reason(&self) {
        if let Ok(mut value) = self.query_fallback_reason.write() {
            *value = None;
        }
    }

    fn on_prometheus_query_success(&self) {
        self.clear_query_fallback_reason();
    }

    pub async fn instant(
        &self,
        metric: Option<&str>,
        component: Option<&str>,
        instance: Option<&str>,
    ) -> Result<Vec<InstantMetric>, String> {
        if !self.observability_conf.enabled {
            return Ok(Vec::new());
        }
        if !self.active_is_prometheus() {
            return Ok(self.builtin.instant(metric, component, instance));
        }
        let Some(provider) = self.prometheus.as_ref() else {
            return Ok(self.builtin.instant(metric, component, instance));
        };
        let end_ms = now_ms();
        match provider
            .instant_async(metric, component, instance, end_ms)
            .await
        {
            Ok(series) => {
                self.on_prometheus_query_success();
                Ok(self.filter_prometheus_master_instant(component, instance, series))
            }
            Err(err) => self.handle_prometheus_query_error("instant", &err, || {
                self.builtin.instant(metric, component, instance)
            }),
        }
    }

    pub async fn range(
        &self,
        metric: Option<&str>,
        component: Option<&str>,
        instance: Option<&str>,
        start_ms: i64,
        end_ms: i64,
        step_ms: Option<i64>,
    ) -> Result<Vec<RangeSeries>, String> {
        if !self.observability_conf.enabled {
            return Ok(Vec::new());
        }
        if !self.active_is_prometheus() {
            return Ok(self
                .builtin
                .range(metric, component, instance, start_ms, end_ms, step_ms));
        }
        let Some(provider) = self.prometheus.as_ref() else {
            return Ok(self
                .builtin
                .range(metric, component, instance, start_ms, end_ms, step_ms));
        };
        match provider
            .range_async(metric, component, instance, start_ms, end_ms, step_ms)
            .await
        {
            Ok(series) => {
                self.on_prometheus_query_success();
                Ok(self.filter_prometheus_master_range(component, instance, series))
            }
            Err(err) => self.handle_prometheus_query_error("range", &err, || {
                self.builtin
                    .range(metric, component, instance, start_ms, end_ms, step_ms)
            }),
        }
    }

    pub fn uses_prometheus_provider(&self) -> bool {
        self.active_is_prometheus()
    }

    pub async fn range_expr(
        &self,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: Option<i64>,
    ) -> Result<Vec<RangeSeries>, String> {
        if !self.observability_conf.enabled {
            return Ok(Vec::new());
        }
        if !self.active_is_prometheus() {
            return Err("Prometheus provider is not active".to_string());
        }
        let Some(provider) = self.prometheus.as_ref() else {
            return Err("Prometheus provider is not configured".to_string());
        };
        match provider
            .range_expr_async(query, start_ms, end_ms, step_ms)
            .await
        {
            Ok(series) => {
                self.on_prometheus_query_success();
                Ok(series)
            }
            Err(err) => self.handle_prometheus_query_error("range", &err, Vec::new),
        }
    }

    pub async fn names(&self) -> Result<Vec<String>, String> {
        if !self.observability_conf.enabled {
            return Ok(Vec::new());
        }
        if !self.active_is_prometheus() {
            return Ok(self.builtin.names());
        }
        let Some(provider) = self.prometheus.as_ref() else {
            return Ok(self.builtin.names());
        };
        match provider.names_async().await {
            Ok(names) => {
                self.on_prometheus_query_success();
                Ok(names)
            }
            Err(err) => self.handle_prometheus_query_error("names", &err, || self.builtin.names()),
        }
    }

    fn handle_prometheus_query_error<T>(
        &self,
        query_kind: &str,
        err: &str,
        builtin_fallback: impl FnOnce() -> T,
    ) -> Result<T, String> {
        resolve_prometheus_query_error(
            self.observability_conf.prometheus.fallback_builtin,
            query_kind,
            err,
            |reason| self.set_query_fallback_reason(Some(reason)),
            || self.builtin_query_fallback_available(),
            builtin_fallback,
        )
    }

    fn builtin_query_fallback_available(&self) -> Result<(), String> {
        builtin_tsdb_fallback_available(
            self.builtin.collector_running(),
            &self.builtin.stats(),
            self.builtin.scrape_interval_ms(),
            now_ms(),
        )
    }

    fn active_is_prometheus(&self) -> bool {
        self.active_provider.load(Ordering::Relaxed) == ActiveProviderKind::Prometheus as u8
    }

    fn active_master_match_set(&self) -> Option<ActiveMasterMatchSet> {
        self.builtin
            .targets()
            .into_iter()
            .find(|target| target.component == "master")
            .map(|target| ActiveMasterMatchSet::from_target(&target, self.builtin.conf()))
    }

    fn filter_prometheus_master_range(
        &self,
        component: Option<&str>,
        instance: Option<&str>,
        series: Vec<RangeSeries>,
    ) -> Vec<RangeSeries> {
        if !should_apply_active_master_filter(component, instance) {
            return series;
        }
        filter_range_series_to_active_master(series, self.active_master_match_set().as_ref())
    }

    fn filter_prometheus_master_instant(
        &self,
        component: Option<&str>,
        instance: Option<&str>,
        metrics: Vec<InstantMetric>,
    ) -> Vec<InstantMetric> {
        if !should_apply_active_master_filter(component, instance) {
            return metrics;
        }
        filter_instant_metrics_to_active_master(metrics, self.active_master_match_set().as_ref())
    }

    fn active_provider_name(&self) -> &'static str {
        if self.active_is_prometheus() {
            "prometheus"
        } else {
            "builtin"
        }
    }

    fn active_stats(&self) -> super::provider::ProviderStats {
        let builtin_stats = self.builtin.stats();
        if self.active_is_prometheus() {
            let prom_stats = self
                .prometheus
                .as_ref()
                .map(|provider| {
                    let _ = provider.provider_id();
                    provider.stats()
                })
                .unwrap_or_default();
            ProviderStats {
                series_count: prom_stats.series_count.max(builtin_stats.series_count),
                sample_count: prom_stats.sample_count.max(builtin_stats.sample_count),
                last_discovery_ms: builtin_stats
                    .last_discovery_ms
                    .or(prom_stats.last_discovery_ms),
                last_collect_ms: prom_stats.last_collect_ms.or(builtin_stats.last_collect_ms),
            }
        } else {
            let _ = self.builtin.provider_id();
            builtin_stats
        }
    }

    fn fallback_reason(&self) -> Option<String> {
        let provider = self
            .provider_fallback_reason
            .read()
            .ok()
            .and_then(|value| value.clone());
        let query = self
            .query_fallback_reason
            .read()
            .ok()
            .and_then(|value| value.clone());
        match (provider, query) {
            (Some(left), Some(right)) => Some(format!("{left}; {right}")),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        }
    }

    fn set_provider_fallback_reason(&self, reason: Option<String>) {
        if let Ok(mut value) = self.provider_fallback_reason.write() {
            *value = reason;
        }
    }

    fn set_query_fallback_reason(&self, reason: Option<String>) {
        if let Ok(mut value) = self.query_fallback_reason.write() {
            *value = reason;
        }
    }

    fn is_prometheus_healthy(&self) -> bool {
        self.prometheus_healthy
            .read()
            .map(|value| *value)
            .unwrap_or(false)
    }

    fn set_active_provider(&self, provider: ActiveProviderKind) {
        self.active_provider
            .store(provider as u8, Ordering::Relaxed);
    }

    fn ensure_health_loop(self: &Arc<Self>, rt: Arc<Runtime>) {
        if self.health_loop_started.swap(true, Ordering::SeqCst) {
            return;
        }
        self.start_prometheus_health_loop(rt);
    }

    async fn apply_configured_mode_async(self: &Arc<Self>, rt: Arc<Runtime>) -> Result<(), String> {
        match self.configured_mode() {
            ObservabilityMode::Builtin => {
                self.set_active_provider(ActiveProviderKind::Builtin);
                self.set_provider_fallback_reason(None);
                self.clear_query_fallback_reason();
                self.builtin.stop_discovery();
                self.builtin.start_collector(rt);
                Ok(())
            }
            ObservabilityMode::Prometheus | ObservabilityMode::Auto => {
                if self.prometheus.is_none() {
                    if self.configured_mode() == ObservabilityMode::Prometheus {
                        return Err("prometheus url not configured".to_string());
                    }
                    warn!(
                        "observability mode=auto but web.observability.prometheus.url is empty; using builtin collector"
                    );
                    self.set_active_provider(ActiveProviderKind::Builtin);
                    self.set_provider_fallback_reason(Some(
                        "prometheus url not configured; using builtin collector".to_string(),
                    ));
                    self.builtin.stop_discovery();
                    self.builtin.start_collector(rt);
                    return Ok(());
                }
                self.builtin.stop_collector();
                self.ensure_health_loop(rt);
                self.sync_provider_for_prometheus_modes().await;
                Ok(())
            }
        }
    }

    async fn sync_provider_for_prometheus_modes(&self) {
        // The health task is intentionally long-lived so a later mode switch can
        // reuse it, but builtin mode must not perform discovery or contact an
        // external Prometheus server.
        if self.configured_mode() == ObservabilityMode::Builtin {
            return;
        }
        if !self.builtin.discovery_running() {
            discover_once(&self.builtin).await;
        }
        let expected_live_workers = self
            .builtin
            .targets()
            .iter()
            .filter(|target| {
                target.component == "worker" && target.worker_state.as_deref() == Some("live")
            })
            .count();

        let healthy = match self.prometheus.as_ref() {
            Some(provider) => provider.check_health(expected_live_workers).await.is_ok(),
            None => false,
        };
        if let Ok(mut value) = self.prometheus_healthy.write() {
            *value = healthy;
        }

        let rt = self.runtime.read().ok().and_then(|value| value.clone());

        match self.configured_mode() {
            ObservabilityMode::Prometheus => {
                if healthy {
                    self.set_active_provider(ActiveProviderKind::Prometheus);
                    self.set_provider_fallback_reason(None);
                    self.clear_query_fallback_reason();
                    self.builtin.stop_collector();
                    if let Some(rt) = rt.as_ref() {
                        if !self.builtin.discovery_running() {
                            self.builtin.start_discovery(rt.clone());
                        }
                    }
                } else if self.observability_conf.prometheus.fallback_builtin {
                    self.set_active_provider(ActiveProviderKind::Builtin);
                    self.set_provider_fallback_reason(Some(
                        "Prometheus unhealthy; fallback to builtin collector".to_string(),
                    ));
                    self.builtin.stop_discovery();
                    if let Some(rt) = rt.as_ref() {
                        if !self.builtin.collector_running() {
                            self.builtin.start_collector(rt.clone());
                        }
                    }
                } else {
                    self.set_active_provider(ActiveProviderKind::Prometheus);
                    self.set_provider_fallback_reason(Some(
                        "Prometheus unhealthy and fallback_builtin=false".to_string(),
                    ));
                    self.builtin.stop_discovery();
                    self.builtin.stop_collector();
                }
            }
            ObservabilityMode::Auto => {
                if healthy {
                    self.set_active_provider(ActiveProviderKind::Prometheus);
                    self.set_provider_fallback_reason(None);
                    self.clear_query_fallback_reason();
                    self.builtin.stop_collector();
                    if let Some(rt) = rt.as_ref() {
                        if !self.builtin.discovery_running() {
                            self.builtin.start_discovery(rt.clone());
                        }
                    }
                } else if self.observability_conf.prometheus.fallback_builtin {
                    self.set_active_provider(ActiveProviderKind::Builtin);
                    self.set_provider_fallback_reason(Some(
                        "Prometheus unhealthy; fallback to builtin collector".to_string(),
                    ));
                    self.builtin.stop_discovery();
                    if let Some(rt) = rt.as_ref() {
                        if !self.builtin.collector_running() {
                            self.builtin.start_collector(rt.clone());
                        }
                    }
                } else {
                    self.set_active_provider(ActiveProviderKind::Prometheus);
                    self.set_provider_fallback_reason(Some(
                        "Prometheus unhealthy and fallback_builtin=false".to_string(),
                    ));
                    self.builtin.stop_discovery();
                    self.builtin.stop_collector();
                }
            }
            ObservabilityMode::Builtin => {}
        }
    }

    fn start_prometheus_health_loop(self: &Arc<Self>, rt: Arc<Runtime>) {
        let service = self.clone();
        rt.spawn(async move {
            loop {
                service.sync_provider_for_prometheus_modes().await;
                sleep(Duration::from_millis(AUTO_HEALTH_CHECK_INTERVAL_MS)).await;
                if service.stopped() {
                    break;
                }
            }
        });
    }
}

fn initial_active_provider(conf: &ObservabilityConf, has_prometheus: bool) -> u8 {
    match conf.mode {
        ObservabilityMode::Builtin => ActiveProviderKind::Builtin as u8,
        ObservabilityMode::Prometheus if has_prometheus => ActiveProviderKind::Prometheus as u8,
        ObservabilityMode::Auto if has_prometheus => ActiveProviderKind::Prometheus as u8,
        _ => ActiveProviderKind::Builtin as u8,
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

/// Whether built-in TSDB data is fresh enough to serve a Prometheus query fallback.
fn builtin_tsdb_fallback_available(
    _collector_running: bool,
    stats: &ProviderStats,
    scrape_interval_ms: u64,
    now_ms: i64,
) -> Result<(), String> {
    if stats.sample_count == 0 {
        return Err("builtin fallback unavailable: no scrape data in TSDB".to_string());
    }
    let Some(last_collect_ms) = stats.last_collect_ms.filter(|value| *value > 0) else {
        return Err("builtin fallback unavailable: no scrape data yet".to_string());
    };
    let max_stale_ms = (scrape_interval_ms * 2).max(120_000) as i64;
    let age = now_ms.saturating_sub(last_collect_ms);
    if age > max_stale_ms {
        return Err(format!(
            "builtin fallback unavailable: last scrape is stale ({age}ms ago, max {max_stale_ms}ms)"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::PrometheusObservabilityConf;

    #[tokio::test]
    async fn builtin_mode_skips_prometheus_health_and_discovery_sync() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let addr = listener.local_addr().expect("address");
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                observed.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });

        let mut conf = ClusterConf::default();
        conf.web.observability.mode = ObservabilityMode::Builtin;
        conf.web.observability.prometheus.url = format!("http://{addr}");
        let unified_fs = UnifiedFileSystem::with_rt(
            conf.clone(),
            Arc::new(curvine_runtime::runtime::Runtime::single()),
        )
        .expect("ufs");
        let service = ObservabilityService::new(conf, unified_fs);

        service.sync_provider_for_prometheus_modes().await;
        tokio::task::yield_now().await;

        assert_eq!(requests.load(Ordering::SeqCst), 0);
        assert!(service.builtin.targets().is_empty());
    }

    #[test]
    fn initial_active_provider_respects_mode() {
        let conf = ObservabilityConf {
            mode: ObservabilityMode::Prometheus,
            ..Default::default()
        };
        assert_eq!(
            initial_active_provider(&conf, true),
            ActiveProviderKind::Prometheus as u8
        );
        assert_eq!(
            initial_active_provider(&conf, false),
            ActiveProviderKind::Builtin as u8
        );
    }

    #[test]
    fn prometheus_provider_requires_url() {
        let conf = PrometheusObservabilityConf::default();
        assert!(PrometheusProvider::new(&conf).is_none());
        let conf = PrometheusObservabilityConf {
            url: "http://127.0.0.1:9090".to_string(),
            ..Default::default()
        };
        assert!(PrometheusProvider::new(&conf).is_some());
    }

    #[test]
    fn prometheus_query_error_falls_back_to_builtin_when_enabled() {
        let value = resolve_prometheus_query_error(
            true,
            "range",
            "timeout",
            |_| {},
            || Ok(()),
            || vec![1, 2, 3],
        )
        .expect("fallback");
        assert_eq!(value, vec![1, 2, 3]);
    }

    #[test]
    fn prometheus_query_error_surfaces_when_fallback_disabled() {
        let err = resolve_prometheus_query_error(
            false,
            "names",
            "HTTP 503",
            |_| {},
            || Ok(()),
            Vec::<String>::new,
        )
        .expect_err("error");
        assert!(err.contains("HTTP 503"));
    }

    #[test]
    fn tsdb_fallback_rejects_stale_snapshot_when_collector_stopped() {
        let stats = ProviderStats {
            sample_count: 42,
            last_collect_ms: Some(1),
            ..Default::default()
        };
        assert!(builtin_tsdb_fallback_available(false, &stats, 60_000, 3_600_000_000).is_err());
    }

    #[test]
    fn tsdb_fallback_rejects_empty_tsdb() {
        assert!(
            builtin_tsdb_fallback_available(false, &ProviderStats::default(), 60_000, 0).is_err()
        );
    }

    #[test]
    fn tsdb_fallback_enforces_freshness_while_collector_running() {
        let stats = ProviderStats {
            sample_count: 10,
            last_collect_ms: Some(1),
            ..Default::default()
        };
        assert!(builtin_tsdb_fallback_available(true, &stats, 60_000, 10_000_000).is_err());
    }

    #[test]
    fn prometheus_query_error_rejects_fallback_when_builtin_unavailable() {
        let err = resolve_prometheus_query_error(
            true,
            "range",
            "timeout",
            |_| {},
            || Err("builtin fallback unavailable: collector is not running".to_string()),
            || vec![1, 2, 3],
        )
        .expect_err("error");
        assert!(err.contains("timeout"));
        assert!(err.contains("collector is not running"));
    }
}
