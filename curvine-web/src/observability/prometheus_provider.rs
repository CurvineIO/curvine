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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use curvine_config::{parse_duration_ms, PrometheusObservabilityConf};
use reqwest::redirect::Policy;
use serde::Deserialize;

use super::model::{InstantMetric, RangePoint, RangeSeries, TargetHealth};
use super::provider::{MetricsProvider, ProviderStats};

// This is both a Prometheus availability probe and a deployment-contract
// probe. Curvine's production scrape config must attach component=master/worker
// target labels; otherwise dashboard selectors cannot distinguish components.
const MASTER_HEALTH_CHECK_QUERY: &str = "up{component=\"master\"}";
const WORKER_HEALTH_CHECK_QUERY: &str = "up{component=\"worker\"}";

#[derive(Clone)]
struct CachedScrapeStatuses {
    fetched_ms: i64,
    by_endpoint: BTreeMap<String, PromScrapeStatus>,
    by_instance: BTreeMap<String, PromScrapeStatus>,
    by_component_instance: BTreeMap<String, PromScrapeStatus>,
    by_component_host: BTreeMap<String, PromScrapeStatus>,
}

#[derive(Clone)]
pub struct PrometheusProvider {
    base_url: String,
    client: reqwest::Client,
    last_success_ms: Arc<AtomicI64>,
    scrape_status_cache: Arc<RwLock<Option<CachedScrapeStatuses>>>,
}

#[derive(Debug, Deserialize)]
struct PromResponse<T> {
    status: String,
    data: Option<T>,
    error: Option<String>,
    #[serde(rename = "errorType")]
    error_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PromQueryData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<PromResult>,
}

#[derive(Debug, Deserialize)]
struct PromTargetsData {
    #[serde(rename = "activeTargets")]
    active_targets: Vec<PromActiveTarget>,
}

#[derive(Debug, Deserialize)]
struct PromActiveTarget {
    #[serde(rename = "scrapeUrl")]
    scrape_url: String,
    health: String,
    #[serde(default, rename = "lastError")]
    last_error: String,
    #[serde(default, rename = "lastScrape")]
    last_scrape: Option<String>,
    #[serde(default, rename = "lastScrapeDuration")]
    last_scrape_duration: Option<f64>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
struct PromScrapeStatus {
    up: bool,
    last_error: String,
    last_scrape_ms: Option<i64>,
    last_scrape_duration_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PromResult {
    metric: BTreeMap<String, String>,
    #[serde(default)]
    value: Option<(f64, String)>,
    #[serde(default)]
    values: Option<Vec<(f64, String)>>,
}

impl PrometheusProvider {
    pub fn new(conf: &PrometheusObservabilityConf) -> Option<Self> {
        let base_url = normalize_base_url(&conf.url)?;
        let timeout_ms = parse_duration_ms(&conf.query_timeout)
            .unwrap_or(5_000)
            .max(1_000);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .redirect(Policy::none())
            .build()
            .ok()?;
        Some(Self {
            base_url,
            client,
            last_success_ms: Arc::new(AtomicI64::new(0)),
            scrape_status_cache: Arc::new(RwLock::new(None)),
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Probe Prometheus availability and Curvine scrape-label contract.
    ///
    /// When `expected_live_workers > 0`, at least one healthy `component=worker`
    /// target must also be present so worker dashboards are not silently empty.
    pub async fn check_health(&self, expected_live_workers: usize) -> Result<(), String> {
        self.check_reachable().await?;

        let master_results = self.query_up_vector(MASTER_HEALTH_CHECK_QUERY).await?;
        if !has_any_up(&master_results) {
            return Err(
                "Prometheus has no healthy Curvine master target labeled component=master"
                    .to_string(),
            );
        }

        if expected_live_workers > 0 {
            let worker_results = self.query_up_vector(WORKER_HEALTH_CHECK_QUERY).await?;
            let healthy_workers = count_unique_up_instances(&worker_results);
            if healthy_workers < expected_live_workers {
                return Err(format!(
                    "Prometheus has {healthy_workers} healthy Curvine worker targets labeled component=worker, expected at least {expected_live_workers}"
                ));
            }
        }

        self.last_success_ms.store(now_ms(), Ordering::Relaxed);
        Ok(())
    }

    pub async fn check_reachable(&self) -> Result<(), String> {
        let healthy_url = format!("{}/-/healthy", self.base_url);
        let response = self
            .client
            .get(&healthy_url)
            .send()
            .await
            .map_err(|err| err.to_string())?;
        if !response.status().is_success() {
            return Err(format!(
                "Prometheus /-/healthy returned HTTP {}",
                response.status()
            ));
        }

        Ok(())
    }

    async fn query_up_vector(&self, query: &str) -> Result<Vec<PromResult>, String> {
        let query_url = format!("{}/api/v1/query", self.base_url);
        let response = self
            .client
            .get(&query_url)
            .query(&[("query", query)])
            .send()
            .await
            .map_err(|err| err.to_string())?;
        if !response.status().is_success() {
            return Err(format!(
                "Prometheus query probe returned HTTP {}",
                response.status()
            ));
        }
        let payload: PromResponse<PromQueryData> = response
            .json()
            .await
            .map_err(|err| format!("invalid Prometheus health response: {err}"))?;
        if payload.status != "success" {
            return Err(payload.error.unwrap_or_else(|| {
                format!(
                    "Prometheus health probe failed: {}",
                    payload.error_type.unwrap_or_else(|| "unknown".to_string())
                )
            }));
        }
        let data = payload
            .data
            .ok_or_else(|| "Prometheus health probe returned no data".to_string())?;
        if data.result_type != "vector" {
            return Err(format!(
                "Prometheus health probe returned {}, expected vector",
                data.result_type
            ));
        }
        Ok(data.result)
    }

    pub async fn instant_async(
        &self,
        metric: Option<&str>,
        component: Option<&str>,
        instance: Option<&str>,
        time_ms: i64,
    ) -> Result<Vec<InstantMetric>, String> {
        let query = build_promql(metric, component, instance)?;
        let url = format!("{}/api/v1/query", self.base_url);
        let time_sec = (time_ms as f64 / 1000.0).max(0.0);
        let response = self
            .client
            .get(&url)
            .query(&[("query", query.as_str()), ("time", &time_sec.to_string())])
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let payload: PromResponse<PromQueryData> = parse_json_response(response).await?;
        let data = payload
            .data
            .ok_or_else(|| "missing Prometheus query data".to_string())?;
        if data.result_type != "vector" {
            return Err(format!(
                "unexpected Prometheus instant result type: {}",
                data.result_type
            ));
        }
        self.last_success_ms.store(now_ms(), Ordering::Relaxed);
        Ok(data
            .result
            .into_iter()
            .filter_map(|item| vector_to_instant(item, time_ms))
            .collect())
    }

    pub async fn range_async(
        &self,
        metric: Option<&str>,
        component: Option<&str>,
        instance: Option<&str>,
        start_ms: i64,
        end_ms: i64,
        step_ms: Option<i64>,
    ) -> Result<Vec<RangeSeries>, String> {
        let query = build_promql(metric, component, instance)?;
        let step = step_ms
            .map(step_ms_to_prometheus)
            .unwrap_or_else(|| "15s".to_string());
        let url = format!("{}/api/v1/query_range", self.base_url);
        let start_sec = (start_ms as f64 / 1000.0).max(0.0);
        let end_sec = (end_ms as f64 / 1000.0).max(start_sec);
        let response = self
            .client
            .get(&url)
            .query(&[
                ("query", query.as_str()),
                ("start", &start_sec.to_string()),
                ("end", &end_sec.to_string()),
                ("step", step.as_str()),
            ])
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let payload: PromResponse<PromQueryData> = parse_json_response(response).await?;
        let data = payload
            .data
            .ok_or_else(|| "missing Prometheus query data".to_string())?;
        if data.result_type != "matrix" {
            return Err(format!(
                "unexpected Prometheus range result type: {}",
                data.result_type
            ));
        }
        self.last_success_ms.store(now_ms(), Ordering::Relaxed);
        Ok(data
            .result
            .into_iter()
            .filter_map(matrix_to_range_series)
            .collect())
    }

    pub async fn range_expr_async(
        &self,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: Option<i64>,
    ) -> Result<Vec<RangeSeries>, String> {
        let query = query.trim();
        if query.is_empty() {
            return Err("empty Prometheus query".to_string());
        }
        let step = step_ms
            .map(step_ms_to_prometheus)
            .unwrap_or_else(|| "15s".to_string());
        let url = format!("{}/api/v1/query_range", self.base_url);
        let start_sec = (start_ms as f64 / 1000.0).max(0.0);
        let end_sec = (end_ms as f64 / 1000.0).max(start_sec);
        let response = self
            .client
            .get(&url)
            .query(&[
                ("query", query),
                ("start", &start_sec.to_string()),
                ("end", &end_sec.to_string()),
                ("step", step.as_str()),
            ])
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let payload: PromResponse<PromQueryData> = parse_json_response(response).await?;
        let data = payload
            .data
            .ok_or_else(|| "missing Prometheus query data".to_string())?;
        if data.result_type != "matrix" {
            return Err(format!(
                "unexpected Prometheus range result type: {}",
                data.result_type
            ));
        }
        self.last_success_ms.store(now_ms(), Ordering::Relaxed);
        Ok(data
            .result
            .into_iter()
            .filter_map(matrix_to_range_series)
            .collect())
    }

    pub async fn names_async(&self) -> Result<Vec<String>, String> {
        let url = format!("{}/api/v1/label/__name__/values", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let payload: PromResponse<Vec<String>> = parse_json_response(response).await?;
        let data = payload
            .data
            .ok_or_else(|| "missing Prometheus label values".to_string())?;
        self.last_success_ms.store(now_ms(), Ordering::Relaxed);
        let mut names = data;
        names.sort();
        names.dedup();
        Ok(names)
    }

    pub async fn enrich_target_health(
        &self,
        targets: &mut [TargetHealth],
        cache_ttl_ms: u64,
    ) -> Result<(), String> {
        let statuses = self.scrape_statuses_cached(cache_ttl_ms).await?;
        for target in targets.iter_mut() {
            let status = lookup_scrape_status(&statuses, target);
            let Some(status) = status else {
                apply_missing_prometheus_scrape_status(target);
                continue;
            };
            target.up = status.up;
            if !status.last_error.is_empty() {
                target.last_error = status.last_error.clone();
            } else {
                target.last_error.clear();
            }
            if let Some(last_scrape_ms) = status.last_scrape_ms {
                target.last_scrape_ms = Some(last_scrape_ms);
                if status.up {
                    target.last_success_ms = Some(last_scrape_ms);
                }
            }
            target.last_scrape_duration_ms = status.last_scrape_duration_ms;
            target.last_warning = remove_prometheus_target_warning(&target.last_warning);
        }
        Ok(())
    }

    async fn scrape_statuses_cached(
        &self,
        cache_ttl_ms: u64,
    ) -> Result<CachedScrapeStatuses, String> {
        let now = now_ms();
        if cache_ttl_ms > 0 {
            if let Ok(cache) = self.scrape_status_cache.read() {
                if let Some(cached) = cache.as_ref() {
                    if now.saturating_sub(cached.fetched_ms) <= cache_ttl_ms as i64 {
                        return Ok(cached.clone());
                    }
                }
            }
        }
        let fetched = self.fetch_scrape_statuses().await?;
        if cache_ttl_ms > 0 {
            if let Ok(mut cache) = self.scrape_status_cache.write() {
                *cache = Some(fetched.clone());
            }
        }
        Ok(fetched)
    }

    async fn fetch_scrape_statuses(&self) -> Result<CachedScrapeStatuses, String> {
        let url = format!("{}/api/v1/targets", self.base_url);
        let response = self
            .client
            .get(&url)
            .query(&[("state", "any")])
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let payload = parse_json_response::<PromTargetsData>(response).await?;
        let data = payload
            .data
            .ok_or_else(|| "Prometheus targets response missing data".to_string())?;
        let mut by_endpoint = BTreeMap::new();
        let mut by_instance = BTreeMap::new();
        let mut by_component_instance = BTreeMap::new();
        let mut by_component_host = BTreeMap::new();
        for target in data.active_targets {
            let duration_ms = target
                .last_scrape_duration
                .filter(|value| value.is_finite() && *value >= 0.0)
                .map(|value| (value * 1000.0) as u64);
            let status = PromScrapeStatus {
                up: target.health == "up",
                last_error: target.last_error,
                last_scrape_ms: parse_prom_timestamp_ms(target.last_scrape.as_deref()),
                last_scrape_duration_ms: duration_ms,
            };
            if let Some(key) = scrape_endpoint_key(&target.scrape_url) {
                by_endpoint.insert(key, status.clone());
            }
            let component = target.labels.get("component").map(String::as_str);
            if let Some(instance) = target.labels.get("instance") {
                if let Some(key) = scrape_instance_key(instance) {
                    by_instance.insert(key, status.clone());
                }
                if let Some(key) = component_instance_key(component, instance) {
                    by_component_instance.insert(key, status.clone());
                }
                if let Some(key) = component_host_key(component, instance) {
                    by_component_host.insert(key, status.clone());
                }
            }
            if let Some(key) = component_host_key(component, &target.scrape_url) {
                by_component_host.insert(key, status.clone());
            }
        }
        Ok(CachedScrapeStatuses {
            fetched_ms: now_ms(),
            by_endpoint,
            by_instance,
            by_component_instance,
            by_component_host,
        })
    }
}

impl MetricsProvider for PrometheusProvider {
    fn provider_id(&self) -> &'static str {
        "prometheus"
    }

    fn instant(
        &self,
        _metric: Option<&str>,
        _component: Option<&str>,
        _instance: Option<&str>,
    ) -> Vec<InstantMetric> {
        Vec::new()
    }

    fn range(
        &self,
        _metric: Option<&str>,
        _component: Option<&str>,
        _instance: Option<&str>,
        _start_ms: i64,
        _end_ms: i64,
        _step_ms: Option<i64>,
    ) -> Vec<RangeSeries> {
        Vec::new()
    }

    fn names(&self) -> Vec<String> {
        Vec::new()
    }

    fn stats(&self) -> ProviderStats {
        ProviderStats {
            series_count: 0,
            sample_count: 0,
            last_discovery_ms: None,
            last_collect_ms: non_zero_i64(self.last_success_ms.load(Ordering::Relaxed)),
        }
    }
}

const NO_PROMETHEUS_TARGET_WARNING: &str =
    "Prometheus has no active scrape target for this endpoint";
const PROMETHEUS_MODE_TARGET_WARNING: &str = "Prometheus mode; scrape health from Prometheus";

fn apply_missing_prometheus_scrape_status(target: &mut TargetHealth) {
    target.up = false;
    target.last_warning =
        merge_prometheus_warning(&target.last_warning, NO_PROMETHEUS_TARGET_WARNING);
}

fn lookup_scrape_status<'a>(
    statuses: &'a CachedScrapeStatuses,
    target: &TargetHealth,
) -> Option<&'a PromScrapeStatus> {
    scrape_endpoint_key(&target.url)
        .as_ref()
        .and_then(|key| statuses.by_endpoint.get(key))
        .or_else(|| {
            scrape_instance_key(&target.instance)
                .as_ref()
                .and_then(|key| statuses.by_instance.get(key))
        })
        .or_else(|| {
            component_instance_key(Some(&target.component), &target.instance)
                .as_ref()
                .and_then(|key| statuses.by_component_instance.get(key))
        })
        .or_else(|| {
            component_host_key(Some(&target.component), &target.url)
                .as_ref()
                .and_then(|key| statuses.by_component_host.get(key))
        })
        .or_else(|| {
            component_host_key(Some(&target.component), &target.instance)
                .as_ref()
                .and_then(|key| statuses.by_component_host.get(key))
        })
}

async fn parse_json_response<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
) -> Result<PromResponse<T>, String> {
    if !response.status().is_success() {
        return Err(format!("Prometheus HTTP {}", response.status()));
    }
    let payload: PromResponse<T> = response
        .json()
        .await
        .map_err(|err| format!("invalid Prometheus JSON response: {err}"))?;
    if payload.status != "success" {
        return Err(payload.error.unwrap_or_else(|| {
            format!(
                "Prometheus query failed: {}",
                payload.error_type.unwrap_or_else(|| "unknown".to_string())
            )
        }));
    }
    Ok(payload)
}

fn scrape_endpoint_key(raw: &str) -> Option<String> {
    let authority = url_authority(raw)?;
    Some(authority.to_lowercase())
}

fn scrape_instance_key(instance: &str) -> Option<String> {
    let trimmed = instance.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_lowercase())
}

fn component_instance_key(component: Option<&str>, instance: &str) -> Option<String> {
    Some(format!(
        "{}:{}",
        normalized_component(component)?,
        scrape_instance_key(instance)?
    ))
}

fn component_host_key(component: Option<&str>, raw: &str) -> Option<String> {
    Some(format!(
        "{}:{}",
        normalized_component(component)?,
        host_from_endpoint(raw)?
    ))
}

fn normalized_component(component: Option<&str>) -> Option<String> {
    let component = component?.trim();
    if component.is_empty() {
        return None;
    }
    Some(component.to_lowercase())
}

fn host_from_endpoint(raw: &str) -> Option<String> {
    let authority = url_authority(raw)?;
    let host = authority
        .rsplit('@')
        .next()
        .unwrap_or(authority.as_str())
        .split(':')
        .next()?
        .trim();
    if host.is_empty() {
        None
    } else {
        Some(host.to_lowercase())
    }
}

fn url_authority(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let without_scheme = trimmed.split("://").nth(1).unwrap_or(trimmed);
    let authority = without_scheme.split('/').next()?.split('?').next()?.trim();
    if authority.is_empty() {
        None
    } else {
        Some(authority.to_string())
    }
}

fn merge_prometheus_warning(existing: &str, message: &str) -> String {
    if existing.is_empty() {
        message.to_string()
    } else if existing.contains(message) {
        existing.to_string()
    } else {
        format!("{existing}; {message}")
    }
}

fn remove_prometheus_target_warning(existing: &str) -> String {
    existing
        .replace(PROMETHEUS_MODE_TARGET_WARNING, "")
        .replace(NO_PROMETHEUS_TARGET_WARNING, "")
        .split(';')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("; ")
}

fn parse_prom_timestamp_ms(raw: Option<&str>) -> Option<i64> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|value| value.timestamp_millis())
}

fn normalize_base_url(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.trim_end_matches('/').to_string())
}

pub fn validate_prometheus_metric_name(metric: Option<&str>) -> Result<(), String> {
    if let Some(name) = metric.map(str::trim).filter(|value| !value.is_empty()) {
        if !is_valid_prometheus_metric_name(name) {
            return Err(format!("invalid Prometheus metric name: {name}"));
        }
    }
    Ok(())
}

pub fn build_promql(
    metric: Option<&str>,
    component: Option<&str>,
    instance: Option<&str>,
) -> Result<String, String> {
    validate_prometheus_metric_name(metric)?;
    let mut labels = Vec::new();
    if let Some(value) = component.filter(|value| !value.is_empty()) {
        labels.push(format!("component=\"{}\"", escape_promql_label(value)));
    }
    if let Some(value) = instance.filter(|value| !value.is_empty()) {
        labels.push(format!("instance=\"{}\"", escape_promql_label(value)));
    }
    let selector = if labels.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", labels.join(","))
    };
    Ok(
        match metric.map(str::trim).filter(|value| !value.is_empty()) {
            Some(name) => format!("{name}{selector}"),
            None if labels.is_empty() => "up".to_string(),
            None => selector,
        },
    )
}

fn is_valid_prometheus_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !is_prometheus_metric_name_start(first) {
        return false;
    }
    chars.all(is_prometheus_metric_name_continue)
}

fn is_prometheus_metric_name_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_' || ch == ':'
}

fn is_prometheus_metric_name_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == ':'
}

fn escape_promql_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn step_ms_to_prometheus(step_ms: i64) -> String {
    let step_ms = step_ms.max(1_000);
    if step_ms % 3_600_000 == 0 {
        format!("{}h", step_ms / 3_600_000)
    } else if step_ms % 60_000 == 0 {
        format!("{}m", step_ms / 60_000)
    } else {
        format!("{}s", step_ms / 1_000)
    }
}

fn vector_to_instant(item: PromResult, fallback_time_ms: i64) -> Option<InstantMetric> {
    let (timestamp_sec, value_str) = item.value?;
    let metric_name = item
        .metric
        .get("__name__")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    let value = value_str.parse::<f64>().ok()?;
    let timestamp_ms = (timestamp_sec * 1000.0).round() as i64;
    let timestamp_ms = if timestamp_ms > 0 {
        timestamp_ms
    } else {
        fallback_time_ms
    };
    let mut labels = item.metric;
    labels.remove("__name__");
    Some(InstantMetric {
        metric: metric_name,
        labels,
        timestamp_ms,
        value,
    })
}

fn matrix_to_range_series(item: PromResult) -> Option<RangeSeries> {
    let values = item.values?;
    if values.is_empty() {
        return None;
    }
    let metric_name = item
        .metric
        .get("__name__")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    let mut labels = item.metric;
    labels.remove("__name__");
    let points: Vec<RangePoint> = values
        .into_iter()
        .filter_map(|(timestamp_sec, value_str)| {
            let value = value_str.parse::<f64>().ok()?;
            Some(RangePoint((timestamp_sec * 1000.0).round() as i64, value))
        })
        .collect();
    if points.is_empty() {
        return None;
    }
    Some(RangeSeries {
        metric: metric_name,
        labels,
        points,
    })
}

fn has_any_up(results: &[PromResult]) -> bool {
    results.iter().any(|item| {
        item.value
            .as_ref()
            .and_then(|(_, value)| value.parse::<f64>().ok())
            .map(|value| value > 0.0)
            .unwrap_or(false)
    })
}

fn count_unique_up_instances(results: &[PromResult]) -> usize {
    results
        .iter()
        .filter(|item| {
            item.value
                .as_ref()
                .and_then(|(_, value)| value.parse::<f64>().ok())
                .map(|value| value > 0.0)
                .unwrap_or(false)
        })
        .filter_map(|item| item.metric.get("instance"))
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        .len()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
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

    #[test]
    fn build_promql_with_metric_and_labels() {
        assert_eq!(
            build_promql(Some("capacity"), Some("master"), Some("active-master")).unwrap(),
            "capacity{component=\"master\",instance=\"active-master\"}"
        );
    }

    #[test]
    fn build_promql_metric_only() {
        assert_eq!(
            build_promql(Some("capacity"), None, None).unwrap(),
            "capacity"
        );
    }

    #[test]
    fn build_promql_defaults_to_up_without_filters() {
        assert_eq!(build_promql(None, None, None).unwrap(), "up");
    }

    #[test]
    fn scrape_endpoint_key_ignores_scheme_and_path() {
        assert_eq!(
            scrape_endpoint_key("http://127.0.0.1:9090/metrics"),
            Some("127.0.0.1:9090".to_string())
        );
        assert_eq!(
            scrape_endpoint_key("https://prometheus.example:9090/"),
            Some("prometheus.example:9090".to_string())
        );
        assert_eq!(
            scrape_endpoint_key("127.0.0.1:9100"),
            Some("127.0.0.1:9100".to_string())
        );
    }

    #[test]
    fn scrape_endpoint_key_matches_different_schemes() {
        assert_eq!(
            scrape_endpoint_key("http://10.0.0.5:8080/metrics"),
            scrape_endpoint_key("https://10.0.0.5:8080/")
        );
    }

    #[test]
    fn scrape_instance_key_normalizes_case() {
        assert_eq!(
            scrape_instance_key("Active-Master:8080"),
            Some("active-master:8080".to_string())
        );
    }

    #[test]
    fn lookup_scrape_status_matches_instance_and_component_host() {
        let mut by_instance = BTreeMap::new();
        let mut by_component_host = BTreeMap::new();
        let status = PromScrapeStatus {
            up: true,
            last_error: String::new(),
            last_scrape_ms: Some(10),
            last_scrape_duration_ms: Some(2),
        };
        by_instance.insert("curvine-demo-worker-1:9001".to_string(), status.clone());
        by_component_host.insert("master:curvine-demo-master-2".to_string(), status.clone());
        let statuses = CachedScrapeStatuses {
            fetched_ms: 10,
            by_endpoint: BTreeMap::new(),
            by_instance,
            by_component_instance: BTreeMap::new(),
            by_component_host,
        };

        let worker = TargetHealth {
            component: "worker".to_string(),
            instance: "curvine-demo-worker-1:9001".to_string(),
            url: "http://172.18.0.6:9001/metrics".to_string(),
            ..Default::default()
        };
        assert!(lookup_scrape_status(&statuses, &worker).is_some());

        let master = TargetHealth {
            component: "master".to_string(),
            instance: "active-master".to_string(),
            url: "http://curvine-demo-master-2:29000/metrics".to_string(),
            ..Default::default()
        };
        assert!(lookup_scrape_status(&statuses, &master).is_some());
    }

    #[test]
    fn normalize_base_url_trims_trailing_slash() {
        assert_eq!(
            normalize_base_url("http://prometheus:9090/"),
            Some("http://prometheus:9090".to_string())
        );
        assert_eq!(normalize_base_url("   "), None);
    }

    #[test]
    fn escape_promql_label_quotes() {
        assert_eq!(escape_promql_label("a\"b"), "a\\\"b");
    }

    #[test]
    fn build_promql_rejects_promql_expression_as_metric() {
        assert!(build_promql(Some("rate(read_bytes[5m])"), None, None).is_err());
        assert!(build_promql(Some("read_bytes{job=\"curvine\"}"), None, None).is_err());
        assert!(build_promql(Some("read_bytes"), Some("worker"), None).is_ok());
        assert!(build_promql(Some(":curvine_metric"), None, None).is_ok());
    }

    #[test]
    fn missing_prometheus_scrape_target_marks_target_down() {
        let mut target = TargetHealth {
            id: "worker:demo:9001".to_string(),
            component: "worker".to_string(),
            instance: "demo:9001".to_string(),
            url: "http://demo:9001/metrics".to_string(),
            up: true,
            ..Default::default()
        };
        apply_missing_prometheus_scrape_status(&mut target);
        assert!(!target.up);
        assert!(target.last_warning.contains(NO_PROMETHEUS_TARGET_WARNING));
    }

    #[test]
    fn remove_prometheus_target_warning_preserves_discovery_warning() {
        assert_eq!(
            remove_prometheus_target_warning(
                "Prometheus mode; scrape health from Prometheus; master info RPC failed"
            ),
            "master info RPC failed"
        );
    }

    #[test]
    fn matrix_to_range_series_parses_points() {
        let mut metric = BTreeMap::new();
        metric.insert("__name__".to_string(), "read_bytes".to_string());
        metric.insert("component".to_string(), "worker".to_string());
        let series = matrix_to_range_series(PromResult {
            metric,
            value: None,
            values: Some(vec![(1.0, "10".to_string()), (16.0, "20".to_string())]),
        })
        .expect("series");
        assert_eq!(series.metric, "read_bytes");
        assert_eq!(series.points.len(), 2);
        assert_eq!(series.points[0], RangePoint(1_000, 10.0));
    }

    #[tokio::test]
    async fn health_check_accepts_vector_probe_response() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let body = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"curvine-master","component":"master"},"value":[1.0,"1"]}]}}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        let conf = curvine_config::PrometheusObservabilityConf {
            url: format!("http://{}", addr),
            ..Default::default()
        };
        let provider = PrometheusProvider::new(&conf).expect("provider");
        provider.check_health(0).await.expect("health");
        assert!(provider.stats().last_collect_ms.is_some());
    }

    #[tokio::test]
    async fn health_check_rejects_partial_worker_coverage() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..n]);
                let body = if request.contains("component%3D%22worker%22")
                    || request.contains("component=\"worker\"")
                {
                    r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"curvine-worker","component":"worker","instance":"worker-1:9001"},"value":[1.0,"1"]}]}}"#
                } else {
                    r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"curvine-master","component":"master","instance":"master-1:9010"},"value":[1.0,"1"]}]}}"#
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        let conf = curvine_config::PrometheusObservabilityConf {
            url: format!("http://{}", addr),
            ..Default::default()
        };
        let provider = PrometheusProvider::new(&conf).expect("provider");
        let error = provider
            .check_health(2)
            .await
            .expect_err("partial worker coverage");
        assert!(error.contains("component=worker"));
        assert!(error.contains("expected at least 2"));
    }

    #[test]
    fn unique_up_instance_count_ignores_duplicates_and_down_targets() {
        fn result(instance: &str, value: &str) -> PromResult {
            let mut metric = BTreeMap::new();
            metric.insert("instance".to_string(), instance.to_string());
            PromResult {
                metric,
                value: Some((1.0, value.to_string())),
                values: None,
            }
        }

        let results = vec![
            result("worker-1:9001", "1"),
            result("worker-1:9001", "1"),
            result("worker-2:9001", "0"),
            result("worker-3:9001", "1"),
        ];
        assert_eq!(count_unique_up_instances(&results), 2);
    }

    #[tokio::test]
    async fn health_check_rejects_missing_curvine_component_contract() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let body = r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        let conf = curvine_config::PrometheusObservabilityConf {
            url: format!("http://{}", addr),
            ..Default::default()
        };
        let provider = PrometheusProvider::new(&conf).expect("provider");
        let error = provider
            .check_health(0)
            .await
            .expect_err("missing master target");
        assert!(error.contains("component=master"));
    }

    #[tokio::test]
    async fn parses_query_range_response() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let body = r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"capacity","component":"master"},"values":[[1.0,"100"],[16.0,"200"]]}]}}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        let conf = curvine_config::PrometheusObservabilityConf {
            url: format!("http://{}", addr),
            ..Default::default()
        };
        let provider = PrometheusProvider::new(&conf).expect("provider");
        let series = provider
            .range_async(
                Some("capacity"),
                Some("master"),
                None,
                0,
                20_000,
                Some(15_000),
            )
            .await
            .expect("range");
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].metric, "capacity");
        assert_eq!(series[0].points.len(), 2);
    }

    #[tokio::test]
    async fn parses_label_name_values_response() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let body = r#"{"status":"success","data":["available","capacity","read_bytes"]}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        let conf = curvine_config::PrometheusObservabilityConf {
            url: format!("http://{}", addr),
            ..Default::default()
        };
        let provider = PrometheusProvider::new(&conf).expect("provider");
        let names = provider.names_async().await.expect("names");
        assert_eq!(names, vec!["available", "capacity", "read_bytes"]);
    }
}
