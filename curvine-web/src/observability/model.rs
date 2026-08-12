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

use std::collections::BTreeMap;

use curvine_config::ObservabilityMode;
use serde::{Deserialize, Serialize};

/// Stable TSDB / legend instance for active-master metrics in HA clusters.
/// Physical leader hostname changes on failover; charts always use this label.
pub const ACTIVE_MASTER_INSTANCE: &str = "active-master";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricTarget {
    pub id: String,
    pub component: String,
    pub instance: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_state: Option<String>,
    /// Active master RPC endpoint from `MasterInfo`, set only for master targets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_master: Option<String>,
}

impl MetricTarget {
    pub fn master(instance: String, url: String, active_master: Option<String>) -> Self {
        Self {
            id: format!("master:{instance}"),
            component: "master".to_string(),
            instance,
            url,
            worker_id: None,
            worker_state: None,
            active_master,
        }
    }

    pub fn worker(instance: String, url: String, worker_id: u32, worker_state: &str) -> Self {
        Self {
            id: format!("worker:{worker_id}"),
            component: "worker".to_string(),
            instance,
            url,
            worker_id: Some(worker_id),
            worker_state: Some(worker_state.to_string()),
            active_master: None,
        }
    }

    pub fn fuse(instance: String, url: String) -> Self {
        Self {
            id: format!("fuse:{instance}"),
            component: "fuse".to_string(),
            instance,
            url,
            worker_id: None,
            worker_state: None,
            active_master: None,
        }
    }

    pub fn transfer(instance: String, url: String) -> Self {
        Self {
            id: format!("transfer:{instance}"),
            component: "transfer".to_string(),
            instance,
            url,
            worker_id: None,
            worker_state: None,
            active_master: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TargetHealth {
    pub id: String,
    pub component: String,
    pub instance: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_state: Option<String>,
    pub up: bool,
    pub last_scrape_ms: Option<i64>,
    pub last_success_ms: Option<i64>,
    pub last_scrape_duration_ms: Option<u64>,
    /// Scrape failure only. Empty when the latest `/metrics` request succeeded.
    pub last_error: String,
    /// Non-fatal discovery issues, e.g. master info RPC failed while fallback scrape succeeded.
    pub last_warning: String,
    pub sample_count: usize,
    pub parse_error_count: usize,
    /// Active master RPC endpoint from discovery, set only for master targets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_master: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricSample {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SeriesKey {
    pub metric: String,
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SeriesPoint {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// API-facing range point; serializes as `[timestamp_ms, value]` per design doc.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RangePoint(pub i64, pub f64);

impl From<&SeriesPoint> for RangePoint {
    fn from(point: &SeriesPoint) -> Self {
        RangePoint(point.timestamp_ms, point.value)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstantMetric {
    pub metric: String,
    pub labels: BTreeMap<String, String>,
    pub timestamp_ms: i64,
    pub value: f64,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct InstantQuery {
    pub metric: Option<String>,
    pub component: Option<String>,
    pub instance: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RangeQuery {
    pub metric: Option<String>,
    pub component: Option<String>,
    pub instance: Option<String>,
    pub range: String,
    pub step: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RangeSeries {
    pub metric: String,
    pub labels: BTreeMap<String, String>,
    pub points: Vec<RangePoint>,
}

/// Parse a duration string such as `15s`, `1m`, `15m`, or `1h` into milliseconds.
pub fn parse_duration_ms(value: &str) -> Result<i64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("duration must not be empty".to_string());
    }
    let (number, unit) = value
        .as_bytes()
        .iter()
        .position(|ch| !ch.is_ascii_digit())
        .map(|index| value.split_at(index))
        .ok_or_else(|| format!("invalid duration: {value}"))?;
    if number.is_empty() {
        return Err(format!("invalid duration: {value}"));
    }
    let amount: i64 = number
        .parse()
        .map_err(|_| format!("invalid duration: {value}"))?;
    let multiplier = match unit {
        "s" | "sec" | "secs" => 1_000,
        "m" | "min" | "mins" => 60_000,
        "h" | "hr" | "hrs" => 3_600_000,
        "d" | "day" | "days" => 86_400_000,
        _ => return Err(format!("unsupported duration unit in: {value}")),
    };
    Ok(amount.saturating_mul(multiplier))
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ObservabilityModeUpdate {
    pub mode: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderHealth {
    pub enabled: bool,
    pub mode: String,
    pub active_provider: String,
    pub prometheus_configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_health: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prometheus_url: Option<String>,
    pub scrape_interval_ms: u64,
    pub retention_ms: u64,
    pub series_count: usize,
    pub sample_count: usize,
    pub last_discovery_ms: Option<i64>,
    pub last_collect_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ObservabilitySettings {
    pub scrape_interval_ms: u64,
    pub retention_ms: u64,
    pub scrape_interval_options_ms: Vec<u64>,
    pub retention_options_ms: Vec<u64>,
    pub max_retention_ms: u64,
    pub max_samples_per_series_cap: usize,
    pub max_total_samples: usize,
    pub max_series: usize,
    pub estimated_peak_samples: usize,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ObservabilitySettingsUpdate {
    pub scrape_interval_ms: Option<u64>,
    pub retention_ms: Option<u64>,
}

/// Parse UI/TOML observability mode strings.
pub fn parse_observability_mode(value: &str) -> Result<ObservabilityMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "builtin" => Ok(ObservabilityMode::Builtin),
        "prometheus" => Ok(ObservabilityMode::Prometheus),
        "auto" => Ok(ObservabilityMode::Auto),
        _ => Err(format!("invalid observability mode: {value}")),
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DashboardQuery {
    pub range: String,
    pub step: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DashboardWindow {
    pub range: String,
    pub step: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub last_collect_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DashboardCard {
    pub id: String,
    pub title: String,
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SeriesSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DashboardSeries {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub points: Vec<RangePoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<SeriesSummary>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PanelKind {
    Timeseries,
    #[allow(dead_code)]
    Table,
    #[allow(dead_code)]
    Instant,
}

#[derive(Clone, Debug, Serialize)]
pub struct DashboardTableRow {
    pub id: String,
    pub component: String,
    pub instance: String,
    pub scrape_up: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_state: Option<String>,
    pub status: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub detail: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct DashboardPanel {
    pub id: String,
    pub title: String,
    pub subtitle: String,
    pub kind: PanelKind,
    pub series: Vec<DashboardSeries>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<DashboardTableRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub section: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl DashboardPanel {
    pub fn with_section(mut self, section: impl Into<String>) -> Self {
        self.section = Some(section.into());
        self
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DashboardGroup {
    pub id: String,
    pub title: String,
    pub panels: Vec<DashboardPanel>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DashboardPayload {
    pub window: DashboardWindow,
    pub cards: Vec<DashboardCard>,
    pub groups: Vec<DashboardGroup>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DiagnosticStatus {
    Pass,
    Warning,
    Fail,
    Skipped,
}

#[derive(Clone, Debug, Serialize)]
pub struct DiagnosticCheck {
    pub id: String,
    pub title: String,
    pub status: DiagnosticStatus,
    pub summary: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub detail: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub suggested_fix: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct MissingTarget {
    pub id: String,
    pub source: String,
    pub component: String,
    pub instance: String,
    pub url: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct DiagnosticsPayload {
    pub generated_ms: i64,
    pub active_provider: String,
    pub overall_status: DiagnosticStatus,
    pub checks: Vec<DiagnosticCheck>,
    pub discovered_master_targets: usize,
    pub discovered_live_worker_targets: usize,
    pub discovered_transfer_targets: usize,
    pub prometheus_covered_targets: usize,
    pub active_covered_targets: usize,
    pub expected_targets: usize,
    pub missing_targets: Vec<MissingTarget>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Critical,
    Warning,
}

#[derive(Clone, Debug, Serialize)]
pub struct OperationsAlert {
    pub id: String,
    pub severity: AlertSeverity,
    pub title: String,
    pub summary: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    pub suggested_fix: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct OperationsPayload {
    pub generated_ms: i64,
    pub diagnostics: DiagnosticsPayload,
    pub alerts: Vec<OperationsAlert>,
}

#[cfg(test)]
mod duration_tests {
    use super::{parse_duration_ms, parse_observability_mode, RangePoint};
    use curvine_config::ObservabilityMode;
    use serde_json::json;

    #[test]
    fn rejects_empty_duration() {
        assert!(parse_duration_ms("").is_err());
    }

    #[test]
    fn range_point_serializes_as_timestamp_value_tuple() {
        let point = RangePoint(1_760_000_000_000, 123.0);
        let value = serde_json::to_value(point).expect("serialize range point");
        assert_eq!(value, json!([1_760_000_000_000i64, 123.0]));
    }

    #[test]
    fn parse_observability_mode_accepts_known_values() {
        assert_eq!(
            parse_observability_mode("builtin").unwrap(),
            ObservabilityMode::Builtin
        );
        assert_eq!(
            parse_observability_mode("Prometheus").unwrap(),
            ObservabilityMode::Prometheus
        );
        assert_eq!(
            parse_observability_mode("auto").unwrap(),
            ObservabilityMode::Auto
        );
        assert!(parse_observability_mode("invalid").is_err());
    }
}
