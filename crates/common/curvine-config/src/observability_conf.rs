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

/// Observability provider mode for curvine-web.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ObservabilityMode {
    #[default]
    Builtin,
    Prometheus,
    Auto,
}

impl ObservabilityMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::Prometheus => "prometheus",
            Self::Auto => "auto",
        }
    }
}

/// Top-level observability settings under `[web.observability]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ObservabilityConf {
    pub enabled: bool,
    pub mode: ObservabilityMode,
    /// Scrape interval for the built-in collector, e.g. `15s`, `1m`.
    pub scrape_interval: String,
    /// Per-target scrape timeout, e.g. `3s`.
    pub scrape_timeout: String,
    /// In-memory TSDB retention window, e.g. `1h`, `24h`.
    pub retention: String,
    pub max_concurrent_scrapes: usize,
    /// Max concurrent Prometheus/builtin range queries during one dashboard refresh.
    pub max_concurrent_dashboard_queries: usize,
    pub max_series: usize,
    pub max_samples_per_series: usize,
    pub fuse: FuseObservabilityConf,
    pub prometheus: PrometheusObservabilityConf,
}

/// FUSE metrics scrape settings under `[web.observability.fuse]`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FuseObservabilityConf {
    /// When true, discover and scrape FUSE `/metrics` endpoints.
    pub enabled: bool,
    /// Scrape targets as `host:port` or full `http://host:port/metrics` URLs.
    /// When empty, defaults to `127.0.0.1:{fuse.web_port}` from cluster config.
    pub targets: Vec<String>,
}

impl Default for ObservabilityConf {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: ObservabilityMode::Builtin,
            scrape_interval: "60s".to_string(),
            scrape_timeout: "3s".to_string(),
            retention: "24h".to_string(),
            max_concurrent_scrapes: 32,
            max_concurrent_dashboard_queries: 8,
            max_series: 100_000,
            max_samples_per_series: 10_000,
            fuse: FuseObservabilityConf::default(),
            prometheus: PrometheusObservabilityConf::default(),
        }
    }
}

/// Prometheus query provider settings under `[web.observability.prometheus]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PrometheusObservabilityConf {
    /// Base URL of the Prometheus server, e.g. `http://prometheus:9090`.
    pub url: String,
    /// HTTP timeout for Prometheus query APIs, e.g. `5s`.
    pub query_timeout: String,
    /// When mode is `auto`, fall back to the built-in collector if Prometheus is unhealthy.
    pub fallback_builtin: bool,
    /// TTL for caching Prometheus `/api/v1/targets` responses when enriching target health.
    pub target_cache_ttl: String,
}

impl Default for PrometheusObservabilityConf {
    fn default() -> Self {
        Self {
            url: String::new(),
            query_timeout: "5s".to_string(),
            fallback_builtin: true,
            target_cache_ttl: "30s".to_string(),
        }
    }
}

/// Parse a duration string such as `15s`, `1m`, or `1h` into milliseconds.
pub fn parse_duration_ms(value: &str) -> Result<u64, String> {
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
    let amount: u64 = number
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_mode_is_builtin() {
        let conf = ObservabilityConf::default();
        assert!(conf.enabled);
        assert_eq!(conf.mode, ObservabilityMode::Builtin);
        assert!(!conf.fuse.enabled);
    }

    #[test]
    fn parse_duration_supports_common_units() {
        assert_eq!(parse_duration_ms("15s").unwrap(), 15_000);
        assert_eq!(parse_duration_ms("1m").unwrap(), 60_000);
        assert_eq!(parse_duration_ms("1h").unwrap(), 3_600_000);
    }

    #[test]
    fn defaults_can_retain_the_full_supported_window() {
        let conf = ObservabilityConf::default();
        assert!(conf.max_samples_per_series >= 8_642);
    }
}
