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

use super::model::{InstantMetric, RangeSeries, TargetHealth};

/// Shared query surface for built-in TSDB and Prometheus-backed providers.
pub trait MetricsProvider: Send + Sync {
    fn provider_id(&self) -> &'static str;

    fn instant(
        &self,
        metric: Option<&str>,
        component: Option<&str>,
        instance: Option<&str>,
    ) -> Vec<InstantMetric>;

    fn range(
        &self,
        metric: Option<&str>,
        component: Option<&str>,
        instance: Option<&str>,
        start_ms: i64,
        end_ms: i64,
        step_ms: Option<i64>,
    ) -> Vec<RangeSeries>;

    fn names(&self) -> Vec<String>;

    fn stats(&self) -> ProviderStats;
}

#[derive(Clone, Debug, Default)]
pub struct ProviderStats {
    pub series_count: usize,
    pub sample_count: usize,
    pub last_discovery_ms: Option<i64>,
    pub last_collect_ms: Option<i64>,
}

/// Scrape target registry for the built-in collector.
pub trait TargetRegistry: Send + Sync {
    fn targets(&self) -> Vec<TargetHealth>;
}
