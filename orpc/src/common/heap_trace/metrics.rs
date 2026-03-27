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

use crate::common::heap_trace::{reduce_topn, HeapProfileSummary, HeapTraceHotspot};
use crate::common::Metrics;
use once_cell::sync::Lazy;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const HOTSPOT_LABELS: [&str; 3] = ["rank", "site_id", "site_name"];
const RUN_STATUS_LABELS: [&str; 1] = ["status"];
const HOTSPOT_LIMIT: usize = 10;

pub static HEAP_TRACE_ENABLED: Lazy<Option<crate::common::Gauge>> = Lazy::new(|| {
    Metrics::new_gauge(
        "curvine_heap_profile_enabled",
        "Whether heap trace runtime is enabled",
    )
    .ok()
});

pub static HEAP_TRACE_LAST_SUCCESS_UNIXTIME: Lazy<Option<crate::common::Gauge>> = Lazy::new(|| {
    Metrics::new_gauge(
        "curvine_heap_profile_last_success_unixtime",
        "Unix timestamp of the last successful heap trace run",
    )
    .ok()
});

pub static HEAP_TRACE_LAST_DURATION_MS: Lazy<Option<crate::common::Gauge>> = Lazy::new(|| {
    Metrics::new_gauge(
        "curvine_heap_profile_last_duration_ms",
        "Duration of the last heap trace run in milliseconds",
    )
    .ok()
});

pub static HEAP_TRACE_RUNS_TOTAL: Lazy<Option<crate::common::CounterVec>> = Lazy::new(|| {
    Metrics::new_counter_vec(
        "curvine_heap_profile_runs_total",
        "Total heap trace runs by terminal state",
        &RUN_STATUS_LABELS,
    )
    .ok()
});

pub static HEAP_TRACE_PARSE_ERRORS_TOTAL: Lazy<Option<crate::common::Counter>> = Lazy::new(|| {
    Metrics::new_counter(
        "curvine_heap_profile_parse_errors_total",
        "Total heap trace parse errors",
    )
    .ok()
});

pub static HEAP_TRACE_HOTSPOT_BYTES: Lazy<Option<crate::common::GaugeVec>> = Lazy::new(|| {
    Metrics::new_gauge_vec(
        "curvine_heap_hotspot_bytes",
        "Heap hotspot bytes by rank and site",
        &HOTSPOT_LABELS,
    )
    .ok()
});

pub static HEAP_TRACE_HOTSPOT_OBJECTS: Lazy<Option<crate::common::GaugeVec>> = Lazy::new(|| {
    Metrics::new_gauge_vec(
        "curvine_heap_hotspot_objects",
        "Heap hotspot object count by rank and site",
        &HOTSPOT_LABELS,
    )
    .ok()
});

pub static HEAP_TRACE_HOTSPOT_GROWTH_BYTES: Lazy<Option<crate::common::GaugeVec>> =
    Lazy::new(|| {
        Metrics::new_gauge_vec(
            "curvine_heap_hotspot_growth_bytes",
            "Heap hotspot growth bytes by rank and site",
            &HOTSPOT_LABELS,
        )
        .ok()
    });

static LATEST_SUMMARY: Lazy<Mutex<Option<HeapProfileSummary>>> = Lazy::new(|| Mutex::new(None));

pub fn init_metrics() {
    Lazy::force(&HEAP_TRACE_ENABLED);
    Lazy::force(&HEAP_TRACE_LAST_SUCCESS_UNIXTIME);
    Lazy::force(&HEAP_TRACE_LAST_DURATION_MS);
    Lazy::force(&HEAP_TRACE_RUNS_TOTAL);
    Lazy::force(&HEAP_TRACE_PARSE_ERRORS_TOTAL);
    Lazy::force(&HEAP_TRACE_HOTSPOT_BYTES);
    Lazy::force(&HEAP_TRACE_HOTSPOT_OBJECTS);
    Lazy::force(&HEAP_TRACE_HOTSPOT_GROWTH_BYTES);
}

pub fn set_enabled(enabled: bool) {
    init_metrics();
    if let Some(metric) = HEAP_TRACE_ENABLED.as_ref() {
        metric.set(i64::from(enabled));
    }
}

pub fn record_run_success(duration: Duration) {
    init_metrics();
    record_run_status("success");

    if let Some(metric) = HEAP_TRACE_LAST_DURATION_MS.as_ref() {
        metric.set(duration.as_millis().min(i64::MAX as u128) as i64);
    }

    if let Some(metric) = HEAP_TRACE_LAST_SUCCESS_UNIXTIME.as_ref() {
        metric.set(now_unix_seconds());
    }
}

pub fn record_run_failure() {
    init_metrics();
    record_run_status("failure");
}

pub fn record_parse_error() {
    init_metrics();
    if let Some(metric) = HEAP_TRACE_PARSE_ERRORS_TOTAL.as_ref() {
        metric.inc();
    }
}

pub fn record_capture_attempt() {
    init_metrics();
    record_run_status("attempt");
}

pub fn update_hotspots(hotspots: &[HeapTraceHotspot]) {
    init_metrics();

    let reduced = reduce_topn(hotspots.to_vec(), HOTSPOT_LIMIT);
    reset_hotspot_gauges();

    for hotspot in reduced {
        let rank = hotspot.rank.to_string();
        let site_id = hotspot.stable_id.as_str();
        let site_name = hotspot.site_name.as_str();
        let labels = [&rank[..], site_id, site_name];

        if let Some(metric) = HEAP_TRACE_HOTSPOT_BYTES.as_ref() {
            metric
                .with_label_values(&labels)
                .set(hotspot.bytes.min(i64::MAX as usize) as i64);
        }

        if let Some(metric) = HEAP_TRACE_HOTSPOT_OBJECTS.as_ref() {
            metric
                .with_label_values(&labels)
                .set(hotspot.objects.min(i64::MAX as usize) as i64);
        }

        if let Some(metric) = HEAP_TRACE_HOTSPOT_GROWTH_BYTES.as_ref() {
            metric.with_label_values(&labels).set(hotspot.growth_bytes);
        }
    }
}

pub fn store_latest_summary(summary: HeapProfileSummary) {
    let mut guard = LATEST_SUMMARY.lock().unwrap();
    *guard = Some(summary);
}

pub fn latest_summary() -> Option<HeapProfileSummary> {
    LATEST_SUMMARY.lock().unwrap().clone()
}

pub fn clear_latest_summary() {
    let mut guard = LATEST_SUMMARY.lock().unwrap();
    *guard = None;
}

fn record_run_status(status: &str) {
    if let Some(metric) = HEAP_TRACE_RUNS_TOTAL.as_ref() {
        metric.with_label_values(&[status]).inc();
    }
}

fn reset_hotspot_gauges() {
    if let Some(metric) = HEAP_TRACE_HOTSPOT_BYTES.as_ref() {
        metric.reset();
    }
    if let Some(metric) = HEAP_TRACE_HOTSPOT_OBJECTS.as_ref() {
        metric.reset();
    }
    if let Some(metric) = HEAP_TRACE_HOTSPOT_GROWTH_BYTES.as_ref() {
        metric.reset();
    }
}

fn now_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64)
        .unwrap_or_default()
}
