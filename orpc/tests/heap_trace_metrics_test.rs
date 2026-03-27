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

#![cfg(feature = "heap-trace")]

use orpc::common::heap_trace::{
    clear_latest_summary, latest_summary, next_profile_id, prune_old_artifacts, record_parse_error,
    record_run_failure, record_run_success, set_enabled, store_latest_summary, update_hotspots,
    write_summary, HeapProfileSummary, HeapTraceHotspot,
};
use orpc::common::Metrics;
use std::fs;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn exports_unified_heap_trace_metrics_with_site_name_labels() {
    set_enabled(true);
    record_run_success(Duration::from_millis(125));
    record_run_failure();
    record_parse_error();
    update_hotspots(&[
        hotspot(
            7,
            "site-123",
            "curvine_worker::cache::allocate",
            4096,
            32,
            512,
        ),
        hotspot(3, "site-999", "rocksdb::DBImpl::Write", 2048, 11, -128),
    ]);

    let output = Metrics::text_output().unwrap();

    assert!(output.contains("curvine_heap_profile_enabled"));
    assert!(output.contains("curvine_heap_profile_last_success_unixtime"));
    assert!(output.contains("curvine_heap_profile_last_duration_ms"));
    assert!(output.contains("curvine_heap_profile_runs_total{status=\"success\"} 1"));
    assert!(output.contains("curvine_heap_profile_runs_total{status=\"failure\"} 1"));
    assert!(output.contains("curvine_heap_profile_parse_errors_total 1"));
    assert!(output.contains("curvine_heap_hotspot_bytes"));
    assert!(output.contains("curvine_heap_hotspot_objects"));
    assert!(output.contains("curvine_heap_hotspot_growth_bytes"));
    assert!(output.contains("site_name=\"curvine_worker::cache::allocate\""));
    assert!(output.contains("site_id=\"site-123\""));
    assert!(output.contains("rank=\"1\""));
}

#[test]
fn stores_latest_summary_and_prunes_old_artifacts() {
    clear_latest_summary();

    let summary = HeapProfileSummary {
        runtime_enabled: true,
        sample_interval_bytes: 4096,
        capture_count: 3,
        last_capture_epoch_ms: Some(42),
    };
    store_latest_summary(summary.clone());

    assert_eq!(latest_summary(), Some(summary.clone()));

    let temp = tempdir().unwrap();
    let dir = temp.path();

    let oldest = dir.join("20260327-000001-profile-summary.json");
    let middle = dir.join("20260327-000002-profile.pb.gz");
    let newest = dir.join("20260327-000003-profile-summary.json");

    write_summary(&oldest, &summary).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    fs::write(&middle, b"profile").unwrap();
    std::thread::sleep(Duration::from_millis(5));
    write_summary(&newest, &summary).unwrap();

    prune_old_artifacts(dir, 2).unwrap();

    assert!(!oldest.exists());
    assert!(middle.exists());
    assert!(newest.exists());
    assert!(dir.join("heap-profile-latest-summary.json").exists());
    assert!(!next_profile_id().is_empty());
}

fn hotspot(
    rank: usize,
    stable_id: &str,
    site_name: &str,
    bytes: usize,
    objects: usize,
    growth_bytes: i64,
) -> HeapTraceHotspot {
    HeapTraceHotspot {
        rank,
        site_name: site_name.to_string(),
        stable_id: stable_id.to_string(),
        bytes,
        objects,
        growth_bytes,
        frames: vec![site_name.to_string()],
    }
}
