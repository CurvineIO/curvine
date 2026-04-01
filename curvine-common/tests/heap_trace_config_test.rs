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

#[test]
fn heap_trace_conf_defaults_disable_runtime() {
    let conf = ClusterConf::default();

    assert!(!conf.heap_trace.runtime_enabled);
    assert_eq!(conf.heap_trace.sample_interval_bytes, 8 * 1024 * 1024);
    assert_eq!(conf.heap_trace.periodic_interval_secs, 60);
}

#[test]
fn cluster_conf_parses_heap_trace_section() {
    let conf: ClusterConf = toml::from_str(
        r#"
            [heap_trace]
            runtime_enabled = true
            sample_interval_bytes = 1048576
            periodic_interval_secs = 120
        "#,
    )
    .unwrap();

    assert!(conf.heap_trace.runtime_enabled);
    assert_eq!(conf.heap_trace.sample_interval_bytes, 1_048_576);
    assert_eq!(conf.heap_trace.periodic_interval_secs, 120);
    assert_eq!(conf.cluster_id, "curvine");
}
