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

//! Regression tests for the `*_str` twin-field normalization.
//!
//! Historically every duration/size knob existed as a pair: a parsed value
//! field (`block_size`) plus a raw string twin (`block_size_str`). Only the
//! twin carried a serde attribute, and it did so as `alias`, so serialization
//! emitted the *twin's own Rust name* (`block_size_str`) while deserialization
//! also accepted the documented name. That made the serializer's output
//! disagree with the documented configuration keys, and it silently broke
//! three knobs whose twins had no alias at all.
//!
//! The twins now carry `rename = "<base>", alias = "<base>_str"`: the
//! documented base name is what TOML/JSON emits and what is written in the
//! config files, and the old `_str` spelling survives as a compatibility
//! alias.

use curvine_config::{ClusterConf, TransferConf};
use std::time::Duration;

/// Serialized TOML must use the documented base names, never the `_str` twin
/// names, in every section touched by the normalization.
#[test]
fn toml_serializes_duration_base_names() {
    let conf = ClusterConf::default();
    let text = conf
        .to_pretty_toml()
        .expect("default cluster conf must serialize");
    let doc: toml::Value = toml::from_str(&text).expect("serialized conf must be valid TOML");

    // Representative base names per section: one early field, one late field,
    // plus the knobs whose `_str` twin name used to leak out.
    let expected: &[(&str, &[&str])] = &[
        (
            "client",
            &[
                "block_size",
                "ttl_ms",
                "small_file_size",
                "seek_absorb_window",
            ],
        ),
        ("job", &["job_life_ttl", "terminal_retention"]),
        (
            "transfer",
            &[
                "lease_timeout",
                "terminal_retention",
                "task_retention",
                "worker_terminal_retention",
                "cluster_snapshot_max_staleness",
            ],
        ),
    ];

    for (section, keys) in expected {
        let table = doc
            .get(section)
            .and_then(|v| v.as_table())
            .unwrap_or_else(|| panic!("serialized conf has no [{section}] table"));

        for key in *keys {
            assert!(
                table.contains_key(*key),
                "[{section}] must serialize the documented key `{key}`; keys: {:?}",
                table.keys().collect::<Vec<_>>()
            );
        }

        // No twin name may survive anywhere in the section.
        let leaked: Vec<&String> = table.keys().filter(|k| k.ends_with("_str")).collect();
        assert!(
            leaked.is_empty(),
            "[{section}] still serializes twin `_str` keys: {leaked:?}"
        );
    }
}

/// The three knobs that used to be unconfigurable must honour their documented
/// base names end-to-end: TOML parsing, `init()` derivation, and the unknown-key
/// audit (which reports `[WARN] unrecognized config key` for anything no field
/// consumes).
#[test]
fn dead_transfer_knobs_accept_documented_names() {
    // `TransferConf` deserializes from the flat body of the `[transfer]` table,
    // while the audit probe walks the whole cluster document.
    let body = r#"
        cluster_snapshot_max_staleness = "90s"
        task_retention = "12h"
        worker_terminal_retention = "5m"
    "#;

    let mut conf: TransferConf = toml::from_str(body).expect("documented names must parse");
    conf.init().expect("documented knobs must init");

    assert_eq!(
        conf.cluster_snapshot_max_staleness,
        Duration::from_secs(90),
        "transfer.cluster_snapshot_max_staleness was silently dropped"
    );
    assert_eq!(
        conf.task_retention,
        Duration::from_secs(12 * 60 * 60),
        "transfer.task_retention was silently dropped"
    );
    assert_eq!(
        conf.worker_terminal_retention,
        Duration::from_secs(5 * 60),
        "transfer.worker_terminal_retention was silently dropped"
    );

    // Values must be the configured ones, not the compiled-in defaults.
    assert_ne!(
        conf.cluster_snapshot_max_staleness_str,
        TransferConf::DEFAULT_CLUSTER_SNAPSHOT_MAX_STALENESS
    );
    assert_ne!(
        conf.task_retention_str,
        TransferConf::DEFAULT_TASK_RETENTION
    );
    assert_ne!(
        conf.worker_terminal_retention_str,
        TransferConf::DEFAULT_WORKER_TERMINAL_RETENTION
    );

    // The audit probe must consider all three keys consumed, otherwise the
    // config loader logs `unrecognized config key` for them.
    let raw = format!("[transfer]\n{body}");
    let unknown = curvine_config::validation::audit_unknown_keys(&raw)
        .expect("audit must parse the same document");
    assert!(
        unknown.is_empty(),
        "documented transfer knobs reported as unknown keys: {unknown:?}"
    );
}

/// The legacy `_str` spelling stays accepted as an alias so existing config
/// files keep loading.
#[test]
fn legacy_str_spelling_remains_accepted() {
    let mut conf: TransferConf = toml::from_str(
        r#"
            cluster_snapshot_max_staleness_str = "75s"
            task_retention_str = "6h"
            worker_terminal_retention_str = "2m"
        "#,
    )
    .expect("legacy `_str` names must still parse");
    conf.init().expect("legacy `_str` knobs must init");

    assert_eq!(conf.cluster_snapshot_max_staleness, Duration::from_secs(75));
    assert_eq!(conf.task_retention, Duration::from_secs(6 * 60 * 60));
    assert_eq!(conf.worker_terminal_retention, Duration::from_secs(2 * 60));
}

/// `to_pretty_toml` output is written straight back to disk as the active
/// cluster config (`curvine_tests::testing` does exactly that), so renaming
/// the serialized keys must not break the read-back. Non-default values on the
/// normalized knobs have to survive serialize -> parse -> init unchanged, and
/// the emitted document must not trip the unknown-key audit.
#[test]
fn serialized_toml_round_trips_back_into_cluster_conf() {
    let mut conf = ClusterConf::default();
    conf.client.block_size_str = "64MB".to_string();
    conf.client.small_file_size_str = "8KB".to_string();
    conf.job.job_life_ttl_str = "1h".to_string();
    conf.transfer.lease_timeout_str = "30s".to_string();
    conf.transfer.terminal_retention_str = "48h".to_string();
    conf.transfer.cluster_snapshot_max_staleness_str = "90s".to_string();
    conf.transfer.task_retention_str = "12h".to_string();
    conf.transfer.worker_terminal_retention_str = "5m".to_string();

    let text = conf
        .to_pretty_toml()
        .expect("non-default cluster conf must serialize");
    let mut parsed: ClusterConf =
        toml::from_str(&text).expect("serialized cluster conf must parse back");
    parsed.client.init().expect("client init");
    parsed.transfer.init().expect("transfer init");

    assert_eq!(parsed.client.block_size_str, "64MB");
    assert_eq!(parsed.client.small_file_size_str, "8KB");
    assert_eq!(parsed.job.job_life_ttl_str, "1h");
    assert_eq!(parsed.transfer.lease_timeout, Duration::from_secs(30));
    assert_eq!(
        parsed.transfer.terminal_retention,
        Duration::from_secs(48 * 3600)
    );
    assert_eq!(
        parsed.transfer.cluster_snapshot_max_staleness,
        Duration::from_secs(90)
    );
    assert_eq!(
        parsed.transfer.task_retention,
        Duration::from_secs(12 * 3600)
    );
    assert_eq!(
        parsed.transfer.worker_terminal_retention,
        Duration::from_secs(5 * 60)
    );

    let unknown: Vec<String> = curvine_config::validation::audit_unknown_keys(&text)
        .expect("serialized cluster conf must be auditable")
        .into_iter()
        .filter(|key| {
            key.starts_with("client.") || key.starts_with("job.") || key.starts_with("transfer.")
        })
        .collect();
    assert!(
        unknown.is_empty(),
        "serialized cluster conf has unknown client/job/transfer keys: {unknown:?}"
    );
}
