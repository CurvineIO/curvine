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

use clap::Parser;
use curvine_common::conf::{ClientConf, ClientConfCliOverrides};

#[derive(Debug, Parser)]
struct CliHarness {
    #[command(flatten)]
    overrides: ClientConfCliOverrides,
}

#[test]
fn parses_pilot_client_cli_flags() {
    let parsed = CliHarness::try_parse_from([
        "curvine-fuse",
        "--client.io-threads",
        "8",
        "--client.block-size",
        "128MB",
        "--client.read-parallel",
        "4",
        "--client.short-circuit",
        "true",
    ])
    .unwrap();

    assert_eq!(parsed.overrides.io_threads, Some(8));
    assert_eq!(parsed.overrides.block_size_str.as_deref(), Some("128MB"));
    assert_eq!(parsed.overrides.read_parallel, Some(4));
    assert_eq!(parsed.overrides.short_circuit, Some(true));
}

#[test]
fn apply_to_updates_pilot_client_fields() {
    let mut conf = ClientConf::default();
    let overrides = ClientConfCliOverrides {
        io_threads: Some(16),
        worker_threads: Some(32),
        block_size_str: Some("64KB".to_string()),
        read_parallel: Some(2),
        short_circuit: Some(false),
        ..Default::default()
    };
    overrides.apply_to(&mut conf).unwrap();
    conf.init().unwrap();

    assert_eq!(conf.io_threads, 16);
    assert_eq!(conf.worker_threads, 32);
    assert_eq!(conf.block_size_str, "64KB");
    assert_eq!(conf.read_parallel, 2);
    assert!(!conf.short_circuit);
}

#[test]
fn normalizes_non_positive_read_parallel_settings_to_defaults() {
    let mut conf = ClientConf {
        read_parallel: 0,
        max_read_parallel: 0,
        ..Default::default()
    };

    conf.init().unwrap();

    assert_eq!(
        (conf.read_parallel, conf.max_read_parallel),
        (
            ClientConf::DEFAULT_READ_PARALLEL,
            ClientConf::DEFAULT_MAX_READ_PARALLEL
        )
    );
}

#[test]
fn parses_c4_chunk_unified_fs_and_audit_flags() {
    let parsed = CliHarness::try_parse_from([
        "curvine-fuse",
        "--client.write-chunk-size",
        "256KB",
        "--client.read-chunk-num",
        "4",
        "--client.enable-unified-fs",
        "true",
        "--client.audit-logging-enabled",
        "false",
    ])
    .unwrap();

    assert_eq!(
        parsed.overrides.write_chunk_size_str.as_deref(),
        Some("256KB")
    );
    assert_eq!(parsed.overrides.read_chunk_num, Some(4));
    assert_eq!(parsed.overrides.enable_unified_fs, Some(true));
    assert_eq!(parsed.overrides.audit_logging_enabled, Some(false));
}

#[test]
fn apply_to_updates_c4_fields() {
    let mut conf = ClientConf::default();
    let overrides = ClientConfCliOverrides {
        write_chunk_size_str: Some("512KB".to_string()),
        enable_rust_read_ufs: Some(true),
        max_cache_block_handles: Some(20),
        ..Default::default()
    };
    overrides.apply_to(&mut conf).unwrap();
    conf.init().unwrap();

    assert_eq!(conf.write_chunk_size_str, "512KB");
    assert!(conf.enable_rust_read_ufs);
    assert_eq!(conf.max_cache_block_handles, 20);
}

#[test]
fn parses_c5_rpc_retry_and_timeout_flags() {
    let parsed = CliHarness::try_parse_from([
        "curvine-fuse",
        "--client.conn-retry-max-duration-ms",
        "120000",
        "--client.conn-retry-min-sleep-ms",
        "100",
        "--client.conn-retry-max-sleep-ms",
        "2000",
        "--client.rpc-retry-max-duration-ms",
        "180000",
        "--client.rpc-retry-min-sleep-ms",
        "200",
        "--client.rpc-retry-max-sleep-ms",
        "10000",
        "--client.rpc-timeout-ms",
        "60000",
        "--client.conn-timeout-ms",
        "30000",
        "--client.data-timeout-ms",
        "90000",
        "--client.rpc-close-idle",
        "false",
        "--client.master-conn-pool-size",
        "5",
    ])
    .unwrap();

    assert_eq!(parsed.overrides.conn_retry_max_duration_ms, Some(120_000));
    assert_eq!(parsed.overrides.conn_retry_min_sleep_ms, Some(100));
    assert_eq!(parsed.overrides.conn_retry_max_sleep_ms, Some(2_000));
    assert_eq!(parsed.overrides.rpc_retry_max_duration_ms, Some(180_000));
    assert_eq!(parsed.overrides.rpc_retry_min_sleep_ms, Some(200));
    assert_eq!(parsed.overrides.rpc_retry_max_sleep_ms, Some(10_000));
    assert_eq!(parsed.overrides.rpc_timeout_ms, Some(60_000));
    assert_eq!(parsed.overrides.conn_timeout_ms, Some(30_000));
    assert_eq!(parsed.overrides.data_timeout_ms, Some(90_000));
    assert_eq!(parsed.overrides.rpc_close_idle, Some(false));
    assert_eq!(parsed.overrides.master_conn_pool_size, Some(5));
}

#[test]
fn apply_to_updates_c5_fields() {
    let mut conf = ClientConf::default();
    let overrides = ClientConfCliOverrides {
        conn_retry_max_duration_ms: Some(120_000),
        rpc_retry_min_sleep_ms: Some(200),
        data_timeout_ms: Some(90_000),
        ..Default::default()
    };
    overrides.apply_to(&mut conf).unwrap();

    assert_eq!(conf.conn_retry_max_duration_ms, 120_000);
    assert_eq!(conf.rpc_retry_min_sleep_ms, 200);
    assert_eq!(conf.data_timeout_ms, 90_000);
}

#[test]
fn rejects_pipeline_timeout_ms_until_wired_to_rpc_conf() {
    let err = CliHarness::try_parse_from(["curvine-fuse", "--client.pipeline-timeout-ms", "90000"])
        .unwrap_err();
    assert!(err.to_string().contains("pipeline-timeout-ms"));
}

#[test]
fn parses_c6_remaining_client_flags() {
    let parsed = CliHarness::try_parse_from([
        "curvine-fuse",
        "--client.hostname",
        "worker-1",
        "--client.replicas",
        "3",
        "--client.storage-type",
        "disk",
        "--client.enable-read-ahead",
        "true",
        "--client.max-read-parallel",
        "16",
        "--client.sequential-read-threshold",
        "10",
    ])
    .unwrap();

    assert_eq!(parsed.overrides.hostname.as_deref(), Some("worker-1"));
    assert_eq!(parsed.overrides.replicas, Some(3));
    assert_eq!(parsed.overrides.storage_type_str.as_deref(), Some("disk"));
    assert_eq!(parsed.overrides.enable_read_ahead, Some(true));
    assert_eq!(parsed.overrides.max_read_parallel, Some(16));
    assert_eq!(parsed.overrides.sequential_read_threshold, Some(10));
}

#[test]
fn apply_to_updates_c6_fields() {
    let mut conf = ClientConf::default();
    let overrides = ClientConfCliOverrides {
        hostname: Some("node-a".to_string()),
        auto_cache_ttl: Some("14d".to_string()),
        failed_worker_ttl_ms: Some(30 * 60 * 1000),
        sync_check_log_tick: Some(5),
        enable_smart_prefetch: Some(false),
        ..Default::default()
    };
    overrides.apply_to(&mut conf).unwrap();
    conf.init().unwrap();

    assert_eq!(conf.hostname, "node-a");
    assert_eq!(conf.auto_cache_ttl, "14d");
    assert_eq!(conf.failed_worker_ttl_ms, 30 * 60 * 1000);
    assert_eq!(conf.sync_check_log_tick, 5);
    assert!(!conf.enable_smart_prefetch);
}

#[test]
fn toml_legacy_time_field_aliases_preserved() {
    // The *_ms time fields were renamed from String (e.g. failed_worker_ttl = "10m")
    // to u64 milliseconds (issue #1023-style hygiene). ClientConf is #[serde(default)]
    // without deny_unknown_fields, so a serde alias on each renamed field keeps legacy
    // numeric configs working. NOTE: legacy *string* values ("10m") no longer parse —
    // this is an intentional breaking change; only numeric (ms) legacy values are kept.
    let toml = r#"
failed_worker_ttl = 600000
mount_update_ttl = 10000
block_conn_idle_time = 60000
"#;
    let conf: ClientConf =
        toml::from_str(toml).expect("legacy numeric time keys must deserialize via alias");
    assert_eq!(conf.failed_worker_ttl_ms, 600000);
    assert_eq!(conf.mount_update_ttl_ms, 10000);
    assert_eq!(conf.block_conn_idle_time_ms, 60000);
}

#[test]
fn toml_legacy_string_duration_is_rejected() {
    // The string->ms migration is intentionally breaking: a human-readable
    // duration like "10m" must NOT silently deserialize into a u64 ms field.
    // This locks the contract so we cannot regress into accepting duration
    // strings again (which would reintroduce the DurationUnit dependency).
    let toml = r#"
failed_worker_ttl = "10m"
"#;
    let result = toml::from_str::<ClientConf>(toml);
    assert!(
        result.is_err(),
        "string duration value must be rejected, got: {:?}",
        result.map(|c| c.failed_worker_ttl_ms)
    );
}

#[test]
fn client_cli_flags_parse_alongside_mount_io_threads() {
    #[derive(Debug, Parser)]
    struct MountLikeArgs {
        #[arg(long)]
        pub io_threads: Option<usize>,
    }

    #[derive(Debug, Parser)]
    struct FlattenHarness {
        #[command(flatten)]
        mount: MountLikeArgs,
        #[command(flatten)]
        client: ClientConfCliOverrides,
    }

    let parsed = FlattenHarness::try_parse_from([
        "curvine-fuse",
        "--io-threads",
        "4",
        "--client.io-threads",
        "9",
    ])
    .unwrap();

    assert_eq!(parsed.mount.io_threads, Some(4));
    assert_eq!(parsed.client.io_threads, Some(9));
}

#[test]
fn rejects_skipped_client_cli_fields() {
    let err = CliHarness::try_parse_from(["curvine-fuse", "--client.default-cache-ttl", "7d"])
        .unwrap_err();
    assert!(err.to_string().contains("default-cache-ttl"));
}

#[test]
fn parses_and_applies_octal_umask() {
    let parsed = CliHarness::try_parse_from(["curvine-fuse", "--client.umask", "022"]).unwrap();
    assert_eq!(parsed.overrides.umask.as_deref(), Some("022"));

    let mut conf = ClientConf::default();
    parsed.overrides.apply_to(&mut conf).unwrap();
    assert_eq!(conf.umask, 0o22);
}

#[test]
fn rejects_invalid_octal_umask() {
    let parsed = CliHarness::try_parse_from(["curvine-fuse", "--client.umask", "9"]).unwrap();
    let mut conf = ClientConf::default();
    let err = parsed.overrides.apply_to(&mut conf).unwrap_err();
    assert!(err.to_string().contains("umask"));
}
