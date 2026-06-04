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
        "--client.rpc-timeout-ms",
        "60000",
        "--client.conn-timeout-ms",
        "30000",
        "--client.rpc-close-idle",
        "false",
        "--client.master-conn-pool-size",
        "5",
    ])
    .unwrap();

    assert_eq!(parsed.overrides.rpc_timeout_ms, Some(60000));
    assert_eq!(parsed.overrides.conn_timeout_ms, Some(30000));
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
        pipeline_timeout_ms: Some(90_000),
        ..Default::default()
    };
    overrides.apply_to(&mut conf).unwrap();

    assert_eq!(conf.conn_retry_max_duration_ms, 120_000);
    assert_eq!(conf.rpc_retry_min_sleep_ms, 200);
    assert_eq!(conf.data_timeout_ms, 90_000);
    assert_eq!(conf.pipeline_timeout_ms, 90_000);
}

#[test]
fn rejects_unannotated_field_in_opt_in_mode() {
    let err =
        CliHarness::try_parse_from(["curvine-fuse", "--client.hostname", "host1"]).unwrap_err();
    assert!(err.to_string().contains("hostname"));
}
