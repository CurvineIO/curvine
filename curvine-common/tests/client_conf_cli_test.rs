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
    };
    overrides.apply_to(&mut conf).unwrap();
    conf.init().unwrap();

    assert_eq!(conf.io_threads, 16);
    assert_eq!(conf.worker_threads, 32);
    assert_eq!(conf.block_size_str, "64KB");
    assert_eq!(conf.read_parallel, 2);
    assert_eq!(conf.short_circuit, false);
}

#[test]
fn rejects_unannotated_field_in_opt_in_mode() {
    let err =
        CliHarness::try_parse_from(["curvine-fuse", "--client.hostname", "host1"]).unwrap_err();
    assert!(err.to_string().contains("hostname"));
}
