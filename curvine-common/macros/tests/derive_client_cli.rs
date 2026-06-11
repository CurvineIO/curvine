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
use curvine_common_macros::ClientCliArgs;
use serde::Deserialize;

#[derive(Debug, Default, Deserialize, ClientCliArgs)]
#[client_cli(prefix = "client", strip_suffix = "_str")]
struct SampleClientConf {
    #[client_cli(skip)]
    master_addrs: String,

    #[client_cli(long = "client.io-threads")]
    io_threads: usize,

    #[serde(alias = "block_size")]
    block_size_str: String,

    #[serde(alias = "hugepage", default)]
    hugepage_str: Option<String>,

    entry_timeout: f64,

    #[client_cli(octal)]
    file_mode: u32,

    #[serde(skip)]
    storage_type: String,
}

#[derive(Parser)]
struct CliHarness {
    #[command(flatten)]
    overrides: SampleClientConfCliOverrides,
}

#[test]
fn apply_to_updates_fields() {
    let mut conf = SampleClientConf {
        master_addrs: "ignored".to_string(),
        io_threads: 1,
        block_size_str: "1MB".to_string(),
        hugepage_str: None,
        entry_timeout: 1.0,
        file_mode: 0,
        storage_type: "memory".to_string(),
    };
    let overrides = SampleClientConfCliOverrides {
        io_threads: Some(8),
        block_size_str: Some("128MB".to_string()),
        hugepage_str: Some("/dev/hugepages".to_string()),
        entry_timeout: Some(2.5),
        ..Default::default()
    };
    overrides.apply_to(&mut conf).unwrap();
    assert_eq!(conf.master_addrs, "ignored");
    assert_eq!(conf.io_threads, 8);
    assert_eq!(conf.block_size_str, "128MB");
    assert_eq!(conf.hugepage_str.as_deref(), Some("/dev/hugepages"));
    assert_eq!(conf.entry_timeout, 2.5);
    assert_eq!(conf.storage_type, "memory");
}

#[test]
fn parses_client_cli_flags() {
    let parsed = CliHarness::try_parse_from([
        "curvine-fuse",
        "--client.io-threads",
        "8",
        "--client.block-size",
        "128MB",
        "--client.hugepage",
        "/dev/hugepages",
        "--client.entry-timeout",
        "2.5",
        "--client.file-mode",
        "0644",
    ])
    .unwrap();

    assert_eq!(parsed.overrides.io_threads, Some(8));
    assert_eq!(parsed.overrides.block_size_str.as_deref(), Some("128MB"));
    assert_eq!(
        parsed.overrides.hugepage_str.as_deref(),
        Some("/dev/hugepages")
    );
    assert_eq!(parsed.overrides.entry_timeout, Some(2.5));
    assert_eq!(parsed.overrides.file_mode.as_deref(), Some("0644"));
}

#[test]
fn apply_to_parses_octal_field() {
    let mut conf = SampleClientConf {
        master_addrs: "ignored".to_string(),
        io_threads: 1,
        block_size_str: "1MB".to_string(),
        hugepage_str: None,
        entry_timeout: 1.0,
        file_mode: 0,
        storage_type: "memory".to_string(),
    };
    SampleClientConfCliOverrides {
        file_mode: Some("0755".to_string()),
        ..Default::default()
    }
    .apply_to(&mut conf)
    .unwrap();
    assert_eq!(conf.file_mode, 0o755);
}
