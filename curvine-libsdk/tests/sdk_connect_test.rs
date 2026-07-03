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

#![cfg(feature = "rust-sdk")]

use curvine_libsdk::lib_curvine::{ConnectOptions, LibCurvine, LibCurvineBuilder};

#[test]
fn connect_options_master_addrs_roundtrip() {
    let opts = ConnectOptions::MasterAddrs(vec!["127.0.0.1:8995".to_string()]);
    match opts {
        ConnectOptions::MasterAddrs(addrs) => assert_eq!(addrs[0], "127.0.0.1:8995"),
        _ => panic!("expected MasterAddrs"),
    }
}

#[test]
fn builder_requires_conf_or_masters() {
    let result = LibCurvineBuilder::new().connect();
    assert!(result.is_err());
    let err = result.err().expect("error");
    assert!(err.to_string().contains("conf_file or masters"));
}

#[test]
fn builder_rejects_conf_and_masters() {
    let result = LibCurvineBuilder::new()
        .conf_file("/tmp/cluster.toml")
        .masters(["127.0.0.1:8995"])
        .connect();
    assert!(result.is_err());
    let err = result.err().expect("error");
    assert!(err.to_string().contains("not both"));
}

#[test]
fn builder_accepts_master_addrs() {
    let built = LibCurvineBuilder::new()
        .masters(["127.0.0.1:8995"])
        .rpc_timeout_ms(42);
    let _ = built;
}
