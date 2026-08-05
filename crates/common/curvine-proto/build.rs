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

use std::path::PathBuf;
use std::{env, fs};

fn main() {
    // Keep a single source of truth for non-raft protos under curvine-proto/proto
    // so Rust, Java, and Python SDK generation stay in sync.
    let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");
    let proto_files = [
        "common.proto",
        "master.proto",
        "worker.proto",
        "job.proto",
        "transfer.proto",
        "mount.proto",
        "replication.proto",
    ];

    for name in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_dir.join(name).display());
    }

    let base = env::var("OUT_DIR").unwrap_or_else(|_| ".".to_string());
    let output = format!("{}/protos", base);
    fs::create_dir_all(&output).unwrap();

    let mut build = prost_build::Config::new();
    build.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    build
        .out_dir(&output)
        .compile_protos(&proto_files, &[proto_dir])
        .unwrap();
}
