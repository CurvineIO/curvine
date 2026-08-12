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

use std::{env, fs};

fn main() {
    let proto_files = ["proto/raft.proto", "proto/eraftpb.proto"];
    for path in &proto_files {
        println!("cargo:rerun-if-changed={path}");
    }

    let base = env::var("OUT_DIR").unwrap_or_else(|_| ".".to_string());
    let output = format!("{}/protos", base);
    fs::create_dir_all(&output).unwrap();

    let src = vec!["raft.proto"];
    let mut build = prost_build::Config::new();
    build
        .out_dir(&output)
        .extern_path(".eraftpb", "::raft::eraftpb")
        .compile_protos(&src, &["proto/", ""])
        .unwrap();
}
