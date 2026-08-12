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

#![allow(unused)]

use curvine_tests::{BenchArgs, CurvineBench};

fn run_bench(action: &str, dir: &str) {
    let args = BenchArgs {
        action: action.to_string(),
        dir: dir.to_string(),
        conf: "../etc/curvine-cluster.toml".to_string(),
        client_threads: 2,
        file_num: 10,
        file_size: "10MB".to_string(),
        buf_size: "64KB".to_string(),
        checksum: true,
        ..Default::default()
    };

    let bench = CurvineBench::new(args);
    bench.run().unwrap();
}

// #[test]
fn fuse() {
    run_bench("fuse.write", "../target/bench");
    run_bench("fuse.read", "../target/bench");
}

// #[test]
fn fs() {
    run_bench("fs.write", "/bench");
    run_bench("fs.read", "/bench");
}
