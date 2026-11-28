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

use std::env;

fn main() {
    // A build script runs on the host, so `#[cfg(target_os)]` here would
    // describe the host rather than the crate being built. Cargo exposes the
    // real target through `CARGO_CFG_*`.
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os != "linux" {
        return;
    }

    let fuse2 = env::var_os("CARGO_FEATURE_FUSE2").is_some();
    let fuse3 = env::var_os("CARGO_FEATURE_FUSE3").is_some();

    let (name, version) = match (fuse2, fuse3) {
        (true, false) => ("fuse", "2.6.0"),
        (false, true) => ("fuse3", "3.0.0"),
        (true, true) => panic!(
            "features fuse2 and fuse3 are mutually exclusive; \
             build with --no-default-features --features fuse2"
        ),
        (false, false) => panic!("one of features fuse2 or fuse3 must be enabled"),
    };

    pkg_config::Config::new()
        .atleast_version(version)
        .probe(name)
        .unwrap_or_else(|err| {
            panic!(
                "failed to find {} >= {} via pkg-config: {}",
                name, version, err
            )
        });
}
