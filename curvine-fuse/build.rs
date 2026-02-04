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

fn main() {
    // Skip FUSE library check when building statically.
    // curvine-fuse uses pure Rust FUSE implementation with direct syscalls,
    // it does NOT link against libfuse at runtime.
    #[cfg(all(target_os = "linux", not(feature = "static")))]
    {
        use pkg_config::Config;

        let (name, version) = if cfg!(feature = "fuse3") {
            ("fuse3", "3.0.0")
        } else {
            ("fuse", "2.6.0")
        };

        Config::new()
            .atleast_version(version)
            .probe(name)
            .map_err(|e| eprintln!("{}", e))
            .unwrap();
    }
}
