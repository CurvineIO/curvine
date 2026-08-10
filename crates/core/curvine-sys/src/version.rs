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

// Include the version constants generated at build time
include!(concat!(env!("OUT_DIR"), "/version.rs"));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_constants_are_available() {
        assert!(!PKG_VERSION.is_empty());
        assert!(!GIT_VERSION.is_empty());
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn build_version_override_is_honored_when_set() {
        if let Ok(build_version) = std::env::var("BUILD_VERSION") {
            if build_version.is_empty() {
                return;
            }
            assert_eq!(PKG_VERSION, build_version);
            assert_eq!(VERSION, build_version);
        }
    }
}
