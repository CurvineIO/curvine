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

use serde::{Deserialize, Serialize};

/// Initial Curvine component protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Lowest Curvine component protocol version accepted by this build.
pub const MIN_PROTOCOL_VERSION: u32 = 1;

/// Structured version metadata emitted by every Curvine component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentVersion {
    pub component: String,
    pub release_version: String,
    pub git_commit: String,
    pub git_tag: String,
    pub git_branch: String,
    pub protocol_version: u32,
    pub min_protocol_version: u32,
    pub capabilities: Vec<String>,
}

impl ComponentVersion {
    pub fn new(component: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            release_version: PKG_VERSION.to_string(),
            git_commit: GIT_VERSION.to_string(),
            git_tag: GIT_TAG.to_string(),
            git_branch: GIT_BRANCH.to_string(),
            protocol_version: PROTOCOL_VERSION,
            min_protocol_version: MIN_PROTOCOL_VERSION,
            capabilities: Vec::new(),
        }
    }

    pub fn to_json_pretty(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }

    pub fn display_version(&self) -> String {
        let mut source = String::new();
        if !self.git_tag.is_empty() && self.git_tag != "unknown" {
            source = format!(", tag: {}", self.git_tag);
        } else if !self.git_branch.is_empty()
            && self.git_branch != "unknown"
            && self.git_branch != "HEAD"
        {
            source = format!(", branch: {}", self.git_branch);
        }

        format!(
            "{} (commit: {}{})",
            self.release_version, self.git_commit, source
        )
    }
}

pub fn component_version(component: impl Into<String>) -> ComponentVersion {
    ComponentVersion::new(component)
}

pub fn component_version_json(component: impl Into<String>) -> serde_json::Result<String> {
    component_version(component).to_json_pretty()
}

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

    #[test]
    fn component_version_json_uses_stable_schema() {
        let json = component_version_json("cli").unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["component"], "cli");
        assert_eq!(value["release_version"], PKG_VERSION);
        assert_eq!(value["git_commit"], GIT_VERSION);
        assert_eq!(value["git_tag"], GIT_TAG);
        assert_eq!(value["git_branch"], GIT_BRANCH);
        assert_eq!(value["protocol_version"], PROTOCOL_VERSION);
        assert_eq!(value["min_protocol_version"], MIN_PROTOCOL_VERSION);
        assert!(value["capabilities"].as_array().unwrap().is_empty());
    }

    #[test]
    fn component_version_display_prefers_tag_over_branch() {
        let version = ComponentVersion {
            component: "master".to_string(),
            release_version: "0.2.0".to_string(),
            git_commit: "abcdef1".to_string(),
            git_tag: "v0.2.0".to_string(),
            git_branch: "main".to_string(),
            protocol_version: PROTOCOL_VERSION,
            min_protocol_version: MIN_PROTOCOL_VERSION,
            capabilities: Vec::new(),
        };

        assert_eq!(
            version.display_version(),
            "0.2.0 (commit: abcdef1, tag: v0.2.0)"
        );
    }
}
