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

//! Client-master version handshake and legacy fallback.
//!
//! During initialization the client reports its own structured version on the
//! `GetFilesystemInfo` request (`component_info`) and caches the master's
//! advertised version / protocol / capabilities from the `compatibility`
//! field of the response. A master that does not advertise a compatibility
//! contract is treated as a **legacy** peer: it carries no version metadata
//! and is never rejected, so existing clusters keep working untouched.

use curvine_proto::{
    CompatibilityModeProto, ComponentInfoProto, GetFilesystemInfoResponse,
    ServerCompatibilityInfoProto,
};

/// Cached outcome of the client-master version handshake.
#[derive(Clone, Debug)]
pub struct MasterHandshake {
    /// Whether the peer master is a legacy component (it did not advertise a
    /// compatibility contract). Legacy peers are never rejected; the client
    /// degrades to the pre-handshake behavior for them.
    pub legacy: bool,
    /// The compatibility contract advertised by the master, when present.
    pub compatibility: Option<ServerCompatibilityInfoProto>,
}

impl Default for MasterHandshake {
    fn default() -> Self {
        // Until a handshake completes (and against legacy masters) assume a
        // legacy peer so no component is ever rejected by default.
        Self::legacy()
    }
}

impl MasterHandshake {
    /// Handshake against a legacy master that did not advertise a
    /// compatibility contract.
    pub fn legacy() -> Self {
        Self {
            legacy: true,
            compatibility: None,
        }
    }

    /// Parse the handshake from a `GetFilesystemInfo` response. Absence of the
    /// `compatibility` field means the master is a legacy peer and the client
    /// must not reject it.
    pub fn from_response(rep: &GetFilesystemInfoResponse) -> Self {
        match &rep.compatibility {
            Some(compatibility) => Self {
                legacy: false,
                compatibility: Some(compatibility.clone()),
            },
            None => Self::legacy(),
        }
    }

    /// The master's structured component version, when advertised.
    pub fn master_version(&self) -> Option<&ComponentInfoProto> {
        self.compatibility.as_ref().map(|c| &c.server)
    }

    /// The protocol version the master speaks, when advertised.
    pub fn protocol_version(&self) -> Option<u32> {
        self.master_version().and_then(|v| v.protocol_version)
    }

    /// The lowest protocol version the master accepts, when advertised.
    pub fn min_protocol_version(&self) -> Option<u32> {
        self.master_version().and_then(|v| v.min_protocol_version)
    }

    /// Capabilities advertised by the master, when any.
    pub fn capabilities(&self) -> &[String] {
        self.master_version()
            .map(|v| v.capabilities.as_slice())
            .unwrap_or(&[])
    }

    /// Compatibility mode the master advertises. An unset/unknown mode is
    /// treated as `DIAGNOSE` (lenient) per the proto contract, so a handshake
    /// never rejects peers by default.
    pub fn compatibility_mode(&self) -> CompatibilityModeProto {
        match self.compatibility.as_ref().map(|c| c.compatibility_mode) {
            Some(mode) if mode == CompatibilityModeProto::Enforce as i32 => {
                CompatibilityModeProto::Enforce
            }
            // Diagnose, unset (0 / UNKNOWN) and unknown values stay lenient.
            _ => CompatibilityModeProto::Diagnose,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_proto::GetFilesystemInfoResponse;

    fn sample_component_info() -> ComponentInfoProto {
        ComponentInfoProto {
            component: Some("master".to_string()),
            release_version: Some("0.4.0-alpha".to_string()),
            git_commit: Some("359fce7d982a15f09c3b4e0b2e62fee4229609dd".to_string()),
            git_tag: Some("v0.4.0-alpha".to_string()),
            git_branch: Some("main".to_string()),
            protocol_version: Some(1),
            min_protocol_version: Some(1),
            capabilities: vec!["transfer".to_string(), "batch-write".to_string()],
        }
    }

    fn sample_compatibility() -> ServerCompatibilityInfoProto {
        ServerCompatibilityInfoProto {
            server: sample_component_info(),
            min_worker_version: None,
            min_client_version: None,
            compatibility_mode: CompatibilityModeProto::Diagnose as i32,
            blocked_versions: vec![],
        }
    }

    fn response_with_compatibility() -> GetFilesystemInfoResponse {
        GetFilesystemInfoResponse {
            active_master: "master-0".to_string(),
            compatibility: Some(sample_compatibility()),
            ..Default::default()
        }
    }

    fn legacy_response() -> GetFilesystemInfoResponse {
        GetFilesystemInfoResponse {
            active_master: "old-master".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn default_handshake_is_legacy_and_never_rejects() {
        // Before any handshake the client must assume a legacy peer: nothing
        // is rejected by default.
        let hs = MasterHandshake::default();
        assert!(hs.legacy);
        assert!(hs.compatibility.is_none());
        assert_eq!(hs.compatibility_mode(), CompatibilityModeProto::Diagnose);
    }

    #[test]
    fn new_master_response_is_cached_as_non_legacy() {
        let hs = MasterHandshake::from_response(&response_with_compatibility());

        assert!(!hs.legacy);
        let compat = hs
            .compatibility
            .as_ref()
            .expect("compatibility must be cached");
        assert_eq!(compat.server.component.as_deref(), Some("master"));
        assert_eq!(hs.compatibility_mode(), CompatibilityModeProto::Diagnose);
    }

    #[test]
    fn master_version_protocol_and_capabilities_are_exposed() {
        let hs = MasterHandshake::from_response(&response_with_compatibility());

        assert_eq!(hs.protocol_version(), Some(1));
        assert_eq!(hs.min_protocol_version(), Some(1));
        assert_eq!(
            hs.capabilities(),
            &["transfer".to_string(), "batch-write".to_string()]
        );
        assert_eq!(
            hs.master_version()
                .and_then(|v| v.release_version.as_deref()),
            Some("0.4.0-alpha")
        );
    }

    #[test]
    fn legacy_master_response_is_legacy_without_metadata() {
        // Old master + new client: the response has no compatibility field, so
        // the client treats the peer as legacy and exposes no version data.
        let hs = MasterHandshake::from_response(&legacy_response());

        assert!(hs.legacy);
        assert!(hs.compatibility.is_none());
        assert!(hs.master_version().is_none());
        assert_eq!(hs.protocol_version(), None);
        assert_eq!(hs.min_protocol_version(), None);
        assert!(hs.capabilities().is_empty());
        assert_eq!(hs.compatibility_mode(), CompatibilityModeProto::Diagnose);
    }

    #[test]
    fn unknown_compatibility_mode_is_treated_as_diagnose() {
        // An unset (0 / UNKNOWN) or invalid mode must stay lenient: the client
        // never fails closed on a master it does not understand.
        let mut compat = sample_compatibility();
        compat.compatibility_mode = 0;
        let hs = MasterHandshake {
            legacy: false,
            compatibility: Some(compat),
        };
        assert_eq!(hs.compatibility_mode(), CompatibilityModeProto::Diagnose);

        let mut compat = sample_compatibility();
        compat.compatibility_mode = 99;
        let hs = MasterHandshake {
            legacy: false,
            compatibility: Some(compat),
        };
        assert_eq!(hs.compatibility_mode(), CompatibilityModeProto::Diagnose);
    }
}
