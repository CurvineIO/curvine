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

//! Compatibility checker and diagnose/enforce policy.
//!
//! A server (master or worker) evaluates a peer's structured version report
//! (`ComponentInfoProto`) against its own compatibility contract:
//!
//! - **protocol layer**: `server.min_protocol_version <= peer.protocol_version
//!   <= server.protocol_version`;
//! - **version layer**: the peer's release version must not be older than the
//!   configured minimum for its role (`min_worker_version` / `min_client_version`);
//! - **capability layer**: a feature is enabled only when both peers declare it;
//! - **blocked versions**: an explicit operator backstop that always rejects.
//!
//! Enforcement mode:
//! - `diagnose` (the default): record a warning and allow the request, so old
//!   components are never rejected without explicit configuration;
//! - `enforce`: reject incompatible requests with an explicit error. Only
//!   reached when the operator explicitly sets `mode = "enforce"`.

use curvine_proto::{CompatibilityModeProto, ComponentInfoProto};
use curvine_sys::version::ReleaseVersion;

/// Compatibility enforcement mode of a server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompatibilityMode {
    /// Record warnings and allow the request (default, lenient).
    #[default]
    Diagnose,
    /// Reject incompatible requests with an explicit error.
    Enforce,
}

impl CompatibilityMode {
    /// Parse a mode from its config string (`"diagnose"` / `"enforce"`).
    /// Unknown values return `None` so callers can fall back to the lenient
    /// default instead of failing closed.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "diagnose" => Some(Self::Diagnose),
            "enforce" => Some(Self::Enforce),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Diagnose => "diagnose",
            Self::Enforce => "enforce",
        }
    }

    /// Map a wire mode to a Rust mode. `UNKNOWN` and any unrecognized value
    /// stay lenient (`Diagnose`) per the proto contract, so a peer never fails
    /// closed on a mode it does not understand.
    pub fn from_proto(mode: i32) -> Self {
        if mode == CompatibilityModeProto::Enforce as i32 {
            Self::Enforce
        } else {
            Self::Diagnose
        }
    }

    pub fn to_proto(self) -> CompatibilityModeProto {
        match self {
            Self::Diagnose => CompatibilityModeProto::Diagnose,
            Self::Enforce => CompatibilityModeProto::Enforce,
        }
    }
}

/// Verdict of a compatibility check against a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompatibilityVerdict {
    /// Peer is fully compatible.
    Compatible,
    /// Peer sent no structured component info at all (legacy peer). Allowed in
    /// diagnose mode; rejected in enforce mode.
    MissingInfo,
    /// Peer's release version is explicitly blocked by the operator. Always
    /// rejected regardless of mode (explicit ops backstop).
    Blocked(String),
    /// Peer's protocol version is outside the server's supported range.
    ProtocolMismatch { peer: u32, min: u32, max: u32 },
    /// Peer's release version is older than the configured minimum for its
    /// role.
    VersionTooOld { peer: String, min: String },
    /// Peer sent `component_info` but its release version is missing, empty or
    /// unparseable, so it cannot be verified against a configured minimum.
    /// Allowed in diagnose mode; rejected in enforce mode.
    VersionUnknown { peer: String },
}

impl CompatibilityVerdict {
    pub fn is_compatible(&self) -> bool {
        matches!(self, Self::Compatible)
    }

    /// Whether a request from this peer must be rejected under the given mode.
    ///
    /// Only blocked versions reject unconditionally: they are an explicit
    /// operator emergency backstop. All other incompatibilities (missing info,
    /// protocol mismatch, too-old version) are allowed in `diagnose` mode and
    /// rejected only in `enforce` mode.
    pub fn rejects(&self, mode: CompatibilityMode) -> bool {
        match self {
            Self::Compatible => false,
            Self::Blocked(_) => true,
            Self::MissingInfo
            | Self::ProtocolMismatch { .. }
            | Self::VersionTooOld { .. }
            | Self::VersionUnknown { .. } => mode == CompatibilityMode::Enforce,
        }
    }

    /// Human-readable description used for warnings and error messages.
    pub fn describe(&self) -> String {
        match self {
            Self::Compatible => "compatible".to_string(),
            Self::MissingInfo => "peer reported no component version info (legacy)".to_string(),
            Self::Blocked(version) => {
                format!("release version {version} is blocked by compatibility policy")
            }
            Self::ProtocolMismatch { peer, min, max } => {
                format!("protocol version {peer} outside supported range [{min}, {max}]")
            }
            Self::VersionTooOld { peer, min } => {
                format!("release version {peer} is older than the minimum supported {min}")
            }
            Self::VersionUnknown { peer } => {
                if peer.is_empty() {
                    "peer release version is missing and cannot be verified".to_string()
                } else {
                    format!("release version {peer} cannot be parsed and verified")
                }
            }
        }
    }
}

/// Server-side compatibility contract used to evaluate peers.
#[derive(Debug, Clone)]
pub struct CompatibilityPolicy {
    pub mode: CompatibilityMode,
    /// Highest protocol version this server speaks.
    pub protocol_version: u32,
    /// Lowest protocol version this server accepts.
    pub min_protocol_version: u32,
    /// Lowest worker release version accepted (master side). `None` = not
    /// enforced, so old components are never rejected by default.
    pub min_worker_version: Option<ReleaseVersion>,
    /// Lowest client release version accepted (master/worker side). `None` =
    /// not enforced.
    pub min_client_version: Option<ReleaseVersion>,
    /// Release versions explicitly rejected regardless of mode.
    pub blocked_versions: Vec<ReleaseVersion>,
}

impl Default for CompatibilityPolicy {
    fn default() -> Self {
        Self {
            mode: CompatibilityMode::Diagnose,
            protocol_version: curvine_sys::version::PROTOCOL_VERSION,
            min_protocol_version: curvine_sys::version::MIN_PROTOCOL_VERSION,
            min_worker_version: None,
            min_client_version: None,
            blocked_versions: vec![],
        }
    }
}

impl CompatibilityPolicy {
    /// Whether evaluating a peer that reported the given component info can
    /// have any effect on this policy.
    ///
    /// Returns `false` only in diagnose mode with no configured version bounds
    /// and no blocklist and no peer component info: every such peer is allowed
    /// (Compatible, or MissingInfo that diagnose permits), so the evaluation
    /// would only emit warnings. Hot-path callers (statfs-backed
    /// GetFilesystemInfo, frequent heartbeats) use this to skip evaluation and
    /// avoid log spam and avoidable overhead.
    pub fn should_evaluate(&self, has_component_info: bool) -> bool {
        if self.mode == CompatibilityMode::Enforce {
            return true;
        }
        // Diagnose mode: the evaluation is actionable only when the operator
        // configured version bounds / a blocklist, or the peer actually
        // reported component info that could be incompatible.
        has_component_info
            || self.min_worker_version.is_some()
            || self.min_client_version.is_some()
            || !self.blocked_versions.is_empty()
    }

    /// Check a worker peer (master side).
    pub fn check_worker(&self, peer: Option<&ComponentInfoProto>) -> CompatibilityVerdict {
        self.check(peer, self.min_worker_version.as_ref())
    }

    /// Check a client peer (master side).
    pub fn check_client(&self, peer: Option<&ComponentInfoProto>) -> CompatibilityVerdict {
        self.check(peer, self.min_client_version.as_ref())
    }

    fn check(
        &self,
        peer: Option<&ComponentInfoProto>,
        min_version: Option<&ReleaseVersion>,
    ) -> CompatibilityVerdict {
        let Some(peer) = peer else {
            return CompatibilityVerdict::MissingInfo;
        };

        // 1. Blocked versions: exact release version match, always rejected.
        if let Some(version) = peer.release_version.as_deref() {
            if let Ok(parsed) = ReleaseVersion::parse(version) {
                if self.blocked_versions.contains(&parsed) {
                    return CompatibilityVerdict::Blocked(version.to_string());
                }
            }
        }

        // 2. Protocol version: server.min_protocol_version <= peer.protocol_version
        //    <= server.protocol_version.
        let peer_protocol = peer.protocol_version.unwrap_or(1);
        if peer_protocol < self.min_protocol_version || peer_protocol > self.protocol_version {
            return CompatibilityVerdict::ProtocolMismatch {
                peer: peer_protocol,
                min: self.min_protocol_version,
                max: self.protocol_version,
            };
        }

        // 3. Version range: peer.release_version >= min (when configured).
        //    A peer that sends component_info without a parseable release
        //    version cannot be verified against a configured minimum, so it is
        //    treated as unknown rather than Compatible: diagnose allows it
        //    (with a warning), enforce rejects it. This prevents a peer from
        //    bypassing minimum-version enforcement by omitting its version.
        if let Some(min) = min_version {
            match peer.release_version.as_deref() {
                None | Some("") => {
                    return CompatibilityVerdict::VersionUnknown {
                        peer: String::new(),
                    };
                }
                Some(version) => match ReleaseVersion::parse(version) {
                    Ok(parsed) if parsed < *min => {
                        return CompatibilityVerdict::VersionTooOld {
                            peer: version.to_string(),
                            min: min.to_string(),
                        };
                    }
                    Ok(_) => {}
                    Err(_) => {
                        return CompatibilityVerdict::VersionUnknown {
                            peer: version.to_string(),
                        };
                    }
                },
            }
        }

        CompatibilityVerdict::Compatible
    }
}

/// A feature is enabled only when **both** the server and the peer declare the
/// capability. Capabilities are finer-grained than versions and take priority
/// for feature negotiation (short-circuit, batch-write, transfer, ...).
pub fn feature_enabled(
    server_capabilities: &[String],
    peer_capabilities: &[String],
    feature: &str,
) -> bool {
    server_capabilities.iter().any(|c| c == feature)
        && peer_capabilities.iter().any(|c| c == feature)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_worker_info() -> ComponentInfoProto {
        ComponentInfoProto {
            component: Some("worker".to_string()),
            release_version: Some("0.4.0-alpha".to_string()),
            git_commit: Some("24c848719b5b4fea74519d91cbe462bb49761b36".to_string()),
            git_tag: Some("v0.4.0-alpha".to_string()),
            git_branch: Some("main".to_string()),
            protocol_version: Some(1),
            min_protocol_version: Some(1),
            capabilities: vec!["transfer".to_string(), "batch-write".to_string()],
        }
    }

    fn version(v: &str) -> ReleaseVersion {
        ReleaseVersion::parse(v).unwrap()
    }

    fn policy() -> CompatibilityPolicy {
        CompatibilityPolicy {
            mode: CompatibilityMode::Diagnose,
            protocol_version: 1,
            min_protocol_version: 1,
            min_worker_version: Some(version("0.2.0")),
            min_client_version: Some(version("0.2.0")),
            blocked_versions: vec![version("0.2.5")],
        }
    }

    #[test]
    fn default_policy_is_lenient_and_never_rejects_old_components() {
        // Acceptance: without explicit config, old components are not rejected.
        let policy = CompatibilityPolicy::default();
        let old_worker = ComponentInfoProto {
            release_version: Some("0.1.0".to_string()),
            protocol_version: Some(1),
            ..sample_worker_info()
        };

        let verdict = policy.check_worker(Some(&old_worker));
        assert_eq!(verdict, CompatibilityVerdict::Compatible);
        assert!(!verdict.rejects(policy.mode));
        // Enforce is only reachable with explicit config; even then the default
        // policy carries no bounds, so nothing is rejected.
        assert!(!verdict.rejects(CompatibilityMode::Enforce));
    }

    #[test]
    fn compatible_worker_within_range_passes() {
        let policy = policy();
        let verdict = policy.check_worker(Some(&sample_worker_info()));
        assert_eq!(verdict, CompatibilityVerdict::Compatible);
        assert!(!verdict.rejects(policy.mode));
        assert!(!verdict.rejects(CompatibilityMode::Enforce));
    }

    #[test]
    fn incompatible_protocol_version_is_rejected_in_enforce_only() {
        let policy = policy();
        let peer = ComponentInfoProto {
            release_version: Some("0.4.0".to_string()),
            protocol_version: Some(2),
            ..sample_worker_info()
        };

        let verdict = policy.check_worker(Some(&peer));
        assert_eq!(
            verdict,
            CompatibilityVerdict::ProtocolMismatch {
                peer: 2,
                min: 1,
                max: 1
            }
        );
        // diagnose records a warning and allows.
        assert!(!verdict.rejects(CompatibilityMode::Diagnose));
        // enforce rejects with the explicit reason.
        assert!(verdict.rejects(CompatibilityMode::Enforce));
        assert!(verdict.describe().contains("protocol version 2"));
    }

    #[test]
    fn too_old_worker_version_is_rejected_in_enforce_only() {
        let policy = policy();
        let peer = ComponentInfoProto {
            release_version: Some("0.1.5".to_string()),
            ..sample_worker_info()
        };

        let verdict = policy.check_worker(Some(&peer));
        assert_eq!(
            verdict,
            CompatibilityVerdict::VersionTooOld {
                peer: "0.1.5".to_string(),
                min: "0.2.0".to_string()
            }
        );
        assert!(!verdict.rejects(CompatibilityMode::Diagnose));
        assert!(verdict.rejects(CompatibilityMode::Enforce));
    }

    #[test]
    fn missing_info_is_allowed_in_diagnose_and_rejected_in_enforce() {
        let policy = policy();
        // A legacy worker sends no component_info at all.
        let verdict = policy.check_worker(None);
        assert_eq!(verdict, CompatibilityVerdict::MissingInfo);
        assert!(!verdict.rejects(CompatibilityMode::Diagnose));
        assert!(verdict.rejects(CompatibilityMode::Enforce));
    }

    #[test]
    fn blocked_version_is_always_rejected_regardless_of_mode() {
        let policy = policy();
        let peer = ComponentInfoProto {
            release_version: Some("0.2.5".to_string()),
            ..sample_worker_info()
        };

        let verdict = policy.check_worker(Some(&peer));
        assert_eq!(verdict, CompatibilityVerdict::Blocked("0.2.5".to_string()));
        // Explicit ops backstop: rejects even in diagnose mode.
        assert!(verdict.rejects(CompatibilityMode::Diagnose));
        assert!(verdict.rejects(CompatibilityMode::Enforce));
    }

    #[test]
    fn min_client_version_applies_to_client_peers() {
        let policy = policy();
        let old_client = ComponentInfoProto {
            component: Some("client".to_string()),
            release_version: Some("0.1.0".to_string()),
            ..sample_worker_info()
        };

        let verdict = policy.check_client(Some(&old_client));
        assert_eq!(
            verdict,
            CompatibilityVerdict::VersionTooOld {
                peer: "0.1.0".to_string(),
                min: "0.2.0".to_string()
            }
        );
        assert!(verdict.rejects(CompatibilityMode::Enforce));
    }

    #[test]
    fn unparseable_peer_version_is_unknown_when_min_configured() {
        // With a minimum version configured, an unparseable release version
        // cannot be verified and must not fall through to Compatible, or a
        // peer could bypass minimum-version enforcement by sending a bogus
        // version. diagnose allows it (warning); enforce rejects it.
        let policy = policy(); // min_worker_version = 0.2.0
        let peer = ComponentInfoProto {
            release_version: Some("not-a-version".to_string()),
            protocol_version: Some(1),
            ..sample_worker_info()
        };

        let verdict = policy.check_worker(Some(&peer));
        assert_eq!(
            verdict,
            CompatibilityVerdict::VersionUnknown {
                peer: "not-a-version".to_string()
            }
        );
        assert!(!verdict.rejects(CompatibilityMode::Diagnose));
        assert!(verdict.rejects(CompatibilityMode::Enforce));
    }

    #[test]
    fn missing_release_version_is_unknown_when_min_configured() {
        // A peer that omits release_version entirely (None or empty) also
        // cannot be verified against a configured minimum.
        let policy = policy();
        for release_version in [None, Some(String::new())] {
            let peer = ComponentInfoProto {
                release_version,
                protocol_version: Some(1),
                ..sample_worker_info()
            };

            let verdict = policy.check_worker(Some(&peer));
            assert_eq!(
                verdict,
                CompatibilityVerdict::VersionUnknown {
                    peer: String::new()
                }
            );
            assert!(verdict.rejects(CompatibilityMode::Enforce));
        }
    }

    #[test]
    fn unknown_version_is_allowed_when_no_min_configured() {
        // Without an explicit minimum, an unparseable/missing release version
        // stays Compatible: old or unversioned components are never rejected
        // by default.
        let policy = CompatibilityPolicy {
            min_worker_version: None,
            ..Default::default()
        };
        for release_version in [Some("not-a-version".to_string()), None] {
            let peer = ComponentInfoProto {
                release_version,
                protocol_version: Some(1),
                ..sample_worker_info()
            };
            assert_eq!(
                policy.check_worker(Some(&peer)),
                CompatibilityVerdict::Compatible
            );
        }
    }

    #[test]
    fn mode_parsing_and_proto_round_trip() {
        assert_eq!(
            CompatibilityMode::parse("diagnose"),
            Some(CompatibilityMode::Diagnose)
        );
        assert_eq!(
            CompatibilityMode::parse("ENFORCE"),
            Some(CompatibilityMode::Enforce)
        );
        assert_eq!(CompatibilityMode::parse("bogus"), None);
        assert_eq!(CompatibilityMode::default(), CompatibilityMode::Diagnose);

        // UNKNOWN (0) and unrecognized values stay lenient.
        assert_eq!(
            CompatibilityMode::from_proto(0),
            CompatibilityMode::Diagnose
        );
        assert_eq!(
            CompatibilityMode::from_proto(99),
            CompatibilityMode::Diagnose
        );
        assert_eq!(CompatibilityMode::from_proto(2), CompatibilityMode::Enforce);

        for mode in [CompatibilityMode::Diagnose, CompatibilityMode::Enforce] {
            assert_eq!(CompatibilityMode::from_proto(mode.to_proto() as i32), mode);
        }
    }

    #[test]
    fn feature_enabled_requires_both_peers_to_declare_it() {
        let server = vec!["transfer".to_string(), "batch-write".to_string()];
        let peer = vec!["transfer".to_string()];

        assert!(feature_enabled(&server, &peer, "transfer"));
        assert!(!feature_enabled(&server, &peer, "batch-write"));
        assert!(!feature_enabled(&server, &peer, "short-circuit"));
        assert!(!feature_enabled(&server, &[], "transfer"));
        assert!(!feature_enabled(&[], &peer, "transfer"));
    }

    #[test]
    fn should_evaluate_skips_only_unactionable_diagnose_peers() {
        // Default diagnose policy with no bounds and no peer component info:
        // evaluation is unactionable (Compatible or MissingInfo that diagnose
        // allows), so hot paths skip it to avoid log spam.
        let default_policy = CompatibilityPolicy::default();
        assert!(!default_policy.should_evaluate(false));

        // Enforce mode always evaluates.
        let enforce = CompatibilityPolicy {
            mode: CompatibilityMode::Enforce,
            ..Default::default()
        };
        assert!(enforce.should_evaluate(false));

        // Component info present -> actionable.
        assert!(default_policy.should_evaluate(true));

        // Configured bounds / blocklist -> actionable even without component
        // info.
        let bounded = CompatibilityPolicy {
            min_worker_version: Some(version("0.2.0")),
            ..Default::default()
        };
        assert!(bounded.should_evaluate(false));

        let blocked = CompatibilityPolicy {
            blocked_versions: vec![version("0.2.5")],
            ..Default::default()
        };
        assert!(blocked.should_evaluate(false));
    }
}
