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

//! Client-side worker pre-check (T10).
//!
//! Before the client opens a data connection to a worker, it pre-checks the
//! worker's structured version (`component_info`) that the master reported on
//! the `GetFilesystemInfo` handshake (`live_workers`):
//!
//! - **protocol layer**: the worker's `protocol_version` must fall inside the
//!   client's own supported range `[MIN_PROTOCOL_VERSION, PROTOCOL_VERSION]`;
//! - **version layer**: the worker's release version must not be older than
//!   the `min_worker_version` the master advertises (when the master reports
//!   one);
//! - **blocked versions**: the master-advertised blocklist is mirrored so a
//!   known-bad worker is surfaced immediately.
//!
//! The pre-check is **diagnose-only** by design (acceptance for T10): an
//! incompatible worker is logged (deduped per worker and per verdict change)
//! but never rejected. A worker whose version is unknown (legacy worker that
//! never reported `component_info`) is allowed to connect — enforcement is
//! left to the worker's own diagnose/enforce policy on the data plane.

use crate::file::MasterHandshake;
use curvine_model::WorkerInfo;
use curvine_model::{CompatibilityMode, CompatibilityPolicy, CompatibilityVerdict};
use curvine_runtime::sync::FastDashMap;
use curvine_sys::version::ReleaseVersion;
use log::warn;

/// Client-side worker pre-check policy and per-worker warning dedup state.
///
/// The policy is rebuilt whenever the client-master handshake refreshes: the
/// client's own protocol range is constant for the process, while the
/// `min_worker_version` / `blocked_versions` bounds come from the master's
/// advertised compatibility contract. The mode is always `diagnose`: the
/// pre-check never rejects, it only surfaces mismatches to the operator.
pub struct WorkerPrecheck {
    /// The diagnose-only evaluation policy.
    policy: CompatibilityPolicy,
    /// Per-worker (worker_id) last-warned verdict, so repeated connections to
    /// an incompatible worker warn on the first occurrence and again only when
    /// the verdict changes.
    warned: FastDashMap<u32, CompatibilityVerdict>,
}

impl Default for WorkerPrecheck {
    fn default() -> Self {
        Self {
            policy: Self::diagnose_policy(None, Vec::new()),
            warned: FastDashMap::default(),
        }
    }
}

impl WorkerPrecheck {
    /// Build the diagnose-only pre-check policy from the client's own protocol
    /// range plus the master-advertised worker bounds.
    fn diagnose_policy(
        min_worker_version: Option<ReleaseVersion>,
        blocked_versions: Vec<ReleaseVersion>,
    ) -> CompatibilityPolicy {
        CompatibilityPolicy {
            mode: CompatibilityMode::Diagnose,
            protocol_version: curvine_sys::version::PROTOCOL_VERSION,
            min_protocol_version: curvine_sys::version::MIN_PROTOCOL_VERSION,
            min_worker_version,
            min_client_version: None,
            blocked_versions,
        }
    }

    /// Refresh the pre-check bounds from the latest client-master handshake.
    /// A legacy master (no compatibility contract) resets the bounds to
    /// none/empty so nothing is flagged without an explicit contract.
    pub fn refresh(&mut self, handshake: &MasterHandshake) {
        let compat = handshake.compatibility();
        let min_worker_version = compat
            .and_then(|c| c.min_worker_version.as_deref())
            .and_then(|v| ReleaseVersion::parse(v).ok());
        let blocked_versions = compat
            .map(|c| {
                c.blocked_versions
                    .iter()
                    .filter_map(|v| ReleaseVersion::parse(v).ok())
                    .collect()
            })
            .unwrap_or_default();
        self.policy = Self::diagnose_policy(min_worker_version, blocked_versions);
    }

    /// Evaluate a worker's structured version against the pre-check policy.
    /// `None` (or a worker without `component_info`) means the worker version
    /// is unknown: allowed, since enforcement is left to the worker side.
    pub fn check_worker(&self, worker: &WorkerInfo) -> CompatibilityVerdict {
        self.policy.check_worker(worker.component_info.as_ref())
    }

    /// Whether evaluating a worker can produce a non-trivial verdict. Mirrors
    /// the master's hot-path avoidance: with no bounds/blocklist and a worker
    /// that reported no component info, the evaluation cannot change anything.
    pub fn should_evaluate(&self, worker: &WorkerInfo) -> bool {
        self.policy.should_evaluate(worker.component_info.is_some())
    }

    /// Run the pre-check for a worker and warn on the first occurrence of an
    /// incompatible verdict (and whenever the verdict changes). Never rejects:
    /// this is a diagnose-only pre-check.
    pub fn warn_if_incompatible(&self, worker: &WorkerInfo) {
        if !self.should_evaluate(worker) {
            return;
        }
        let verdict = self.check_worker(worker);
        if verdict.is_compatible() {
            // The worker is compatible again; forget any previous warning so a
            // future incompatibility is surfaced.
            self.warned.remove(&worker.worker_id());
            return;
        }
        let changed = self
            .warned
            .get(&worker.worker_id())
            .map(|last| *last != verdict)
            .unwrap_or(true);
        if changed {
            self.warned.insert(worker.worker_id(), verdict.clone());
            warn!(
                "worker {} pre-check: {} (worker {}:{})",
                worker.worker_id(),
                verdict.describe(),
                worker.address.hostname,
                worker.address.rpc_port
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::WorkerAddress;
    use curvine_proto::{ComponentInfoProto, ServerCompatibilityInfoProto};
    use curvine_sys::version::ReleaseVersion;

    fn worker_info(id: u32, component_info: Option<ComponentInfoProto>) -> WorkerInfo {
        WorkerInfo {
            address: WorkerAddress {
                worker_id: id,
                ip_addr: "127.0.0.1".to_string(),
                rpc_port: 6666,
                ..Default::default()
            },
            component_info,
            ..Default::default()
        }
    }

    fn worker_component_info(release_version: &str, protocol_version: u32) -> ComponentInfoProto {
        ComponentInfoProto {
            component: Some("worker".to_string()),
            release_version: Some(release_version.to_string()),
            git_commit: Some("abc".to_string()),
            git_tag: Some(String::new()),
            git_branch: Some("main".to_string()),
            protocol_version: Some(protocol_version),
            min_protocol_version: Some(protocol_version),
            capabilities: vec!["batch-write".to_string()],
        }
    }

    fn handshake_with(min_worker_version: Option<&str>, blocked: &[&str]) -> MasterHandshake {
        MasterHandshake::from_response(&curvine_proto::GetFilesystemInfoResponse {
            compatibility: Some(ServerCompatibilityInfoProto {
                server: worker_component_info("0.4.0-alpha", 1),
                min_worker_version: min_worker_version.map(|s| s.to_string()),
                min_client_version: None,
                compatibility_mode: 0,
                blocked_versions: blocked.iter().map(|s| s.to_string()).collect(),
            }),
            ..Default::default()
        })
    }

    #[test]
    fn default_precheck_never_flags_compatible_or_legacy_workers() {
        // Acceptance: with no explicit contract, nothing is rejected — a
        // compatible worker and a legacy worker both stay compatible.
        let precheck = WorkerPrecheck::default();

        let compatible = worker_info(1, Some(worker_component_info("0.2.0", 1)));
        assert_eq!(
            precheck.check_worker(&compatible),
            CompatibilityVerdict::Compatible
        );

        let legacy = worker_info(2, None);
        assert_eq!(
            precheck.check_worker(&legacy),
            CompatibilityVerdict::MissingInfo
        );
        // Diagnose-only: missing info never rejects.
        assert!(!precheck
            .check_worker(&legacy)
            .rejects(CompatibilityMode::Diagnose));
    }

    #[test]
    fn protocol_mismatch_worker_is_flagged_but_never_rejected() {
        // 新 worker + 旧 client (from the client's perspective): a worker
        // speaking protocol 2 against a client that only supports 1 is
        // ProtocolMismatch — warned, but the connection is still allowed.
        let precheck = WorkerPrecheck::default();
        let new_worker = worker_info(1, Some(worker_component_info("0.4.0", 2)));

        assert_eq!(
            precheck.check_worker(&new_worker),
            CompatibilityVerdict::ProtocolMismatch {
                peer: 2,
                min: 1,
                max: 1
            }
        );
        assert!(!precheck
            .check_worker(&new_worker)
            .rejects(CompatibilityMode::Diagnose));
    }

    #[test]
    fn master_min_worker_version_flags_old_worker() {
        // 旧 worker + 新 client: with the master advertising min_worker_version
        // = 0.2.0, an older worker (0.1.5) is flagged VersionTooOld but never
        // rejected by the pre-check.
        let mut precheck = WorkerPrecheck::default();
        precheck.refresh(&handshake_with(Some("0.2.0"), &[]));

        let old_worker = worker_info(1, Some(worker_component_info("0.1.5", 1)));
        assert_eq!(
            precheck.check_worker(&old_worker),
            CompatibilityVerdict::VersionTooOld {
                peer: "0.1.5".to_string(),
                min: "0.2.0".to_string()
            }
        );
        assert!(!precheck
            .check_worker(&old_worker)
            .rejects(CompatibilityMode::Diagnose));

        // An at-minimum worker passes.
        let ok_worker = worker_info(2, Some(worker_component_info("0.2.0", 1)));
        assert_eq!(
            precheck.check_worker(&ok_worker),
            CompatibilityVerdict::Compatible
        );
    }

    #[test]
    fn master_blocked_versions_are_flagged() {
        let mut precheck = WorkerPrecheck::default();
        precheck.refresh(&handshake_with(None, &["0.2.5"]));

        let blocked_worker = worker_info(1, Some(worker_component_info("0.2.5", 1)));
        assert_eq!(
            precheck.check_worker(&blocked_worker),
            CompatibilityVerdict::Blocked("0.2.5".to_string())
        );
        // The pre-check surfaces the blocked worker as a warning (it is
        // recorded for dedup), but never refuses the connection: the hook is
        // diagnose-only.
        precheck.warn_if_incompatible(&blocked_worker);
        assert!(precheck.warned.contains_key(&1));
    }

    #[test]
    fn refresh_resets_bounds_on_legacy_handshake() {
        // A legacy master (no compatibility contract) resets the bounds so no
        // worker is flagged.
        let mut precheck = WorkerPrecheck::default();
        precheck.refresh(&handshake_with(Some("0.2.0"), &["0.2.5"]));
        assert_eq!(
            precheck.check_worker(&worker_info(1, Some(worker_component_info("0.1.5", 1)))),
            CompatibilityVerdict::VersionTooOld {
                peer: "0.1.5".to_string(),
                min: "0.2.0".to_string()
            }
        );

        precheck.refresh(&MasterHandshake::legacy());
        assert_eq!(
            precheck.check_worker(&worker_info(1, Some(worker_component_info("0.1.5", 1)))),
            CompatibilityVerdict::Compatible
        );
    }

    #[test]
    fn legacy_worker_version_is_unknown_and_allowed() {
        // 旧 worker without component_info: unknown, allowed — enforcement is
        // left to the worker side (diagnose/enforce).
        let precheck = WorkerPrecheck::default();
        let legacy = worker_info(7, None);
        assert_eq!(
            precheck.check_worker(&legacy),
            CompatibilityVerdict::MissingInfo
        );
        assert!(!precheck.should_evaluate(&legacy));
    }

    #[test]
    fn warn_dedup_tracks_verdict_changes_per_worker() {
        // The dedup map records the last warned verdict; a compatible worker
        // clears its entry so a later incompatibility is surfaced again.
        let precheck = WorkerPrecheck::default();
        let worker = worker_info(1, Some(worker_component_info("0.4.0", 2)));

        // Incompatible (protocol mismatch) -> recorded.
        precheck.warn_if_incompatible(&worker);
        assert!(precheck.warned.contains_key(&1));

        // Same verdict again -> unchanged, no re-warn.
        precheck.warn_if_incompatible(&worker);
        assert_eq!(
            precheck.warned.get(&1).map(|v| v.clone()),
            Some(CompatibilityVerdict::ProtocolMismatch {
                peer: 2,
                min: 1,
                max: 1
            })
        );

        // Compatible worker -> entry cleared.
        let ok_worker = worker_info(1, Some(worker_component_info("0.4.0", 1)));
        precheck.warn_if_incompatible(&ok_worker);
        assert!(!precheck.warned.contains_key(&1));
    }

    #[test]
    fn release_version_bound_parsing_is_lenient() {
        // Unparseable master bounds degrade to none instead of failing closed.
        let mut precheck = WorkerPrecheck::default();
        let handshake = MasterHandshake::from_response(&curvine_proto::GetFilesystemInfoResponse {
            compatibility: Some(ServerCompatibilityInfoProto {
                server: worker_component_info("0.4.0-alpha", 1),
                min_worker_version: Some("not-a-version".to_string()),
                min_client_version: None,
                compatibility_mode: 0,
                blocked_versions: vec!["junk".to_string()],
            }),
            ..Default::default()
        });
        precheck.refresh(&handshake);
        assert_eq!(
            precheck.check_worker(&worker_info(1, Some(worker_component_info("0.1.5", 1)))),
            CompatibilityVerdict::Compatible
        );
    }

    #[test]
    fn release_version_ordering_matches_policy() {
        let v = |s: &str| ReleaseVersion::parse(s).unwrap();
        assert!(v("0.1.5") < v("0.2.0"));
        assert!(v("0.2.0") < v("0.4.0-alpha"));
    }
}
