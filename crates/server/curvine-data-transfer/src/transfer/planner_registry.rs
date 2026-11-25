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

use super::planner::TransferPlannerPlugin;
use curvine_error::{FsError, FsResult};
use curvine_model::TransferKind;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Compile-time registry of planners keyed by the persisted transfer kind.
///
/// Registration is explicit and duplicate kinds are rejected, so adding a
/// planner cannot silently replace the implementation of an existing kind.
#[derive(Default)]
pub struct PlannerRegistry {
    planners: HashMap<TransferKind, Arc<dyn TransferPlannerPlugin>>,
}

impl PlannerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a planner for every kind it declares.
    ///
    /// The check runs over all declared kinds first, so a rejected registration
    /// never leaves the registry partially populated.
    pub fn register<P>(&mut self, planner: P) -> FsResult<()>
    where
        P: TransferPlannerPlugin + 'static,
    {
        let kinds = planner.kinds();
        if kinds.is_empty() {
            return Err(FsError::common(
                "transfer planner must declare at least one kind",
            ));
        }
        let mut seen = HashSet::new();
        for &kind in kinds {
            if !seen.insert(kind) {
                return Err(FsError::common(format!(
                    "transfer planner declares duplicate kind {kind:?}"
                )));
            }
            if self.planners.contains_key(&kind) {
                return Err(FsError::common(format!(
                    "transfer planner already registered for kind {kind:?}"
                )));
            }
        }
        let planner: Arc<dyn TransferPlannerPlugin> = Arc::new(planner);
        for kind in kinds {
            self.planners.insert(*kind, planner.clone());
        }
        Ok(())
    }

    /// Look up the planner for `kind`.
    ///
    /// An unknown kind is a permanent configuration error: it is returned as an
    /// ordinary error rather than being routed to a default implementation, so
    /// a persisted kind can never be planned by the wrong code path.
    pub fn get(&self, kind: TransferKind) -> FsResult<Arc<dyn TransferPlannerPlugin>> {
        self.planners.get(&kind).cloned().ok_or_else(|| {
            FsError::common(format!(
                "no transfer planner registered for kind {:?}",
                kind
            ))
        })
    }

    pub fn contains(&self, kind: TransferKind) -> bool {
        self.planners.contains_key(&kind)
    }

    /// Declared kinds in a deterministic order.
    pub fn kinds(&self) -> Vec<TransferKind> {
        let mut kinds: Vec<_> = self.planners.keys().copied().collect();
        kinds.sort_by_key(|kind| *kind as i32);
        kinds
    }

    pub fn len(&self) -> usize {
        self.planners.len()
    }

    pub fn is_empty(&self) -> bool {
        self.planners.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfer::planner::{LoadExportPlanner, PlannedTransfer};
    use curvine_model::{MountInfo, TransferJobInfo, TransferJobRecord};

    struct TestPlanner(&'static [TransferKind]);

    #[async_trait::async_trait]
    impl TransferPlannerPlugin for TestPlanner {
        fn kinds(&self) -> &'static [TransferKind] {
            self.0
        }

        async fn plan(&self, _job: &TransferJobRecord) -> FsResult<PlannedTransfer> {
            unreachable!("registry tests never execute a planner")
        }

        fn transfer_job_info(
            &self,
            _job: &TransferJobRecord,
            _mount: &MountInfo,
        ) -> TransferJobInfo {
            unreachable!("registry tests never execute a planner")
        }
    }

    #[test]
    fn registry_unknown_kind_is_error() {
        let registry = PlannerRegistry::new();
        let err = match registry.get(TransferKind::Load) {
            Ok(_) => panic!("an empty registry must reject every kind"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("no transfer planner registered"));
        assert!(registry.get(TransferKind::Export).is_err());
    }

    #[test]
    fn registry_rejects_duplicate_kind_without_partial_install() {
        let mut registry = PlannerRegistry::new();
        registry
            .register(TestPlanner(&[TransferKind::Load]))
            .unwrap();
        let err = registry
            .register(TestPlanner(&[TransferKind::Export, TransferKind::Load]))
            .unwrap_err();
        assert!(err.to_string().contains("already registered"));
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.kinds(), vec![TransferKind::Load]);
    }

    #[test]
    fn registry_rejects_empty_and_repeated_kinds() {
        let mut registry = PlannerRegistry::new();
        for kinds in [&[][..], &[TransferKind::Load, TransferKind::Load]] {
            assert!(registry.register(TestPlanner(kinds)).is_err());
            assert!(registry.is_empty());
        }
    }

    #[test]
    fn registry_lists_declared_kinds_in_order() {
        let mut registry = PlannerRegistry::new();
        registry
            .register(TestPlanner(&[TransferKind::Load]))
            .unwrap();
        assert!(registry.contains(TransferKind::Load));
        assert!(!registry.contains(TransferKind::Export));
        assert_eq!(registry.kinds(), vec![TransferKind::Load]);
    }

    #[test]
    fn load_export_planner_declares_both_legacy_kinds() {
        assert_eq!(
            LoadExportPlanner::KINDS,
            &[TransferKind::Load, TransferKind::Export]
        );
    }
}
