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

use super::JobPlugin;
use curvine_error::{FsError, FsResult};
use curvine_model::TransferKind;
use std::collections::HashMap;
use std::sync::Arc;

/// The worker's set of task plugins.
///
/// Registration is explicit and happens during worker construction. The
/// registry owns plugins behind `Arc` so the same plugin can be used by task
/// submission and capability reporting without imposing a concrete plugin type
/// on either caller.
#[derive(Default)]
pub struct JobRegistry {
    plugins: HashMap<TransferKind, Arc<dyn JobPlugin>>,
}

impl JobRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a plugin for its declared task kind.
    ///
    /// Duplicate registrations are rejected rather than silently replacing an
    /// existing plugin. This catches configuration mistakes during startup and
    /// keeps dispatch deterministic.
    pub fn register<P>(&mut self, plugin: P) -> FsResult<()>
    where
        P: JobPlugin + 'static,
    {
        let kind = plugin.task_kind();
        if self.plugins.contains_key(&kind) {
            return Err(FsError::common(format!(
                "job plugin already registered for transfer kind {:?}",
                kind
            )));
        }
        self.plugins.insert(kind, Arc::new(plugin));
        Ok(())
    }

    /// Return the plugin for `kind`.
    ///
    /// An unknown kind is a permanent dispatch/configuration error. It is
    /// intentionally represented by the ordinary error result and carries no
    /// retryable admission signal.
    pub fn get(&self, kind: TransferKind) -> FsResult<Arc<dyn JobPlugin>> {
        self.plugins.get(&kind).cloned().ok_or_else(|| {
            FsError::common(format!(
                "no job plugin registered for transfer kind {:?}",
                kind
            ))
        })
    }

    /// Return registered task kinds in deterministic order.
    ///
    /// `TransferKind` is a plain wire enum without a derived `Ord`, so the sort
    /// key is the stable wire value rather than declaration order.
    pub fn supported_kinds(&self) -> Vec<TransferKind> {
        let mut kinds: Vec<_> = self.plugins.keys().copied().collect();
        kinds.sort_by_key(|kind| i32::from(*kind));
        kinds
    }

    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    pub fn len(&self) -> usize {
        self.plugins.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::task::{JobExecutor, JobFuture, ResourceClass, TaskExecCtx, TaskOutcome};

    struct TestExecutor;

    impl JobExecutor for TestExecutor {
        fn run(self: Box<Self>, ctx: TaskExecCtx) -> JobFuture {
            Box::pin(async move {
                if ctx.is_cancelled() {
                    Ok(TaskOutcome::Canceled)
                } else {
                    Ok(TaskOutcome::Completed)
                }
            })
        }
    }

    struct TestPlugin {
        kind: TransferKind,
        name: &'static str,
    }

    impl JobPlugin for TestPlugin {
        fn task_kind(&self) -> TransferKind {
            self.kind
        }

        fn name(&self) -> &'static str {
            self.name
        }

        fn resource_class(&self) -> ResourceClass {
            ResourceClass::Cpu
        }

        fn create_executor(&self, _payload: &[u8]) -> FsResult<Box<dyn JobExecutor>> {
            Ok(Box::new(TestExecutor))
        }
    }

    fn plugin(kind: TransferKind, name: &'static str) -> TestPlugin {
        TestPlugin { kind, name }
    }

    #[test]
    fn owning_executor_observes_cancellation_after_construction() {
        use crate::worker::task::TaskContext;
        use curvine_model::{TransferJobInfo, TransferTaskInfo};

        let task = Arc::new(TaskContext::new(TransferTaskInfo {
            job: TransferJobInfo {
                job_id: "job".into(),
                source_path: String::new(),
                target_path: String::new(),
                block_size: 0,
                replicas: 1,
                storage_type: Default::default(),
                ttl_ms: 0,
                ttl_action: Default::default(),
                mount_info: Default::default(),
                create_time: 0,
                overwrite: None,
            },
            task_id: "task".into(),
            worker: Default::default(),
            source_path: String::new(),
            target_path: String::new(),
            create_time: 0,
            source_read_plan_json: String::new(),
            transfer_report: None,
        }));
        let ctx = TaskExecCtx::new(task.clone());
        assert!(!ctx.is_cancelled());
        let future = Box::new(TestExecutor).run(ctx);
        task.request_cancel("superseded");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let outcome = rt.block_on(async { tokio::spawn(future).await.unwrap().unwrap() });
        assert_eq!(outcome, TaskOutcome::Canceled);
    }

    #[test]
    fn registry_returns_registered_plugin() {
        let mut registry = JobRegistry::new();
        registry
            .register(plugin(TransferKind::Load, "load-test"))
            .unwrap();

        let registered = registry.get(TransferKind::Load).unwrap();
        assert_eq!(registered.name(), "load-test");
        assert_eq!(registered.task_kind(), TransferKind::Load);
        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());
    }

    #[test]
    fn registry_rejects_unknown_kind() {
        let registry = JobRegistry::new();

        let error = match registry.get(TransferKind::Export) {
            Ok(_) => panic!("unregistered transfer kind unexpectedly resolved"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("no job plugin registered for transfer kind"));
    }

    #[test]
    fn registry_rejects_duplicate_registration() {
        let mut registry = JobRegistry::new();
        registry
            .register(plugin(TransferKind::Load, "first"))
            .unwrap();

        let error = registry
            .register(plugin(TransferKind::Load, "second"))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("job plugin already registered for transfer kind"));
        assert_eq!(registry.get(TransferKind::Load).unwrap().name(), "first");
    }

    #[test]
    fn supported_kinds_lists_registered_plugins() {
        let mut registry = JobRegistry::new();
        registry
            .register(plugin(TransferKind::Export, "export-test"))
            .unwrap();
        registry
            .register(plugin(TransferKind::Load, "load-test"))
            .unwrap();

        assert_eq!(
            registry.supported_kinds(),
            vec![TransferKind::Load, TransferKind::Export]
        );
    }
}
