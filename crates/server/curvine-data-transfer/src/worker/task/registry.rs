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

    /// Register a plugin for its declared task type.
    ///
    /// Duplicate registrations are rejected rather than silently replacing an
    /// existing plugin. This catches configuration mistakes during startup and
    /// keeps dispatch deterministic.
    pub fn register<P>(&mut self, plugin: P) -> FsResult<()>
    where
        P: JobPlugin + 'static,
    {
        for task_kind in plugin.task_kinds() {
            if self.plugins.contains_key(&task_kind) {
                return Err(FsError::common(format!(
                    "job plugin already registered for task kind {:?}",
                    task_kind
                )));
            }
        }
        let plugin = Arc::new(plugin);
        for task_kind in plugin.task_kinds() {
            self.plugins.insert(task_kind, plugin.clone());
        }
        Ok(())
    }

    /// Return the plugin for `task_kind`.
    ///
    /// An unknown type is a permanent dispatch/configuration error. It is
    /// intentionally represented by the ordinary error result and carries no
    /// retryable admission signal.
    pub fn get(&self, task_kind: TransferKind) -> FsResult<Arc<dyn JobPlugin>> {
        self.plugins.get(&task_kind).cloned().ok_or_else(|| {
            FsError::common(format!(
                "no job plugin registered for task type {:?}",
                task_kind
            ))
        })
    }

    /// Return registered task types in deterministic order.
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
    use crate::worker::task::{
        JobExecutor, JobFuture, ProgressReporter, ResourceClass, TaskExecCtx, TaskOutcome,
    };

    use std::sync::Arc;

    struct TestExecutor;

    impl JobExecutor for TestExecutor {
        fn run(self: Box<Self>, ctx: TaskExecCtx) -> JobFuture {
            Box::pin(async move {
                if ctx.is_cancelled() {
                    Ok(TaskOutcome::Canceled.into())
                } else {
                    Ok(TaskOutcome::Completed.into())
                }
            })
        }
    }

    struct TestPlugin {
        task_kind: TransferKind,
        name: &'static str,
    }

    impl JobPlugin for TestPlugin {
        fn task_kinds(&self) -> Vec<TransferKind> {
            vec![self.task_kind]
        }

        fn name(&self) -> &'static str {
            self.name
        }

        fn resource_class(&self) -> ResourceClass {
            ResourceClass::Cpu
        }

        fn create_executor(
            &self,
            _task: Arc<crate::worker::task::TaskContext>,
            _reporter: ProgressReporter,
        ) -> FsResult<Box<dyn JobExecutor>> {
            Ok(Box::new(TestExecutor))
        }
    }

    fn plugin(task_kind: TransferKind, name: &'static str) -> TestPlugin {
        TestPlugin { task_kind, name }
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
            source_meta: Default::default(),
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
        assert_eq!(outcome.outcome, TaskOutcome::Canceled);
    }

    #[test]
    fn registry_returns_registered_plugin() {
        let mut registry = JobRegistry::new();
        registry
            .register(plugin(TransferKind::Load, "load-test"))
            .unwrap();

        let registered = registry.get(TransferKind::Load).unwrap();
        assert_eq!(registered.name(), "load-test");
        assert_eq!(registered.task_kinds(), vec![TransferKind::Load]);
        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());
    }

    #[test]
    fn registry_rejects_unknown_type() {
        let registry = JobRegistry::new();

        let error = match registry.get(TransferKind::Export) {
            Ok(_) => panic!("unregistered task type unexpectedly resolved"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("no job plugin registered for task type"));
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
            .contains("job plugin already registered for task kind"));
        assert_eq!(registry.get(TransferKind::Load).unwrap().name(), "first");
    }

    #[test]
    fn supported_types_lists_registered_plugins() {
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
