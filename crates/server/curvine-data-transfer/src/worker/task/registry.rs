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
use curvine_model::JobTaskType;
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
    plugins: HashMap<JobTaskType, Arc<dyn JobPlugin>>,
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
        let task_type = plugin.task_type();
        if self.plugins.contains_key(&task_type) {
            return Err(FsError::common(format!(
                "job plugin already registered for task type {:?}",
                task_type
            )));
        }
        self.plugins.insert(task_type, Arc::new(plugin));
        Ok(())
    }

    /// Return the plugin for `task_type`.
    ///
    /// An unknown type is a permanent dispatch/configuration error. It is
    /// intentionally represented by the ordinary error result and carries no
    /// retryable admission signal.
    pub fn get(&self, task_type: JobTaskType) -> FsResult<Arc<dyn JobPlugin>> {
        self.plugins.get(&task_type).cloned().ok_or_else(|| {
            FsError::common(format!(
                "no job plugin registered for task type {:?}",
                task_type
            ))
        })
    }

    /// Return registered task types in deterministic order.
    pub fn supported_types(&self) -> Vec<JobTaskType> {
        let mut types: Vec<_> = self.plugins.keys().copied().collect();
        types.sort();
        types
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
    use crate::worker::task::{JobExecutor, JobFuture, ResourceClass, TaskOutcome};

    struct TestExecutor;

    impl JobExecutor for TestExecutor {
        fn run(&mut self) -> JobFuture<'_> {
            Box::pin(async { Ok(TaskOutcome::Completed) })
        }
    }

    struct TestPlugin {
        task_type: JobTaskType,
        name: &'static str,
    }

    impl JobPlugin for TestPlugin {
        fn task_type(&self) -> JobTaskType {
            self.task_type
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

    fn plugin(task_type: JobTaskType, name: &'static str) -> TestPlugin {
        TestPlugin { task_type, name }
    }

    #[test]
    fn registry_returns_registered_plugin() {
        let mut registry = JobRegistry::new();
        registry
            .register(plugin(JobTaskType::Load, "load-test"))
            .unwrap();

        let registered = registry.get(JobTaskType::Load).unwrap();
        assert_eq!(registered.name(), "load-test");
        assert_eq!(registered.task_type(), JobTaskType::Load);
        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());
    }

    #[test]
    fn registry_rejects_unknown_type() {
        let registry = JobRegistry::new();

        let error = match registry.get(JobTaskType::Export) {
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
            .register(plugin(JobTaskType::Load, "first"))
            .unwrap();

        let error = registry
            .register(plugin(JobTaskType::Load, "second"))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("job plugin already registered for task type"));
        assert_eq!(registry.get(JobTaskType::Load).unwrap().name(), "first");
    }

    #[test]
    fn supported_types_lists_registered_plugins() {
        let mut registry = JobRegistry::new();
        registry
            .register(plugin(JobTaskType::Export, "export-test"))
            .unwrap();
        registry
            .register(plugin(JobTaskType::Load, "load-test"))
            .unwrap();

        assert_eq!(
            registry.supported_types(),
            vec![JobTaskType::Load, JobTaskType::Export]
        );
    }
}
