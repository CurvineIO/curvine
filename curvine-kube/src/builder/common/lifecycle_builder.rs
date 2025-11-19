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

use k8s_openapi::api::core::v1::{ExecAction, Lifecycle, LifecycleHandler};

pub struct LifecycleBuilder;

impl LifecycleBuilder {
    pub fn build_graceful_shutdown(
        component: &str,
        graceful_shutdown: bool,
        shutdown_delay: u32,
    ) -> Option<Lifecycle> {
        if !graceful_shutdown {
            return None;
        }

        let stop_command = format!(
            "sleep {} && /app/curvine/bin/cv {} stop || true",
            shutdown_delay, component
        );

        Some(Lifecycle {
            pre_stop: Some(LifecycleHandler {
                exec: Some(ExecAction {
                    command: Some(vec!["/bin/sh".to_string(), "-c".to_string(), stop_command]),
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    pub fn build_default_graceful_shutdown(
        component: &str,
        graceful_shutdown: bool,
    ) -> Option<Lifecycle> {
        Self::build_graceful_shutdown(component, graceful_shutdown, 10)
    }
}
