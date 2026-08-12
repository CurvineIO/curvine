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

#![cfg(feature = "fault-injection")]

use curvine_fault::{fault_point, ConfigureFaultRule, FaultAction, FaultErrorSpec, FaultRuntime};

fn embedded_client_point() -> Result<(), String> {
    fault_point! {
        sync,
        name: "client.process_runtime.before_call",
        description: "Client point executed inside a process-owned runtime",
        context: {},
        return_error: |fault| Err(fault.message),
    }
    Ok(())
}

#[allow(dead_code)]
fn linked_but_unreached_master_point() {
    fault_point! {
        sync,
        name: "master.process_runtime.unreachable",
        description: "A linked point that this test workload does not call",
        context: {},
    }
}

#[test]
fn process_runtime_owns_all_linked_points_and_default_macro_uses_it() {
    // No startup hook installs a Runtime. Reaching the default macro form is
    // sufficient to initialize the crate-owned process Runtime.
    assert!(embedded_client_point().is_ok());
    let runtime = FaultRuntime::process();

    let status = runtime.status();
    assert!(status
        .points
        .iter()
        .any(|point| point.name == "client.process_runtime.before_call"));
    assert!(status
        .points
        .iter()
        .any(|point| point.name == "master.process_runtime.unreachable"));
    runtime
        .configure(
            "inactive-master-rule",
            ConfigureFaultRule {
                point: "master.process_runtime.unreachable".to_string(),
                action: FaultAction::Record,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        runtime
            .rule("inactive-master-rule")
            .unwrap()
            .unwrap()
            .executions,
        0,
        "configuring a linked point does not imply that the workload reached it"
    );
    runtime
        .configure(
            "embedded-client-error",
            ConfigureFaultRule {
                point: "client.process_runtime.before_call".to_string(),
                action: FaultAction::ReturnError {
                    error: FaultErrorSpec {
                        message: "expected".to_string(),
                    },
                },
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(embedded_client_point().unwrap_err(), "expected");
    assert_eq!(
        runtime
            .rule("embedded-client-error")
            .unwrap()
            .unwrap()
            .executions,
        1
    );
    let reused = FaultRuntime::process();
    assert!(std::ptr::eq(runtime, reused));
    assert!(reused.rule("embedded-client-error").unwrap().is_some());
    runtime.clear().unwrap();
}
