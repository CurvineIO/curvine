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

use curvine_fault::{fault_point, FaultRuntime};

#[allow(dead_code)]
fn first_declaration(runtime: &FaultRuntime) {
    fault_point! {
        sync,
        name: "client.test.conflicting_contract",
        description: "first declaration",
        runtime: runtime,
        context: {"rpc_code" => 1_i32},
    }
}

#[allow(dead_code)]
fn second_declaration(runtime: &FaultRuntime) {
    fault_point! {
        sync,
        name: "client.test.conflicting_contract",
        description: "second declaration",
        runtime: runtime,
        context: {"rpc_code" => 2_i32},
    }
}

#[test]
fn conflicting_implicit_declarations_report_both_call_sites() {
    let error = FaultRuntime::isolated().unwrap_err().to_string();

    assert!(error.contains("client.test.conflicting_contract"));
    assert!(error.matches("catalog_conflict.rs:").count() >= 2);
}
