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

#![cfg(not(feature = "fault-injection"))]

use curvine_fault::fault_point;
use std::sync::atomic::{AtomicUsize, Ordering};

static CONTEXT_EVALUATIONS: AtomicUsize = AtomicUsize::new(0);

#[allow(dead_code)]
fn context_value() -> i64 {
    CONTEXT_EVALUATIONS.fetch_add(1, Ordering::SeqCst);
    1
}

fn disabled_point() {
    fault_point! {
        sync,
        name: "test.feature_off.noop",
        description: "Feature-off macro must not register or evaluate context",
        context: {"value" => context_value()},
    }
}

#[test]
fn feature_off_macro_has_no_registration_or_context_side_effect() {
    disabled_point();
    assert_eq!(CONTEXT_EVALUATIONS.load(Ordering::SeqCst), 0);
    assert!(curvine_fault::FAULT_POINT_REGISTRATIONS
        .iter()
        .all(|registration| registration.point.name != "test.feature_off.noop"));
}
