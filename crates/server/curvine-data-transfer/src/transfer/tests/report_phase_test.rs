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

//! Phase-metric timestamps for terminal reports.
//!
//! The worker echoes the dispatch time it was handed, so the report path must
//! not touch the store. Only a report from an older worker (field absent)
//! falls back to reading the task row.

use std::cell::Cell;

use super::service::resolve_attempt_started_at;

#[test]
fn a_reported_attempt_start_needs_no_store_lookup() {
    let looked_up = Cell::new(false);
    let started = resolve_attempt_started_at(1_700_000_000_000, || {
        looked_up.set(true);
        Some(1)
    });

    assert_eq!(started, Some(1_700_000_000_000));
    assert!(
        !looked_up.get(),
        "the echoed dispatch time must short-circuit the store read"
    );
}

#[test]
fn an_absent_attempt_start_falls_back_to_the_task_row() {
    assert_eq!(resolve_attempt_started_at(0, || Some(42)), Some(42));
    // Nothing usable on either side: the caller must skip the observation
    // instead of inventing a timestamp.
    assert_eq!(resolve_attempt_started_at(0, || Some(0)), None);
    assert_eq!(resolve_attempt_started_at(0, || None), None);
    // A negative report is as unusable as a missing one.
    assert_eq!(resolve_attempt_started_at(-5, || Some(42)), Some(42));
}
