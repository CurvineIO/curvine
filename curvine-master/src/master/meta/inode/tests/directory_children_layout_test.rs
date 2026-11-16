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

//! Pins the cache-line layout that keeps the Arc refcount off the hot fields.
//!
//! Nothing else in the codebase depends on this, so the alignment could be
//! dropped without a compile error and the only symptom would be a silent
//! throughput regression on the read paths that clone the token. These cases
//! make that regression impossible to introduce unnoticed.

use super::*;
use std::sync::Arc;

/// `DirectoryChildren` must start on its own 64-byte line, so the `ArcInner`
/// refcount (strong/weak, immediately before the payload) cannot share a line
/// with `sequence` or `children`.
#[test]
fn directory_children_is_cache_line_aligned() {
    assert_eq!(
        std::mem::align_of::<DirectoryChildren>(),
        64,
        "the hot fields must be cache-line aligned"
    );
    assert_eq!(
        std::mem::size_of::<DirectoryChildren>() % 64,
        0,
        "the payload must be padded to whole cache lines"
    );
}

/// The ArcInner header holds two `usize` counters directly before the payload,
/// so the payload address minus two words is the refcount address. Those two
/// addresses must land in different 64-byte lines.
#[test]
fn the_refcount_does_not_share_a_line_with_the_payload() {
    let arc = Arc::new(DirectoryChildren::new());
    let payload = Arc::as_ptr(&arc) as usize;
    let refcount = payload - 2 * std::mem::size_of::<usize>();

    assert_eq!(
        payload % 64,
        0,
        "the payload must start on a fresh cache line"
    );
    assert_ne!(
        refcount / 64,
        payload / 64,
        "refcount at {refcount:#x} and payload at {payload:#x} share a cache line"
    );
}

/// A cloned handle is the case that does the refcount RMW, so the invariant has
/// to hold for a shared Arc too, not only for a freshly allocated one.
#[test]
fn a_cloned_handle_keeps_the_payload_line_separate() {
    let arc = Arc::new(DirectoryChildren::new());
    let clone = Arc::clone(&arc);
    let payload = Arc::as_ptr(&clone) as usize;
    let refcount = payload - 2 * std::mem::size_of::<usize>();

    assert_eq!(payload, Arc::as_ptr(&arc) as usize, "same allocation");
    assert_ne!(refcount / 64, payload / 64);
}
