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

//! Pins the published read mirror that lets readers skip the children lock.
//!
//! Two things can go wrong with a mirror and neither shows up as a compile
//! error: a reader seeing a half-applied write, and a mirror that is newer than
//! the sequence version reads validate against. Both are asserted here.

use super::*;
use crate::master::meta::inode::InodeFile;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn file(name: &str, id: i64) -> InodeView {
    InodeView::new_file(name.to_string(), InodeFile::new(id, 0))
}

#[test]
fn a_write_becomes_visible_in_the_mirror_after_the_guard_drops() {
    let children = DirectoryChildren::new();
    assert_eq!(children.children_snap.load().len(), 0);

    {
        let mut mutation = children.begin_mutation("a");
        mutation.add_child(file("a", 1)).expect("add");
        // Inside the guard the mirror is still the pre-write table: a reader
        // must not be able to observe the mutation half way through it.
        assert_eq!(
            children.children_snap.load().len(),
            0,
            "the mirror must not publish before the writer commits"
        );
    }

    // Guard dropped: published, and the write lock has been released.
    assert_eq!(children.children_snap.load().len(), 1);
}

#[test]
fn the_mirror_is_published_with_the_sequence_it_belongs_to() {
    // A reader validates a walk against the sequence. If the mirror were newer
    // than the version, the reader could judge a stale table current.
    let children = DirectoryChildren::new();
    let before = children.sequence.version();
    assert_eq!(before & 1, 0, "no writer is in flight initially");

    {
        let mut mutation = children.begin_mutation("b");
        mutation.add_child(file("b", 2)).expect("add");
        // While the writer holds the window the version is odd, which is why a
        // reader's `is_current` fails and it retries instead of trusting a
        // half-written table.
        assert_ne!(
            children.sequence.version() & 1,
            0,
            "an in-flight writer must mark the sequence as busy"
        );
    }

    let after = children.sequence.version();
    assert_ne!(before, after, "committing must advance the generation");
    assert_eq!(after & 1, 0, "the window is closed after the guard drops");
    // Both the table and the version are post-write now: no mixed pair exists.
    assert_eq!(children.children_snap.load().len(), 1);
    assert!(
        children.sequence.is_current(after),
        "the version is current"
    );
}

#[test]
fn concurrent_readers_never_see_a_name_missing_from_its_own_snapshot() {
    let children = Arc::new(DirectoryChildren::new());
    let stop = Arc::new(AtomicBool::new(false));

    let reader = {
        let children = Arc::clone(&children);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut max_len = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let snap = children.children_snap.load();
                let len = snap.len();
                // Every name visible in this snapshot must resolve in the same
                // snapshot. A torn view would show up here.
                for view in snap.all_views() {
                    assert!(
                        snap.get_child_item(view.name()).is_some(),
                        "name {} missing from the snapshot that listed it",
                        view.name()
                    );
                }
                // Readers may lag, but this case only inserts, so a newer
                // snapshot must never be shorter than one already observed.
                assert!(
                    len >= max_len,
                    "snapshot went backwards: {max_len} -> {len}"
                );
                max_len = len;
                std::hint::spin_loop();
            }
            max_len
        })
    };

    for i in 0..200 {
        let name = format!("f{i}");
        let mut mutation = children.begin_mutation(&name);
        mutation
            .add_child(file(&name, 1000 + i as i64))
            .expect("add");
    }
    stop.store(true, Ordering::Relaxed);
    let observed = reader.join().expect("reader");

    assert!(
        observed > 0,
        "the reader never observed a published snapshot"
    );
    assert_eq!(children.children_snap.load().len(), 200);
}
