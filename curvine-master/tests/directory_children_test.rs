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

use curvine_master::master::meta::inode::{DirectoryChildren, InodeDir, InodeView};
use std::sync::{mpsc, Arc};

fn file_edge(id: i64) -> InodeView {
    InodeView::new_file(
        format!("f{id}"),
        curvine_master::master::meta::inode::InodeFile::new(id, 1),
    )
}

#[test]
fn concurrent_add_and_read_never_serves_stale_version_as_current() {
    let children = Arc::new(DirectoryChildren::new());
    children.add_child(file_edge(1)).unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    for round in 0..2000 {
        let writer = children.clone();
        let writer_done = done_tx.clone();
        let t = std::thread::spawn(move || {
            for i in 0..8 {
                writer
                    .add_child(InodeView::new_file(
                        format!("w{}-{}", round, i),
                        curvine_master::master::meta::inode::InodeFile::new(1, 1),
                    ))
                    .unwrap();
            }
            writer_done.send(()).unwrap();
        });
        let (view, version) = children.read_child_view("f1");
        assert!(view.is_some(), "f1 must always be present");
        // The read may overlap the writer; is_current then reports false. It
        // must never claim an odd (in-flight) version is current.
        assert!(!(!version.is_current() && children.version_is(&version)));
        t.join().unwrap();
    }
    drop(done_tx);
    assert!(done_rx.iter().count() == 2000);
}

#[test]
fn delete_child_returns_owned_inode_and_keeps_handle_consistent() {
    let mut dir = InodeDir::new(1, 1);
    dir.add_child(file_edge(2)).unwrap();
    assert_eq!(dir.children_len(), 1);
    let removed = dir.delete_child(2, "f2").unwrap();
    assert_eq!(removed.id(), 2);
    assert_eq!(dir.children_len(), 0);
}
