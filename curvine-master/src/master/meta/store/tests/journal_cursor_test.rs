// Copyright 2026 OPPO
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

use super::tests::new_store;
use std::time::Duration;

#[test]
fn cursor_store_roundtrip_and_prune() {
    let store = new_store("cursor-roundtrip").unwrap();
    let cursor = store.cursor_store();
    assert!(cursor.get_applied_req(7).unwrap().is_none());
    cursor.put_applied_req(7, 199, 3).unwrap();
    cursor.put_applied_req(8, 200, 4).unwrap();
    let record = cursor.get_applied_req(7).unwrap().unwrap();
    assert_eq!((record.index, record.term), (199, 3));
    cursor.prune_applied_req_below(200).unwrap();
    assert!(cursor.get_applied_req(7).unwrap().is_none());
    let record = cursor.get_applied_req(8).unwrap().unwrap();
    assert_eq!((record.index, record.term), (200, 4));
}

#[test]
fn cursor_store_follows_the_store_across_restore() {
    let mut store = new_store("cursor-restore").unwrap();
    let cursor = store.cursor_store();
    let held = cursor.clone();
    cursor.put_applied_req(11, 110, 2).unwrap();
    cursor.put_applied_req(12, 120, 2).unwrap();
    let checkpoint = store.create_checkpoint(1).unwrap();
    cursor.put_applied_req(13, 130, 2).unwrap();

    // Both opaque handles survive the real ref_count == 2 restore check.
    let guard = store.begin_restore(Duration::from_secs(5)).unwrap();
    store.restore_with_guard(&guard, &checkpoint).unwrap();
    drop(guard);
    assert_eq!(cursor.get_applied_req(11).unwrap().unwrap().index, 110);
    assert_eq!(held.get_applied_req(12).unwrap().unwrap().index, 120);
    assert!(cursor.get_applied_req(13).unwrap().is_none());
}

#[test]
fn invalidated_cursor_refuses_operations() {
    let store = new_store("cursor-invalidate").unwrap();
    let cursor = store.cursor_store();
    cursor.put_applied_req(5, 50, 1).unwrap();
    store.invalidate_cursor_store();
    for error in [
        cursor.get_applied_req(5).map(|_| ()).unwrap_err(),
        cursor.put_applied_req(5, 50, 1).unwrap_err(),
        cursor.prune_applied_req_below(1).unwrap_err(),
    ] {
        assert!(error.to_string().contains("cursor store unavailable"));
    }
}
