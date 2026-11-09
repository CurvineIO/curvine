// Copyright 2026 OPPO.
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

use curvine_config::DBConf;
use curvine_master::master::meta::store::RocksInodeStore;
use curvine_model::{persist_children_count, DirectoryAttributes};
use curvine_runtime::common::Utils;
use std::sync::Mutex;

// `persist_children_count` is process-wide state shared by every test in
// this binary; serialise the two format cases behind one lock and always
// restore the default on exit.
static FRESH_FORMAT_LOCK: Mutex<()> = Mutex::new(());
struct ResetChildrenCount;
impl Drop for ResetChildrenCount {
    fn drop(&mut self) {
        persist_children_count(false);
    }
}

// A freshly minted directory writes its attribute row with the format the
// merge path will use (legacy when child counts are not persisted). Writing a
// v4 row here would mix generations once the first legacy delta merges in,
// and the merge operator then drops the child count. Regression for the
// format mismatch introduced by the fresh-write fast path.
#[test]
fn fresh_directory_attributes_match_merge_format() {
    let _guard = FRESH_FORMAT_LOCK.lock().unwrap();
    let _reset = ResetChildrenCount;
    persist_children_count(false);
    let path = Utils::test_sub_dir(format!("dir-attrs-fresh-{}", Utils::rand_str(8)));
    let store = RocksInodeStore::new(DBConf::new(&path), true).unwrap();
    let mut batch = store.new_batch();
    batch
        .write_fresh_directory_attributes(100, DirectoryAttributes::new(1, 2, 2))
        .unwrap();
    batch.commit().unwrap();
    let attrs = store
        .get_directory_attributes(100)
        .unwrap()
        .expect("fresh row must exist");
    assert_eq!(
        attrs.children, None,
        "fresh row must use the legacy format (no child count) when counts are not persisted"
    );
    drop(store);
    std::fs::remove_dir_all(path).unwrap();

    // With the format switch on, fresh rows carry the persisted child count.
    persist_children_count(true);
    let path = Utils::test_sub_dir(format!("dir-attrs-fresh-v4-{}", Utils::rand_str(8)));
    let store = RocksInodeStore::new(DBConf::new(&path), true).unwrap();
    let mut batch = store.new_batch();
    batch
        .write_fresh_directory_attributes(100, DirectoryAttributes::new(1, 2, 2))
        .unwrap();
    batch.commit().unwrap();
    let attrs = store
        .get_directory_attributes(100)
        .unwrap()
        .expect("fresh row must exist");
    assert_eq!(
        attrs.children,
        Some(0),
        "fresh row must carry a child count when counts persist"
    );
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
