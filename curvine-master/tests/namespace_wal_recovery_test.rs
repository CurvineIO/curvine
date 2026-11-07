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

use curvine_config::DBConf;
use curvine_master::master::meta::inode::{InodeFile, InodeView};
use curvine_master::master::meta::store::RocksInodeStore;
use curvine_runtime::common::Utils;
use std::process::Command;

#[test]
fn synced_cursor_preserves_preceding_namespace_batch_after_process_exit() {
    const ENV: &str = "CURVINE_NAMESPACE_WAL_CRASH_DIR";
    if let Ok(path) = std::env::var(ENV) {
        let store = RocksInodeStore::new(DBConf::new(&path), true).unwrap();
        let mut batch = store.new_batch();
        batch
            .write_inode(&InodeView::new_file("file".into(), InodeFile::new(2, 1)))
            .unwrap();
        batch.add_child(1, "file", 2).unwrap();
        batch.commit().unwrap();
        store
            .put_common_kv(RocksInodeStore::JOURNAL_APPLIED_KEY, b"cursor")
            .unwrap();
        // No destructors or database flush: simulate process termination after cursor sync.
        std::process::exit(0);
    }

    let path = Utils::test_sub_dir(format!("namespace_wal_crash_{}", Utils::rand_str(12)));
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "synced_cursor_preserves_preceding_namespace_batch_after_process_exit",
        ])
        .env(ENV, &path)
        .status()
        .unwrap();
    assert!(status.success(), "child process failed: {status}");
    let store = RocksInodeStore::new(DBConf::new(&path), false).unwrap();
    assert_eq!(
        store
            .get_common_kv(RocksInodeStore::JOURNAL_APPLIED_KEY)
            .unwrap(),
        Some(b"cursor".to_vec())
    );
    assert!(
        store.get_inode(2).unwrap().is_some(),
        "synced cursor must not outlive inode batch"
    );
    assert_eq!(
        store
            .get_child_ids(1, None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![2]
    );
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
