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
use curvine_model::MountInfo;
use curvine_runtime::common::Utils;

#[test]
fn namespace_reset_preserves_mount_version_and_clears_request_ids() {
    let path = Utils::test_sub_dir(format!("namespace-reset-{}", Utils::rand_str(8)));
    let conf = DBConf::new(&path);
    let store = RocksInodeStore::new(conf.clone(), true).unwrap();
    let mount = MountInfo {
        mount_id: 7,
        cv_path: "/mounted".into(),
        ufs_path: "file:///mounted".into(),
        ..MountInfo::default()
    };
    store.add_mountpoint(mount.mount_id, &mount).unwrap();
    store.save_mount_table_version(42).unwrap();
    store.put_ufs_deferred(1, b"deferred").unwrap();
    for id in [i64::MIN, -1, 0, 1, i64::MAX] {
        store.put_applied_req(id, 123, 2).unwrap();
    }
    store.reset_namespace_for_replay().unwrap();
    assert_eq!(store.get_mount_table_version().unwrap(), 42);
    assert_eq!(
        store.get_mount_info(mount.mount_id).unwrap(),
        Some(mount.clone())
    );
    assert!(store.ufs_deferred_records().unwrap().is_empty());
    for id in [i64::MIN, -1, 0, 1, i64::MAX] {
        assert!(store.get_applied_req(id).unwrap().is_none(), "id {id}");
    }
    drop(store);

    let store = RocksInodeStore::new(conf, false).unwrap();
    store.reset_namespace_for_replay().unwrap();
    assert_eq!(store.get_mount_table_version().unwrap(), 42);
    assert_eq!(store.get_mount_info(mount.mount_id).unwrap(), Some(mount));
    for id in [i64::MIN, -1, 0, 1, i64::MAX] {
        assert!(store.get_applied_req(id).unwrap().is_none(), "id {id}");
    }
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
