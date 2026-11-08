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

use curvine_config::ChildrenContainer;
use curvine_master::master::journal::JournalSystem;
use curvine_master::master::meta::inode::InodePath;
use curvine_master::Master;
use curvine_model::{FileLock, LockType};
use curvine_runtime::common::{LocalTime, Utils};

#[test]
fn set_lock_resolves_alias_and_rejects_missing_path() {
    Master::init_test_metrics();
    let mut conf = curvine_config::ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!("set-lock-path-{}", Utils::rand_str(8)));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("set-lock-journal-{}", Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.create("/dir/file", true).unwrap();
    fs.link("/dir/file", "/dir/alias").unwrap();
    let lock = FileLock {
        client_id: "owner".into(),
        owner_id: 1,
        acquire_time: LocalTime::mills(),
        lock_type: LockType::WriteLock,
        end: 100,
        ..FileLock::default()
    };
    assert!(fs.set_lock("/dir/alias", lock.clone()).unwrap().is_none());
    let query = FileLock {
        client_id: "other".into(),
        owner_id: 2,
        end: 100,
        ..FileLock::default()
    };
    assert!(fs.get_lock("/dir/file", query.clone()).unwrap().is_some());
    assert!(fs.set_lock("/dir/missing", query.clone()).is_err());
    assert!(fs.set_lock("/dir/file/child", query).is_err());
    fs.restore_from_rocksdb().unwrap();
    assert!(fs
        .get_lock(
            "/dir/file",
            FileLock {
                client_id: "other".into(),
                owner_id: 2,
                end: 100,
                ..FileLock::default()
            }
        )
        .unwrap()
        .is_some());
}

#[test]
fn get_lock_matches_full_path_resolution() {
    const CONTAINER_ENV: &str = "CURVINE_TEST_LOCK_PATH_CONTAINER";
    let container = match std::env::var(CONTAINER_ENV).as_deref() {
        Ok("list") => ChildrenContainer::List,
        Ok("map") => ChildrenContainer::Map,
        Ok("packed") => ChildrenContainer::Packed,
        Ok(other) => panic!("unknown container {other}"),
        Err(_) => {
            // The container is fixed process-wide on first use.
            for name in ["list", "map", "packed"] {
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .env(CONTAINER_ENV, name)
                    .args(["--exact", "get_lock_matches_full_path_resolution"])
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "container {name}: {}\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            return;
        }
    };
    Master::init_test_metrics();
    let mut conf = curvine_config::ClusterConf::format();
    conf.master.children_container = container;
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!("get-lock-path-{}", Utils::rand_str(8)));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("get-lock-journal-{}", Utils::rand_str(8)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.create("/dir/file", true).unwrap();
    fs.mkdir("/dir/sub", false).unwrap();

    let held = FileLock {
        client_id: "owner".into(),
        owner_id: 1,
        acquire_time: LocalTime::mills(),
        lock_type: LockType::WriteLock,
        end: 100,
        ..FileLock::default()
    };
    assert!(fs.set_lock("/dir/file", held).unwrap().is_none());
    let query = FileLock {
        client_id: "reader".into(),
        owner_id: 2,
        end: 100,
        ..FileLock::default()
    };

    for path in [
        "/",
        "/dir",
        "/dir/file",
        "/dir/sub",
        "/missing",
        "/dir/missing",
        "/dir/file/child",
        "/dir//file",
        "",
        "relative",
        "/dir/",
        "//",
    ] {
        let actual = fs.get_lock(path, query.clone()).map_err(|e| e.to_string());
        let expected = {
            let guard = fs.fs_dir.read();
            InodePath::resolve(guard.root_ptr(), path, guard.store_ref())
                .map_err(|e| e.to_string())
                .and_then(|inp| {
                    guard
                        .get_lock(inp, &query, conf.master.lock_expire_time_ms())
                        .map_err(|e| e.to_string())
                })
        };
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => assert_eq!(actual, expected, "path: {path}"),
            (Err(actual), Err(expected)) => assert_eq!(
                actual.split('(').next(),
                expected.split('(').next(),
                "path: {path}"
            ),
            (actual, expected) => {
                panic!("path: {path}, actual: {actual:?}, expected: {expected:?}")
            }
        }
    }
    assert!(fs.get_lock("/dir/file", query.clone()).unwrap().is_some());
    assert!(fs.get_lock("/.Trash/.pending/file", query.clone()).is_err());

    if container != ChildrenContainer::Packed {
        return;
    }
    let file_id = fs.file_status("/dir/file").unwrap().id;
    {
        let guard = fs.fs_dir.write();
        let mut batch = guard.store_ref().new_batch();
        batch.delete_inode(file_id).unwrap();
        batch.commit().unwrap();
    }
    for path in ["/dir/file", "/dir/file/child"] {
        let error = fs.get_lock(path, query.clone()).unwrap_err();
        assert!(
            error.to_string().contains("Failed to load inode"),
            "{error}"
        );
    }
}
