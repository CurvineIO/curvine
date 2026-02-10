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

use curvine_common::conf::ClusterConf;
use curvine_common::fs::Path;
use curvine_common::state::MountOptions;
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::mount::MountTable;
use curvine_server::master::Master;
use orpc::common::Utils;
use orpc::CommonResult;
use std::collections::HashMap;

struct TableEnv {
    _conf: ClusterConf,
    _js: JournalSystem,
    table: MountTable,
}

fn new_table_env(name: &str, format_master: bool) -> TableEnv {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("mount-table-test/{}", name));
    conf.format_master = format_master;
    conf.journal.enable = false;

    let js = JournalSystem::from_conf(&conf).unwrap();
    let fs = MasterFilesystem::with_js(&conf, &js);
    let table = MountTable::new(fs.fs_dir.clone());

    TableEnv {
        _conf: conf,
        _js: js,
        table,
    }
}

#[test]
fn test_mount_table_conflict_lookup_and_error_paths() -> CommonResult<()> {
    let env = new_table_env(&format!("conflict-{}", Utils::rand_str(6)), true);
    let opts = MountOptions::builder().build();

    env.table.add_mount(100, "/cv/a", "oss://bucket/a", &opts)?;

    // Existing path is child of incoming path: /cv/a has prefix /cv.
    let err = env
        .table
        .check_conflict("/cv", "oss://bucket-new")
        .unwrap_err();
    assert!(err.to_string().contains("is a prefix of"));

    // UFS child conflict: incoming ufs is under existing ufs.
    let err = env
        .table
        .check_conflict("/other/path", "oss://bucket/a/sub")
        .unwrap_err();
    assert!(err.to_string().contains("is a prefix of"));

    // UFS parent conflict: existing ufs is under incoming ufs.
    let err = env
        .table
        .check_conflict("/other/path2", "oss://bucket")
        .unwrap_err();
    assert!(err.to_string().contains("is a prefix of"));

    // Lookup miss path.
    let miss = env.table.get_mount_info(&Path::from_str("/not/found")?)?;
    assert!(miss.is_none());

    // Missing umount path error.
    let err = env.table.umount("/not/found").unwrap_err();
    assert!(err.to_string().contains("failed found"));

    // Missing id lookup error.
    let err = env.table.get_mount_info_by_id(999999).unwrap_err();
    assert!(err.to_string().contains("failed found"));

    // Ensure assign id fast path is exercised.
    let assigned = env.table.assign_mount_id()?;
    assert_ne!(assigned, 0);

    Ok(())
}

#[test]
fn test_mount_table_get_ufs_conf_paths() -> CommonResult<()> {
    let env = new_table_env(&format!("ufs-{}", Utils::rand_str(6)), true);

    let mut props = HashMap::new();
    props.insert("ak".to_string(), "sk".to_string());
    props.insert("endpoint".to_string(), "http://example".to_string());
    let opts = MountOptions::builder().set_properties(props).build();

    env.table.add_mount(200, "/m", "oss://bucket/m", &opts)?;

    let conf = env.table.get_ufs_conf(&"oss://bucket/m".to_string())?;
    assert_eq!(conf.get("ak"), Some("sk"));
    assert_eq!(conf.get("endpoint"), Some("http://example"));

    let err = env
        .table
        .get_ufs_conf(&"oss://bucket/missing".to_string())
        .unwrap_err();
    assert!(err.to_string().contains("failed found"));

    Ok(())
}

#[test]
fn test_mount_table_restore_path() -> CommonResult<()> {
    let name = format!("restore-{}", Utils::rand_str(6));

    {
        let env = new_table_env(&name, true);
        let opts = MountOptions::builder().build();
        env.table
            .add_mount(300, "/restore/a", "oss://restore/a", &opts)?;
        assert_eq!(env.table.get_mount_table()?.len(), 1);
    }

    let env2 = new_table_env(&name, false);
    assert!(env2.table.get_mount_table()?.is_empty());

    env2.table.restore();
    assert_eq!(env2.table.get_mount_table()?.len(), 1);

    Ok(())
}
