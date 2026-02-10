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
use curvine_server::master::{Master, MountManager};
use orpc::common::Utils;
use orpc::CommonResult;
use std::collections::HashMap;
use std::sync::Arc;

struct MountEnv {
    _conf: ClusterConf,
    _js: JournalSystem,
    _fs: MasterFilesystem,
    mnt_mgr: Arc<MountManager>,
}

fn new_mount_env(name: &str, format_master: bool) -> MountEnv {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("mount-manager-test/{}", name));
    conf.format_master = format_master;
    conf.journal.enable = false;

    let js = JournalSystem::from_conf(&conf).unwrap();
    let fs = MasterFilesystem::with_js(&conf, &js);
    let mnt_mgr = js.mount_manager();

    MountEnv {
        _conf: conf,
        _js: js,
        _fs: fs,
        mnt_mgr,
    }
}

#[test]
fn test_mount_basic_lookup_and_unmount_by_id() -> CommonResult<()> {
    let env = new_mount_env(&format!("basic-{}", Utils::rand_str(6)), true);

    let mut props = HashMap::new();
    props.insert("k".to_string(), "v".to_string());
    let opts = MountOptions::builder().set_properties(props).build();

    env.mnt_mgr
        .mount(None, "/data", "oss://bucket/data", &opts)?;

    // Explicit mount id path (Some) should also work.
    let explicit_id = 42_4242_u32;
    env.mnt_mgr.mount(
        Some(explicit_id),
        "/data-explicit",
        "oss://bucket/explicit",
        &opts,
    )?;

    // Existing mount point path should take the "already exists" directory branch.
    env._fs.mkdir("/pre-created", true)?;
    env.mnt_mgr
        .mount(None, "/pre-created", "oss://bucket/pre-created", &opts)?;

    let table = env.mnt_mgr.get_mount_table()?;
    assert_eq!(table.len(), 3);
    let mount_id = table
        .iter()
        .find(|x| x.cv_path == "/data")
        .unwrap()
        .mount_id;
    let pre_created_id = table
        .iter()
        .find(|x| x.cv_path == "/pre-created")
        .unwrap()
        .mount_id;
    assert!(table.iter().any(|x| x.mount_id == explicit_id));

    let cv_child = Path::from_str("/data/a/b.txt")?;
    let cv_info = env.mnt_mgr.get_mount_info(&cv_child)?.unwrap();
    assert_eq!(cv_info.cv_path, "/data");
    assert_eq!(cv_info.ufs_path, "oss://bucket/data");

    let ufs_child = Path::from_str("oss://bucket/data/a/b.txt")?;
    let ufs_info = env.mnt_mgr.get_mount_info(&ufs_child)?.unwrap();
    assert_eq!(ufs_info.cv_path, "/data");

    env.mnt_mgr.unmount_by_id(mount_id)?;
    env.mnt_mgr.unmount_by_id(pre_created_id)?;
    env.mnt_mgr.unmount_by_id(explicit_id)?;
    assert!(env.mnt_mgr.get_mount_table()?.is_empty());

    Ok(())
}

#[test]
fn test_mount_conflicts_and_duplicate_checks() -> CommonResult<()> {
    let env = new_mount_env(&format!("conflict-{}", Utils::rand_str(6)), true);
    let opts = MountOptions::builder().build();

    env.mnt_mgr.mount(None, "/cv/a", "oss://bucket/a", &opts)?;

    // Duplicate UFS path
    let err = env
        .mnt_mgr
        .mount(None, "/cv/other", "oss://bucket/a", &opts)
        .unwrap_err();
    assert!(err.to_string().contains("already exists in mount table"));

    // Duplicate CV mount path
    let err = env
        .mnt_mgr
        .mount(None, "/cv/a", "oss://bucket/other", &opts)
        .unwrap_err();
    assert!(err.to_string().contains("already exists in mount table"));

    // CV path prefix conflict
    let err = env
        .mnt_mgr
        .mount(None, "/cv/a/sub", "oss://bucket/new", &opts)
        .unwrap_err();
    assert!(err.to_string().contains("is a prefix of"));

    // UFS path prefix conflict
    let err = env
        .mnt_mgr
        .mount(None, "/cv/new", "oss://bucket/a/sub", &opts)
        .unwrap_err();
    assert!(err.to_string().contains("is a prefix of"));

    Ok(())
}

#[test]
fn test_mount_update_mode_overwrites_existing_entry() -> CommonResult<()> {
    let env = new_mount_env(&format!("update-{}", Utils::rand_str(6)), true);

    let mut props1 = HashMap::new();
    props1.insert("old".to_string(), "1".to_string());
    let opts1 = MountOptions::builder().set_properties(props1).build();
    env.mnt_mgr
        .mount(None, "/cv/update", "oss://bucket-old/cv/update", &opts1)?;

    // Update non-existing path should fail.
    let missing_update = MountOptions::builder().update(true).build();
    let err = env
        .mnt_mgr
        .mount(
            None,
            "/cv/not-exists",
            "oss://bucket-new/cv/not-exists",
            &missing_update,
        )
        .unwrap_err();
    assert!(err.to_string().contains("does not exist"));

    // Update existing path should succeed and replace properties/ufs path.
    let mut props2 = HashMap::new();
    props2.insert("new".to_string(), "2".to_string());
    let update_opts = MountOptions::builder()
        .update(true)
        .set_properties(props2)
        .build();

    let updated_id = 77_7777_u32;
    env.mnt_mgr.mount(
        Some(updated_id),
        "/cv/update",
        "oss://bucket-new/cv/update",
        &update_opts,
    )?;

    let info = env
        .mnt_mgr
        .get_mount_info(&Path::from_str("/cv/update/a.txt")?)?
        .unwrap();

    assert_eq!(info.ufs_path, "oss://bucket-new/cv/update");
    assert_eq!(info.mount_id, updated_id);
    assert_eq!(info.properties.get("new"), Some(&"2".to_string()));
    assert!(!info.properties.contains_key("old"));

    Ok(())
}

#[test]
fn test_mount_restore_and_unprotected_paths() -> CommonResult<()> {
    let name = format!("restore-{}", Utils::rand_str(6));

    {
        let env = new_mount_env(&name, true);
        let opts = MountOptions::builder().build();
        env.mnt_mgr
            .mount(None, "/restore/x", "oss://restore/x", &opts)?;
        assert_eq!(env.mnt_mgr.get_mount_table()?.len(), 1);
    }

    let env2 = new_mount_env(&name, false);
    assert!(env2.mnt_mgr.get_mount_table()?.is_empty());

    env2.mnt_mgr.restore();
    let table = env2.mnt_mgr.get_mount_table()?;
    assert_eq!(table.len(), 1);

    let id = table[0].mount_id;
    env2.mnt_mgr.unprotected_umount_by_id(id)?;
    assert!(env2.mnt_mgr.get_mount_table()?.is_empty());

    // Unprotected remove missing id should fail.
    let err = env2.mnt_mgr.unprotected_umount_by_id(id).unwrap_err();
    assert!(err.to_string().contains("failed found"));

    Ok(())
}
