use curvine_config::ClusterConf;
use curvine_master::master::fs::MasterFilesystem;
use curvine_master::master::journal::JournalSystem;
use curvine_master::master::meta::inode::{InodePath, InodeView};
use curvine_master::Master;
use curvine_model::SetAttrOpts;
use curvine_runtime::common::Utils;

fn filesystem(name: &str) -> MasterFilesystem {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.journal.enable = false;
    conf.master.meta_dir =
        Utils::test_sub_dir(format!("metadata-research/{}/{name}", std::process::id()));
    JournalSystem::fs_only_for_test(&conf).unwrap()
}

#[test]
fn setattr_rejects_restore_admission_without_changing_metadata() {
    let fs = filesystem("restore-admission-attrs");
    fs.mkdir("/dir", false).unwrap();
    let before = fs.file_status("/dir").unwrap();
    let restore = fs
        .fs_dir
        .read()
        .store_ref()
        .begin_restore(std::time::Duration::from_secs(1))
        .unwrap();
    let opts = SetAttrOpts {
        mode: Some(0o600),
        ..Default::default()
    };
    let error = fs.set_attr("/dir", opts.clone()).unwrap_err();
    assert!(error.to_string().contains("snapshot restore"));
    drop(restore);
    assert_eq!(fs.file_status("/dir").unwrap().mode, before.mode);
    assert_eq!(fs.set_attr("/dir", opts).unwrap().mode, 0o600);
}

#[test]
fn setattr_missing_and_reserved_paths_do_not_change_existing_directory() {
    let fs = filesystem("missing-reserved-attrs");
    fs.mkdir("/parent", false).unwrap();
    let before = fs.file_status("/parent").unwrap();
    for path in ["/parent/missing", "/.Trash/.pending/hidden"] {
        assert!(fs
            .set_attr(
                path,
                SetAttrOpts {
                    mode: Some(0o600),
                    ..Default::default()
                },
            )
            .is_err());
    }
    let after = fs.file_status("/parent").unwrap();
    assert_eq!((before.id, before.mode), (after.id, after.mode));
}

#[cfg(feature = "fault-injection")]
#[test]
fn failed_recursive_setattr_keeps_memory_and_store_unchanged() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let name = format!("tree-{}", Utils::rand_str(8));
    let root = format!("/{name}");
    let dir = format!("{root}/a");
    let file = format!("{dir}/file");
    let fs = filesystem("failed-recursive-attrs");
    fs.create(&file, true).unwrap();
    let paths = [&root, &dir, &file];
    let before: Vec<_> = paths
        .iter()
        .map(|path| fs.file_status(path).unwrap())
        .collect();
    let runtime = FaultRuntime::process();
    let rule_id = format!("setattr-failure-{name}");
    let rule = FaultRuleBuilder::named("master.metadata.set_attr.before_commit")
        .matches("root_name", name.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected setattr commit failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = fs.set_attr(
        &root,
        SetAttrOpts {
            recursive: true,
            mode: Some(0o600),
            owner: Some("changed-owner".into()),
            ..Default::default()
        },
    );
    runtime.remove(&rule_id).unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected setattr commit failure"));
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        for (path, expected) in paths.iter().zip(&before) {
            let actual = fs.file_status(path).unwrap();
            assert_eq!(
                (actual.id, actual.mode, &actual.owner, actual.mtime),
                (expected.id, expected.mode, &expected.owner, expected.mtime),
                "{path}"
            );
        }
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn failed_single_inode_setattr_keeps_memory_and_store_unchanged() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let name = format!("single-file-{}", Utils::rand_str(8));
    let file = format!("/{name}/file");
    let fs = filesystem("failed-single-attrs");
    fs.create(&file, true).unwrap();
    let before = fs.file_status(&file).unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = format!("setattr-single-failure-{name}");
    let rule = FaultRuleBuilder::named("master.metadata.set_attr.before_commit")
        .matches("root_name", "file")
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected single setattr commit failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = fs.set_attr(
        &file,
        SetAttrOpts {
            mode: Some(0o600),
            ..Default::default()
        },
    );
    runtime.remove(&rule_id).unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected single setattr commit failure"));

    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        let actual = fs.file_status(&file).unwrap();
        assert_eq!((actual.id, actual.mode), (before.id, before.mode));
    }
}

#[test]
fn recursive_setattr_rejects_missing_child_inode_without_partial_updates() {
    let fs = filesystem("recursive-attrs-dangling");
    fs.create("/tree/a/file", true).unwrap();
    let tree_before = fs.file_status("/tree").unwrap();
    let dir_before = fs.file_status("/tree/a").unwrap();
    let file_before = fs.file_status("/tree/a/file").unwrap();
    {
        let dir = fs.fs_dir.write();
        let path = InodePath::resolve(dir.root_ptr(), "/tree/a", dir.store_ref()).unwrap();
        let mut parent = path.get_last_inode().unwrap();
        parent
            .add_child(InodeView::new_entry("dangling".into(), i64::MAX))
            .unwrap();
    }
    let result = fs.set_attr(
        "/tree",
        SetAttrOpts {
            recursive: true,
            mode: Some(0o600),
            ..Default::default()
        },
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("references missing inode"));
    for restored in [false, true] {
        if restored {
            fs.restore_from_rocksdb().unwrap();
        }
        assert_eq!(fs.file_status("/tree").unwrap().mode, tree_before.mode);
        assert_eq!(fs.file_status("/tree/a").unwrap().mode, dir_before.mode);
        assert_eq!(
            fs.file_status("/tree/a/file").unwrap().mode,
            file_before.mode
        );
    }
}

#[test]
fn directory_setattr_keeps_tree_and_persists_recursive_attributes() {
    let fs = filesystem("directory-attrs");
    for path in ["/tree/a/file", "/tree/b/file"] {
        fs.create(path, true).unwrap();
    }
    let before = fs.file_status("/tree/a/file").unwrap();
    {
        let dir = fs.fs_dir.read();
        for path in ["/tree", "/tree/a", "/tree/b"] {
            let resolved = InodePath::resolve(dir.root_ptr(), path, dir.store_ref()).unwrap();
            let children = resolved
                .get_last_inode()
                .unwrap()
                .as_dir_ref()
                .unwrap()
                .children_handle();
            children.seed_shards_for_bulk_load(2048);
            assert!(format!("{children:?}").contains("sharded: true"));
        }
    }
    let root_before = fs.file_status("/tree").unwrap();
    let root = fs
        .set_attr(
            "/tree",
            SetAttrOpts {
                mode: Some(0o700),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(root.children_num, root_before.children_num);
    assert_eq!(root.nlink, root_before.nlink);
    assert_eq!(fs.file_status("/tree/a/file").unwrap().mode, before.mode);
    assert_eq!(fs.file_status("/tree/a/file").unwrap().id, before.id);
    assert_eq!(fs.list_status("/tree").unwrap().len(), 2);
    fs.set_attr(
        "/tree",
        SetAttrOpts {
            mode: Some(0o750),
            owner: Some("research-owner".into()),
            recursive: true,
            ..Default::default()
        },
    )
    .unwrap();
    for path in ["/tree/a", "/tree/b"] {
        let status = fs.file_status(path).unwrap();
        assert_eq!(status.mode, 0o750, "{path}");
        assert_eq!(status.owner, "research-owner", "{path}");
    }
    fs.restore_from_rocksdb().unwrap();
    for path in [
        "/tree",
        "/tree/a",
        "/tree/b",
        "/tree/a/file",
        "/tree/b/file",
    ] {
        let status = fs.file_status(path).unwrap();
        assert_eq!(status.mode, 0o750);
        assert_eq!(status.owner, "research-owner");
    }
    assert_eq!(fs.file_status("/tree/a/file").unwrap().id, before.id);
}
