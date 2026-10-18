use curvine_config::ClusterConf;
use curvine_master::master::fs::MasterFilesystem;
use curvine_master::master::journal::JournalSystem;
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
fn directory_setattr_keeps_tree_and_persists_recursive_attributes() {
    let fs = filesystem("directory-attrs");
    for path in ["/tree/a/file", "/tree/b/file"] {
        fs.create(path, true).unwrap();
    }
    let before = fs.file_status("/tree/a/file").unwrap();
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
