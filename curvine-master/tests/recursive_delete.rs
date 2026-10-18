use curvine_config::ClusterConf;
use curvine_master::master::fs::MasterFilesystem;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_model::{
    ClientAddress, CommitBlock, CreateFileOpts, HeartbeatStatus, OpenFlags, WorkerCommand,
    WorkerInfo,
};
use curvine_runtime::common::Utils;

// FileSystemStats uses global metrics; serialize this test binary's fixtures.
static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn filesystem() -> MasterFilesystem {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!("recursive-delete-{}", Utils::rand_str(12)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    let mut worker = WorkerInfo {
        capacity: 1 << 40,
        available: 1 << 40,
        ..Default::default()
    };
    worker.address.worker_id = 11;
    fs.add_test_worker(worker);
    fs
}

fn prepare(fs: &MasterFilesystem, path: &str, count: usize) -> Vec<CommitBlock> {
    let mut opts = CreateFileOpts::with_create(true);
    opts.block_size = 1048576;
    fs.create_with_opts(path, opts, OpenFlags::new_create())
        .unwrap();
    let mut commits: Vec<CommitBlock> = Vec::new();
    let mut previous = None;
    for index in 0..count {
        let mut located = fs
            .add_block(
                path,
                None,
                ClientAddress::default(),
                commits.last().cloned().into_iter().collect(),
                vec![],
                index as i64 * 1048576,
                previous,
            )
            .unwrap();
        located.block.len = if index + 1 == count { 513 } else { 1048576 };
        previous = Some(located.block.clone());
        commits.push(CommitBlock::from(&located));
    }
    let len = if count == 0 {
        0
    } else {
        (count as i64 - 1) * 1048576 + 513
    };
    fs.complete_file(path, None, len, commits.clone(), "", false, None)
        .unwrap();
    commits
}

#[test]
fn deep_delete_preserves_external_hardlink_and_restore() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = filesystem();
    let mut path = String::from("/victim");
    for depth in 0..64 {
        path.push_str(&format!("/level-{depth}"));
        fs.create(format!("{path}/file"), true).unwrap();
    }
    let original = fs.file_status(format!("{path}/file")).unwrap();
    fs.link(format!("{path}/file"), "/survivor".to_owned())
        .unwrap();
    let unrelated = fs.create("/unrelated/file", true).unwrap();
    let result = fs.delete("/victim", true).unwrap();
    assert_eq!(result.inodes, 63);
    assert!(!fs.exists("/victim").unwrap());
    assert_eq!(fs.file_status("/survivor").unwrap().id, original.id);
    assert_eq!(fs.file_status("/survivor").unwrap().nlink, 1);
    let counts = fs.get_file_counts();
    assert_eq!(counts, (1, 2));
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.get_file_counts(), counts);
    assert!(!fs.exists("/victim").unwrap());
    assert_eq!(fs.file_status("/survivor").unwrap().id, original.id);
    assert_eq!(fs.file_status("/survivor").unwrap().nlink, 1);
    assert_eq!(fs.file_status("/unrelated/file").unwrap().id, unrelated.id);
}

#[test]
fn wide_delete_preserves_unrelated_namespace_and_counts() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = filesystem();
    let keep = fs.create("/keep", false).unwrap();
    for branch in 0..16 {
        for file in 0..32 {
            fs.create(format!("/victim/b{branch}/file-{file}"), true)
                .unwrap();
        }
    }
    fs.mkdir("/victim/empty/空目录", true).unwrap();
    let result = fs.delete("/victim", true).unwrap();
    assert_eq!(result.inodes, 512);
    assert_eq!(fs.get_file_counts(), (0, 1));
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.get_file_counts(), (0, 1));
    assert_eq!(fs.list_status("/").unwrap().len(), 1);
    assert_eq!(fs.file_status("/keep").unwrap().id, keep.id);
}

#[test]
fn rejected_delete_does_not_change_tree() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = filesystem();
    let child = fs.create("/victim/child", true).unwrap();
    let counts = fs.get_file_counts();
    assert!(fs.delete("/victim", false).is_err());
    assert!(fs.delete("/", true).is_err());
    assert!(fs.delete("/missing", true).is_err());
    assert_eq!(fs.get_file_counts(), counts);
    assert_eq!(fs.file_status("/victim/child").unwrap().id, child.id);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.get_file_counts(), counts);
    assert_eq!(fs.file_status("/victim/child").unwrap().id, child.id);
}

#[test]
fn recursive_delete_collects_nonempty_blocks_before_detaching_tree() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = filesystem();
    prepare(&fs, "/victim/a/file", 4);
    let before = fs.get_block_locations("/victim/a/file").unwrap();
    let expected: Vec<_> = before.block_locs.iter().map(|b| b.id).collect();
    let result = fs.delete("/victim", true).unwrap();
    assert_eq!(result.inodes, 1);
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = 11;
    let commands = fs
        .worker_manager
        .write()
        .heartbeat(
            &ClusterConf::default().cluster_id,
            HeartbeatStatus::Running,
            worker.address,
            worker.weight,
            worker.worker_session_id,
            worker.transfer_capabilities,
            worker.software_version,
            worker.startup_time_ms,
            vec![],
            None,
        )
        .unwrap();
    let mut actual = Vec::new();
    for command in commands {
        let WorkerCommand::DeleteBlock(cmd) = command;
        actual.extend(cmd.blocks);
    }
    let mut expected = expected;
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(fs.get_file_counts(), (0, 0));
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.get_file_counts(), (0, 0));
    assert!(!fs.exists("/victim").unwrap());
}
