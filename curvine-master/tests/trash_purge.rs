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

use curvine_config::ClusterConf;
use curvine_master::master::fs::MasterFilesystem;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_runtime::common::{LocalTime, Utils};

// FileSystemStats uses global metrics; serialize this test binary's fixtures.
static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn trash_fs(name: &str) -> MasterFilesystem {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.journal.enable = false;
    conf.master.trash_enabled = true;
    conf.master.meta_dir =
        Utils::test_sub_dir(format!("trash-purge/meta-{}-{}", name, Utils::rand_str(6)));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "trash-purge/journal-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    JournalSystem::fs_only_for_test(&conf).unwrap()
}

fn trash_dir(fs: &MasterFilesystem, entry: &str) -> String {
    fs.create(format!("{entry}/a/file-a"), true).unwrap();
    fs.create(format!("{entry}/a/file-b"), true).unwrap();
    fs.create(format!("{entry}/b/file-c"), true).unwrap();
    fs.delete_with_opts(entry, true, false).unwrap();
    fs.list_trash().unwrap().remove(0).path
}

#[test]
fn purge_paths_removes_only_the_listed_batch() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("partial");
    let entry = trash_dir(&fs, "/victim");
    let before = fs.get_file_counts();

    // First batch: the two leaves of /a only.
    let first = vec![format!("{entry}/a/file-a"), format!("{entry}/a/file-b")];
    let res = {
        let fs_dir = fs.fs_dir();
        let mut fs_dir = fs_dir.write();
        fs_dir
            .purge_paths(&first, LocalTime::mills() as i64)
            .unwrap()
    };
    assert_eq!(res.inodes, 2);
    assert!(!fs.exists(&format!("{entry}/a/file-a")).unwrap());
    assert!(fs.exists(&format!("{entry}/b/file-c")).unwrap());

    // Second batch: the now-empty directory and its sibling leaf.
    let second = vec![format!("{entry}/a"), format!("{entry}/b/file-c")];
    let res = {
        let fs_dir = fs.fs_dir();
        let mut fs_dir = fs_dir.write();
        fs_dir
            .purge_paths(&second, LocalTime::mills() as i64)
            .unwrap()
    };
    assert_eq!(res.inodes, 1);
    assert!(fs.exists(&format!("{entry}/b")).unwrap());

    // The entry directory itself is the producer's last batch.
    let res = {
        let fs_dir = fs.fs_dir();
        let mut fs_dir = fs_dir.write();
        fs_dir
            .purge_paths(&[entry.clone()], LocalTime::mills() as i64)
            .unwrap()
    };
    assert_eq!(res.inodes, 0);
    assert!(!fs.exists(&entry).unwrap());
    assert!(fs.exists("/.Trash").unwrap());
    // `get_file_counts` is (dirs, files): the entry, its two subdirectories and
    // the three files are gone, so only the trash root remains.
    assert_eq!(before, (4, 3));
    assert_eq!(fs.get_file_counts(), (1, 0));
}

#[test]
fn purge_paths_is_idempotent_across_retries() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("idempotent");
    let entry = trash_dir(&fs, "/victim");

    let batch = vec![
        format!("{entry}/a/file-a"),
        format!("{entry}/a/file-b"),
        format!("{entry}/missing"),
    ];
    let first = {
        let fs_dir = fs.fs_dir();
        let mut fs_dir = fs_dir.write();
        fs_dir
            .purge_paths(&batch, LocalTime::mills() as i64)
            .unwrap()
    };
    assert_eq!(first.inodes, 2);

    // A replayed or retried batch must be a no-op instead of failing on the
    // paths it already removed.
    let second = {
        let fs_dir = fs.fs_dir();
        let mut fs_dir = fs_dir.write();
        fs_dir
            .purge_paths(&batch, LocalTime::mills() as i64)
            .unwrap()
    };
    assert_eq!(second.inodes, 0);
    assert_eq!(second.bytes, 0);
    assert!(second.blocks.is_empty());
    assert!(fs.exists(&format!("{entry}/b/file-c")).unwrap());

    fs.restore_from_rocksdb().unwrap();
    assert!(!fs.exists(&format!("{entry}/a/file-a")).unwrap());
    assert!(fs.exists(&format!("{entry}/b/file-c")).unwrap());
}
