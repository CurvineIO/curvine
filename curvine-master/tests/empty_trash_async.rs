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
use curvine_master::master::fs::{DeleteMode, MasterFilesystem};
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_raft::raft::RoleState;
use curvine_runtime::common::Utils;

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
        Utils::test_sub_dir(format!("empty-trash/meta-{}-{}", name, Utils::rand_str(6)));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "empty-trash/journal-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    fs
}

/// Trashes `entries` directories of `per_dir` files each and returns the number
/// of file inodes that a full purge must reclaim.
fn seed_trash(fs: &MasterFilesystem, entries: usize, per_dir: usize) -> u64 {
    for entry in 0..entries {
        for file in 0..per_dir {
            fs.create(format!("/data/e{entry}/f{file}"), true).unwrap();
        }
        fs.delete_with_mode(format!("/data/e{entry}"), true, DeleteMode::Default)
            .unwrap();
    }
    assert_eq!(fs.list_trash().unwrap().len(), entries);
    (entries * per_dir) as u64
}

fn pending_children(fs: &MasterFilesystem) -> usize {
    // `.pending` is hidden from client-facing listings, so count it through the
    // purger's own scan.
    fs.scan_pending_trash(usize::MAX, None)
        .map(|page| page.paths.len())
        .unwrap_or(0)
}

#[test]
fn async_empty_acknowledges_then_the_purger_reclaims() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("ack");
    let expected = seed_trash(&fs, 10, 100);

    // Non-wait mode renames the entries into the staging directory and returns
    // immediately: no subtree is walked, so no inode is reported.
    let (accepted, res) = fs.empty_trash0(false).unwrap();
    assert_eq!(accepted, 10);
    assert_eq!(res.inodes, 0);
    assert_eq!(res.bytes, 0);
    assert!(
        fs.list_trash().unwrap().is_empty(),
        "the bin is empty at once"
    );
    assert_eq!(pending_children(&fs), 10, "entries wait in the purger lane");

    // The background purger (or an explicit drain) reclaims them in bounded
    // batches through the same engine as a retention sweep.
    let drained = fs.drain_pending_trash().unwrap().unwrap();
    assert_eq!(drained.inodes, expected);
    assert_eq!(pending_children(&fs), 0);
    assert_eq!(fs.get_file_counts().1, 0, "every file inode is reclaimed");
}

#[test]
fn wait_mode_still_reports_real_counts() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("wait");
    let expected = seed_trash(&fs, 4, 25);

    let res = fs.empty_trash().unwrap();

    assert_eq!(res.inodes, expected);
    assert!(
        res.blocks.is_empty(),
        "blocks are released by the purge batches"
    );
    assert_eq!(pending_children(&fs), 0);
    assert_eq!(fs.get_file_counts().1, 0);
}

#[test]
fn staged_empty_survives_a_restart() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("restart");
    let expected = seed_trash(&fs, 3, 20);

    let (accepted, _) = fs.empty_trash0(false).unwrap();
    assert_eq!(accepted, 3);
    // A master restart reloads the namespace from rocksdb; the staged entries
    // are part of it, so the next purger cycle finishes the job.
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(pending_children(&fs), 3);

    let drained = fs.drain_pending_trash().unwrap().unwrap();
    assert_eq!(drained.inodes, expected);
    assert_eq!(pending_children(&fs), 0);
}

#[test]
fn async_empty_is_idempotent() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("idempotent");
    seed_trash(&fs, 2, 5);

    let (first, _) = fs.empty_trash0(false).unwrap();
    assert_eq!(first, 2);

    // Nothing is left to stage, and the staged entries are still drained by the
    // waiting variant, so neither call fails or double counts.
    let (second, res) = fs.empty_trash0(false).unwrap();
    assert_eq!(second, 0);
    assert_eq!(res.inodes, 0);
    assert_eq!(fs.drain_pending_trash().unwrap().unwrap().inodes, 10);
    assert_eq!(fs.drain_pending_trash().unwrap().unwrap().inodes, 0);
}
