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

use curvine_config::ClusterConf;
use curvine_error::FsError;
use curvine_master::master::fs::{DeleteMode, MasterFilesystem, PurgeStop, TrashPurger};
use curvine_master::master::journal::JournalSystem;
use curvine_master::master::MasterMonitor;
use curvine_master::Master;
use curvine_raft::raft::RoleState;
use curvine_runtime::common::Utils;
use curvine_runtime::sync::StateCtl;

static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn leader(term: u64) -> MasterMonitor {
    let monitor = MasterMonitor::new(StateCtl::new(RoleState::Leader.into()), StateCtl::new(0));
    monitor.set_term_for_test(term);
    monitor
}

#[test]
fn serving_waits_for_an_entry_of_the_current_term() {
    let monitor = leader(7);

    monitor.set_applied_term_for_test(6);
    assert!(!monitor.is_serving());

    monitor.set_applied_term_for_test(7);
    assert!(monitor.is_serving());

    monitor.set_term_for_test(8);
    assert!(!monitor.is_serving());
}

#[test]
fn followers_never_serve_writes() {
    let monitor = MasterMonitor::new(StateCtl::new(RoleState::Follower.into()), StateCtl::new(0));
    monitor.set_term_for_test(3);
    monitor.set_applied_term_for_test(3);
    assert!(!monitor.is_serving());
}

fn filesystem(name: &str) -> MasterFilesystem {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.journal.enable = false;
    conf.master.trash_enabled = true;
    conf.master.meta_dir =
        Utils::test_sub_dir(format!("serving-gate/meta-{}-{}", name, Utils::rand_str(6)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    // Elected for term 2 while the inherited tail of term 1 is still unapplied.
    fs.master_monitor.set_term_for_test(2);
    fs.master_monitor.set_applied_term_for_test(1);
    fs
}

#[test]
fn namespace_mutations_recheck_serving_under_the_lock() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = filesystem("mkdir");

    let err = match fs.mkdir("/blocked", false) {
        Ok(status) => panic!("a replaying leader allocated {:?}", status),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("is not serving writes"),
        "unexpected error: {err}"
    );
    assert!(fs.file_status("/blocked").is_err());

    fs.master_monitor.set_applied_term_for_test(2);
    fs.mkdir("/served", false).unwrap();
    assert!(fs.file_status("/served").unwrap().is_dir);
}

#[test]
fn trash_claim_waits_for_the_term_start_noop() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = filesystem("trash-claim");

    // Claiming creates `/.Trash/.pending` when it is missing, so it allocates an
    // inode id: a replaying leader must refuse before that.
    assert_eq!(fs.claim_trash_entry_for_purge("/.Trash/1-7").unwrap(), None);

    fs.master_monitor.set_applied_term_for_test(2);
    assert_eq!(
        fs.claim_trash_entry_for_purge("/.Trash/1-7").unwrap(),
        Some(None)
    );
}

#[test]
fn trash_delete_waits_for_the_term_start_noop() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = filesystem("trash-delete");

    fs.master_monitor.set_applied_term_for_test(2);
    fs.create("/kept/a.log", true).unwrap();

    // Elected for term 3 while the tail of term 2 is still unapplied.
    fs.master_monitor.set_term_for_test(3);
    fs.master_monitor.set_applied_term_for_test(2);

    // Both trash modes refuse before `/.Trash` can be created: a soft delete
    // creates the bin through `ensure_trash_dir`, which allocates an inode id.
    assert!(fs
        .delete_with_mode_if_active("/kept/a.log", false, DeleteMode::Default)
        .unwrap()
        .is_none());
    assert!(fs
        .delete_with_mode_if_active("/kept", true, DeleteMode::AsyncPurge)
        .unwrap()
        .is_none());
    assert!(fs.file_status("/kept/a.log").is_ok());
    assert!(
        fs.file_status("/.Trash").is_err(),
        "a refused delete created /.Trash"
    );

    // Once the tail replays the same call goes through and trashes the entry.
    fs.master_monitor.set_applied_term_for_test(3);
    let res = fs
        .delete_with_mode_if_active("/kept/a.log", false, DeleteMode::Default)
        .unwrap()
        .expect("a serving leader deletes");
    assert!(res.trashed);
    assert!(fs.file_status("/.Trash").is_ok());
}

#[test]
fn trash_stage_waits_for_the_term_start_noop() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = filesystem("trash-stage");

    fs.master_monitor.set_applied_term_for_test(2);
    fs.create("/staged.log", true).unwrap();
    fs.delete_with_mode("/staged.log", false, DeleteMode::Default)
        .unwrap();
    assert_eq!(fs.list_trash().unwrap().len(), 1);

    fs.master_monitor.set_term_for_test(3);
    fs.master_monitor.set_applied_term_for_test(2);

    // A non-empty bin is rejected instead of being renamed into `/.pending`.
    let err = fs.stage_trash_for_purge().unwrap_err();
    assert!(
        matches!(err, FsError::NotLeaderMaster(_)),
        "unexpected error: {err}"
    );
    assert_eq!(fs.list_trash().unwrap().len(), 1);
    assert!(fs.scan_pending_trash(16, None).unwrap().paths.is_empty());

    fs.master_monitor.set_applied_term_for_test(3);
    assert_eq!(fs.stage_trash_for_purge().unwrap(), 1);
    assert!(fs.list_trash().unwrap().is_empty());
}

#[test]
fn trash_purge_waits_for_the_term_start_noop() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = filesystem("trash-purge");

    fs.master_monitor.set_applied_term_for_test(2);
    fs.create("/purged/a.log", true).unwrap();
    fs.delete_with_mode("/purged/a.log", false, DeleteMode::Default)
        .unwrap();
    let entry = fs.list_trash().unwrap().remove(0).path;
    let claimed = fs
        .claim_trash_entry_for_purge(&entry)
        .unwrap()
        .expect("a serving leader claims")
        .expect("the entry exists");
    let purger = TrashPurger::new(fs.clone(), fs.master_monitor.clone());

    fs.master_monitor.set_term_for_test(3);
    fs.master_monitor.set_applied_term_for_test(2);

    // A replaying leader applies no batch: the entry it claimed stays in
    // `/.Trash/.pending` for the next serving master.
    assert!(purger.purge_entry(&claimed).unwrap().is_none());
    let (outcome, stop) = purger.purge_entry_partial(&claimed);
    assert!(
        matches!(stop, Some(PurgeStop::NotServing)),
        "unexpected stop: {stop:?}"
    );
    assert_eq!(outcome.batches, 0, "a refused purge applied a batch");
    assert_eq!(outcome.inodes, 0);
    // The claimed entry is still staged: `file_status` hides `/.Trash/.pending`
    // internals, so the pending scan is the observable proof.
    assert_eq!(fs.scan_pending_trash(16, None).unwrap().paths.len(), 1);

    fs.master_monitor.set_applied_term_for_test(3);
    let outcome = purger
        .purge_entry(&claimed)
        .unwrap()
        .expect("a serving leader purges");
    assert!(outcome.batches > 0);
    assert!(outcome.inodes > 0);
    assert!(fs.scan_pending_trash(16, None).unwrap().paths.is_empty());
}
