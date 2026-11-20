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
use curvine_master::master::fs::MasterFilesystem;
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
