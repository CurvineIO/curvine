// Copyright 2026 OPPO
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

//! The public link/symlink entries must route a sharded `Fallback` to the
//! exclusive owner body instead of reporting success without executing.

use super::*;
use curvine_model::state::FileType;

#[test]
fn link_fallback_surfaces_the_real_destination_conflict() {
    let fs = test_fs("link-fallback");
    fs.create("/src", true).unwrap();
    fs.mkdir("/dir", true).unwrap();

    // Park the sharded helper after planning, before any inode lock, with a
    // driver-side entered ack; the named sender stays alive until the driver
    // has finished its own change, so the channel never closes early.
    // A random destination keeps the process-global gate key unique per run.
    let dst = format!("/dir/dst-{}", curvine_runtime::common::Utils::rand_str(8));
    let (release, entered) = memory_read_gates::install_with_entered(&format!("link-plan-{dst}"));
    let worker_fs = fs.clone();
    let dst_for_worker = dst.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || tx.send(worker_fs.link("/src", &dst_for_worker)));

    entered
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the link helper must reach its plan gate");
    // A normal sharded create on the parked worker's destination path: the
    // parent write is free because the helper holds no inode lock yet.
    fs.create(&dst, true).unwrap();
    let driver_dst_id = fs.file_status(&dst).unwrap().id;
    release.send(()).unwrap();
    drop(release);

    let sent = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("link must finish after the gate is released");
    let _ = worker.join().expect("link worker must not panic");
    let err = sent.unwrap_err();
    assert!(
        matches!(err, FsError::FileAlreadyExists(_)),
        "the helper's Fallback must run the real cold body, got {err:?}"
    );
    // The driver-created file is untouched (exact id and kind) and the link
    // did not exist.
    let driver_file = fs.file_status(&dst).unwrap();
    assert_eq!(driver_file.id, driver_dst_id);
    assert_eq!(driver_file.file_type, FileType::File);
    assert_eq!(driver_file.nlink, 1);
    assert_eq!(fs.file_status("/src").unwrap().nlink, 1);
}

#[test]
fn symlink_fallback_surfaces_the_real_destination_conflict() {
    let fs = test_fs("symlink-fallback");
    fs.create("/real", true).unwrap();
    fs.mkdir("/dir", true).unwrap();

    // Same per-run uniqueness for the symlink gate key.
    let link = format!("/dir/link-{}", curvine_runtime::common::Utils::rand_str(8));
    let (release, entered) =
        memory_read_gates::install_with_entered(&format!("symlink-plan-{link}"));
    let worker_fs = fs.clone();
    let link_for_worker = link.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send(worker_fs.symlink_with_owner_group(
            "/real",
            &link_for_worker,
            false,
            0o644,
            None,
            None,
        ))
    });

    entered
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the symlink helper must reach its plan gate");
    fs.create(&link, true).unwrap();
    let driver_link_id = fs.file_status(&link).unwrap().id;
    release.send(()).unwrap();
    drop(release);

    let sent = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("symlink must finish after the gate is released");
    let _ = worker.join().expect("symlink worker must not panic");
    let err = sent.unwrap_err();
    assert!(
        matches!(err, FsError::FileAlreadyExists(_)),
        "the helper's Fallback must run the real cold body, got {err:?}"
    );
    let driver_file = fs.file_status(&link).unwrap();
    assert_eq!(driver_file.id, driver_link_id);
    assert_eq!(driver_file.file_type, FileType::File);
    assert_eq!(driver_file.nlink, 1);
}

#[test]
fn rename_with_a_missing_path_reports_file_not_found() {
    let fs = test_fs("rename-missing");
    fs.mkdir("/p", true).unwrap();

    // Both paths miss their last component: the same-parent comparison used
    // to treat the shared parent id as "same inode" and return Ok(true).
    let err = fs
        .rename("/p/missing1", "/p/missing2", RenameFlags::empty())
        .unwrap_err();
    assert!(
        matches!(err, FsError::FileNotFound(_)),
        "renaming two missing paths must be FileNotFound, got {err:?}"
    );
    assert!(fs.file_status("/p").is_ok());

    // A missing source with an existing destination is also a real error.
    fs.create("/p/exists", true).unwrap();
    let err = fs
        .rename("/p/missing", "/p/exists", RenameFlags::empty())
        .unwrap_err();
    assert!(matches!(err, FsError::FileNotFound(_)), "got {err:?}");
    assert!(fs.file_status("/p/exists").is_ok());

    // Exact same path keeps the original false contract (bff4e7dc5,
    // b1d6bd568); two distinct hardlink names of one inode still report true.
    assert!(!fs
        .rename("/p/exists", "/p/exists", RenameFlags::empty())
        .unwrap());
}

#[test]
fn pending_reads_are_rejected_by_the_public_block_read_entries() {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.trash_enabled = true;
    conf.master.meta_dir = Utils::test_sub_dir(format!(
        "master-fs-pending-reads/meta-{}",
        Utils::rand_str(6)
    ));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "master-fs-pending-reads/journal-{}",
        Utils::rand_str(6)
    ));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.master_monitor
        .set_role_for_test(curvine_raft::raft::RoleState::Leader);

    // Build a real pending entry through the trash flow.
    fs.create("/data/a.log", true).unwrap();
    fs.delete_with_mode("/data/a.log", false, DeleteMode::Default)
        .unwrap();
    let entry = fs.list_trash().unwrap().remove(0);
    let claimed = fs.claim_trash_entry_for_purge(&entry.path).unwrap();
    assert!(claimed.is_some(), "the entry must be claimable");
    let pending_path = trash::pending_entry_path(entry.path.rsplit('/').next().unwrap());

    // The pending entry resolves internally but both public block-read
    // entries reject it with the resolve_path FileNotFound boundary.
    let internal = InodePath::resolve(
        fs.fs_dir.view().unwrap().root_node(),
        &pending_path,
        &fs.fs_dir.view().unwrap().store,
    )
    .unwrap();
    assert!(internal.is_full() && internal.get_last_inode().is_some());

    let locations = fs.get_block_locations(&pending_path);
    let details = fs.get_file_block_details(&pending_path);
    for (entry_name, err) in [
        ("get_block_locations", locations.err()),
        ("get_file_block_details", details.err()),
    ] {
        let err = err.unwrap_or_else(|| panic!("{entry_name} must fail on a pending path"));
        assert!(
            matches!(err, FsError::FileNotFound(_)),
            "{entry_name} on a pending path must be FileNotFound, got {err:?}"
        );
    }
}
