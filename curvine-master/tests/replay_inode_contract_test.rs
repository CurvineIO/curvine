// Copyright 2026 Curvine Authors
// Licensed under the Apache License, Version 2.0 (see LICENSE).

//! Replay contract regression: an entry whose recorded inode ID no longer
//! exists must be an idempotent skip, not an error.
//!
//! Regression origin, input commit
//! `d8dae4a51e9eb83f1854dda3906d4ca322d4ecad`: `JournalLoader::with_locked_replay_inode`
//! pre-checked the recorded ID and returned `FileNotFound` before the
//! operation ran, which bypassed the `FileNotFound => Ok(())` skip that
//! `add_block` / `complete_file` already implement. That pre-check is the
//! reason these tests were red on d8; it is not a statement about any later
//! revision.
//!
//! These two tests pin the intended contract at the `apply_entry` boundary:
//! when the recorded inode is gone, replay returns Ok, does not rewrite a
//! replacement that now occupies the same path, and leaves the old ID absent.
//!
//! Scope: contract regression only. The assertions must not be relaxed to
//! match a failing implementation.
//!
//! Not asserted here: journal echo. `FsDir::journal_writer` and
//! `JournalWriter::take_entries` are `pub(crate)`, so an integration test
//! cannot read the writer's pending entries without widening production
//! visibility, which this task forbids.

use curvine_config::ClusterConf;
use curvine_error::FsResult;
use curvine_master::master::fs::MasterFilesystem;
use curvine_master::master::journal::{
    AddBlockEntry, CompleteFileEntry, JournalEntry, JournalSystem,
};
use curvine_master::master::meta::inode::{InodeFile, InodeView};
use curvine_master::master::meta::{BlockMeta, InodeId};
use curvine_master::Master;
use serde_json::json;
use std::path::PathBuf;

const DIR: &str = "/rc";
const PATH: &str = "/rc/file";
/// Length written to the old file so a wrong `complete_file` replay (which
/// replaces the inode payload) is distinguishable from the empty replacement.
const OLD_LEN: i64 = 4_096;

/// Removes the whole test tree when the test ends, including on early return.
struct TestTree(PathBuf);

impl Drop for TestTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn test_conf(name: &str) -> (ClusterConf, TestTree) {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(name);
    let base = PathBuf::from(&conf.master.meta_dir)
        .parent()
        .expect("meta dir has a parent")
        .to_path_buf();
    (conf, TestTree(base))
}

/// Reads one inode as a file DTO straight from the store.
fn inode_file(fs: &MasterFilesystem, id: i64) -> Option<InodeFile> {
    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    match view.store_ref().get_inode(id, None).expect("store read") {
        // `InodeView::File` holds a `NamedFile` whose `file` field is public.
        Some(InodeView::File(named)) => Some(named.file),
        Some(_) => panic!("inode {id} is not a file"),
        None => None,
    }
}

fn inode_absent(fs: &MasterFilesystem, id: i64) -> bool {
    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    view.store_ref()
        .get_inode(id, None)
        .expect("store read")
        .is_none()
}

/// Creates the old file, returns its recorded ID.
fn seed_old_file(fs: &MasterFilesystem) -> FsResult<i64> {
    fs.mkdir(DIR, true)?;
    fs.create(PATH, true)?;
    Ok(fs.file_status(PATH)?.id)
}

/// Deletes the only path of `old_id` and puts a fresh inode at the same path.
/// Returns the replacement's ID.
fn replace_path(fs: &MasterFilesystem, old_id: i64) -> FsResult<i64> {
    // `delete` uses SkipTrash, so the inode row is really removed rather than
    // parked in trash.
    fs.delete(PATH, false)?;
    assert!(
        inode_absent(fs, old_id),
        "fixture: recorded inode {old_id} must be gone from the store"
    );
    fs.create(PATH, true)?;
    let replacement = fs.file_status(PATH)?;
    assert_ne!(replacement.id, old_id, "replacement must be a new inode");
    Ok(replacement.id)
}

fn add_block_entry(path: &str, old_id: i64) -> FsResult<AddBlockEntry> {
    let block_id = InodeId::create_block_id(old_id, 1)?;
    let blocks = vec![BlockMeta::new(block_id, 1_024)];
    // Entry fields are `pub(crate)`; the type itself is pub and serialisable,
    // so the fixture goes through serde instead of widening visibility.
    let value = json!({
        "op_id": 1u64,
        "rpc_id": 0i64,
        "path": path,
        "blocks": serde_json::to_value(&blocks).expect("serialize blocks"),
        "commit_block": [],
    });
    Ok(serde_json::from_value(value).expect("AddBlockEntry fixture"))
}

fn complete_file_entry(path: &str, old_file: InodeFile) -> CompleteFileEntry {
    let value = json!({
        "op_id": 2u64,
        "rpc_id": 0i64,
        "path": path,
        "file": serde_json::to_value(&old_file).expect("serialize InodeFile"),
        "commit_blocks": [],
    });
    serde_json::from_value(value).expect("CompleteFileEntry fixture")
}

#[test]
fn add_block_replay_skips_when_recorded_inode_is_gone() -> FsResult<()> {
    let (conf, _tree) = test_conf("replay-contract-add-block");
    let js = JournalSystem::from_conf(&conf)?;
    let fs = js.fs();
    let loader = js.journal_loader();

    let old_id = seed_old_file(&fs)?;
    let entry = add_block_entry(PATH, old_id)?;
    let replacement_id = replace_path(&fs, old_id)?;

    // Contract: the recorded inode is gone, so replay is an idempotent skip.
    let replay = loader.apply_entry(JournalEntry::AddBlock(entry));
    assert!(
        replay.is_ok(),
        "add_block replay for a deleted recorded inode must be an idempotent \
         skip, got: {replay:?}"
    );

    assert!(
        inode_absent(&fs, old_id),
        "replay must not resurrect the deleted inode {old_id}"
    );
    let replacement = inode_file(&fs, replacement_id).expect("replacement inode must still exist");
    assert!(
        replacement.block_ids().is_empty(),
        "replay must not write the recorded blocks onto the replacement: {:?}",
        replacement.block_ids()
    );
    assert_eq!(
        fs.file_status(PATH)?.len,
        0,
        "replay must not change the replacement's length"
    );

    drop(loader);
    drop(fs);
    js.shutdown();
    Ok(())
}

#[test]
fn complete_file_replay_skips_when_recorded_inode_is_gone() -> FsResult<()> {
    let (conf, _tree) = test_conf("replay-contract-complete-file");
    let js = JournalSystem::from_conf(&conf)?;
    let fs = js.fs();
    let loader = js.journal_loader();

    let old_id = seed_old_file(&fs)?;
    // Give the old inode a non-zero length so a wrong replay is detectable:
    // `complete_file` replay replaces the whole inode payload with entry.file.
    fs.resize(PATH, curvine_model::FileAllocOpts::with_truncate(OLD_LEN))?;
    assert_eq!(
        fs.file_status(PATH)?.len,
        OLD_LEN,
        "fixture: old file must carry a distinguishing length"
    );
    let old_file = inode_file(&fs, old_id).expect("old inode must exist before deletion");
    let entry = complete_file_entry(PATH, old_file);
    let replacement_id = replace_path(&fs, old_id)?;

    let replay = loader.apply_entry(JournalEntry::CompleteFile(entry));
    assert!(
        replay.is_ok(),
        "complete_file replay for a deleted recorded inode must be an \
         idempotent skip, got: {replay:?}"
    );

    assert!(
        inode_absent(&fs, old_id),
        "replay must not resurrect the deleted inode {old_id}"
    );
    let replacement = inode_file(&fs, replacement_id).expect("replacement inode must still exist");
    assert_eq!(
        fs.file_status(PATH)?.len,
        0,
        "replay must not overwrite the replacement's payload with the recorded inode"
    );
    assert!(
        replacement.block_ids().is_empty(),
        "replay must not copy the recorded blocks onto the replacement"
    );

    drop(loader);
    drop(fs);
    js.shutdown();
    Ok(())
}
