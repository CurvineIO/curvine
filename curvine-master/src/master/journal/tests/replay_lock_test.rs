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

//! Journal replay lock regressions, run against a real JournalSystem through
//! `apply_entry` (the replay path, not the RPC layer).

use super::*;
use crate::master::journal::{AddBlockEntry, LinkEntry, RenameEntry};
use crate::master::meta::inode::InodeView;
use crate::master::Master;
use curvine_config::ClusterConf;

#[test]
fn rename_replay_accepts_same_parent_and_cross_parent() -> FsResult<()> {
    let (conf, root) = tests::ignore_reply_error_conf("rename-replay", true);
    let js = JournalSystem::from_conf(&conf)?;
    let fs = js.fs();
    let loader = js.journal_loader();

    // Same parent: both chains share the root and the parent, so the old
    // merged-dedup validation (which kept duplicate roots) rejected it.
    fs.mkdir("/same", true)?;
    let a = fs.create("/same/a", true)?;
    let b = fs.create("/same/b", true)?;
    let src_id = fs.file_status("/same/a")?.id;
    loader.apply_entry(JournalEntry::Rename(RenameEntry {
        op_id: 1,
        rpc_id: 0,
        src: "/same/a".to_string(),
        dst: "/same/c".to_string(),
        mtime: 5,
        flags: 0,
        src_inode_id: 0,
        dst_inode_id: 0,
    }))?;
    assert!(fs.file_status("/same/a").is_err());
    assert_eq!(fs.file_status("/same/c")?.id, src_id);
    assert_eq!(fs.file_status("/same/b")?.id, b.id);

    // Cross parent: two distinct parents under the shared root.
    fs.mkdir("/x", true)?;
    fs.mkdir("/y", true)?;
    let moved_id = fs.file_status("/same/c")?.id;
    loader.apply_entry(JournalEntry::Rename(RenameEntry {
        op_id: 2,
        rpc_id: 0,
        src: "/same/c".to_string(),
        dst: "/y/c".to_string(),
        mtime: 6,
        flags: 0,
        src_inode_id: 0,
        dst_inode_id: 0,
    }))?;
    assert!(fs.file_status("/same/c").is_err());
    assert_eq!(fs.file_status("/y/c")?.id, moved_id);
    assert!(fs.file_status("/x").is_ok());

    drop(loader);
    drop(fs);
    js.shutdown();
    let _ = fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn link_replay_accepts_same_parent_and_cross_parent() -> FsResult<()> {
    let (conf, root) = tests::ignore_reply_error_conf("link-replay", true);
    let js = JournalSystem::from_conf(&conf)?;
    let fs = js.fs();
    let loader = js.journal_loader();

    fs.mkdir("/ld", true)?;
    let f = fs.create("/ld/a", true)?;
    loader.apply_entry(JournalEntry::Link(LinkEntry {
        op_id: 1,
        rpc_id: 0,
        mtime: 5,
        src_path: "/ld/a".to_string(),
        dst_path: "/ld/b".to_string(),
    }))?;
    assert_eq!(fs.file_status("/ld/b")?.id, f.id);
    assert_eq!(fs.file_status("/ld/b")?.nlink, 2);

    fs.mkdir("/l2", true)?;
    loader.apply_entry(JournalEntry::Link(LinkEntry {
        op_id: 2,
        rpc_id: 0,
        mtime: 6,
        src_path: "/ld/a".to_string(),
        dst_path: "/l2/b".to_string(),
    }))?;
    assert_eq!(fs.file_status("/l2/b")?.id, f.id);

    drop(loader);
    drop(fs);
    js.shutdown();
    let _ = fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn add_block_replay_locks_the_recorded_inode_not_the_current_path() -> FsResult<()> {
    let (conf, root) = tests::ignore_reply_error_conf("add-block-replay", true);
    let js = JournalSystem::from_conf(&conf)?;
    let fs = js.fs();
    let loader = js.journal_loader();

    fs.mkdir("/ab", true)?;
    fs.create("/ab/file", true)?;
    let old = fs.file_status("/ab/file")?;
    // Build the replay entry from the pre-rename identity, then replace
    // the path with a different inode before replaying it. A hard link
    // keeps the recorded inode row alive after the rename.
    use crate::master::meta::{BlockMeta, InodeId};
    let block_id = InodeId::create_block_id(old.id, 1)?;
    let blocks = vec![BlockMeta::new(block_id, 4_096)];
    let commit = vec![curvine_model::CommitBlock {
        block_id,
        block_len: 4_096,
        locations: vec![],
    }];
    let entry = AddBlockEntry {
        op_id: 1,
        rpc_id: 0,
        path: "/ab/file".to_string(),
        blocks: blocks.clone(),
        commit_block: commit,
    };
    fs.link("/ab/file", "/ab/keep")?;
    fs.delete("/ab/file", false)?;
    fs.create("/ab/file", true)?;

    // Holding the recorded inode's read lock must block the replay: the
    // entry's ID, not the current path occupant, is what gets locked.
    let inode_locks = fs.inode_locks_for_test();
    let held = inode_locks.read_inode(old.id);
    let loader2 = js.journal_loader();
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let (tx, rx) = std::sync::mpsc::channel();
    let applier = std::thread::spawn(move || {
        started_tx.send(()).expect("applier started channel");
        tx.send(loader2.apply_entry(JournalEntry::AddBlock(entry)))
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("applier thread started");
    // The replay must still be waiting while the read lock is held.
    assert!(
        rx.recv_timeout(std::time::Duration::from_millis(100))
            .is_err(),
        "add_block replay must wait for the recorded inode's write lock"
    );
    drop(held);
    let sent = rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("replay must finish after the lock is released");
    applier.join().expect("applier thread must not panic");
    sent?;

    // Only the recorded inode got the block; the path replacement is
    // untouched.
    let replaced_id = fs.file_status("/ab/file")?.id;
    assert_ne!(replaced_id, old.id);
    let recorded = fs
        .fs_dir
        .view()?
        .store
        .get_inode(old.id, None)?
        .expect("recorded inode still exists via hard-link semantics");
    let InodeView::File(file) = &recorded else {
        panic!("recorded inode must be a file");
    };
    assert_eq!(file.blocks.len(), 1);
    assert_eq!(file.blocks[0].id, block_id);
    let replaced = fs
        .fs_dir
        .view()?
        .store
        .get_inode(replaced_id, None)?
        .unwrap();
    let InodeView::File(rf) = &replaced else {
        panic!("replacement must be a file");
    };
    assert!(rf.blocks.is_empty(), "the replacement must stay untouched");

    drop(loader);
    drop(fs);
    js.shutdown();
    let _ = fs::remove_dir_all(&root);
    Ok(())
}
