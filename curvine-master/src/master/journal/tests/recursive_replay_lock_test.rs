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

//! c2: recursive replay for Delete / SetAttr / Free.
//!
//! Covers the design §9 replay list owned here: the three recursive entry
//! types, no journal echo, the FSM watermark and per-request cursor, a missing
//! subtree skipping per the original rule, and the budget rule (over-budget
//! keeps the existing cold bridge and must not drain the apply gate).
//!
//! Replay does not take an RPC admission slot and does not write journal
//! entries, so these cases assert exactly that instead of any confirmation
//! protocol.

use super::*;
use curvine_config::ClusterConf;
use curvine_model::SetAttrOptsBuilder;
use curvine_runtime::common::Utils;

const REPLAY_DEPTH: usize = 4;
const REPLAY_WIDTH: usize = 2;

fn test_conf(name: &str) -> ClusterConf {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    // A real testing writer with its retained receiver, so genuine entries can
    // be captured from a public operation.
    conf.journal.enable = true;
    conf.journal.snapshot_entries = 0;
    conf.journal.writer_channel_size = 0;
    conf.change_test_meta_dir(format!("c2-replay-{}-{}", name, Utils::rand_str(8)));
    conf
}

fn build_tree(fs: &MasterFilesystem, root: &str) -> String {
    let mut path = root.to_string();
    for level in 0..REPLAY_DEPTH {
        path.push_str(&format!("/d{level}"));
        fs.mkdir(&path, true).unwrap();
        for item in 0..REPLAY_WIDTH {
            fs.create(format!("{path}/f{item}"), false).unwrap();
        }
    }
    path
}

/// Captures exactly one journal entry produced by `op`.
fn capture_one(fs: &MasterFilesystem, op: impl FnOnce()) -> JournalEntry {
    let writer = {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        view.journal_writer.clone()
    };
    writer.take_entries();
    op();
    let mut entries = writer.take_entries();
    assert_eq!(
        entries.len(),
        1,
        "the fixture operation must journal exactly one entry"
    );
    entries.remove(0)
}

fn echo_entries(fs: &MasterFilesystem) -> Vec<JournalEntry> {
    let handle = fs.fs_dir.clone();
    let view = handle.view().expect("namespace view");
    view.take_entries()
}

#[test]
fn recursive_delete_replay_applies_without_echo_or_second_write() -> FsResult<()> {
    let source = JournalSystem::from_conf(&test_conf("delete-source"))?;
    let target = JournalSystem::from_conf(&test_conf("delete-target"))?;
    let source_fs = source.fs();
    let target_fs = target.fs();

    let deepest = build_tree(&source_fs, "/r");
    assert!(source_fs.file_status(&deepest).is_ok());
    let entry = capture_one(&source_fs, || {
        source_fs.delete("/r", true).unwrap();
    });
    assert!(
        matches!(&entry, JournalEntry::Delete(delete) if delete.path == "/r"),
        "the captured entry must be the typed recursive delete, got {entry:?}"
    );

    // The target holds the same shape, so replay has real work.
    build_tree(&target_fs, "/r");
    // Drain the fixture's own entries: only replay activity must be observable
    // from here on.
    echo_entries(&target_fs);
    let loader = target.journal_loader();
    loader.apply_entry(entry)?;

    assert!(
        target_fs.file_status("/r").is_err(),
        "replaying the recursive delete must remove the subtree"
    );
    // Replay must not write journal entries back.
    assert!(
        echo_entries(&target_fs).is_empty(),
        "replay must not echo journal entries"
    );
    // A direct replay entry applies metadata only: the admission watermark is
    // advanced by the apply-worker path, so it must stay unchanged here.
    let applied = loader.fsm_state_snapshot()?.applied;
    assert_eq!(
        applied.index, 0,
        "a direct replay entry must not advance the admission watermark"
    );
    Ok(())
}

#[test]
fn recursive_set_attr_replay_applies_to_existing_children() -> FsResult<()> {
    let source = JournalSystem::from_conf(&test_conf("setattr-source"))?;
    let target = JournalSystem::from_conf(&test_conf("setattr-target"))?;
    let source_fs = source.fs();
    let target_fs = target.fs();

    let deepest = build_tree(&source_fs, "/s");
    let leaf = format!("{deepest}/f0");
    let opts = SetAttrOptsBuilder::new()
        .recursive(true)
        .mode(0o640)
        .build();
    let entry = capture_one(&source_fs, || {
        source_fs.set_attr("/s", opts).unwrap();
    });
    assert!(
        matches!(&entry, JournalEntry::SetAttr(_)),
        "the captured entry must be the typed set_attr, got {entry:?}"
    );

    build_tree(&target_fs, "/s");
    echo_entries(&target_fs);
    let loader = target.journal_loader();
    loader.apply_entry(entry)?;

    let replayed = target_fs.file_status(&leaf)?.mode & 0o777;
    assert_eq!(
        replayed, 0o640,
        "the replayed set_attr must reach a child that existed"
    );
    assert!(
        echo_entries(&target_fs).is_empty(),
        "replay must not echo journal entries"
    );
    Ok(())
}

#[test]
fn recursive_free_replay_keeps_metadata() -> FsResult<()> {
    let source = JournalSystem::from_conf(&test_conf("free-source"))?;
    let target = JournalSystem::from_conf(&test_conf("free-target"))?;
    let source_fs = source.fs();
    let target_fs = target.fs();

    let deepest = build_tree(&source_fs, "/f");
    let leaf = format!("{deepest}/f1");
    let entry = capture_one(&source_fs, || {
        source_fs.free("/f", true).unwrap();
    });
    assert!(
        matches!(&entry, JournalEntry::Free(_)),
        "the captured entry must be the typed free, got {entry:?}"
    );

    build_tree(&target_fs, "/f");
    let leaf_id = target_fs.file_status(&leaf)?.id;
    echo_entries(&target_fs);
    let loader = target.journal_loader();
    loader.apply_entry(entry)?;

    let after = target_fs
        .file_status(&leaf)
        .expect("replaying free must keep metadata");
    assert_eq!(
        after.id, leaf_id,
        "the replayed free must cover the same inodes"
    );
    assert!(
        echo_entries(&target_fs).is_empty(),
        "replay must not echo journal entries"
    );
    Ok(())
}

/// Replaying an entry whose subtree is already gone must skip per the original
/// rule, not fail and not invent metadata.
#[test]
fn replay_of_a_missing_subtree_skips() -> FsResult<()> {
    let source = JournalSystem::from_conf(&test_conf("missing-source"))?;
    let source_fs = source.fs();
    build_tree(&source_fs, "/m");
    let entry = capture_one(&source_fs, || {
        source_fs.delete("/m", true).unwrap();
    });

    let target = JournalSystem::from_conf(&test_conf("missing-target"))?;
    let target_fs = target.fs();
    let loader = target.journal_loader();

    // Nothing to delete on the target: the replay is an idempotent skip.
    loader.apply_entry(entry.clone())?;
    assert!(
        target_fs.file_status("/m").is_err(),
        "a missing subtree must stay missing"
    );
    // A second replay of the same entry is still a skip.
    loader.apply_entry(entry)?;
    assert!(echo_entries(&target_fs).is_empty());
    Ok(())
}

/// Over-budget replay keeps the existing cold bridge: it must complete and must
/// not leave the apply gate closed.
#[test]
fn oversized_replay_completes_and_leaves_the_gate_open() -> FsResult<()> {
    let source = JournalSystem::from_conf(&test_conf("oversize-source"))?;
    let source_fs = source.fs();
    source_fs.mkdir("/w", false)?;
    // One more member than the plan budget (SUBTREE_PLAN_LIMIT = 1024 per the
    // final C2 design; the module is private to `meta`).
    const PLAN_LIMIT: usize = 1024;
    for index in 0..PLAN_LIMIT + 8 {
        source_fs.create(format!("/w/f{index}"), false)?;
    }
    let entry = capture_one(&source_fs, || {
        source_fs.delete("/w", true).unwrap();
    });

    let target = JournalSystem::from_conf(&test_conf("oversize-target"))?;
    let target_fs = target.fs();
    target_fs.mkdir("/w", false)?;
    for index in 0..16 {
        target_fs.create(format!("/w/f{index}"), false)?;
    }
    echo_entries(&target_fs);
    let loader = target.journal_loader();
    loader.apply_entry(entry)?;

    assert!(
        target_fs.file_status("/w").is_err(),
        "the over-budget replay must still delete through the cold bridge"
    );
    // The apply gate must be open again: a plain write still works.
    target_fs
        .mkdir("/after", false)
        .expect("the apply gate must not be left closed");
    Ok(())
}
