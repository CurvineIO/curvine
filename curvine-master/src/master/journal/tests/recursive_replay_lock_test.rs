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
use crate::master::meta::InodeLockMode;
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

    // G2: a recursive set_attr has need_locations=false, so it must stay on the
    // ordinary sharded path instead of the cold bridge. Holding a view makes
    // that observable: `with_replay_cold` opens with `begin_cold`, which waits
    // for every view strong reference and would fail here after the 30s cold
    // timeout, while the hot path is unaffected by the held view.
    //
    // Scope of this assertion: it holds for a subtree small enough to form a
    // plan - the fixture below is 3 levels x 2 children. It does NOT cover what
    // `need_locations=false` should do when the topology itself exceeds the
    // plan budget; that path is outside this case (and outside this row).
    // Cost of a regression: this case goes red after the cold timeout rather
    // than immediately, which is the price of observing the real mechanism.
    let held = target_fs.fs_dir.view().expect("namespace view");
    let replayed_result = loader.apply_entry(entry);
    drop(held);
    replayed_result.expect(
        "a need_locations=false set_attr replay must not enter the cold bridge \
         (a held view would have blocked begin_cold)",
    );

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
    use crate::master::meta::subtree_write_plan::SUBTREE_PLAN_LIMIT;

    let source = JournalSystem::from_conf(&test_conf("oversize-source"))?;
    let source_fs = source.fs();
    // The source only has to produce the entry; its own size proves nothing
    // about the target.
    source_fs.mkdir("/w", false)?;
    source_fs.create("/w/f0", false)?;
    let entry = capture_one(&source_fs, || {
        source_fs.delete("/w", true).unwrap();
    });

    let target = JournalSystem::from_conf(&test_conf("oversize-target"))?;
    let target_fs = target.fs();
    target_fs.mkdir("/w", false)?;
    // The TARGET is the oversized one: one more member than the plan budget.
    for index in 0..SUBTREE_PLAN_LIMIT + 1 {
        target_fs.create(format!("/w/f{index}"), false)?;
    }
    echo_entries(&target_fs);

    // Direct evidence that this target cannot be planned, taken under a short
    // view that is dropped before anything else runs.
    {
        let handle = target_fs.fs_dir.clone();
        let view = handle.view()?;
        let plan = crate::master::meta::subtree_write_plan::SubtreeWritePlan::capture(
            &view,
            "/w",
            InodeLockMode::Write,
        )?;
        assert!(
            plan.is_none(),
            "the target subtree itself must exceed the {SUBTREE_PLAN_LIMIT}-entry plan budget"
        );
    }

    let loader = target.journal_loader();
    loader.apply_entry(entry)?;

    assert!(
        target_fs.file_status("/w").is_err(),
        "the over-budget replay must still delete through the cold bridge"
    );
    assert!(
        echo_entries(&target_fs).is_empty(),
        "replay must not echo journal entries"
    );
    // The apply gate must be open again, and the plan must be reachable for a
    // small path afterwards (an oversized one must not poison later replays).
    target_fs
        .mkdir("/after", false)
        .expect("the apply gate must not be left closed");
    {
        let handle = target_fs.fs_dir.clone();
        let view = handle.view()?;
        let plan = crate::master::meta::subtree_write_plan::SubtreeWritePlan::capture(
            &view,
            "/after",
            InodeLockMode::Write,
        )?;
        assert!(
            plan.is_some(),
            "a small subtree must still be plannable after an oversized replay"
        );
    }
    Ok(())
}

/// Regression contract for the `with_replay_locks` fix (1304cb662): a body that
/// returns a real error without moving the publication sequence must propagate
/// the error verbatim and leave the holder OPEN. Retiring the holder on a pure
/// business error would make every later replay fail.
#[test]
fn replay_body_error_propagates_and_keeps_the_holder_open() -> FsResult<()> {
    let js = JournalSystem::from_conf(&test_conf("replay-body-error"))?;
    let fs = js.fs();
    let loader = js.journal_loader();
    build_tree(&fs, "/e");

    let seq_before = fs.journal_writer.failure_seq();
    let outcome = loader.with_replay_locks("/e/d0", InodeLockMode::Write, false, |_view| {
        Err::<(), _>(curvine_error::FsError::file_not_found("/e/d0"))
    });

    let error = outcome.expect_err("the body error must propagate");
    assert!(
        error.to_string().contains("/e/d0"),
        "the original body error must surface, got {error}"
    );
    // G7: the loader keeps the body error as a typed object (not a flattened
    // string), so callers can still classify it. A missing path must stay
    // recoverable as FileNotFound instead of degrading into text matching.
    let typed = error
        .downcast_ref::<curvine_error::FsError>()
        .expect("the propagated body error must keep its concrete FsError type");
    assert!(
        matches!(typed.kind(), curvine_error::ErrorKind::FileNotFound),
        "a missing path must classify as FileNotFound, got {:?}",
        typed.kind()
    );
    assert_eq!(
        fs.journal_writer.failure_seq(),
        seq_before,
        "a pure body error must not count as a publication failure"
    );
    assert!(
        !fs.fs_dir.is_failed(),
        "a pure business error must not retire the holder"
    );
    assert!(
        fs.fs_dir.view().is_ok(),
        "the namespace view must stay accessible after a body error"
    );

    // A later replay still executes on the same loader.
    let entry = capture_one(&fs, || {
        fs.create("/e/after", false).unwrap();
    });
    loader.apply_entry(entry)?;
    assert!(
        fs.file_status("/e/after").is_ok(),
        "a later replay must still apply after a pure body error"
    );
    Ok(())
}

/// Real admission boundary, not a direct `apply_entry`: an entry applied
/// through the public AppStorage entry point must advance the FSM watermark and
/// persist its per-request record. The earlier cases in this file only prove
/// that a direct apply leaves the watermark alone, which is a different claim.
#[test]
fn app_storage_apply_advances_the_real_fsm_and_request_record() -> FsResult<()> {
    use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
    use curvine_raft::utils::SerdeUtils;
    use raft::eraftpb::Entry;

    let js = JournalSystem::from_conf(&test_conf("app-storage"))?;
    let fs = js.fs();
    let loader = js.journal_loader();
    build_tree(&fs, "/g");
    echo_entries(&fs);
    let op = capture_one(&fs, || {
        fs.create("/g/after", false).unwrap();
    });

    let req_id: i64 = 9_001;
    let mut batch = JournalBatch::new(1);
    batch.push(op);
    let entry = Entry {
        index: 1,
        term: 1,
        data: batch
            .encode()
            .map_err(|e| curvine_error::FsError::from(e.to_string()))?,
        context: SerdeUtils::serialize(&req_id)
            .map_err(|e| curvine_error::FsError::from(e.to_string()))?
            .into(),
        ..Default::default()
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(async { loader.apply(true, ApplyMsg::new_entry(entry)).await })
        .expect("AppStorage apply must succeed");

    let applied = loader.fsm_state_snapshot()?.applied;
    assert_eq!(
        (applied.index, applied.term),
        (1, 1),
        "an entry applied through AppStorage must advance the real FSM watermark"
    );
    let record = {
        let handle = fs.fs_dir.clone();
        let view = handle.view().expect("namespace view");
        view.get_rocks_store()
            .get_applied_req(req_id)
            .expect("read applied-request record")
    };
    assert_eq!(
        record.map(|row| (row.index, row.term)),
        Some((1, 1)),
        "the applied-request record must be persisted for the admission path"
    );
    Ok(())
}

#[path = "recursive_replay_location_test.rs"]
mod recursive_replay_location_test;

#[path = "replay_free_test.rs"]
mod replay_free_test;
