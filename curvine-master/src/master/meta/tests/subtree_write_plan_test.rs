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

//! c2: pure planning unit tests for `SubtreeWritePlan`.
//!
//! Scope is design §9's planning list only: empty directories, FileId edges,
//! every file name taking a Write lock, root filtering, stale restore epoch,
//! deep version changes, token-before-enumerate, duplicate hard-link names in
//! the budget, and the bounded enumeration / limit boundary. No business
//! routing, no fault injection, no call into the public write RPC.
//!
//! The module under test (`meta/subtree_write_plan.rs`) is owned by the main
//! developer and is not in this tree yet, so this file is not registered or
//! compiled here. The registration line and the real default/fault run happen
//! in the main developer's integration tree.

use super::super::subtree_write_plan::{SubtreeWritePlan, SUBTREE_PLAN_LIMIT};
use super::super::{InodeLockMode, InodeLockRequest};
use crate::master::fs::MasterFilesystem;
use crate::master::journal::JournalSystem;
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_runtime::common::Utils;
use std::time::Duration;

fn test_fs(name: &str) -> MasterFilesystem {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.change_test_meta_dir(format!("subtree-plan-{}-{}", name, Utils::rand_str(8)));
    JournalSystem::fs_only_for_test(&conf).expect("metadata filesystem")
}

/// Requests for one inode id, whatever depth it carries.
fn modes_for(requests: &[InodeLockRequest], id: i64) -> Vec<InodeLockMode> {
    requests
        .iter()
        .filter(|request| request.inode_id == id)
        .map(|request| request.mode)
        .collect()
}

fn has_write(requests: &[InodeLockRequest], id: i64) -> bool {
    modes_for(requests, id)
        .iter()
        .any(|mode| *mode == InodeLockMode::Write)
}

#[test]
fn empty_directory_plan_covers_the_directory_itself() -> curvine_error::FsResult<()> {
    let fs = test_fs("empty-dir");
    fs.mkdir("/empty", false)?;
    let dir_id = fs.file_status("/empty")?.id;

    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    let plan = SubtreeWritePlan::capture(&view, "/empty", InodeLockMode::Write)
        .expect("capture must succeed")
        .expect("an empty directory is a usable plan");

    let requests = plan.inode_requests();
    assert!(
        has_write(requests, dir_id),
        "an empty directory must still take a Write lock on itself: {requests:?}"
    );
    // No file ids may be invented for an empty directory.
    assert_eq!(
        requests.iter().filter(|r| r.inode_id == dir_id).count(),
        1,
        "the target directory must appear exactly once: {requests:?}"
    );
    Ok(())
}

#[test]
fn file_children_are_locked_by_id() -> curvine_error::FsResult<()> {
    let fs = test_fs("file-id");
    fs.mkdir("/a", false)?;
    fs.create("/a/f", false)?;
    let file_id = fs.file_status("/a/f")?.id;

    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    let plan = SubtreeWritePlan::capture(&view, "/a", InodeLockMode::Write)
        .expect("capture must succeed")
        .expect("plan expected");

    let requests = plan.inode_requests();
    assert!(
        has_write(requests, file_id),
        "a FileEntry child must enter the Write set by its file id: {requests:?}"
    );
    Ok(())
}

#[test]
fn every_file_name_in_the_subtree_gets_a_write_lock() -> curvine_error::FsResult<()> {
    let fs = test_fs("all-files");
    fs.mkdir("/tree/d0/d1", true)?;
    fs.create("/tree/d0/a", false)?;
    fs.create("/tree/d0/d1/b", false)?;
    let a = fs.file_status("/tree/d0/a")?.id;
    let b = fs.file_status("/tree/d0/d1/b")?.id;
    let d0 = fs.file_status("/tree/d0")?.id;
    let d1 = fs.file_status("/tree/d0/d1")?.id;

    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    let plan = SubtreeWritePlan::capture(&view, "/tree", InodeLockMode::Write)
        .expect("capture must succeed")
        .expect("plan expected");

    let requests = plan.inode_requests();
    for (label, id) in [("d0", d0), ("d1", d1), ("a", a), ("b", b)] {
        assert!(
            has_write(requests, id),
            "every directory and file in the subtree must be Write ({label}): {requests:?}"
        );
    }
    Ok(())
}

#[test]
fn root_is_written_when_it_is_the_target_and_filtered_as_ancestor() -> curvine_error::FsResult<()> {
    let fs = test_fs("root-filter");
    fs.mkdir("/x", false)?;
    let root_id = {
        let handle = fs.fs_dir();
        let view = handle.view().expect("namespace view");
        view.root_view().id()
    };
    let x = fs.file_status("/x")?.id;

    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");

    // The mutated parent keeps its requested mode even when it is the root:
    // design §4 keeps a root-Write that is the delete target's parent, and
    // the sharded delete path retains it the same way (parent_index == 0).
    let inner = SubtreeWritePlan::capture(&view, "/x", InodeLockMode::Write)
        .expect("capture must succeed")
        .expect("plan expected");
    assert!(
        has_write(inner.inode_requests(), root_id),
        "the mutated parent root must stay Write for /x: {:?}",
        inner.inode_requests()
    );
    assert!(has_write(inner.inode_requests(), x));

    // A root that is only a plain ancestor (deeper target) is filtered.
    fs.mkdir("/x/deep", false)?;
    let deep = fs.file_status("/x/deep")?.id;
    let deeper = SubtreeWritePlan::capture(&view, "/x/deep", InodeLockMode::Write)
        .expect("capture must succeed")
        .expect("plan expected");
    assert!(
        !has_write(deeper.inode_requests(), root_id),
        "a plain-ancestor root must not be Write: {:?}",
        deeper.inode_requests()
    );
    assert!(has_write(deeper.inode_requests(), deep));

    // Root as the real target: its Write must not be filtered away.
    let whole = SubtreeWritePlan::capture(&view, "/", InodeLockMode::Write)
        .expect("capture must succeed")
        .expect("plan expected");
    assert!(
        has_write(whole.inode_requests(), root_id),
        "root as the real target must stay Write: {:?}",
        whole.inode_requests()
    );
    Ok(())
}

#[test]
fn stale_restore_epoch_invalidates_the_plan() -> curvine_error::FsResult<()> {
    let fs = test_fs("stale-epoch");
    fs.mkdir("/e/d0", true)?;

    let handle = fs.fs_dir();
    let plan = {
        let view = handle.view().expect("namespace view");
        SubtreeWritePlan::capture(&view, "/e", InodeLockMode::Write)
            .expect("capture must succeed")
            .expect("plan expected")
    };

    // A restore generation change between capture and revalidation must
    // invalidate the plan before any side effect.
    {
        let view = handle.view().expect("namespace view");
        let guard = view
            .store
            .begin_restore(Duration::from_secs(1))
            .expect("restore guard");
        let revalidated = plan
            .revalidate(&view, "/e")
            .expect("revalidate must not error");
        assert!(
            revalidated.is_none(),
            "a plan captured before a restore generation change must not revalidate"
        );
        drop(guard);
    }
    Ok(())
}

#[test]
fn deep_version_change_invalidates_the_plan() -> curvine_error::FsResult<()> {
    let fs = test_fs("deep-version");
    fs.mkdir("/v/d0/d1", true)?;
    let d0 = fs.file_status("/v/d0")?.id;

    let handle = fs.fs_dir();
    let plan = {
        let view = handle.view().expect("namespace view");
        SubtreeWritePlan::capture(&view, "/v", InodeLockMode::Write)
            .expect("capture must succeed")
            .expect("plan expected")
    };

    // A child edge change deep in the subtree bumps that directory's version,
    // which the captured token must catch.
    fs.create("/v/d0/news", false)?;

    let view = handle.view().expect("namespace view");
    let revalidated = plan
        .revalidate(&view, "/v")
        .expect("revalidate must not error");
    assert!(
        revalidated.is_none(),
        "a version change at depth 1 ({d0}) must invalidate the plan"
    );
    Ok(())
}

#[test]
fn empty_directory_still_records_a_version_token() -> curvine_error::FsResult<()> {
    let fs = test_fs("empty-token");
    fs.mkdir("/t/empty", true)?;

    let handle = fs.fs_dir();
    let plan = {
        let view = handle.view().expect("namespace view");
        SubtreeWritePlan::capture(&view, "/t", InodeLockMode::Write)
            .expect("capture must succeed")
            .expect("plan expected")
    };

    // The empty child had no edges to enumerate, but its version token must
    // still have been captured: adding a first child invalidates the plan.
    fs.create("/t/empty/first", false)?;

    let view = handle.view().expect("namespace view");
    let revalidated = plan
        .revalidate(&view, "/t")
        .expect("revalidate must not error");
    assert!(
        revalidated.is_none(),
        "an empty directory's version token must be captured and checked"
    );
    Ok(())
}

#[test]
fn duplicate_hardlink_names_are_counted_separately() -> curvine_error::FsResult<()> {
    let fs = test_fs("hardlink-budget");
    fs.mkdir("/h", true)?;
    fs.create("/h/file", false)?;
    let file_id = fs.file_status("/h/file")?.id;

    // Fill the directory up to the limit with extra names of the same inode:
    // the budget counts names/edges, not distinct inode ids.
    let extra = SUBTREE_PLAN_LIMIT + 4;
    for index in 0..extra {
        fs.link("/h/file".to_string(), format!("/h/alias{index}"))?;
    }

    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    let plan =
        SubtreeWritePlan::capture(&view, "/h", InodeLockMode::Write).expect("capture must succeed");

    assert!(
        plan.is_none(),
        "{} names of one inode exceed the {SUBTREE_PLAN_LIMIT} budget: names, \
         not distinct inodes, are counted",
        extra + 1
    );
    // The file keeps its id; the assertion above is about the budget.
    assert_eq!(fs.file_status("/h/file")?.id, file_id);
    Ok(())
}

#[test]
fn oversized_subtree_returns_no_plan() -> curvine_error::FsResult<()> {
    let fs = test_fs("oversized");
    fs.mkdir("/wide", false)?;
    for index in 0..SUBTREE_PLAN_LIMIT + 8 {
        fs.create(format!("/wide/f{index}"), false)?;
    }

    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    let plan = SubtreeWritePlan::capture(&view, "/wide", InodeLockMode::Write)
        .expect("an over-budget subtree is a fallback, not an error");

    assert!(
        plan.is_none(),
        "a subtree larger than {SUBTREE_PLAN_LIMIT} members must return None so \
         the caller falls back to the exclusive path"
    );
    Ok(())
}

#[test]
fn exactly_at_the_limit_still_produces_a_plan() -> curvine_error::FsResult<()> {
    let fs = test_fs("limit-boundary");
    fs.mkdir("/edge", false)?;
    // Design §3 counts path-chain entries plus actually visited edges:
    // chain [root, target] = 2, so exactly-at-limit means LIMIT - 2
    // children, each reached through one counted edge.
    for index in 0..SUBTREE_PLAN_LIMIT - 2 {
        fs.create(format!("/edge/f{index}"), false)?;
    }

    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    let plan = SubtreeWritePlan::capture(&view, "/edge", InodeLockMode::Write)
        .expect("capture must succeed");

    assert!(
        plan.is_some(),
        "a subtree exactly at {SUBTREE_PLAN_LIMIT} entries is still plannable"
    );
    Ok(())
}
