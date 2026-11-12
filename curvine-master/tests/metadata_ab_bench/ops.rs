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

//! One case per metadata operation. Each case separates fixture preparation
//! (untimed) from the measured call, and every measured call works on its own
//! pre-built object so a case never times setup or cleanup.
//!
//! Only `MasterFilesystem` public API present in both `bff4e7dc5` and the
//! candidate branch is used, so this file compiles unchanged on both.

use crate::harness::{Env, Layout};
use curvine_error::FsResult;
use curvine_master::master::fs::MasterFilesystem;
use curvine_model::{
    ClientAddress, CreateFileOpts, FileAllocOpts, ListOptions, OpenFlags, RenameFlags,
    SetAttrOptsBuilder, WorkerAddress, WorkerInfo, WorkerStatus,
};

/// Children per listed directory. Fixed so both builds list the same shape.
const LIST_SIZE: usize = 256;
/// Page size for `list_options`.
const LIST_PAGE: usize = 64;
/// Attribute written by the `set_attr` case; re-applying it is idempotent.
const SET_ATTR_MODE: u32 = 0o640;

pub struct Case {
    pub name: &'static str,
    /// Builds the objects the timed phase consumes. Runs outside the timed
    /// region and fails loudly: a broken fixture must not pass silently.
    pub prepare: fn(&MasterFilesystem, &Env) -> FsResult<()>,
    pub body: fn(&MasterFilesystem, &Env, usize, u64) -> FsResult<()>,
}

/// Registers one live worker with ample capacity. Needed by cases that
/// allocate blocks (`add_block`, `get_block_locations`).
pub fn register_worker(fs: &MasterFilesystem) {
    let mut worker = WorkerInfo::new(
        WorkerAddress {
            worker_id: 1,
            ..Default::default()
        },
        1,
    );
    worker.status = WorkerStatus::Live;
    worker.capacity = i64::MAX / 4;
    worker.available = i64::MAX / 4;
    fs.add_test_worker(worker);
}

fn client() -> ClientAddress {
    ClientAddress {
        client_name: "bench".to_string(),
        hostname: "localhost".to_string(),
        ip_addr: "127.0.0.1".to_string(),
        port: 0,
    }
}

/// Creates each distinct working directory exactly once (`Same` shares one).
fn make_dirs(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
    let mut seen: Vec<String> = Vec::with_capacity(env.threads());
    for thread in 0..env.threads() {
        let dir = env.dir(thread);
        if seen.contains(&dir) {
            continue;
        }
        fs.mkdir(&dir, true)?;
        seen.push(dir);
    }
    Ok(())
}

/// One empty file per measured call.
fn make_files(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
    make_dirs(fs, env)?;
    for thread in 0..env.threads() {
        for seq in 0..env.ops() {
            fs.create(env.path(thread, seq), false)?;
        }
    }
    Ok(())
}

/// One shared read target per thread: enough for the layout matrix without
/// turning fixture preparation into the dominant cost.
fn make_read_targets(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
    make_dirs(fs, env)?;
    for thread in 0..env.threads() {
        fs.create(env.path(thread, 0), false)?;
    }
    Ok(())
}

fn list_entry(env: &Env, thread: usize, i: usize) -> String {
    match env.layout() {
        Layout::Same => format!("{}/e{i}", env.dir(thread)),
        Layout::Multi => format!("{}/e{thread}-{i}", env.dir(thread)),
    }
}

fn make_list_dirs(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
    make_dirs(fs, env)?;
    match env.layout() {
        Layout::Same => {
            for i in 0..LIST_SIZE {
                fs.create(list_entry(env, 0, i), false)?;
            }
        }
        Layout::Multi => {
            for thread in 0..env.threads() {
                for i in 0..LIST_SIZE {
                    fs.create(list_entry(env, thread, i), false)?;
                }
            }
        }
    }
    Ok(())
}

fn rename_dst_dir(env: &Env, thread: usize) -> String {
    format!("{}/dst{thread}", env.root())
}

fn make_rename_cross_dirs(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
    make_files(fs, env)?;
    let mut seen: Vec<String> = Vec::with_capacity(env.threads());
    for thread in 0..env.threads() {
        let dir = rename_dst_dir(env, thread);
        if seen.contains(&dir) {
            continue;
        }
        fs.mkdir(&dir, true)?;
        seen.push(dir);
    }
    Ok(())
}

fn make_block_targets(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
    make_read_targets(fs, env)?;
    for thread in 0..env.threads() {
        fs.add_block(env.path(thread, 0), None, client(), vec![], vec![], 0, None)?;
    }
    Ok(())
}

pub const CASES: &[Case] = &[
    Case {
        name: "create",
        prepare: make_dirs,
        body: |fs, env, t, seq| {
            fs.create(env.path(t, seq), false)?;
            Ok(())
        },
    },
    Case {
        name: "mkdir",
        prepare: make_dirs,
        body: |fs, env, t, seq| {
            fs.mkdir(format!("{}/d{t}-{seq}", env.dir(t)), false)?;
            Ok(())
        },
    },
    Case {
        name: "delete",
        prepare: make_files,
        body: |fs, env, t, seq| {
            fs.delete(env.path(t, seq), false)?;
            Ok(())
        },
    },
    Case {
        name: "rename_same",
        prepare: make_files,
        body: |fs, env, t, seq| {
            let dst = format!("{}/r{t}-{seq}", env.dir(t));
            fs.rename(env.path(t, seq), dst, RenameFlags::empty())?;
            Ok(())
        },
    },
    Case {
        name: "rename_cross",
        prepare: make_rename_cross_dirs,
        body: |fs, env, t, seq| {
            let dst = format!("{}/f{t}-{seq}", rename_dst_dir(env, t));
            fs.rename(env.path(t, seq), dst, RenameFlags::empty())?;
            Ok(())
        },
    },
    Case {
        name: "set_attr",
        prepare: make_files,
        body: |fs, env, t, seq| {
            let opts = SetAttrOptsBuilder::new().mode(SET_ATTR_MODE).build();
            fs.set_attr(env.path(t, seq), opts)?;
            Ok(())
        },
    },
    Case {
        name: "link",
        prepare: make_files,
        body: |fs, env, t, seq| {
            let dst = format!("{}/l{t}-{seq}", env.dir(t));
            fs.link(env.path(t, seq), dst)?;
            Ok(())
        },
    },
    Case {
        name: "symlink",
        prepare: make_dirs,
        body: |fs, env, t, seq| {
            let link = format!("{}/s{t}-{seq}", env.dir(t));
            fs.symlink("/bench-target".to_string(), link, false, 0o777)?;
            Ok(())
        },
    },
    Case {
        name: "overwrite",
        prepare: make_files,
        body: |fs, env, t, seq| {
            // `create` sets O_TRUNC, so an existing path takes the overwrite
            // branch (`FsDir::overwrite_file`) rather than the create branch.
            fs.create(env.path(t, seq), false)?;
            Ok(())
        },
    },
    Case {
        name: "open",
        prepare: make_files,
        body: |fs, env, t, seq| {
            // Write-only without O_TRUNC/O_CREAT takes `FsDir::reopen_file`,
            // the real client open-for-write path. Read-only would just
            // delegate to `get_block_locations` and measure nothing new.
            fs.open_file(
                env.path(t, seq),
                CreateFileOpts::with_create(false),
                OpenFlags::new_write_only(),
            )?;
            Ok(())
        },
    },
    Case {
        name: "add_block",
        prepare: make_files,
        body: |fs, env, t, seq| {
            fs.add_block(env.path(t, seq), None, client(), vec![], vec![], 0, None)?;
            Ok(())
        },
    },
    Case {
        name: "complete_file",
        prepare: make_files,
        body: |fs, env, t, seq| {
            // Closing a freshly created empty file. Empty commit_blocks and
            // len 0 are consistent (complete_len == 0 == len), so this is the
            // real close path, not a retry shortcut.
            fs.complete_file(
                env.path(t, seq),
                None,
                0,
                vec![],
                "bench".to_string(),
                false,
                None,
            )?;
            Ok(())
        },
    },
    Case {
        name: "free",
        prepare: make_files,
        body: |fs, env, t, seq| {
            fs.free(env.path(t, seq), false)?;
            Ok(())
        },
    },
    Case {
        name: "resize",
        prepare: make_files,
        body: |fs, env, t, seq| {
            fs.resize(env.path(t, seq), FileAllocOpts::with_truncate(0))?;
            Ok(())
        },
    },
    Case {
        name: "file_status",
        prepare: make_read_targets,
        body: |fs, env, t, _seq| {
            fs.file_status(env.path(t, 0))?;
            Ok(())
        },
    },
    Case {
        name: "exists",
        prepare: make_read_targets,
        body: |fs, env, t, _seq| {
            fs.exists(env.path(t, 0))?;
            Ok(())
        },
    },
    Case {
        name: "list_status",
        prepare: make_list_dirs,
        body: |fs, env, t, _seq| {
            let entries = fs.list_status(env.dir(t))?;
            assert_eq!(entries.len(), LIST_SIZE, "list_status returned wrong count");
            Ok(())
        },
    },
    Case {
        name: "list_page",
        prepare: make_list_dirs,
        body: |fs, env, t, _seq| {
            let entries = fs.list_options(
                env.dir(t),
                ListOptions {
                    limit: Some(LIST_PAGE),
                    start_after: None,
                },
            )?;
            assert_eq!(entries.len(), LIST_PAGE, "list_page returned wrong count");
            Ok(())
        },
    },
    Case {
        name: "get_block_locations",
        prepare: make_block_targets,
        body: |fs, env, t, _seq| {
            fs.get_block_locations(env.path(t, 0))?;
            Ok(())
        },
    },
];

/// Removes a scenario's whole tree so the next case starts from a clean
/// namespace. Runs outside the timed region.
pub fn cleanup(fs: &MasterFilesystem, env: &Env) {
    let _ = fs.delete(env.root(), true);
}

// ---------------------------------------------------------------------------
// Hot-parent background scenario fixtures.
//
// Shape: `/bench/hot/parent` is the contended hot parent. A deep chain under
// it is the foreground target, so foreground ancestor read locks and
// background parent write locks contend on the same subtree.
// ---------------------------------------------------------------------------

const HOT_ROOT: &str = "/bench/hot";
const HOT_PROBE: &str = "probe";

pub fn hot_parent_dir() -> String {
    format!("{HOT_ROOT}/parent")
}

fn hot_deep_dir() -> String {
    let mut path = hot_parent_dir();
    path.push_str("/deep");
    for depth in 0..crate::background::HOT_PARENT_DEPTH {
        path.push_str(&format!("/d{depth}"));
    }
    path
}

pub fn prepare_hot_parent(fs: &MasterFilesystem) -> FsResult<()> {
    let deep = hot_deep_dir();
    fs.mkdir(&deep, true)?;
    fs.create(format!("{deep}/{HOT_PROBE}"), false)?;
    Ok(())
}

/// One background cycle on the hot parent: create then delete. Both calls are
/// timed together as one background operation, because the background is an
/// offered load, not a per-call measurement.
pub fn hot_parent_cycle(fs: &MasterFilesystem, path: &str) -> FsResult<()> {
    fs.create(path, false)?;
    fs.delete(path, false)?;
    Ok(())
}

pub fn hot_parent_status(fs: &MasterFilesystem) -> FsResult<()> {
    fs.file_status(format!("{}/{HOT_PROBE}", hot_deep_dir()))?;
    Ok(())
}

pub fn hot_parent_create(fs: &MasterFilesystem, thread: usize, seq: u64) -> FsResult<()> {
    fs.create(format!("{}/c{thread}-{seq}", hot_deep_dir()), false)?;
    Ok(())
}

pub fn cleanup_hot_parent(fs: &MasterFilesystem) {
    let _ = fs.delete(HOT_ROOT, true);
}
