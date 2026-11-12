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

//! Fixture preparation and per-call argument builders. Everything here runs
//! outside the timed region.

use crate::harness::{Env, Layout};
use crate::ops::{Arg, LIST_SIZE};
use curvine_error::FsResult;
use curvine_master::master::fs::MasterFilesystem;
use curvine_model::ClientAddress;

/// Symlink target. Never dereferenced by the master, so it need not exist.
const SYMLINK_TARGET: &str = "/bench-target";
/// Client name recorded by close.
const CLIENT_NAME: &str = "bench";

// ---------------------------------------------------------------------------
// Fixtures (untimed).
// ---------------------------------------------------------------------------

/// Creates each distinct working directory exactly once (`Same` shares one).
pub(crate) fn make_dirs(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
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
pub(crate) fn make_files(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
    make_dirs(fs, env)?;
    for thread in 0..env.threads() {
        for seq in 0..env.ops() {
            fs.create(env.path(thread, seq), false)?;
        }
    }
    Ok(())
}

/// One read target per thread: enough for the layout matrix without turning
/// fixture preparation into the dominant cost.
pub(crate) fn make_read_targets(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
    make_dirs(fs, env)?;
    for thread in 0..env.threads() {
        fs.create(env.path(thread, 0), false)?;
    }
    Ok(())
}

/// Each listed directory holds exactly `LIST_SIZE` entries in both layouts;
/// only the number of such directories differs (`Same` one, `Multi` per
/// thread). Directory size must not be a second variable.
pub(crate) fn list_entry(env: &Env, thread: usize, i: usize) -> String {
    match env.layout() {
        Layout::Same => format!("{}/e{i}", env.dir(thread)),
        Layout::Multi => format!("{}/e{thread}-{i}", env.dir(thread)),
    }
}

pub(crate) fn make_list_dirs(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
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

pub(crate) fn rename_dst_dir(env: &Env, thread: usize) -> String {
    format!("{}/dst{thread}", env.root())
}

pub(crate) fn make_rename_cross_dirs(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
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

pub(crate) fn make_block_targets(fs: &MasterFilesystem, env: &Env) -> FsResult<()> {
    make_read_targets(fs, env)?;
    for thread in 0..env.threads() {
        fs.add_block(env.path(thread, 0), None, client(), vec![], vec![], 0, None)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Argument builders (untimed).
// ---------------------------------------------------------------------------

pub(crate) fn path_arg(env: &Env, thread: usize, seq: u64) -> Arg {
    arg(env.path(thread, seq))
}

pub(crate) fn mkdir_arg(env: &Env, thread: usize, seq: u64) -> Arg {
    arg(format!("{}/d{thread}-{seq}", env.dir(thread)))
}

pub(crate) fn rename_same_arg(env: &Env, thread: usize, seq: u64) -> Arg {
    arg2(
        env.path(thread, seq),
        format!("{}/r{thread}-{seq}", env.dir(thread)),
    )
}

pub(crate) fn rename_cross_arg(env: &Env, thread: usize, seq: u64) -> Arg {
    arg2(
        env.path(thread, seq),
        format!("{}/f{thread}-{seq}", rename_dst_dir(env, thread)),
    )
}

pub(crate) fn link_arg(env: &Env, thread: usize, seq: u64) -> Arg {
    arg2(
        env.path(thread, seq),
        format!("{}/l{thread}-{seq}", env.dir(thread)),
    )
}

pub(crate) fn symlink_arg(env: &Env, thread: usize, seq: u64) -> Arg {
    arg2(
        SYMLINK_TARGET.to_string(),
        format!("{}/s{thread}-{seq}", env.dir(thread)),
    )
}

/// Close records the client name; it is pre-built into `b`.
pub(crate) fn close_arg(env: &Env, thread: usize, seq: u64) -> Arg {
    arg2(env.path(thread, seq), CLIENT_NAME.to_string())
}

pub(crate) fn read_arg(env: &Env, thread: usize, _seq: u64) -> Arg {
    arg(env.path(thread, 0))
}

pub(crate) fn list_arg(env: &Env, thread: usize, _seq: u64) -> Arg {
    arg(env.dir(thread))
}

pub(crate) fn client() -> ClientAddress {
    ClientAddress {
        client_name: CLIENT_NAME.to_string(),
        hostname: "localhost".to_string(),
        ip_addr: "127.0.0.1".to_string(),
        port: 0,
    }
}

pub(crate) fn arg(a: String) -> Arg {
    Arg {
        a,
        b: String::new(),
        client: client(),
    }
}

pub(crate) fn arg2(a: String, b: String) -> Arg {
    Arg {
        a,
        b,
        client: client(),
    }
}
