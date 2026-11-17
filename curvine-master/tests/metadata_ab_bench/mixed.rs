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

//! Mixed read/write scenario: readers and writers run at the same time.
//!
//! The ordinary cells run one operation on every thread, so they never show what
//! a sharded lock is for: a global lock makes every reader wait for every writer
//! and every writer wait for every other writer, while sharded locks let work in
//! different parent directories proceed independently. This scenario puts both
//! kinds of thread in one cell.
//!
//! Shape: `readers + writers` threads. A reader loops `file_status` on a file
//! that always exists. A writer loops `create` + `delete` on its own names (a
//! steady state: the directory never grows). `Layout::Multi` gives every thread
//! its own parent directory; `Layout::Same` puts them all under one parent, which
//! is the control: sharded locks should not win there, which shows the gain comes
//! from distinct parents and not from the harness.
//!
//! The timed region of each loop holds exactly one `MasterFilesystem` call; all
//! paths are built before the barrier. Reads and writes are reported separately
//! (their latencies are not comparable), plus the combined throughput.

use crate::harness::{budget_millis, merge_into, Layout, Report, ThreadResult};
use curvine_error::FsResult;
use curvine_master::master::fs::MasterFilesystem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

const ROOT: &str = "/bench/mixed";
const LAT_CAPACITY: usize = 65536;
/// Names per writer, recycled create/delete cycle. Small on purpose: the cell
/// measures lock behaviour, not directory growth.
const WRITER_NAMES: u64 = 256;

/// `(readers, writers)` splits, every one 16 threads in total so the ratio is the
/// only thing that varies: read-heavy, balanced, write-heavy.
pub const SPLITS: [(usize, usize); 3] = [(14, 2), (8, 8), (2, 14)];

pub struct Mixed {
    pub readers: usize,
    pub writers: usize,
    pub read: Report,
    pub write: Report,
}

fn dir_for(layout: Layout, thread: usize) -> String {
    match layout {
        Layout::Same => ROOT.to_string(),
        Layout::Multi => format!("{ROOT}/t{thread}"),
    }
}

fn reader_target(layout: Layout, thread: usize) -> String {
    format!("{}/r{thread}", dir_for(layout, thread))
}

fn writer_path(layout: Layout, thread: usize, seq: u64) -> String {
    format!("{}/w{thread}-{seq}", dir_for(layout, thread))
}

fn empty() -> ThreadResult {
    ThreadResult {
        ok: 0,
        err: 0,
        lat: Vec::with_capacity(LAT_CAPACITY),
        first_err: None,
        elapsed_ns: 0,
        start_ns: None,
        end_ns: 0,
    }
}

fn record(out: &mut ThreadResult, ns: u64, result: FsResult<()>) {
    match result {
        Ok(()) => {
            out.ok += 1;
            if out.lat.len() < LAT_CAPACITY {
                out.lat.push(ns);
            }
        }
        Err(e) => {
            out.err += 1;
            if out.first_err.is_none() {
                out.first_err = Some(e.to_string());
            }
        }
    }
}

pub fn prepare(fs: &MasterFilesystem, layout: Layout, threads: usize) -> FsResult<()> {
    match layout {
        Layout::Same => {
            fs.mkdir(ROOT, true)?;
        }
        Layout::Multi => {
            for thread in 0..threads {
                fs.mkdir(dir_for(layout, thread), true)?;
            }
        }
    }
    for thread in 0..threads {
        fs.create(reader_target(layout, thread), false)?;
    }
    Ok(())
}

pub fn run(
    fs: &Arc<MasterFilesystem>,
    layout: Layout,
    readers: usize,
    writers: usize,
    ops: u64,
) -> Mixed {
    let threads = readers + writers;
    let barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let mut reader_handles = Vec::with_capacity(readers);
    let mut writer_handles = Vec::with_capacity(writers);

    // Readers take thread ids 0..readers, writers readers..threads, so every
    // thread has a distinct directory under `Multi`.
    for thread in 0..readers {
        let (fs, barrier, stop) = (fs.clone(), barrier.clone(), stop.clone());
        let target = reader_target(layout, thread);
        reader_handles.push(std::thread::spawn(move || {
            let mut out = empty();
            barrier.wait();
            let started = Instant::now();
            out.start_ns = Some(0);
            let mut seq = 0u64;
            while seq < ops && !stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                let result = fs.file_status(&target).map(|_| ());
                record(&mut out, t0.elapsed().as_nanos() as u64, result);
                seq += 1;
            }
            out.elapsed_ns = started.elapsed().as_nanos() as u64;
            out
        }));
    }

    for thread in readers..threads {
        let (fs, barrier, stop) = (fs.clone(), barrier.clone(), stop.clone());
        let paths: Vec<String> = (0..WRITER_NAMES)
            .map(|seq| writer_path(layout, thread, seq))
            .collect();
        writer_handles.push(std::thread::spawn(move || {
            let mut out = empty();
            barrier.wait();
            let started = Instant::now();
            out.start_ns = Some(0);
            let mut seq = 0u64;
            while seq < ops && !stop.load(Ordering::Relaxed) {
                let path = &paths[(seq as usize) % paths.len()];
                // One timed call per iteration: create and delete alternate so the
                // name is free for the next create. Both calls are writes and both
                // are timed individually.
                let t0 = Instant::now();
                let created = fs.create(path, false).map(|_| ());
                record(&mut out, t0.elapsed().as_nanos() as u64, created);
                let t1 = Instant::now();
                let deleted = fs.delete(path, false).map(|_| ());
                record(&mut out, t1.elapsed().as_nanos() as u64, deleted);
                seq += 1;
            }
            out.elapsed_ns = started.elapsed().as_nanos() as u64;
            out
        }));
    }

    barrier.wait();
    // One wall-clock budget for the whole cell, as the ordinary cells use: every
    // thread stops together, so reads and writes cover the same interval.
    std::thread::sleep(std::time::Duration::from_millis(budget_millis()));
    stop.store(true, Ordering::Release);

    let mut read = empty();
    let mut write = empty();
    for handle in reader_handles {
        merge_into(&mut read, handle.join().expect("reader panicked"));
    }
    for handle in writer_handles {
        merge_into(&mut write, handle.join().expect("writer panicked"));
    }
    Mixed {
        readers,
        writers,
        read: finish(read),
        write: finish(write),
    }
}

fn finish(mut acc: ThreadResult) -> Report {
    acc.lat.sort_unstable();
    Report {
        ok: acc.ok,
        err: acc.err,
        lat: acc.lat,
        elapsed_s: acc.elapsed_ns as f64 / 1e9,
        first_err: acc.first_err,
    }
}

impl Mixed {
    /// Two machine-parsable lines (reads, writes). `op` carries the split so the
    /// existing parsers key on it; the combined throughput is `read + write`.
    pub fn lines(&self, ver: &str, layout: Layout) -> [String; 2] {
        let tag = format!("mixed_r{}w{}", self.readers, self.writers);
        [
            self.read.line(
                ver,
                &format!("{tag}_read"),
                layout,
                self.readers + self.writers,
            ),
            self.write.line(
                ver,
                &format!("{tag}_write"),
                layout,
                self.readers + self.writers,
            ),
        ]
    }
}
