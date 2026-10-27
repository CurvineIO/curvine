// Copyright 2026 OPPO.
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

use crate::master::fs::MasterFilesystem;
use crate::master::meta::inode::{InodePath, InodeView, PATH_SEPARATOR};
use crate::master::meta::FsDir;
use crate::master::Master;
use crate::master::MasterMonitor;
use curvine_error::{FsError, FsResult};
use curvine_model::ListOptions;
use curvine_runtime::runtime::LoopTask;
use log::{debug, info, warn};
use std::time::{Duration, Instant};

// Bound on how many children one directory page examines per namespace lock
// hold. Keeps a directory with millions of children from building the whole
// listing in memory while the lock is held.
const CHILD_PAGE: usize = 256;

// Upper bound on the summed path bytes of one purge batch. A batch is a single
// journal record and Raft proposal, so it must stay well below the 16 MiB RPC
// frame (`MAX_DATE_SIZE`) even when every path is near `max_path_len` (8000 by
// default): 4096 such paths would be ~32 MiB. 4 MiB leaves room for encoding
// overhead and for other entries the sender aggregates into the same proposal.
pub const PURGE_BATCH_MAX_BYTES: usize = 4 * 1024 * 1024;

/// One directory still being enumerated. `after` is the last child already
/// accounted for, so a batch that ends mid-directory resumes from the next
/// name instead of rescanning the directory from the start.
#[derive(Debug, Clone)]
struct Frame {
    path: String,
    after: Option<String>,
}

/// What one directory page of the walk found.
enum ChildPage {
    /// The path no longer resolves: restored, emptied or already purged.
    Missing,
    /// The path resolves but is not a directory. A trashed entry can be a single
    /// file or link, so the root frame is emitted without descending.
    Leaf,
    Children(Vec<PurgeChild>),
}

/// A child seen while walking a trashed subtree.
struct PurgeChild {
    name: String,
    path: String,
    is_dir: bool,
    has_children: bool,
}

/// Result of purging one trash entry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PurgeOutcome {
    /// Number of batches applied. Exposed for tests that assert the purge is
    /// actually bounded rather than one large batch.
    pub batches: u64,
    pub inodes: u64,
    pub bytes: u64,
}

/// Reclaims trashed entries in bounded batches.
///
/// Each batch removes at most `trash_purge_batch_inodes` inodes and carries at
/// most [`PURGE_BATCH_MAX_BYTES`] of path bytes, is journaled
/// as one [`crate::master::journal::TrashPurgeEntry`], and then releases the
/// namespace write lock for `trash_purge_yield_ms`. A trashed directory with
/// millions of inodes therefore never blocks metadata operations for longer
/// than one batch.
///
/// The walk is a depth-first post-order traversal: children are removed before
/// their parent, so every emitted path is a file or an empty directory and
/// removing it cannot touch inodes that a later batch still needs. Each batch
/// is applied atomically and then journaled; replay is idempotent because
/// paths that no longer resolve are skipped.
pub struct TrashPurger {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
    batch_inodes: usize,
    batch_bytes: usize,
    yield_ms: u64,
}

impl TrashPurger {
    pub fn new(fs: MasterFilesystem, monitor: MasterMonitor) -> Self {
        let batch_inodes = fs.conf.trash_purge_batch_inodes.max(1);
        let yield_ms = fs.conf.trash_purge_yield_ms;
        Self {
            fs,
            monitor,
            batch_inodes,
            batch_bytes: PURGE_BATCH_MAX_BYTES,
            yield_ms,
        }
    }

    /// Overrides the per-batch path byte bound. Test hook: a small bound lets a
    /// test exercise byte-limited batching without megabytes of path data.
    pub fn with_batch_bytes(mut self, batch_bytes: usize) -> Self {
        self.batch_bytes = batch_bytes.max(1);
        self
    }

    /// Purges `entry` completely. Returns `None` when this node lost leadership
    /// mid-purge; the entry then stays in the trash for the next active master.
    pub fn purge_entry(&self, entry: &str) -> FsResult<Option<PurgeOutcome>> {
        let mut frames = vec![Frame {
            path: entry.to_string(),
            after: None,
        }];
        let mut outcome = PurgeOutcome::default();

        loop {
            if !self.monitor.is_active() {
                return Ok(None);
            }

            // The walk only reads metadata, and the batch it produces is bounded,
            // so the read lock is held for one page of children at most.
            let batch = {
                let fs_dir = self.fs.fs_dir();
                let fs_dir = fs_dir.read();
                Self::next_batch(&fs_dir, &mut frames, self.batch_inodes, self.batch_bytes)?
            };
            if batch.is_empty() {
                return Ok(Some(outcome));
            }

            let started = Instant::now();
            let applied = self.fs.purge_trash_batch(entry, &batch);
            let elapsed_ms = started.elapsed().as_millis() as u64;

            let (inodes, bytes) = match applied {
                Ok(Some(applied)) => {
                    Self::record_batch(elapsed_ms, applied.0, true);
                    applied
                }
                // A demotion is not a purge failure: this node simply stopped.
                Ok(None) => return Ok(None),
                Err(e) => {
                    Self::record_batch(elapsed_ms, 0, false);
                    return Err(e);
                }
            };
            outcome.batches += 1;
            outcome.inodes += inodes;
            outcome.bytes += bytes;
            debug!(
                "trash purge: entry={} batch={} paths={} removed={}",
                entry,
                outcome.batches,
                batch.len(),
                inodes
            );

            if self.yield_ms > 0 {
                std::thread::sleep(Duration::from_millis(self.yield_ms));
            }
        }
    }

    /// Records one applied (or failed) batch. Metrics are best-effort: a missing
    /// registry must never fail a purge.
    fn record_batch(elapsed_ms: u64, inodes: u64, applied: bool) {
        let Ok(metrics) = Master::get_metrics() else {
            return;
        };
        metrics.trash_purge_batches.inc();
        metrics
            .trash_purge_batch_duration_ms
            .inc_by(elapsed_ms as i64);
        if applied {
            metrics.trash_purge_inodes.inc_by(inodes as i64);
        } else {
            metrics.trash_purge_errors.inc();
        }
    }

    /// Collects the next bounded batch of leaf paths, deepest first. The batch
    /// stops at `limit` paths or once its path bytes reach `max_bytes`; a batch
    /// always holds at least one path, so a single path larger than the bound
    /// still makes progress.
    fn next_batch(
        fs_dir: &FsDir,
        frames: &mut Vec<Frame>,
        limit: usize,
        max_bytes: usize,
    ) -> FsResult<Vec<String>> {
        let limit = limit.max(1);
        let mut batch = Vec::with_capacity(limit.min(CHILD_PAGE));
        let mut bytes = 0usize;

        while batch.len() < limit && bytes < max_bytes {
            let Some(frame) = frames.last() else {
                break;
            };
            let frame_index = frames.len() - 1;
            let page_size = (limit - batch.len()).min(CHILD_PAGE);

            let children = match Self::children_of(
                fs_dir,
                &frame.path,
                frames[frame_index].after.as_deref(),
                page_size,
            )? {
                ChildPage::Children(children) => children,
                // Emit a non-directory frame (a trashed single file) and stay
                // silent for a path that is already gone, so a finished or
                // restored entry never journals a no-op batch.
                ChildPage::Leaf => {
                    let leaf = frames.pop().expect("frame was checked above");
                    bytes += leaf.path.len();
                    batch.push(leaf.path);
                    continue;
                }
                ChildPage::Missing => {
                    frames.pop();
                    continue;
                }
            };

            if children.is_empty() {
                // Every child is accounted for, so this directory is either
                // already empty or becomes empty when this batch is applied.
                let done = frames.pop().expect("frame was checked above");
                bytes += done.path.len();
                batch.push(done.path);
                continue;
            }

            let mut descended = false;
            for child in children {
                // Leave the cursor on the first child this batch cannot hold so
                // the next batch starts exactly here.
                if batch.len() >= limit || bytes >= max_bytes {
                    break;
                }

                frames[frame_index].after = Some(child.name.clone());
                if child.is_dir && child.has_children {
                    frames.push(Frame {
                        path: child.path,
                        after: None,
                    });
                    descended = true;
                    break;
                }

                bytes += child.path.len();
                batch.push(child.path);
            }

            if descended {
                continue;
            }
        }

        Ok(batch)
    }

    /// One name-ordered page of `path`'s children.
    fn children_of(
        fs_dir: &FsDir,
        path: &str,
        after: Option<&str>,
        limit: usize,
    ) -> FsResult<ChildPage> {
        let inp = match InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store) {
            Ok(inp) => inp,
            Err(_) => return Ok(ChildPage::Missing),
        };
        let Some(inode) = inp.get_last_inode() else {
            return Ok(ChildPage::Missing);
        };
        let InodeView::Dir(dir) = inode.as_ref() else {
            return Ok(ChildPage::Leaf);
        };

        let options = ListOptions {
            limit: Some(limit),
            start_after: after.map(str::to_string),
        };
        let prefix = path.trim_end_matches(PATH_SEPARATOR);

        Ok(ChildPage::Children(
            dir.list_options(&options)
                .into_iter()
                .map(|child| PurgeChild {
                    name: child.name().to_string(),
                    path: format!("{}{}{}", prefix, PATH_SEPARATOR, child.name()),
                    is_dir: child.is_dir(),
                    has_children: matches!(child, InodeView::Dir(d) if d.children_len() > 0),
                })
                .collect(),
        ))
    }
}

// Bound on how many pending entries one drain cycle dequeues per scan page.
const PENDING_SCAN_BATCH: usize = 64;

/// Drains `/.Trash/.pending`, the staging directory written by `async_delete`.
///
/// This runs regardless of `trash_enabled` and of `trash_retention`: an async
/// delete promises reclamation without a retention window, so its entries are
/// reclaimed as soon as this task sees them. The task is an accelerator, not the
/// durable state — a pending entry that survives a restart is simply picked up
/// by the next cycle.
pub struct PendingTrashPurger {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
    purger: TrashPurger,
    scan_batch: usize,
}

impl PendingTrashPurger {
    pub fn new(fs: MasterFilesystem, monitor: MasterMonitor) -> Self {
        let purger = TrashPurger::new(fs.clone(), monitor.clone());
        Self {
            fs,
            monitor,
            purger,
            scan_batch: PENDING_SCAN_BATCH,
        }
    }

    /// Overrides the scan page size. Test hook, mirroring `MasterMonitor::set_role_for_test`:
    /// a small page makes `drain_pending` walk the staging directory over several pages.
    pub fn with_scan_batch(mut self, scan_batch: usize) -> Self {
        self.scan_batch = scan_batch.max(1);
        self
    }

    /// Purges every currently staged entry, page by page. Returns the number of
    /// entries reclaimed, or `None` when the master was demoted mid-cycle.
    ///
    /// The drain is skipped when a blocking empty-trash call owns the pending
    /// directory: both walk the same entries, and only the blocking caller has
    /// to report per-entry totals, so it must be the one that reclaims them.
    /// A skipped cycle costs nothing — the entries stay staged for the next one.
    pub fn drain_pending(&self) -> FsResult<Option<(u64, u64, u64)>> {
        let Some(_guard) = self.fs.try_lock_pending_drain() else {
            debug!(
                "pending trash purger skipped: a blocking empty-trash drain owns the staging dir"
            );
            return Ok(Some((0, 0, 0)));
        };

        let mut entries = 0u64;
        let mut inodes = 0u64;
        let mut bytes = 0u64;
        let mut start_after = None;

        // Sample the queue depth once per cycle. The probe is one bounded page,
        // so a backlog larger than the page reports the page size.
        if let Ok(probe) = self.fs.scan_pending_trash(self.scan_batch, None) {
            if let Ok(metrics) = Master::get_metrics() {
                metrics.trash_pending_entries.set(probe.paths.len() as i64);
            }
        }

        loop {
            if !self.monitor.is_active() {
                return Ok(None);
            }

            let page = self
                .fs
                .scan_pending_trash(self.scan_batch, start_after.as_deref())?;
            for path in page.paths {
                // An entry staged after this page was scanned is picked up by the
                // next cycle; a restored or already purged one resolves to a
                // no-op inside the purger.
                match self.purger.purge_entry(&path) {
                    Ok(Some(outcome)) => {
                        entries += 1;
                        inodes += outcome.inodes;
                        bytes += outcome.bytes;
                    }
                    Ok(None) => return Ok(None),
                    Err(FsError::FileNotFound(_)) => {}
                    Err(e) => warn!("failed to purge pending trash entry {}: {}", path, e),
                }
            }

            match page.next_after {
                Some(next) => start_after = Some(next),
                None => break,
            }
        }

        Ok(Some((entries, inodes, bytes)))
    }
}

impl LoopTask for PendingTrashPurger {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        if !self.monitor.is_active() {
            return Ok(());
        }

        if let Some((entries, inodes, bytes)) = self.drain_pending()? {
            if entries > 0 {
                info!(
                    "trash purger reclaimed {} async-deleted entries ({} inodes, {} bytes)",
                    entries, inodes, bytes
                );
            }
        }
        Ok(())
    }

    fn terminate(&self) -> bool {
        self.monitor.is_stop()
    }
}

#[cfg(test)]
mod tests {
    use super::TrashPurger;
    use crate::master::Master;

    // Counters only increase, so delta assertions stay valid even when other
    // tests in this binary purge concurrently and add their own increments.
    #[test]
    fn records_batch_metrics() {
        Master::init_test_metrics();
        let metrics = Master::get_metrics().unwrap();
        let batches = metrics.trash_purge_batches.get();
        let duration = metrics.trash_purge_batch_duration_ms.get();
        let inodes = metrics.trash_purge_inodes.get();
        let errors = metrics.trash_purge_errors.get();

        TrashPurger::record_batch(7, 3, true);
        TrashPurger::record_batch(5, 4, false);

        assert!(
            metrics.trash_purge_batches.get() >= batches + 2,
            "every batch must be counted"
        );
        assert!(
            metrics.trash_purge_batch_duration_ms.get() >= duration + 12,
            "batch durations must be summed"
        );
        assert!(
            metrics.trash_purge_inodes.get() >= inodes + 3,
            "only applied batches contribute inodes"
        );
        assert!(
            metrics.trash_purge_errors.get() > errors,
            "failed batches are counted separately"
        );
    }
}
