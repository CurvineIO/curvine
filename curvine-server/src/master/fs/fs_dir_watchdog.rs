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

use crate::master::fs::MasterFilesystem;
use crate::master::{Master, MasterMetrics};
use curvine_common::error::FsError;
use curvine_common::FsResult;
use log::{error, warn};
use orpc::common::{LocalTime, TimeSpent};
use orpc::runtime::LoopTask;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::TryLockError;

/// Watches the single `fs_dir` metadata lock and surfaces a stall as an
/// observable signal instead of a silent multi-minute control-plane freeze.
///
/// The metadata lock is a single global `std::sync::RwLock` and cannot be
/// sharded yet. When it wedges (for example a reader that never releases while
/// writers queue), every metadata RPC blocks with no log or metric. This probe
/// takes a non-blocking `try_read()` each tick: a lock that stays unacquirable
/// past the threshold is reported. It never blocks and never aborts the
/// process; recovery decisions stay with the operator / k8s.
pub struct FsDirWatchdog {
    fs: MasterFilesystem,
    metrics: Option<&'static MasterMetrics>,
    stall_threshold_ms: i64,
    first_unavailable_ms: AtomicI64,
    stall_reported: AtomicBool,
}

impl FsDirWatchdog {
    pub fn new(fs: MasterFilesystem, stall_threshold_ms: i64) -> Self {
        Self {
            fs,
            metrics: Master::get_metrics().ok(),
            stall_threshold_ms,
            first_unavailable_ms: AtomicI64::new(0),
            stall_reported: AtomicBool::new(false),
        }
    }

    fn on_available(&self, probe_us: i64) {
        if let Some(metrics) = self.metrics {
            metrics.fs_dir_probe_acquire_us.set(probe_us);
        }
        if self.stall_reported.swap(false, Ordering::SeqCst) {
            let stalled_ms =
                LocalTime::mills() as i64 - self.first_unavailable_ms.load(Ordering::SeqCst);
            warn!(
                "fs_dir metadata lock recovered after ~{} ms unavailable",
                stalled_ms
            );
            if let Some(metrics) = self.metrics {
                metrics.fs_dir_stalled.set(0);
            }
        }
        self.first_unavailable_ms.store(0, Ordering::SeqCst);
    }

    fn on_unavailable(&self, poisoned: bool) {
        let now = LocalTime::mills() as i64;
        let first = self.first_unavailable_ms.load(Ordering::SeqCst);
        let first = if first == 0 {
            self.first_unavailable_ms.store(now, Ordering::SeqCst);
            now
        } else {
            first
        };

        let unavailable_ms = now - first;
        if poisoned {
            error!("fs_dir metadata lock is poisoned; a holder panicked while mutating metadata");
        }
        if unavailable_ms >= self.stall_threshold_ms
            && !self.stall_reported.swap(true, Ordering::SeqCst)
        {
            warn!(
                "fs_dir metadata lock unacquirable for ~{} ms (threshold {} ms); control plane \
                 metadata RPCs are likely blocked on the global lock",
                unavailable_ms, self.stall_threshold_ms
            );
            if let Some(metrics) = self.metrics {
                metrics.fs_dir_stalled.set(1);
                metrics.fs_dir_stall_total.inc();
            }
        }
    }
}

impl LoopTask for FsDirWatchdog {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        let spent = TimeSpent::new();
        // Non-blocking shared probe. It never blocks the watchdog thread.
        match self.fs.fs_dir().try_read() {
            Ok(_guard) => self.on_available(spent.used_us() as i64),
            Err(TryLockError::WouldBlock) => self.on_unavailable(false),
            Err(TryLockError::Poisoned(_)) => self.on_unavailable(true),
        }
        Ok(())
    }

    fn terminate(&self) -> bool {
        false
    }
}
