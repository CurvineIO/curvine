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

//! Active-only dir-space coordinator task.
//!
//! One `dir-space-coordinator` loop ticks every second (well below the
//! configured scan intervals) and drives the [`DirSpaceManager`]:
//!
//! - Scans only fire while the master is active; standby ticks do nothing.
//! - A standby → active transition pulls the global next-due forward to now,
//!   so a newly promoted master scans immediately instead of waiting a full
//!   interval (design doc §9).
//! - A round whose previous attempt is still in flight (long scan) reports
//!   `Busy` on the manager permit and leaves the next-due untouched.
//! - Metrics ages (global snapshot age) are refreshed on each tick.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use curvine_error::FsError;
use curvine_runtime::runtime::LoopTask;

use crate::master::space::manager::{DirSpaceManager, ScanOutcome};
use crate::master::MasterMonitor;

#[allow(dead_code)]
fn _assert_send_sync<T: Send + Sync>() {}

#[allow(dead_code)]
fn _task_is_send() {
    _assert_send_sync::<DirSpaceCoordinatorTask>();
}

/// Coordinator tick interval: 1 second, per design doc §9.
pub const COORDINATOR_TICK_MS: u64 = 1000;

pub struct DirSpaceCoordinatorTask {
    manager: std::sync::Arc<DirSpaceManager>,
    monitor: MasterMonitor,
    global_interval_ms: u64,
    point_interval_ms: u64,
    /// Last-tick active state, used to detect standby → active transitions.
    was_active: std::sync::atomic::AtomicBool,
    /// Monotonic instant of the next allowed global round.
    global_next_due: std::sync::Mutex<Instant>,
    /// Monotonic instant of the next allowed tracked-point round.
    point_next_due: std::sync::Mutex<Instant>,
    /// Round-robin cursor into the effective tracked path list.
    point_cursor: AtomicUsize,
}

impl DirSpaceCoordinatorTask {
    pub fn new(manager: std::sync::Arc<DirSpaceManager>, monitor: MasterMonitor) -> Self {
        let global_interval_ms = manager.interval_ms();
        let point_interval_ms = manager.point_interval_ms();
        Self {
            manager,
            monitor,
            global_interval_ms,
            point_interval_ms,
            was_active: std::sync::atomic::AtomicBool::new(false),
            global_next_due: std::sync::Mutex::new(Instant::now()),
            point_next_due: std::sync::Mutex::new(Instant::now()),
            point_cursor: AtomicUsize::new(0),
        }
    }

    fn active(&self) -> bool {
        self.monitor.is_active()
    }

    fn schedule_global_after(&self, next_due: &mut Instant, outcome: ScanOutcome) {
        match outcome {
            ScanOutcome::Success
            | ScanOutcome::Failed
            | ScanOutcome::Timeout
            | ScanOutcome::Limit
            | ScanOutcome::Disabled => {
                *next_due =
                    Instant::now() + std::time::Duration::from_millis(self.global_interval_ms);
            }
            ScanOutcome::Busy => {}
            ScanOutcome::RoleLost => {}
        }
    }

    fn schedule_point_after(
        &self,
        next_due: &mut Instant,
        outcome: ScanOutcome,
        round_complete: bool,
    ) {
        match outcome {
            ScanOutcome::Success
            | ScanOutcome::Failed
            | ScanOutcome::Timeout
            | ScanOutcome::Limit
                if round_complete =>
            {
                *next_due =
                    Instant::now() + std::time::Duration::from_millis(self.point_interval_ms);
            }
            ScanOutcome::Disabled if round_complete => {
                *next_due =
                    Instant::now() + std::time::Duration::from_millis(self.point_interval_ms);
            }
            ScanOutcome::Busy | ScanOutcome::RoleLost => {}
            _ => {}
        }
    }

    /// One scheduler tick: promote next-due on standby→active, then run a
    /// global or tracked-point round when due (never both in one tick).
    fn tick(&self) {
        let active = self.active();
        let was_active = self
            .was_active
            .swap(active, std::sync::atomic::Ordering::SeqCst);

        let mut global_next_due = self.global_next_due.lock().unwrap();
        let mut point_next_due = self.point_next_due.lock().unwrap();
        if active && !was_active {
            // Newly promoted (or first active tick): scan immediately.
            *global_next_due = Instant::now();
            *point_next_due = Instant::now();
        }
        if !active {
            return;
        }

        // Snapshot ages and point aggregates are refreshed every tick (§12).
        self.manager.refresh_global_age();
        self.manager.refresh_point_metrics();

        let now = Instant::now();
        let global_due = now >= *global_next_due;
        let point_due = now >= *point_next_due;

        if global_due && (!point_due || *global_next_due <= *point_next_due) {
            let outcome = self.manager.try_scan_global();
            self.schedule_global_after(&mut global_next_due, outcome);
            return;
        }

        if point_due {
            let start = self.point_cursor.load(Ordering::SeqCst);
            let (outcome, next_cursor, round_complete) =
                self.manager.try_scan_tracked_points(start);
            self.point_cursor.store(next_cursor, Ordering::SeqCst);
            self.schedule_point_after(&mut point_next_due, outcome, round_complete);
        }
    }
}

impl LoopTask for DirSpaceCoordinatorTask {
    type Error = FsError;

    fn run(&self) -> Result<(), FsError> {
        self.tick();
        Ok(())
    }

    fn terminate(&self) -> bool {
        self.monitor.is_stop()
    }
}
