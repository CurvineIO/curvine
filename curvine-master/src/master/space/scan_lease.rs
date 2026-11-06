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

//! Coordination between metadata snapshot scans and Raft snapshot restore.
//!
//! The dir-space scanner reads RocksDB through long-lived read snapshots while
//! holding an `Arc` clone of the underlying store. `InodeStore::restore`
//! replaces that store and requires the strong reference count to drop to 1.
//! A scan running concurrently with a restore would therefore either block the
//! restore forever or break the scanner mid-read.
//!
//! `MetadataScanControl` fixes this with a cancel-epoch protocol:
//!
//! - `InodeStore::begin_snapshot_scan()` acquires a `MetadataScanLease`. The
//!   lease temporarily raises the store strong count and exposes the store's
//!   consistent-read snapshot.
//! - `InodeStore::begin_restore()` marks the control as restoring, bumps the
//!   cancel epoch (which flips every active lease to cancelled) and waits up to
//!   a configured timeout for all active leases to drop. The returned
//!   `MetadataRestoreGuard` keeps the restoring state for the whole restore.
//! - Scanner loops periodically check `lease.is_cancelled()` and drop the
//!   lease (releasing snapshot, iterators and the store Arc) as soon as a
//!   restore is pending, which wakes the waiting restore.
//!
//! While a restore is pending or in progress, new leases are rejected as busy.
//! If the wait times out, restore fails before touching the DB or the in-memory
//! tree, so both remain fully queryable.

use curvine_core_error::{err_box, CommonResult};
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::time::Duration;

use crate::master::meta::store::{InodeStoreSnapshot, RocksInodeStore};

/// Shared cancel/wait state between scan leases and restores.
#[derive(Debug)]
struct ScanControlState {
    /// Number of currently live scan leases.
    active_leases: u64,
    /// True between a successful `begin_restore()` and guard drop.
    restoring: bool,
    /// Incremented whenever a restore begins; leases observe a change as a
    /// cancellation request.
    cancel_epoch: u64,
}

impl ScanControlState {
    fn new() -> Self {
        Self {
            active_leases: 0,
            restoring: false,
            cancel_epoch: 0,
        }
    }
}

/// Coordination point shared by one `InodeStore`.
#[derive(Debug)]
pub struct MetadataScanControl {
    state: Mutex<ScanControlState>,
    idle: Condvar,
}

impl MetadataScanControl {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(ScanControlState::new()),
            idle: Condvar::new(),
        }
    }

    /// Current cancel epoch. A lease created under epoch N is cancelled as
    /// soon as the epoch differs from N.
    pub fn cancel_epoch(&self) -> u64 {
        self.state.lock().cancel_epoch
    }

    /// True while a restore is pending or in progress.
    pub fn is_restoring(&self) -> bool {
        self.state.lock().restoring
    }

    /// Wait until an active restore has finished before inspecting its data directory.
    pub(crate) fn wait_until_restored(&self) {
        let mut state = self.state.lock();
        while state.restoring {
            self.idle.wait(&mut state);
        }
    }

    /// Try to register a new scan lease.
    ///
    /// Returns `Err` when a restore is pending/in progress so that scanners
    /// back off instead of keeping the store busy.
    pub fn try_acquire_lease(&self) -> CommonResult<LeaseTicket> {
        let mut state = self.state.lock();
        if state.restoring {
            return err_box!(
                "metadata scan lease rejected: snapshot restore is pending or in progress"
            );
        }
        state.active_leases += 1;
        Ok(LeaseTicket {
            epoch: state.cancel_epoch,
        })
    }

    /// Begin a restore: mark restoring, bump the cancel epoch and wait for all
    /// active leases to be dropped (scanners observe the epoch change and
    /// cancel). A second concurrent restore is rejected so only its owner can
    /// reopen scan admission. On timeout the restore is aborted and scans can
    /// continue against the untouched store.
    pub fn begin_restore(&self, wait_timeout: Duration) -> CommonResult<RestoreTicket> {
        let deadline = std::time::Instant::now() + wait_timeout;
        let mut state = self.state.lock();
        if state.restoring {
            return err_box!("metadata snapshot restore is already pending or in progress");
        }

        state.restoring = true;
        state.cancel_epoch = state.cancel_epoch.wrapping_add(1);

        let wait_start = std::time::Instant::now();
        while state.active_leases > 0 {
            let now = std::time::Instant::now();
            if now >= deadline {
                // Roll back so the store stays usable for scans.
                state.restoring = false;
                self.idle.notify_all();
                return err_box!(
                    "metadata snapshot restore timed out after {:?} waiting for {} active scan lease(s) to finish",
                    wait_start.elapsed(),
                    state.active_leases
                );
            }
            self.idle.wait_for(&mut state, deadline - now);
        }

        Ok(RestoreTicket)
    }

    fn release_lease(&self) {
        let mut state = self.state.lock();
        state.active_leases = state.active_leases.saturating_sub(1);
        if state.active_leases == 0 {
            self.idle.notify_all();
        }
    }

    fn end_restore(&self) {
        let mut state = self.state.lock();
        state.restoring = false;
        self.idle.notify_all();
    }
}

impl Default for MetadataScanControl {
    fn default() -> Self {
        Self::new()
    }
}

/// Registration returned by `MetadataScanControl::try_acquire_lease`.
///
/// Dropping it only marks the ticket consumed; the caller (`MetadataScanLease`)
/// releases the lease slot so the control reference is available at that point.
#[derive(Debug)]
pub struct LeaseTicket {
    epoch: u64,
}

impl LeaseTicket {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

/// Marker returned by a successful `MetadataScanControl::begin_restore`.
#[derive(Debug)]
pub struct RestoreTicket;

/// A live metadata scan lease.
///
/// Holds the store Arc (so the scanner can read through it), the shared
/// control and the lease ticket. Dropping the lease releases both the ticket
/// and the store reference, allowing a pending restore to proceed.
pub struct MetadataScanLease {
    control: Arc<MetadataScanControl>,
    store: Option<Arc<RocksInodeStore>>,
    ticket: Option<LeaseTicket>,
}

impl MetadataScanLease {
    pub(crate) fn new(
        control: Arc<MetadataScanControl>,
        store: Arc<RocksInodeStore>,
        ticket: LeaseTicket,
    ) -> Self {
        Self {
            control,
            store: Some(store),
            ticket: Some(ticket),
        }
    }

    /// Store access for the scanner. Only single-round scan leases may
    /// temporarily hold this Arc; scanners must not clone it out.
    #[allow(dead_code)]
    pub(crate) fn store(&self) -> &RocksInodeStore {
        self.store.as_deref().expect("live lease has a store")
    }

    /// Create a typed consistent-read snapshot over the inode/edge column
    /// families. The returned reader borrows from this lease; the underlying
    /// RocksDB snapshot is released when the reader (then the lease) drops.
    pub fn snapshot_reader(&self) -> CommonResult<InodeStoreSnapshot<'_>> {
        Ok(InodeStoreSnapshot::new(self.store().read_snapshot()))
    }

    /// Whether a restore has started after this lease was acquired. Scanners
    /// must check this cooperatively (every batch) and drop the lease early.
    pub fn is_cancelled(&self) -> bool {
        self.control.cancel_epoch() != self.ticket.as_ref().map(|t| t.epoch).unwrap_or(u64::MAX)
    }
}

impl Drop for MetadataScanLease {
    fn drop(&mut self) {
        if self.ticket.take().is_some() {
            // Restore may proceed immediately after release_lease wakes it.
            // Drop the DB reference first so the lease count reaching zero
            // actually means all lease-owned store references are gone.
            drop(self.store.take());
            self.control.release_lease();
        }
    }
}

/// RAII marker held for the duration of a restore.
///
/// Dropping it clears the restoring flag (on both success and failure paths)
/// and wakes any thread waiting in `begin_restore`.
pub struct MetadataRestoreGuard {
    control: Arc<MetadataScanControl>,
}

impl MetadataRestoreGuard {
    pub(crate) fn new(control: Arc<MetadataScanControl>) -> Self {
        Self { control }
    }

    pub(crate) fn belongs_to(&self, control: &Arc<MetadataScanControl>) -> bool {
        Arc::ptr_eq(&self.control, control)
    }
}

impl Drop for MetadataRestoreGuard {
    fn drop(&mut self) {
        self.control.end_restore();
    }
}
