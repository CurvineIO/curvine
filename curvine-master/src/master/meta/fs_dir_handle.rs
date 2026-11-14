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

//! Lifecycle holder for the current namespace view (S6-a).
//!
//! The view is the whole `FsDir`: root and store are always obtained together,
//! so no reader can pair a root of one generation with a store of another.
//! Hot reads borrow the view through an arc-swap guard (no per-request global
//! lock, no per-RPC RMW counter, no RocksDB snapshot). Cold exclusivity for
//! restore or tree rebuild closes new access, drains already-issued guards
//! through the swap's debt repayment, then proves unique ownership
//! with `Arc::get_mut` (safe Rust only).
//!
//! Failure boundaries: a timed-out cold window puts the same view back and
//! reopens access without touching the database. A panic inside the cold
//! window fails closed — the view is marked unavailable, the journal is
//! fenced and further access is rejected. A possibly half-mutated view is
//! never handed back out as a success. Detach-after-failure semantics stay
//! owned by the store as before.
//!
//! Calling discipline (checked per call site at S6-c, not by this type): a
//! cold owner must not wait while holding a view guard or any object lock;
//! hot planning guards are released before `lock_many` and re-taken for the
//! locked window; no guard is held across a journal commit wait.

use crate::master::journal::JournalWriter;
use crate::master::meta::FsDir;
use arc_swap::{ArcSwapOption, Guard};
use curvine_error::FsError;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const STATE_OPEN: u8 = 0;
const STATE_COLD: u8 = 1;
const STATE_FAILED: u8 = 2;

impl From<NamespaceError> for FsError {
    fn from(e: NamespaceError) -> Self {
        match e {
            NamespaceError::Busy => {
                FsError::in_progress_msg("namespace is temporarily unavailable (cold window)")
            }
            NamespaceError::Unavailable => {
                FsError::journal_unavailable("namespace permanently unavailable after failure")
            }
        }
    }
}

impl From<ColdError> for FsError {
    fn from(e: ColdError) -> Self {
        match e {
            ColdError::Busy => {
                FsError::in_progress_msg("another cold exclusivity window is active")
            }
            ColdError::DrainTimeout => {
                FsError::in_progress_msg("cold window drain timed out waiting for view guards")
            }
            ColdError::Unavailable => {
                FsError::journal_unavailable("namespace permanently unavailable after failure")
            }
            ColdError::InvalidTimeout => {
                FsError::invalid_argument("cold window timeout overflows the deadline")
            }
        }
    }
}

/// Default timeout for cold exclusivity used by regular consumers.
pub const DEFAULT_COLD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceError {
    Busy,
    Unavailable,
}

impl std::fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => write!(f, "namespace view temporarily unavailable (cold window)"),
            Self::Unavailable => write!(f, "namespace permanently unavailable after failure"),
        }
    }
}

impl std::error::Error for NamespaceError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdError {
    Busy,
    Unavailable,
    DrainTimeout,
    InvalidTimeout,
}

impl std::fmt::Display for ColdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => write!(f, "another cold exclusivity window is active"),
            Self::Unavailable => write!(f, "namespace permanently unavailable after failure"),
            Self::DrainTimeout => write!(f, "cold window drain timed out waiting for view guards"),
            Self::InvalidTimeout => write!(f, "cold window timeout overflows the deadline"),
        }
    }
}

impl std::error::Error for ColdError {}

pub struct FsDirHandle {
    current: ArcSwapOption<FsDir>,
    state: AtomicU8,
    /// Test-only interleaving hook for the reopen path: a fixed point, an
    /// out-channel announcing it was reached and an in-channel the driver
    /// releases. The hook closure never captures the handle, so no reference
    /// cycle keeps the FsDir alive. Production builds carry no extra field.
    #[cfg(test)]
    reopen_hook: std::sync::Mutex<Option<ReopenHookChannels>>,
}

/// Channels of the test-only reopen hook. `Mutex<Option<_>>` on the holder;
/// the driver owns the far ends.
#[cfg(test)]
struct ReopenHookChannels {
    point: &'static str,
    reached: std::sync::mpsc::Sender<&'static str>,
    release: std::sync::mpsc::Receiver<()>,
}

impl FsDirHandle {
    pub fn new(view: FsDir) -> Self {
        Self {
            current: ArcSwapOption::from_pointee(view),
            state: AtomicU8::new(STATE_OPEN),
            #[cfg(test)]
            reopen_hook: std::sync::Mutex::new(None),
        }
    }

    /// Installs a test-only hook that announces one fixed point of the reopen
    /// path and blocks there until the driver releases it.
    #[cfg(test)]
    pub(crate) fn set_reopen_hook(
        &self,
        point: &'static str,
        reached: std::sync::mpsc::Sender<&'static str>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        *self.reopen_hook.lock().unwrap() = Some(ReopenHookChannels {
            point,
            reached,
            release,
        });
    }

    /// Removes the hook so a finished scenario cannot block a later one.
    #[cfg(test)]
    pub(crate) fn clear_reopen_hook(&self) {
        *self.reopen_hook.lock().unwrap() = None;
    }

    /// Test-only: whether the view slot is currently empty.
    #[cfg(test)]
    pub(crate) fn slot_is_empty(&self) -> bool {
        self.current.load().is_none()
    }

    /// Fires the test-only hook. Announces the reached point over the
    /// out-channel and waits for the driver's release; an already-closed
    /// channel pair simply falls through.
    #[cfg(test)]
    fn fire_reopen_hook(&self, point: &'static str) {
        let hook = self.reopen_hook.lock().unwrap();
        if let Some(hook) = hook.as_ref() {
            if hook.point == point {
                // A send error only means the driver is gone; fall through.
                let _ = hook.reached.send(point);
                // A receive error only means the driver is gone; fall through.
                let _ = hook.release.recv();
            }
        }
    }

    /// Borrows the current view without exposing its `Arc`. Callers must
    /// retain the store availability and metadata epoch checks.
    pub fn view(&self) -> Result<NamespaceGuard<'_>, NamespaceError> {
        let guard = self.current.load();
        let state = self.state.load(Ordering::Acquire);
        if state == STATE_FAILED {
            return Err(NamespaceError::Unavailable);
        }
        // A None load can race with a cold window reopening: retry outside
        // object locks rather than misreporting a permanent failure.
        if state == STATE_COLD || guard.is_none() {
            return Err(NamespaceError::Busy);
        }
        Ok(NamespaceGuard {
            inner: guard,
            _handle: PhantomData,
        })
    }

    /// 永久失效判定：True 时视图不可达。
    pub fn is_failed(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::Acquire) == STATE_FAILED
    }

    /// 冷窗中判定。
    pub fn is_cold(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::Acquire) == STATE_COLD
    }

    /// Terminal failure for a holder that may not own the view right now.
    ///
    /// One-way: the state becomes FAILED and never returns to OPEN/COLD. The
    /// given writer is fenced unconditionally — not only when the slot still
    /// holds a view — because the caller is reporting a failed confirmation.
    /// A view still parked in the slot is retired and its cursor invalidated.
    ///
    /// When another cold owner or drain has already taken the view, this call
    /// cannot reach that store: the cursor invalidation for it is completed by
    /// that owner when it exits. From this call onwards every new `view` and
    /// `begin_cold` is rejected.
    pub(crate) fn fail_closed(&self, writer: &JournalWriter) {
        self.state.store(STATE_FAILED, Ordering::Release);
        writer.fail_metadata_holder_terminal();
        if let Some(fs_dir) = self.current.swap(None) {
            fs_dir.store.invalidate_cursor_store();
        }
    }

    /// Publishes a taken view back into the slot and reopens access.
    ///
    /// Order matters: the slot is filled first and only then CAS COLD→OPEN, so
    /// an OPEN state never coexists with an empty slot. If the state turned
    /// FAILED in the meantime, the just-published slot is retracted and its
    /// store invalidated instead of leaving a late `Some` behind.
    ///
    /// Returns true when access was reopened, false when a concurrent terminal
    /// failure won the race.
    fn publish_reopen(&self, owned: Arc<FsDir>) -> bool {
        #[cfg(test)]
        self.fire_reopen_hook("before_store");
        // Move the view into the slot: no extra Arc copy keeps a new cold
        // window waiting for a reference that only this helper still held.
        self.current.store(Some(owned));
        #[cfg(test)]
        self.fire_reopen_hook("after_store");
        #[cfg(test)]
        self.fire_reopen_hook("before_cas");
        let reopened = self
            .state
            .compare_exchange(STATE_COLD, STATE_OPEN, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        #[cfg(test)]
        self.fire_reopen_hook("after_cas");
        if !reopened {
            // A terminal failure won: take the late view back out and retire
            // it. A None here means the terminal already swapped it away and
            // is retiring it itself.
            if let Some(late) = self.current.swap(None) {
                late.store.invalidate_cursor_store();
            }
        }
        reopened
    }

    /// Takes exclusive mutable access to the current view.
    ///
    /// Caller constraint: never wait here while holding this handle's view
    /// guard or any object lock — the drain below waits for view guards, so
    /// holding one would deadlock against ourselves.
    pub fn begin_cold(&self, timeout: Duration) -> Result<ColdFsDirGuard<'_>, ColdError> {
        // Validate the timeout before touching any state: an overflowing
        // deadline must not leave an emptied slot behind.
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(ColdError::InvalidTimeout)?;
        if let Err(actual) =
            self.state
                .compare_exchange(STATE_OPEN, STATE_COLD, Ordering::AcqRel, Ordering::Acquire)
        {
            return Err(if actual == STATE_FAILED {
                ColdError::Unavailable
            } else {
                ColdError::Busy
            });
        }
        let Some(mut taken) = self.current.swap(None) else {
            // OPEN requires a populated slot; fail closed if it is absent.
            self.state.store(STATE_FAILED, Ordering::Release);
            return Err(ColdError::Unavailable);
        };
        loop {
            // A terminal failure while we are still draining must not hand a
            // view to a new owner: retire it and report the terminal state.
            if self.is_failed() {
                taken.store.invalidate_cursor_store();
                return Err(ColdError::Unavailable);
            }
            // Safety proof: the swap's wait_for_readers converts every live
            // guard's debt into a strong reference, so a strong count of one
            // means no guard and no outside `Arc` holder remains; only then is
            // `Arc::get_mut` a safe-Rust uniqueness proof.
            if Arc::strong_count(&taken) == 1 && Arc::get_mut(&mut taken).is_some() {
                // Re-check immediately before handing ownership out: a
                // terminal that arrived after the last observation must not be
                // reported as a successful cold acquisition.
                if self.is_failed() {
                    taken.store.invalidate_cursor_store();
                    return Err(ColdError::Unavailable);
                }
                return Ok(ColdFsDirGuard {
                    handle: self,
                    taken: Some(taken),
                });
            }
            if Instant::now() >= deadline {
                // Put the same view back and reopen access through the checked
                // path: without a terminal failure this is the same view and
                // the original DrainTimeout; with one, the late slot is
                // retracted and the caller sees the terminal state.
                if !self.publish_reopen(taken) {
                    return Err(ColdError::Unavailable);
                }
                return Err(ColdError::DrainTimeout);
            }
            // ponytail: cold-only 1ms polling; add notifications if its latency matters.
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Borrowed hot view. Dereferences to `&FsDir`.
pub struct NamespaceGuard<'h> {
    inner: Guard<Option<Arc<FsDir>>>,
    // Only ties the guard to the handle borrow: the guard must not outlive
    // the holder. This is a lifetime bound, not a proof of the calling
    // discipline (commit-wait and object-lock rules are checked per call
    // site at S6-c).
    _handle: PhantomData<&'h FsDirHandle>,
}

impl std::ops::Deref for NamespaceGuard<'_> {
    type Target = FsDir;
    fn deref(&self) -> &FsDir {
        // Arc-swap's debt or owned reference keeps this Some alive across swaps.
        self.inner
            .as_deref()
            .expect("namespace guard contains a view")
    }
}

/// Exclusive cold window over the current view.
///
/// Normal drop reopens access. Panic or explicit `fail_closed` permanently
/// rejects access through this handle and fences the journal.
pub struct ColdFsDirGuard<'h> {
    handle: &'h FsDirHandle,
    taken: Option<Arc<FsDir>>,
}

impl ColdFsDirGuard<'_> {
    /// Unique mutable access to the view. During the cold window this guard
    /// holds the only strong reference (slot is empty and no `Arc` escapes),
    /// so `Arc::get_mut` is the safe-Rust uniqueness proof.
    pub fn view(&mut self) -> &mut FsDir {
        Arc::get_mut(self.taken.as_mut().expect("cold guard owns the view"))
            .expect("cold exclusivity guarantees a single strong reference")
    }

    /// Discards a possibly half-mutated view and fences the journal.
    /// Recovery requires rebuilding the handle; retrying Busy cannot help.
    pub fn fail_closed(mut self) {
        self.discard_view();
    }

    fn discard_view(&mut self) {
        // Share the terminal retirement with `FsDirHandle::fail_closed`: the
        // taken store is invalidated, the writer fenced, the state set to
        // FAILED and any parked slot removed so no late view survives.
        self.handle.state.store(STATE_FAILED, Ordering::Release);
        if let Some(fs_dir) = self.taken.take() {
            fs_dir.store.invalidate_cursor_store();
            fs_dir.journal_writer.fail_metadata_holder_terminal();
        }
        self.handle.current.store(None);
    }
}

impl std::ops::Deref for ColdFsDirGuard<'_> {
    type Target = FsDir;
    fn deref(&self) -> &FsDir {
        self.taken.as_ref().expect("cold guard owns the view")
    }
}

impl std::ops::DerefMut for ColdFsDirGuard<'_> {
    fn deref_mut(&mut self) -> &mut FsDir {
        Arc::get_mut(self.taken.as_mut().expect("cold guard owns the view"))
            .expect("cold exclusivity guarantees a single strong reference")
    }
}

impl Drop for ColdFsDirGuard<'_> {
    fn drop(&mut self) {
        // A guard whose handle is already terminal must never reopen access or
        // leave a parked slot: it retires whatever it still owns.
        if std::thread::panicking() || self.handle.is_failed() {
            self.discard_view();
        } else if let Some(taken) = self.taken.take() {
            self.handle.publish_reopen(taken);
        }
    }
}

/// Confirmation-window guard shared by the exclusive path and the C2
/// recursive coordination entry (design §8.1). Armed in the body's lexical
/// scope after the view/lock declarations, disarmed only when the real
/// confirmation reports the apply/cursor ack. On a confirmation error — or
/// if the wait panics while armed — the namespace holder goes terminal
/// before the surrounding resources are released. Borrows only the holder
/// and the writer: no namespace view, object locks, DB Arc, admission slot
/// or exclusive gate. `handle.take` keeps the single terminal/disarm
/// mechanism; there is no second legacy implementation.
pub(crate) struct MetadataConfirmation<'a> {
    handle: Option<&'a FsDirHandle>,
    writer: &'a JournalWriter,
}

impl<'a> MetadataConfirmation<'a> {
    /// Arms a guard over one namespace holder. The caller controls the
    /// declaration point (§8.1: after the view/lock bindings, inside the
    /// body's lexical scope).
    pub(crate) fn arm(handle: &'a FsDirHandle, writer: &'a JournalWriter) -> Self {
        Self {
            handle: Some(handle),
            writer,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.handle = None;
    }

    pub(crate) fn fail_terminal(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.fail_closed(self.writer);
            // F6.5: the observation checkpoint fires ONLY when an actual
            // terminal transition happened (handle was Some and was taken).
            // A successful disarm sets handle=None first, so the guard's
            // Drop calls fail_terminal but never reaches this branch — no
            // fabricated "terminal happened" event from a successful Drop.
            #[cfg(test)]
            if let Some((reached, release)) = self.writer.consume_terminal_hook() {
                let _ = reached.send(());
                let _ = release.recv_timeout(std::time::Duration::from_secs(60));
            }
        }
    }
}

impl Drop for MetadataConfirmation<'_> {
    fn drop(&mut self) {
        self.fail_terminal();
    }
}
