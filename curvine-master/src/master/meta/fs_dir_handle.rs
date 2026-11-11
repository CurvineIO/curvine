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
}

impl FsDirHandle {
    pub fn new(view: FsDir) -> Self {
        Self {
            current: ArcSwapOption::from_pointee(view),
            state: AtomicU8::new(STATE_OPEN),
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
            // Safety proof: the swap's wait_for_readers converts every live
            // guard's debt into a strong reference, so a strong count of one
            // means no guard and no outside `Arc` holder remains; only then is
            // `Arc::get_mut` a safe-Rust uniqueness proof.
            if Arc::strong_count(&taken) == 1 && Arc::get_mut(&mut taken).is_some() {
                return Ok(ColdFsDirGuard {
                    handle: self,
                    taken: Some(taken),
                });
            }
            if Instant::now() >= deadline {
                // Put the same view back and reopen access: the database is
                // not touched and the view is not dropped.
                self.current.store(Some(taken));
                self.state.store(STATE_OPEN, Ordering::Release);
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
        if let Some(fs_dir) = self.taken.take() {
            fs_dir.store.invalidate_cursor_store();
            fs_dir.journal_writer.fail_metadata_restore();
        }
        self.handle.state.store(STATE_FAILED, Ordering::Release);
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
        if std::thread::panicking() {
            self.discard_view();
        } else if let Some(taken) = self.taken.take() {
            self.handle.current.store(Some(taken));
            self.handle.state.store(STATE_OPEN, Ordering::Release);
        }
    }
}
