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

//! Test-only gate that parks a memory-read thread inside its locked window
//! until the driving test releases it, so lock-window tests confirm entry
//! and writer arrival instead of racing a fixed sleep.
//!
//! Registered from `mod tests` in master_filesystem.rs; the core file keeps
//! only that registration and the light `park` calls.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::LazyLock;
use std::time::Duration;

type GateEntry = (Receiver<()>, Option<Sender<()>>);

static GATES: LazyLock<Mutex<HashMap<String, GateEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Registers a release channel for `point`; returns the release handle.
#[cfg_attr(not(feature = "fault-injection"), allow(dead_code))]
pub fn install(point: &str) -> Sender<()> {
    let (sender, receiver) = channel();
    GATES.lock().insert(point.to_string(), (receiver, None));
    sender
}

/// Like [`install`], but also returns a channel that fires when the
/// worker actually reached the gate, so a test can confirm the
/// interleaving before acting instead of racing a sleep.
#[cfg_attr(not(feature = "fault-injection"), allow(dead_code))]
pub fn install_with_entered(point: &str) -> (Sender<()>, Receiver<()>) {
    let (release_tx, release_rx) = channel();
    let (entered_tx, entered_rx) = channel();
    GATES
        .lock()
        .insert(point.to_string(), (release_rx, Some(entered_tx)));
    (release_tx, entered_rx)
}

/// Blocks the caller until the gate is released or the safety timeout
/// expires, so a failing test can never hang the suite.
pub fn park(point: &str, timeout: Duration) {
    let entry: Option<GateEntry> = GATES.lock().remove(point);
    if let Some((receiver, entered)) = entry {
        if let Some(entered) = entered {
            let _ = entered.send(());
        }
        let _ = receiver.recv_timeout(timeout);
    }
}

/// Deterministic branch counters for the exists version fast path.
///
/// Counted per queried path, not process-wide: a global counter also collects
/// the `exists` calls of unrelated tests, so a parallel run cannot assert a
/// delta. Tests assert the delta of their own path instead.
type VpCounts = std::collections::HashMap<String, (u64, u64)>;

static VP_COUNTS: std::sync::Mutex<Option<VpCounts>> = std::sync::Mutex::new(None);

fn vp_counts() -> std::sync::MutexGuard<'static, Option<VpCounts>> {
    VP_COUNTS.lock().unwrap_or_else(|e| e.into_inner())
}

fn vp_bump(path: &str, hit: bool) {
    let mut guard = vp_counts();
    let counts = guard.get_or_insert_with(VpCounts::new);
    let entry = counts.entry(path.to_string()).or_insert((0, 0));
    if hit {
        entry.0 += 1;
    } else {
        entry.1 += 1;
    }
}

pub fn vp_hits_for(path: &str) -> u64 {
    vp_counts()
        .as_ref()
        .and_then(|c| c.get(path))
        .map_or(0, |e| e.0)
}

pub fn vp_fallbacks_for(path: &str) -> u64 {
    vp_counts()
        .as_ref()
        .and_then(|c| c.get(path))
        .map_or(0, |e| e.1)
}

pub(crate) fn vp_record_hit(path: &str) {
    vp_bump(path, true);
}

pub(crate) fn vp_record_fallback(path: &str) {
    vp_bump(path, false);
}

/// Whether a parked reader was actually released by the test, or left on its
/// own when the timeout expired.
///
/// `park` swallows the timeout, so a test that only observes "the thread is
/// still running" can believe its mutation landed inside the window while the
/// reader had already walked on. Reporting the exit turns that silent escape
/// into an observable failure.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ParkExit {
    Released,
    TimedOut,
}

static VP_GATE_TIMEOUTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A non-zero delta means the reader left a gate on its own, so the mutation
/// ordering of that case was never actually controlled.
pub fn vp_gate_timeouts() -> u64 {
    VP_GATE_TIMEOUTS.load(std::sync::atomic::Ordering::Relaxed)
}

pub(crate) fn park_reporting(point: &str, timeout: std::time::Duration) -> ParkExit {
    let entry: Option<GateEntry> = GATES.lock().remove(point);
    match entry {
        Some((receiver, entered)) => {
            if let Some(entered) = entered {
                let _ = entered.send(());
            }
            match receiver.recv_timeout(timeout) {
                Ok(()) => ParkExit::Released,
                Err(_) => {
                    VP_GATE_TIMEOUTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    ParkExit::TimedOut
                }
            }
        }
        // No gate installed: nothing is controlling this window, so there is
        // nothing to time out against.
        None => ParkExit::Released,
    }
}
