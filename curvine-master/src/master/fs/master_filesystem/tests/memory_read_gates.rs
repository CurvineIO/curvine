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
