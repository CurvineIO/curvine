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

//! Test-only interleaving gate for the apply worker's `CreateSnapshot` branch.
//!
//! The gate is keyed by the loader's metadata directory, so a snapshot apply
//! running on a different loader cannot consume another test's gate. The
//! driving test installs the gate, observes that the worker entered it, and
//! then releases it explicitly instead of racing a sleep.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::LazyLock;
use std::time::Duration;

/// Release channel plus the "worker entered" signal.
type Entry = (Receiver<()>, Sender<()>);

static GATES: LazyLock<Mutex<HashMap<String, Entry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Registers a gate for `identity`; returns the release handle and a channel
/// that fires once the apply worker actually entered the gate.
pub fn install(identity: &str) -> (Sender<()>, Receiver<()>) {
    let (release_tx, release_rx) = channel();
    let (entered_tx, entered_rx) = channel();
    GATES
        .lock()
        .insert(identity.to_string(), (release_rx, entered_tx));
    (release_tx, entered_rx)
}

/// Parks the apply worker until the gate is released or the safety timeout
/// expires, so a failing test can never hang the suite. A worker whose
/// identity has no installed gate proceeds immediately.
pub fn park(identity: &str) {
    let entry: Option<Entry> = GATES.lock().remove(identity);
    if let Some((release_rx, entered_tx)) = entry {
        let _ = entered_tx.send(());
        let _ = release_rx.recv_timeout(Duration::from_secs(10));
    }
}
