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

//! Hot-reload extension points.
//!
//! This module wires up the *shapes* only; no file watcher or admin RPC is
//! connected yet. The pipeline in [`crate::pipeline`] is a pure function from
//! layers to a validated [`ClusterConf`], which makes both planned mechanisms
//! straightforward follow-ups:
//!
//! 1. **File-change reload** (persistent semantics): a watcher re-runs
//!    `build_document` + deserialization + `ConfValidate` on the new file text,
//!    then swaps the result into a [`ConfigHandle`]. Failed validation keeps
//!    the previous snapshot — a bad edit must never take a process down.
//!
//! 2. **Admin-API push** (ephemeral semantics): handlers write dotted-path
//!    overrides into a [`RuntimeOverlay`] and trigger the same
//!    rebuild-and-swap flow with the overlay merged above the file layer.
//!    Overrides are never persisted: restarting the process deliberately
//!    reverts to on-disk configuration.
//!
//! Effective layer order with an overlay active:
//! `file -> environment -> RuntimeOverlay -> deserialize`, so an operator's
//! pushed value cannot be clobbered by `CURVINE_*` env vars.
//!
//! Readers go through [`ConfigHandle::get`], cloning an `Arc` snapshot — no
//! locks held while the config is in use, so a swap never tears a request
//! that started under the previous values.

use crate::ClusterConf;
use curvine_core_error::CommonResult;
use std::sync::{Arc, RwLock};

/// Marker for a configuration value that is safe to swap at runtime.
///
/// Sections whose state is captured by long-lived components (open pools,
/// bound sockets, data directories) must NOT implement this trait: swapping
/// their value would silently diverge from actual component behavior.
///
/// Known future candidates, deliberately NOT yet implemented:
/// - `LogConf` — `Logger::init` captures it once behind a `OnceCell` and
///   installs a global subscriber; hot-swapping the value would not change
///   the running logger until Logger gains a real reload hook.
pub trait Reloadable {
    /// Section name used in reload audit logs (e.g. `"log"`).
    fn reload_name(&self) -> &'static str;
}

/// Ephemeral override layer for admin-pushed values.
///
/// Entries are dotted paths into the cluster TOML document (same addressing as
/// [`crate::pipeline`]) and are merged above the file layer on every rebuild.
/// Intentionally memory-only: restart discards them by design.
#[derive(Default)]
pub struct RuntimeOverlay {
    entries: RwLock<Vec<(String, toml::Value)>>,
}

impl RuntimeOverlay {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pushes (or replaces) one override at a dotted path.
    pub fn set(&self, path: impl Into<String>, value: toml::Value) {
        let path = path.into();
        let mut entries = self.entries.write().expect("runtime overlay poisoned");
        match entries.iter_mut().find(|(p, _)| *p == path) {
            Some(entry) => entry.1 = value,
            None => entries.push((path, value)),
        }
    }

    /// Drops all overrides (e.g. an admin "revert to file" action).
    pub fn clear(&self) {
        self.entries
            .write()
            .expect("runtime overlay poisoned")
            .clear();
    }

    /// Merges the overrides into the post-environment document (layer rank:
    /// file -> env -> overlay). Public for the future file-watcher/admin
    /// rebuild flow; unused in production code paths today.
    ///
    /// Fails on the first invalid override path so the caller can abort the
    /// rebuild and keep the previously stored [`ConfigHandle`] snapshot,
    /// instead of silently applying a partial set of overrides.
    pub fn apply_to(&self, doc: &mut toml::Value) -> CommonResult<()> {
        let entries = self.entries.read().expect("runtime overlay poisoned");
        for (path, value) in entries.iter() {
            crate::pipeline::set_dotted(doc, path, value.clone())?;
        }
        Ok(())
    }
}

/// Shared holder for the currently-active [`ClusterConf`].
///
/// Components that want to observe reloads hold a handle and re-read
/// [`ConfigHandle::get`] where they consume a tunable; components that capture
/// values once at startup simply keep doing so until they opt in.
pub struct ConfigHandle {
    current: RwLock<Arc<ClusterConf>>,
}

impl ConfigHandle {
    pub fn new(conf: ClusterConf) -> Self {
        Self {
            current: RwLock::new(Arc::new(conf)),
        }
    }

    /// Cheap snapshot: cloning the `Arc`, not the configuration.
    pub fn get(&self) -> Arc<ClusterConf> {
        Arc::clone(&self.current.read().expect("config handle poisoned"))
    }

    /// Swaps in a freshly built configuration. Callers must have run the full
    /// pipeline (deserialize → validate → init) before storing; this type does
    /// no validation itself so it stays usable for rollback paths too.
    pub fn store(&self, conf: ClusterConf) {
        *self.current.write().expect("config handle poisoned") = Arc::new(conf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_overlay_sets_replaces_and_clears() {
        let overlay = RuntimeOverlay::new();
        overlay.set("client.rpc_timeout_ms", toml::Value::Integer(5_000));
        overlay.set("client.rpc_timeout_ms", toml::Value::Integer(9_000));
        overlay.set("log.level", toml::Value::String("debug".into()));

        let mut doc: toml::Value = toml::from_str("[client]\nrpc_timeout_ms = 120000\n").unwrap();
        overlay.apply_to(&mut doc).unwrap();

        assert_eq!(doc["client"]["rpc_timeout_ms"].as_integer(), Some(9_000));
        assert_eq!(doc["log"]["level"].as_str(), Some("debug"));

        overlay.clear();
        let mut doc: toml::Value = toml::from_str("[client]\nrpc_timeout_ms = 120000\n").unwrap();
        overlay.apply_to(&mut doc).unwrap();
        assert_eq!(doc["client"]["rpc_timeout_ms"].as_integer(), Some(120_000));
    }

    // A collision path (parent is a scalar) must surface as an error, not be
    // swallowed — otherwise list() would advertise an override that never
    // landed and the rebuild would run with a partial overlay.
    #[test]
    fn apply_to_fails_on_colliding_override_path() {
        let overlay = RuntimeOverlay::new();
        overlay.set("client.rpc_timeout_ms.extra", toml::Value::Integer(1));

        let mut doc: toml::Value = toml::from_str("[client]\nrpc_timeout_ms = 120000\n").unwrap();
        let res: CommonResult<()> = overlay.apply_to(&mut doc);
        assert!(res.is_err(), "scalar-parent collision must fail");
    }

    #[test]
    fn config_handle_swaps_snapshots_atomically() {
        let handle = ConfigHandle::new(ClusterConf::default());
        let before = handle.get();

        let mut updated = ClusterConf::default();
        updated.master.hostname = "reloaded".into();
        handle.store(updated);

        // Old snapshot is unaffected; new readers see the swap.
        assert_eq!(before.master.hostname, "localhost");
        assert_eq!(handle.get().master.hostname, "reloaded");
    }
}
