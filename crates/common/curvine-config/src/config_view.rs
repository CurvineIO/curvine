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

//! Process-scoped effective configuration view.
//!
//! The web console has always shown the *startup snapshot* it happened to load,
//! which is not what a running master/worker/transfer is actually using: a
//! process-local runtime override changes behaviour without touching the file.
//! Without a view that merges both, an operator sees the file value and then
//! observes a different behaviour, with nothing in between to explain it.
//!
//! This module builds that merged view. It is deliberately read-only and
//! process-local: nothing here persists, and nothing here can change what the
//! process is running.
//!
//! # Why the view is rebuilt instead of patched
//!
//! Overrides are applied to a copy of the serialized configuration and the
//! result is re-deserialized through the normal validation path, so the view
//! reports what `ClusterConf` *would be* if the overrides were real. Patching
//! the serialized tree in place would skip derivation: `JobConf::task_timeout`
//! is populated by `init()` from its `_str` twin, so an overlay that lands on
//! the right path still reads `0ns` until validation runs.
//!
//! # What the view does not do
//!
//! * It does not widen what is writable. Only registered keys are editable; the
//!   rest of `values` is context, so the console never implies a knob is tunable
//!   when nothing would consume a change.
//! * It does not hide unregistered fields. `values` is the full effective tree,
//!   because the console's job is to explain the whole process, not just the
//!   tunable part.

use curvine_core_error::CommonResult;
use serde::Serialize;
use serde_json::Value;

use crate::pipeline::{self, ConfigSource, ConfigSourceMap};
use crate::runtime_registry::RuntimeConfigRegistry;
use crate::validation::ConfValidate;
use crate::ClusterConf;

/// Configuration subset a process actually consumes.
///
/// The transfer service loads a reduced profile ([`ClusterConf::from_transfer`]),
/// so its view must be validated the same way; running full validation on it
/// would reject configurations the process accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewProfile {
    /// `Master` and `Worker` services: full `ClusterConf::validate`.
    Full,
    /// `Transfer` service: client + transfer validation only.
    Transfer,
}

/// One registered runtime parameter, resolved against the configuration tree.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RuntimeEntryView {
    /// Runtime key accepted by `POST /api/update_config`.
    pub key: String,
    /// Dotted configuration path the key is backed by.
    pub path: String,
    /// Value read from the configured file, ignoring any override.
    pub file_value: Value,
    /// Active override, already translated into a configuration-shaped value.
    pub override_value: Option<Value>,
    /// Value the process is running with.
    pub effective_value: Value,
}

/// Effective configuration plus the provenance needed to explain it.
#[derive(Clone, Debug)]
pub struct ConfigView {
    /// Full effective configuration tree, serialized the way the console renders it.
    pub values: Value,
    /// Which layer supplied each leaf, keyed by dotted configuration path.
    pub sources: ConfigSourceMap,
    /// Registered parameters that map onto a real configuration path.
    pub runtime: Vec<RuntimeEntryView>,
    /// Runtime keys that may be pushed. Superset of the `runtime` entries' keys:
    /// a knob with no configuration-file entry is writable but has no field to
    /// show, so it appears here and nowhere else.
    pub writable: Vec<String>,
    /// Registered keys whose declared configuration path does not exist in the
    /// tree. Reported rather than dropped, so a misnamed key is visible to an
    /// operator instead of silently swallowing their writes.
    pub unresolved: Vec<String>,
}

/// Builds the effective view for `conf` with `registry`'s overrides applied.
///
/// `base_sources` is the layer map from [`ClusterConf::from_with_sources`].
/// Overridden paths are re-attributed to [`ConfigSource::Runtime`]; every other
/// leaf keeps the attribution it was loaded with.
pub fn effective_config_view(
    conf: &ClusterConf,
    profile: ViewProfile,
    registry: &RuntimeConfigRegistry,
    base_sources: &ConfigSourceMap,
) -> CommonResult<ConfigView> {
    let base = toml::Value::try_from(conf)?;
    let mut doc = base.clone();
    let mut sources = base_sources.clone();
    let mut runtime = Vec::new();
    let mut writable = Vec::new();
    let mut unresolved = Vec::new();

    for key in registry.keys() {
        writable.push(key.to_string());

        // Only an explicit declaration identifies a configuration-file field. A
        // key registered without one is a runtime-only knob whose "file value"
        // may come from a constant, so `config_path()`'s fallback to the key
        // would invent a field the console could not actually show.
        let Some(Some(path)) = registry.declared_config_path(key) else {
            continue;
        };

        if !path_exists(&base, path) {
            // The declaration is wrong, or the field was renamed. Report it: a
            // silently ignored write is the worst outcome for an operator who
            // just pushed a value and saw a success response.
            unresolved.push(key.to_string());
            continue;
        }

        let entry = registry.get(key)?;
        let overlay = entry.override_value.as_deref().map(translate);

        if let Some(value) = &overlay {
            pipeline::set_dotted(&mut doc, path, value.clone())?;
            sources.insert(path.to_string(), ConfigSource::Runtime);
        }

        // The overlay is TOML-shaped because that is what `set_dotted` writes;
        // the view reports JSON because that is what the console renders.
        let override_value = match &overlay {
            Some(value) => Some(serde_json::to_value(value)?),
            None => None,
        };

        runtime.push(RuntimeEntryView {
            key: key.to_string(),
            path: path.to_string(),
            file_value: entry.file_value,
            override_value,
            effective_value: entry.effective_value,
        });
    }

    let effective = deserialize_and_validate(&doc, profile)?;
    let mut values = serde_json::to_value(&effective)?;

    // The serialized tree is *not* the whole story for a tunable field: some
    // fields are normalized or re-parsed after deserialization, and the derived
    // form is `#[serde(skip)]`, so it never reaches `values`. `tracked_paths` is
    // the clearest case — the tree keeps the raw entries while the process runs
    // on `tracked_paths_normalized`. Reporting both invites an operator to read
    // the tree and conclude the process is tracking a path it dropped.
    //
    // `effective_value` is read straight from the live component, so it is the
    // single authority: pin the tree to it rather than letting a second
    // computation path diverge.
    for entry in &runtime {
        set_json_path(&mut values, &entry.path, entry.effective_value.clone());
    }

    // A path can exist structurally and still be ignored by `serde` (a sibling
    // key next to the nested table it was meant to address). The audit catches
    // exactly that, so re-check the surviving entries.
    let unknown = crate::validation::audit_unknown_keys_in(&doc);
    for path in unknown {
        if let Some(entry) = runtime.iter().find(|entry| entry.path == path) {
            if !unresolved.contains(&entry.key) {
                unresolved.push(entry.key.clone());
            }
        }
    }

    runtime.sort_by(|a, b| a.key.cmp(&b.key));
    writable.sort();
    unresolved.sort();

    Ok(ConfigView {
        values,
        sources,
        runtime,
        writable,
        unresolved,
    })
}

/// Deserializes the overlaid document through the same gate the loader uses.
///
/// Validation is not optional: the `*_str` twins are parsed into their typed
/// fields by `init()`, so skipping this step makes every duration in the view
/// read `0`.
fn deserialize_and_validate(doc: &toml::Value, profile: ViewProfile) -> CommonResult<ClusterConf> {
    let mut conf: ClusterConf = doc.clone().try_into()?;
    match profile {
        ViewProfile::Full => conf.validate()?,
        ViewProfile::Transfer => {
            conf.client.validate()?;
            conf.transfer.validate()?;
        }
    }
    Ok(conf)
}

/// Writes `value` at `path` inside a JSON tree, replacing whatever was there.
///
/// Creates intermediate objects but never invents a leaf: a path that does not
/// resolve is left alone, because the caller already reported it as unresolved
/// and a value the tree does not have a slot for cannot be shown.
fn set_json_path(tree: &mut Value, path: &str, value: Value) {
    let mut cursor = tree;
    let mut segments = path.split('.').peekable();
    while let Some(segment) = segments.next() {
        let Some(object) = cursor.as_object_mut() else {
            return;
        };
        if segments.peek().is_none() {
            if object.contains_key(segment) {
                object.insert(segment.to_string(), value);
            }
            return;
        }
        match object.get_mut(segment) {
            Some(next) => cursor = next,
            None => return,
        }
    }
}

/// Whether `path` addresses a leaf that exists in `doc`.
fn path_exists(doc: &toml::Value, path: &str) -> bool {
    let mut cursor = doc;
    for segment in path.split('.') {
        let Some(next) = cursor.get(segment) else {
            return false;
        };
        cursor = next;
    }
    !cursor.is_table()
}

/// Translates a registry's raw string value into a configuration-shaped value.
///
/// The registry accepts everything as text (it arrives in a query string) while
/// the configuration tree is typed. Durations, sizes and enums are stored as
/// their documented string form and pass through unchanged; booleans, integers
/// and arrays are JSON in the registry and must become real TOML values, or
/// re-deserialization rejects them.
fn translate(raw: &str) -> toml::Value {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(value) => match toml::Value::try_from(value) {
            Ok(value) => value,
            Err(_) => toml::Value::String(raw.to_string()),
        },
        // Not JSON at all: `"1800000ms"`, `"10GB"`, `"postgres://..."` and the
        // other documented unit forms live here.
        Err(_) => toml::Value::String(raw.to_string()),
    }
}
