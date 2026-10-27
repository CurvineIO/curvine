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

//! Process-local runtime configuration registry (admin push, non-persistent overrides).
//!
//! Each component (master, worker, transfer, …) builds a [`RuntimeConfigRegistry`] at
//! startup, registers only the keys it supports, and exposes them through HTTP/RPC.
//! Overrides are **volatile**: they are lost on process restart (same semantics as
//! worker #236 `persistent=false`).

use curvine_core_error::{err_box, CommonResult};
use log::info;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// One registered parameter and its effective value view.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeConfigEntryView {
    pub key: String,
    pub file_value: Value,
    pub override_value: Option<String>,
    pub effective_value: Value,
}

/// Result of a successful `POST /api/update_config`-style update.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeConfigUpdateResult {
    pub config: String,
    pub previous_value: Value,
    pub value: Value,
    pub persistent: bool,
}

type RuntimeValueFn = Arc<dyn Fn() -> Value + Send + Sync>;
type RuntimeApplyFn = Arc<dyn Fn(&str) -> CommonResult<(Value, Value)> + Send + Sync>;
type RuntimeResetFn = Arc<dyn Fn() -> CommonResult<Value> + Send + Sync>;

struct EntryInner {
    key: &'static str,
    /// Dotted path of the on-disk configuration field this key is backed by.
    ///
    /// `None` means "identical to `key`" — either because the runtime key is already
    /// the real config path, or because the parameter has no configuration-file entry
    /// at all (runtime-only knob). Use [`RuntimeConfigRegistry::config_path`] to read
    /// the resolved value.
    config_path: Option<&'static str>,
    file_value: RuntimeValueFn,
    effective: RuntimeValueFn,
    apply: RuntimeApplyFn,
    reset: RuntimeResetFn,
}

/// Builder for [`RuntimeConfigRegistry`].
pub struct RuntimeConfigRegistryBuilder {
    entries: Vec<EntryInner>,
}

impl Default for RuntimeConfigRegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeConfigRegistryBuilder {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Registers one hot-updatable key. `apply` must be all-or-nothing: on error the
    /// runtime state and override map are unchanged.
    ///
    /// The configuration path defaults to the runtime key itself; use
    /// [`Self::register_at`] when the key name and the on-disk config path differ.
    pub fn register(
        self,
        key: &'static str,
        file_value: impl Fn() -> Value + Send + Sync + 'static,
        effective: impl Fn() -> Value + Send + Sync + 'static,
        apply: impl Fn(&str) -> CommonResult<(Value, Value)> + Send + Sync + 'static,
        reset: impl Fn() -> CommonResult<Value> + Send + Sync + 'static,
    ) -> Self {
        self.register_at(key, None, file_value, effective, apply, reset)
    }

    /// Registers one hot-updatable key and declares the dotted configuration path
    /// that backs it.
    ///
    /// `config_path` exists so a runtime key can keep a stable, documented name even
    /// when the value lives elsewhere in the TOML tree (for example
    /// `master.dir_space_enabled` backing `master.dir_space.enabled`). Passing `None`
    /// keeps the legacy behaviour: the key is treated as the config path itself.
    pub fn register_at(
        mut self,
        key: &'static str,
        config_path: Option<&'static str>,
        file_value: impl Fn() -> Value + Send + Sync + 'static,
        effective: impl Fn() -> Value + Send + Sync + 'static,
        apply: impl Fn(&str) -> CommonResult<(Value, Value)> + Send + Sync + 'static,
        reset: impl Fn() -> CommonResult<Value> + Send + Sync + 'static,
    ) -> Self {
        self.entries.push(EntryInner {
            key,
            config_path,
            file_value: Arc::new(file_value),
            effective: Arc::new(effective),
            apply: Arc::new(apply),
            reset: Arc::new(reset),
        });
        self
    }

    pub fn build(self) -> RuntimeConfigRegistry {
        let mut map = HashMap::new();
        for entry in self.entries {
            map.insert(entry.key, entry);
        }
        RuntimeConfigRegistry {
            inner: Arc::new(RegistryState {
                entries: map,
                overrides: RwLock::new(HashMap::new()),
            }),
        }
    }
}

struct RegistryState {
    entries: HashMap<&'static str, EntryInner>,
    overrides: RwLock<HashMap<String, String>>,
}

/// Shared registry; cheap to clone via [`Arc`].
#[derive(Clone)]
pub struct RuntimeConfigRegistry {
    inner: Arc<RegistryState>,
}

impl RuntimeConfigRegistry {
    pub fn builder() -> RuntimeConfigRegistryBuilder {
        RuntimeConfigRegistryBuilder::new()
    }

    pub fn keys(&self) -> Vec<&'static str> {
        let mut keys: Vec<_> = self.inner.entries.keys().copied().collect();
        keys.sort_unstable();
        keys
    }

    /// Dotted configuration path backing `key`.
    ///
    /// Returns `Some(entry.config_path.unwrap_or(entry.key))` for a registered key — so
    /// keys registered through [`RuntimeConfigRegistryBuilder::register`] resolve to
    /// themselves — and `None` for a key that was never registered.
    ///
    /// Note this is a *resolved* view: an entry that declares no config path (see
    /// [`Self::declared_config_path`]) still falls back to its own key. Use
    /// [`Self::declared_config_path`] when the caller must tell "no configuration-file
    /// field at all" apart from "config path happens to equal the key".
    pub fn config_path(&self, key: &str) -> Option<&'static str> {
        self.inner
            .entries
            .get(key)
            .map(|entry| entry.config_path.unwrap_or(entry.key))
    }

    /// Raw config-path declaration, without the `key` fallback.
    ///
    /// `None` = the key was never registered; `Some(None)` = registered without an
    /// explicit path (the key *is* the config path); `Some(Some(path))` = backed by an
    /// explicit config path.
    ///
    /// This is what lets a caller distinguish a runtime-only knob (registered with
    /// `None` through [`RuntimeConfigRegistryBuilder::register_at`]) from one whose
    /// config path merely coincides with its key.
    pub fn declared_config_path(&self, key: &str) -> Option<Option<&'static str>> {
        self.inner.entries.get(key).map(|entry| entry.config_path)
    }

    pub fn set(
        &self,
        key: &str,
        value: &str,
        audit_source: Option<&str>,
    ) -> CommonResult<RuntimeConfigUpdateResult> {
        let Some(entry) = self.inner.entries.get(key) else {
            return err_box!(
                "unsupported runtime configuration {:?}; writable: {:?}",
                key,
                self.keys()
            );
        };
        let previous_effective = (entry.effective)();
        let (previous_value, new_value) = (entry.apply)(value)?;
        {
            let mut overrides = self
                .inner
                .overrides
                .write()
                .expect("runtime overrides poisoned");
            overrides.insert(key.to_string(), value.to_string());
        }
        info!(
            "runtime config update: key={} source={} previous_effective={} new_effective={} raw={:?}",
            key,
            audit_source.unwrap_or("-"),
            previous_effective,
            new_value,
            value
        );
        Ok(RuntimeConfigUpdateResult {
            config: key.to_string(),
            previous_value,
            value: new_value,
            persistent: false,
        })
    }

    pub fn reset(&self, key: &str, audit_source: Option<&str>) -> CommonResult<Value> {
        let Some(entry) = self.inner.entries.get(key) else {
            return err_box!(
                "unsupported runtime configuration {:?}; writable: {:?}",
                key,
                self.keys()
            );
        };
        let new_effective = (entry.reset)()?;
        {
            let mut overrides = self
                .inner
                .overrides
                .write()
                .expect("runtime overrides poisoned");
            overrides.remove(key);
        }
        info!(
            "runtime config reset: key={} source={} effective={}",
            key,
            audit_source.unwrap_or("-"),
            new_effective
        );
        Ok(new_effective)
    }

    pub fn get(&self, key: &str) -> CommonResult<RuntimeConfigEntryView> {
        let Some(entry) = self.inner.entries.get(key) else {
            return err_box!(
                "unsupported runtime configuration {:?}; writable: {:?}",
                key,
                self.keys()
            );
        };
        Ok(self.view_entry(entry))
    }

    pub fn list(&self) -> Vec<RuntimeConfigEntryView> {
        let mut views: Vec<_> = self
            .inner
            .entries
            .values()
            .map(|e| self.view_entry(e))
            .collect();
        views.sort_by(|a, b| a.key.cmp(&b.key));
        views
    }

    /// Flat JSON map for HTTP `GET` handlers (plus caller-supplied metadata).
    pub fn list_as_json(&self) -> Value {
        let mut obj = serde_json::Map::new();
        for view in self.list() {
            obj.insert(view.key.clone(), view.effective_value);
        }
        Value::Object(obj)
    }

    fn view_entry(&self, entry: &EntryInner) -> RuntimeConfigEntryView {
        let override_value = self
            .inner
            .overrides
            .read()
            .expect("runtime overrides poisoned")
            .get(entry.key)
            .cloned();
        RuntimeConfigEntryView {
            key: entry.key.to_string(),
            file_value: (entry.file_value)(),
            override_value,
            effective_value: (entry.effective)(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    #[test]
    fn set_get_reset_and_reject_unknown() {
        let live = Arc::new(AtomicI64::new(10));
        let file = Arc::new(10i64);
        let live_apply = live.clone();
        let live_reset = live.clone();
        let file_reset = file.clone();

        let registry = RuntimeConfigRegistry::builder()
            .register(
                "demo_limit",
                move || Value::from(*file),
                move || Value::from(live.load(Ordering::Relaxed)),
                move |raw| {
                    let v: i64 = match raw.parse() {
                        Ok(v) => v,
                        Err(_) => return err_box!("bad int"),
                    };
                    if v <= 0 {
                        return err_box!("must be positive");
                    }
                    let previous = live_apply.swap(v, Ordering::Relaxed);
                    Ok((Value::from(previous), Value::from(v)))
                },
                move || {
                    let v = *file_reset;
                    live_reset.store(v, Ordering::Relaxed);
                    Ok(Value::from(v))
                },
            )
            .build();

        let updated = registry.set("demo_limit", "20", Some("test")).unwrap();
        assert_eq!(updated.previous_value, 10);
        assert_eq!(updated.value, 20);
        assert!(!updated.persistent);

        let view = registry.get("demo_limit").unwrap();
        assert_eq!(view.file_value, 10);
        assert_eq!(view.override_value.as_deref(), Some("20"));
        assert_eq!(view.effective_value, 20);

        assert!(registry.set("demo_limit", "nope", None).is_err());
        assert_eq!(registry.get("demo_limit").unwrap().effective_value, 20);

        registry.reset("demo_limit", None).unwrap();
        let after = registry.get("demo_limit").unwrap();
        assert_eq!(after.effective_value, 10);
        assert!(after.override_value.is_none());

        assert!(registry.set("missing", "1", None).is_err());
    }

    #[test]
    fn list_json_sorted_and_complete() {
        let registry = RuntimeConfigRegistry::builder()
            .register(
                "b_key",
                || Value::from(1),
                || Value::from(2),
                |_| Ok((Value::from(1), Value::from(2))),
                || Ok(Value::from(1)),
            )
            .register(
                "a_key",
                || Value::from(true),
                || Value::from(false),
                |_| Ok((Value::from(true), Value::from(false))),
                || Ok(Value::from(true)),
            )
            .build();
        let listed = registry.list();
        assert_eq!(listed[0].key, "a_key");
        assert_eq!(listed[1].key, "b_key");
        assert_eq!(registry.list_as_json()["a_key"], Value::from(false));
    }

    #[test]
    fn config_path_defaults_to_key() {
        let registry = RuntimeConfigRegistry::builder()
            .register(
                "demo.limit",
                || Value::from(1),
                || Value::from(1),
                |_| Ok((Value::from(1), Value::from(1))),
                || Ok(Value::from(1)),
            )
            .build();
        assert_eq!(registry.config_path("demo.limit"), Some("demo.limit"));
        // `register` is sugar for `register_at(.., None, ..)`: nothing explicit declared.
        assert_eq!(registry.declared_config_path("demo.limit"), Some(None));
    }

    #[test]
    fn register_at_declares_custom_path() {
        let registry = RuntimeConfigRegistry::builder()
            .register_at(
                "demo_limit",
                Some("a.b.c"),
                || Value::from(1),
                || Value::from(1),
                |_| Ok((Value::from(1), Value::from(1))),
                || Ok(Value::from(1)),
            )
            .build();
        // The key keeps its own identity…
        assert_eq!(registry.keys(), vec!["demo_limit"]);
        // …while the config path points at the real field.
        assert_eq!(registry.config_path("demo_limit"), Some("a.b.c"));
        assert_eq!(
            registry.declared_config_path("demo_limit"),
            Some(Some("a.b.c"))
        );
    }

    #[test]
    fn register_at_none_declares_no_config_path() {
        // A runtime-only knob: `config_path` falls back to the key, but the declaration
        // itself stays empty so a view builder can tell "no file field" apart from
        // "config path happens to equal the key".
        let registry = RuntimeConfigRegistry::builder()
            .register_at(
                "runtime_only",
                None,
                || Value::from(1),
                || Value::from(1),
                |_| Ok((Value::from(1), Value::from(1))),
                || Ok(Value::from(1)),
            )
            .build();
        assert_eq!(registry.config_path("runtime_only"), Some("runtime_only"));
        assert_eq!(registry.declared_config_path("runtime_only"), Some(None));
    }

    #[test]
    fn config_path_of_unregistered_key_is_none() {
        let registry = RuntimeConfigRegistry::builder()
            .register(
                "demo.limit",
                || Value::from(1),
                || Value::from(1),
                |_| Ok((Value::from(1), Value::from(1))),
                || Ok(Value::from(1)),
            )
            .build();
        assert_eq!(registry.config_path("not.registered"), None);
        assert_eq!(registry.declared_config_path("not.registered"), None);
    }
}
