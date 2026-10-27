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

//! Contract for `effective_config_view`.
//!
//! The view answers one question honestly: *what is this process running, and
//! where did each value come from?* Each test here pins one specific way that
//! answer can silently become wrong — an override that lands next to the field
//! it was meant to address, a mistyped value that the deserializer rejects, a
//! duration left unparsed because validation was skipped.

use curvine_config::config_view::{effective_config_view, ConfigView, ViewProfile};
use curvine_config::{
    ClusterConf, ConfigSource, ConfigSourceMap, RuntimeConfigRegistry, RuntimeConfigRegistryBuilder,
};
use serde_json::Value;
use std::sync::{Arc, RwLock};

/// A stub knob backed by a shared cell, the way a real registry is backed by the
/// live component it controls (an `AtomicUsize` on the task manager, an override
/// slot on the dir-space manager).
///
/// The cell is the point. `effective_value` is read from it, while `values` is
/// rebuilt from the pushed override, so those are two independent computations —
/// and a stub that hard-codes `effective_value` would hide the case where they
/// disagree, which is exactly the defect the console must never show.
struct Knob {
    key: &'static str,
    path: Option<&'static str>,
    file: Value,
    live: Arc<RwLock<Value>>,
}

impl Knob {
    /// `initial` must match what `ClusterConf::default()` holds at `path`, or the
    /// view would report a state the process never starts in.
    fn new(key: &'static str, path: Option<&'static str>, initial: Value) -> Self {
        Self {
            key,
            path,
            file: initial.clone(),
            live: Arc::new(RwLock::new(initial)),
        }
    }

    fn registry(&self) -> RuntimeConfigRegistry {
        let file = self.file.clone();
        let live_read = Arc::clone(&self.live);
        let live_apply = Arc::clone(&self.live);
        let file_reset = self.file.clone();
        RuntimeConfigRegistryBuilder::new()
            .register_at(
                self.key,
                self.path,
                move || file.clone(),
                move || live_read.read().expect("knob cell").clone(),
                move |raw| {
                    let previous = live_apply.read().expect("knob cell").clone();
                    let next = parse_like_configuration(raw);
                    *live_apply.write().expect("knob cell") = next.clone();
                    Ok((previous, next))
                },
                move || Ok(file_reset.clone()),
            )
            .build()
    }
}

/// How a real `apply` turns the query-string payload into a typed value.
fn parse_like_configuration(raw: &str) -> Value {
    match serde_json::from_str::<Value>(raw) {
        Ok(value) => value,
        Err(_) => Value::String(raw.to_string()),
    }
}

fn build(conf: &ClusterConf, registry: &RuntimeConfigRegistry) -> ConfigView {
    effective_config_view(conf, ViewProfile::Full, registry, &ConfigSourceMap::new())
        .expect("view must build")
}

fn build_with_sources(
    conf: &ClusterConf,
    registry: &RuntimeConfigRegistry,
    sources: &ConfigSourceMap,
) -> ConfigView {
    effective_config_view(conf, ViewProfile::Full, registry, sources).expect("view must build")
}

fn at<'a>(values: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cursor = values;
    for segment in path.split('.') {
        cursor = cursor.get(segment)?;
    }
    Some(cursor)
}

/// Without an override the view is just the effective configuration, and the
/// entry is still listed so the console can show its file value.
#[test]
fn view_reports_effective_values_without_override() {
    let conf = ClusterConf::default();
    let knob = Knob::new(
        "probe_key",
        Some("master.dir_space.enabled"),
        Value::from(false),
    );
    let view = build(&conf, &knob.registry());

    assert_eq!(
        at(&view.values, "master.dir_space.enabled"),
        Some(&Value::from(false))
    );
    assert_eq!(view.runtime.len(), 1);
    assert_eq!(view.runtime[0].path, "master.dir_space.enabled");
    assert_eq!(view.runtime[0].override_value, None);
    assert_eq!(view.writable, vec!["probe_key".to_string()]);
    assert!(view.unresolved.is_empty());
}

/// An override must change the value at the *declared* path, and the path must be
/// re-attributed to the runtime layer instead of still claiming the file.
#[test]
fn override_lands_on_declared_config_path() {
    let conf = ClusterConf::default();
    let knob = Knob::new(
        "probe_key",
        Some("master.dir_space.enabled"),
        Value::from(false),
    );
    let registry = knob.registry();
    registry
        .set("probe_key", "true", Some("test"))
        .expect("override must apply");

    let mut sources = ConfigSourceMap::new();
    sources.insert("master.dir_space.enabled".to_string(), ConfigSource::File);
    let view = build_with_sources(&conf, &registry, &sources);

    assert_eq!(
        at(&view.values, "master.dir_space.enabled"),
        Some(&Value::from(true)),
        "the override must land on the nested field, not beside it"
    );
    assert_eq!(
        view.sources.get("master.dir_space.enabled"),
        Some(&ConfigSource::Runtime),
        "an overridden path must not keep claiming the file layer"
    );
    assert_eq!(view.runtime[0].override_value, Some(Value::from(true)));
}

/// A declared path that does not exist in the tree must be reported, not dropped.
/// This is the shape of the bug where `master.dir_space_enabled` (flat) is
/// registered while the field is really `master.dir_space.enabled` (nested): the
/// write succeeds, the value never changes, and nothing says why.
#[test]
fn phantom_config_path_is_reported_as_unresolved() {
    let conf = ClusterConf::default();
    let knob = Knob::new(
        "phantom_key",
        Some("master.dir_space_enabled"),
        Value::from(false),
    );
    let registry = knob.registry();
    registry
        .set("phantom_key", "true", Some("test"))
        .expect("override must apply");

    let view = build(&conf, &registry);

    assert_eq!(
        view.unresolved,
        vec!["phantom_key".to_string()],
        "an unresolvable declaration must be surfaced"
    );
    assert!(
        view.runtime.is_empty(),
        "an unresolvable key must not be advertised as a configurable field"
    );
    assert_eq!(
        at(&view.values, "master.dir_space_enabled"),
        None,
        "the phantom path must not be invented in the tree"
    );
    assert_eq!(
        at(&view.values, "master.dir_space.enabled"),
        Some(&Value::from(false)),
        "the real field must be untouched"
    );
    // Still writable: the endpoint accepts it, so hiding it from the console would
    // make a supported operation undiscoverable.
    assert_eq!(view.writable, vec!["phantom_key".to_string()]);
}

/// A JSON array pushed as a registry string must be translated into a real array.
/// Passing the raw string through makes re-deserialization fail outright, because
/// `Vec<String>` does not accept a JSON blob.
#[test]
fn array_override_is_translated_from_json_string() {
    let conf = ClusterConf::default();
    let knob = Knob::new(
        "paths_key",
        Some("master.dir_space.tracked_paths"),
        Value::from(Vec::<String>::new()),
    );
    let registry = knob.registry();
    registry
        .set("paths_key", r#"["/a","/b"]"#, Some("test"))
        .expect("override must apply");

    let view = build(&conf, &registry);

    assert_eq!(
        at(&view.values, "master.dir_space.tracked_paths"),
        Some(&Value::from(vec!["/a", "/b"])),
        "a JSON array must land as an array, not as a string"
    );
    assert_eq!(
        view.runtime[0].override_value,
        Some(Value::from(vec!["/a", "/b"]))
    );
}

/// The view is rebuilt through validation, so an override the configuration
/// would reject must fail loudly instead of being displayed as if it took effect.
///
/// `master.conn_limit` is the case that matters: `MasterConf::validate` rejects
/// zero, and a view that only patched the serialized tree would happily render
/// `conn_limit: 0` as the effective value — a number the process can never run
/// with, since `LimitConf::new` asserts it is positive.
#[test]
fn override_rejected_by_validation_fails_the_view() {
    let conf = ClusterConf::default();
    let knob = Knob::new("limit_key", Some("master.conn_limit"), Value::from(8));
    let registry = knob.registry();
    registry
        .set("limit_key", "0", Some("test"))
        .expect("the registry itself accepts it; validation is the gate");

    let err = effective_config_view(&conf, ViewProfile::Full, &registry, &ConfigSourceMap::new())
        .expect_err("a zero connection limit must not survive the view's validation gate");
    let message = err.to_string();
    assert!(
        message.contains("conn_limit"),
        "the error must name the rejected field, got: {message}"
    );
}

/// A valid duration override is shown under the documented base name, and the
/// value is the one that was pushed — not the compiled-in default.
#[test]
fn duration_override_lands_on_documented_name() {
    let conf = ClusterConf::default();
    let knob = Knob::new("timeout_key", Some("job.task_timeout"), Value::from("1h"));
    let registry = knob.registry();
    registry
        .set("timeout_key", "1800000ms", Some("test"))
        .expect("override must apply");

    let view = build(&conf, &registry);

    assert_eq!(
        at(&view.values, "job.task_timeout"),
        Some(&Value::from("1800000ms")),
        "the documented base name must carry the pushed value"
    );
    assert_eq!(
        at(&view.values, "job.task_timeout_str"),
        None,
        "the twin name must not survive serialization"
    );
}

/// Loaded attributions must survive untouched: only overridden paths change layer.
#[test]
fn base_sources_survive_without_override() {
    let conf = ClusterConf::default();
    let knob = Knob::new(
        "probe_key",
        Some("master.dir_space.enabled"),
        Value::from(false),
    );
    let registry = knob.registry();

    let mut sources = ConfigSourceMap::new();
    sources.insert("master.hostname".to_string(), ConfigSource::File);
    sources.insert("worker.hostname".to_string(), ConfigSource::Env);
    let view = build_with_sources(&conf, &registry, &sources);

    assert_eq!(
        view.sources.get("master.hostname"),
        Some(&ConfigSource::File)
    );
    assert_eq!(
        view.sources.get("worker.hostname"),
        Some(&ConfigSource::Env)
    );
    assert_eq!(
        view.sources.get("master.dir_space.enabled"),
        None,
        "an untouched path must not gain a layer it was never assigned"
    );
}

/// A knob with no configuration-file entry stays writable but is not advertised
/// as a field. The worker's inflight budget is the real case: its file value is a
/// compiled-in constant, so there is no path to render.
#[test]
fn entry_without_config_path_is_writable_but_hidden() {
    let conf = ClusterConf::default();
    let knob = Knob::new("runtime_only_key", None, Value::from("unused"));
    let view = build(&conf, &knob.registry());

    assert_eq!(
        view.writable,
        vec!["runtime_only_key".to_string()],
        "it must remain pushable"
    );
    assert!(
        view.runtime.is_empty(),
        "it must not be rendered as a configuration field"
    );
    assert!(view.unresolved.is_empty());
    assert_eq!(
        at(&view.values, "runtime_only_key"),
        None,
        "a runtime-only knob must not appear in the tree"
    );
}

/// The tree must report the value the process runs with, not a second
/// computation of the same knob.
///
/// `dir_space.tracked_paths` is the observed case: the pushed entries go through
/// `normalize_tracked_path_list` (trailing slashes dropped, duplicates collapsed,
/// non-absolute entries rejected), and the normalized list lands in
/// `tracked_paths_normalized` — a `#[serde(skip)]` field that never reaches
/// `values`. Rebuilding `values` from the pushed override therefore showed
/// `['/a///']` while the manager was tracking `['/a']`: two different answers for
/// one setting, and the wrong one was on screen.
#[test]
fn values_report_the_live_value_not_a_second_computation() {
    let conf = ClusterConf::default();
    // The live cell holds what normalization left behind; the override keeps the
    // operator's raw spelling. A stub that let both sides agree would not test
    // anything, because the defect is precisely their disagreement.
    let knob = Knob::new(
        "paths_key",
        Some("master.dir_space.tracked_paths"),
        Value::from(Vec::<String>::new()),
    );
    let registry = knob.registry();
    registry
        .set("paths_key", r#"["/a///","/b/"]"#, Some("test"))
        .expect("override must apply");
    *knob.live.write().expect("knob cell") = Value::from(vec!["/a".to_string(), "/b".to_string()]);

    let view = build(&conf, &registry);

    assert_eq!(
        at(&view.values, "master.dir_space.tracked_paths"),
        Some(&Value::from(vec!["/a", "/b"])),
        "the tree must show the value the process is running with"
    );
    assert_eq!(
        view.runtime[0].override_value,
        Some(Value::from(vec!["/a///", "/b/"])),
        "the pushed spelling stays visible as the override, so the difference can be explained"
    );
    assert_ne!(
        at(&view.values, "master.dir_space.tracked_paths"),
        view.runtime[0].override_value.as_ref(),
        "the two columns disagreeing is the defect this pins"
    );
}
