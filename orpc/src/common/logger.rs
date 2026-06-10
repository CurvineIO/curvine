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

use crate::common::LogFormatter;
use once_cell::sync::OnceCell;
use rolling_file::{RollingConditionBasic, RollingFileAppender};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use tracing::Level;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::filter::{filter_fn, LevelFilter};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, Layer, Registry};

// If log_dir = "" or "stdout", logs go to stdout; "stderr" goes to stderr;
// otherwise logs are written to files under log_dir.
// If file_name = "", defaults to "curvine".
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LogConf {
    pub level: String,
    pub log_dir: String,
    pub file_name: String,
    pub max_log_files: usize,
    // Size-based rotation: single file size limit in MB, 0 means no size limit
    pub max_file_size_mb: u64,
    // Time-based rotation: "" (none) | "daily" | "hourly"
    // Can be combined with max_file_size_mb; rotation triggers on whichever condition fires first
    pub time_rotation: String,
    // Whether to output thread name and id
    pub display_thread: bool,
    // Whether to output the logging location
    pub display_position: bool,
    pub targets: Vec<String>,
}

impl Default for LogConf {
    fn default() -> Self {
        Self {
            level: "INFO".to_string(),
            log_dir: Logger::TARGET_STDOUT.to_string(),
            file_name: String::new(),
            max_log_files: 10,
            max_file_size_mb: 100,
            time_rotation: String::new(),
            display_thread: false,
            display_position: true,
            targets: vec![],
        }
    }
}

static INSTANCE: OnceCell<Logger> = OnceCell::new();

#[derive(Debug)]
pub struct Logger {
    #[allow(dead_code)]
    inner: Vec<WorkerGuard>,
}

impl Logger {
    pub const TARGET_STDOUT: &'static str = "stdout";
    pub const TARGET_STDERR: &'static str = "stderr";

    pub fn new(conf: LogConf) -> Self {
        if !conf.targets.is_empty() {
            Self::init_with_target(conf)
        } else {
            let level = Level::from_str(&conf.level).unwrap_or(Level::INFO);
            let (writer, guard) = Self::create_writer(&conf);
            tracing_subscriber::fmt()
                .with_max_level(level)
                .with_ansi(false)
                .event_format(LogFormatter::new(&conf))
                .with_writer(writer)
                .init();
            Logger { inner: vec![guard] }
        }
    }

    fn init_with_target(conf: LogConf) -> Self {
        let level = Level::from_str(&conf.level).unwrap_or(Level::INFO);
        // Share the target list across filter closures via Arc, avoiding per-closure Vec clones
        let targets: Arc<[String]> = conf.targets.clone().into();

        let mut guards: Vec<WorkerGuard> = Vec::with_capacity(conf.targets.len() + 1);
        let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> =
            Vec::with_capacity(conf.targets.len() + 1);

        // Default layer: all events that do NOT belong to any named target
        let (w0, g0) = Self::create_writer(&conf);
        let ts = targets.clone();
        let default_layer = fmt::layer()
            .with_ansi(false)
            .with_writer(w0)
            .event_format(LogFormatter::new(&conf))
            .with_filter(filter_fn(move |m| {
                !ts.iter().any(|t| m.target() == t.as_str())
            }));
        guards.push(g0);
        layers.push(Box::new(default_layer));

        // One dedicated layer per target, written to its own file.
        //
        // Deduplication is performed on the *sanitized filename* (not the raw
        // target string) so that two distinct targets that map to the same name
        // after sanitization (e.g. "a::b" and "a/b" both → "a_b") are treated
        // as a single physical file, preventing two independent RollingFileAppender
        // instances from racing over the same path.
        //
        // The default log file base is pre-inserted so that no target can
        // accidentally collide with it.
        let default_base = {
            let raw = if conf.file_name.is_empty() {
                "curvine"
            } else {
                &conf.file_name
            };
            raw.strip_suffix(".log").unwrap_or(raw).to_string()
        };
        let mut seen_files = HashSet::new();
        seen_files.insert(default_base);

        for target in conf.targets.iter() {
            // Sanitize target name for use as a filename:
            // replace "::" and "/" with "_" to keep filenames clean on all platforms.
            let file_name = target.replace("::", "_").replace('/', "_");

            // Guard against degenerate targets (e.g. empty string or "::") that
            // produce an empty filename, which would create a hidden ".log" file.
            if file_name.is_empty() {
                continue;
            }

            if !seen_files.insert(file_name.clone()) {
                // Either a duplicate target or a collision with another sanitized
                // target / the default log file — skip to keep one owner per file.
                continue;
            }
            let (w, g) = Self::create_writer_named(&conf, &file_name);
            guards.push(g);
            let t = target.clone();
            // NOTE: the filter uses an *exact* match on the tracing target, which
            // is the full module path at the call site (e.g. "curvine_server::master").
            // Events from sub-modules (e.g. "curvine_server::master::fs") are NOT
            // routed here; configure targets with full module paths accordingly.
            let layer = fmt::layer()
                .with_ansi(false)
                .with_writer(w)
                .event_format(LogFormatter::new(&conf))
                .with_filter(filter_fn(move |m| m.target() == t.as_str()));
            layers.push(Box::new(layer));
        }

        // Global level filter: sets MAX_LEVEL_HINT so tracing callsite macros
        // skip disabled events before constructing them, consistent with the
        // simple path's .with_max_level(level).
        tracing_subscriber::registry()
            .with(layers)
            .with(LevelFilter::from_level(level))
            .init();
        Logger { inner: guards }
    }

    pub fn default() {
        Self::init(LogConf::default())
    }

    pub fn init(conf: LogConf) {
        INSTANCE.get_or_init(|| Self::new(conf));
    }

    pub fn create_writer(conf: &LogConf) -> (NonBlocking, WorkerGuard) {
        let file_name = if conf.file_name.is_empty() {
            "curvine"
        } else {
            &conf.file_name
        };
        Self::create_writer_named(conf, file_name)
    }

    fn create_writer_named(conf: &LogConf, file_name: &str) -> (NonBlocking, WorkerGuard) {
        let log_dir = &conf.log_dir;
        if log_dir.is_empty() || log_dir.eq_ignore_ascii_case(Self::TARGET_STDOUT) {
            tracing_appender::non_blocking(io::stdout())
        } else if log_dir.eq_ignore_ascii_case(Self::TARGET_STDERR) {
            tracing_appender::non_blocking(io::stderr())
        } else {
            // Strip ".log" suffix if already present to avoid "name.log.log".
            // Fall back to "curvine" if stripping yields an empty string (e.g.
            // file_name = ".log"), to avoid creating a hidden ".log" file.
            let base = {
                let s = file_name.strip_suffix(".log").unwrap_or(file_name);
                if s.is_empty() {
                    "curvine"
                } else {
                    s
                }
            };
            let file_path = format!("{}/{}.log", log_dir, base);
            let condition = Self::build_rolling_condition(conf);
            let appender = RollingFileAppender::new(&file_path, condition, conf.max_log_files)
                .expect("initializing rolling file appender failed");
            tracing_appender::non_blocking(appender)
        }
    }

    fn build_rolling_condition(conf: &LogConf) -> RollingConditionBasic {
        let mut condition = RollingConditionBasic::new();

        if conf.max_file_size_mb > 0 {
            let max_bytes = conf.max_file_size_mb.saturating_mul(1024 * 1024);
            condition = condition.max_size(max_bytes);
        }

        if conf.time_rotation.eq_ignore_ascii_case("daily") {
            condition = condition.daily();
        } else if conf.time_rotation.eq_ignore_ascii_case("hourly") {
            condition = condition.hourly();
        } else {
            // Warn about unrecognized values so typos (e.g. "Daily", "weekly") are
            // surfaced immediately. Use eprintln! because this runs during logger
            // initialization, before tracing subscribers are in place.
            if !conf.time_rotation.is_empty() {
                eprintln!(
                    "[logger] unrecognized time_rotation {:?}; valid values: \"daily\", \"hourly\"",
                    conf.time_rotation
                );
            }
            if conf.max_file_size_mb == 0 {
                // Neither size nor time rotation configured; fall back to daily
                // to prevent unbounded file growth.
                condition = condition.daily();
            }
        }

        condition
    }
}
