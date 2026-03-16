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
use crate::CommonResult;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::io;
use std::str::FromStr;
use tracing::Level;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, Layer};

// If log_dir = "", the log is output to standard output
// If file_name = "", the default is server.log
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LogConf {
    pub level: String,
    pub log_dir: String,
    pub file_name: String,
    pub max_log_files: usize,

    // Whether to output thread name and id
    pub display_thread: bool,
    // Whether to output the logging location
    pub display_position: bool,

    pub targets: Vec<String>,
}

impl LogConf {
    pub fn init(&mut self) -> CommonResult<()> {
        Ok(())
    }
}

impl Default for LogConf {
    fn default() -> Self {
        Self {
            level: "INFO".to_string(),
            log_dir: Logger::TARGET_STDOUT.to_string(),
            file_name: "".to_string(),
            max_log_files: 10,
            display_thread: false,
            display_position: true,
            targets: vec![],
        }
    }
}

static INSTANCE: OnceCell<Logger> = OnceCell::new();

#[allow(unused)]
#[derive(Debug)]
pub struct Logger {
    inner: Vec<WorkerGuard>,
}

impl Logger {
    pub const TARGET_STDOUT: &'static str = "stdout";

    pub const TARGET_STDERR: &'static str = "stderr";

    pub fn new(conf: LogConf) -> Self {
        if !conf.targets.is_empty() {
            Self::with_target(conf)
        } else {
            let level = Level::from_str(&conf.level).unwrap();
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(level)
                .with_ansi(false)
                .event_format(LogFormatter::new(&conf));

            let (writer, guard) = Self::create_writer(&conf);
            subscriber.with_writer(writer).init();

            Logger { inner: vec![guard] }
        }
    }

    pub fn with_target(conf: LogConf) -> Self {
        let level = Level::from_str(&conf.level).unwrap();

        let target = conf.targets[0].to_string();
        let (w1, g1) = Self::create_writer(&conf);
        let layer1 = fmt::layer()
            .with_ansi(false)
            .with_writer(w1)
            .event_format(LogFormatter::new(&conf))
            .with_filter(filter_fn(move |m| {
                m.target() != target && m.level() <= &level
            }));

        let mut conf1 = conf.clone();
        let target1 = conf.targets[0].to_string();
        conf1.file_name = format!("{}.log", target1);
        let (w2, g2) = Self::create_writer(&conf1);
        let layer2 = fmt::layer()
            .with_ansi(false)
            .with_writer(w2)
            .event_format(LogFormatter::new(&conf))
            .with_filter(filter_fn(move |m| {
                m.target() == target1 && m.level() <= &level
            }));

        tracing_subscriber::registry()
            .with(layer1)
            .with(layer2)
            .init();

        Logger {
            inner: vec![g1, g2],
        }
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

        if conf.log_dir.to_ascii_lowercase() == Self::TARGET_STDOUT || conf.log_dir.is_empty() {
            tracing_appender::non_blocking(io::stdout())
        } else if conf.log_dir.to_ascii_lowercase() == Self::TARGET_STDERR {
            tracing_appender::non_blocking(io::stderr())
        } else {
            let appender = RollingFileAppender::builder()
                .rotation(Rotation::DAILY)
                .filename_prefix(file_name)
                .max_log_files(conf.max_log_files)
                .build(&conf.log_dir)
                .expect("initializing rolling file appender failed");
            tracing_appender::non_blocking(appender)
        }
    }
}
