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

use std::fmt;

use tracing::{Event, Subscriber};
use tracing_log::NormalizeEvent;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::{
    format::{FormatEvent, FormatFields},
    FmtContext, FormattedFields,
};
use tracing_subscriber::registry::LookupSpan;

use crate::common::{LocalTime, LogConf};

// Thread name and id are immutable for the lifetime of a thread; cache them
// to avoid a TLS lookup + Arc ref-count on every log event.
thread_local! {
    static THREAD_INFO: String = {
        let t = std::thread::current();
        match t.name() {
            Some(name) => format!("{}-{:?}", name, t.id()),
            None => format!("{:?}", t.id()),
        }
    };
}

pub struct LogFormatter {
    display_thread: bool,
    display_position: bool,
}

impl LogFormatter {
    pub fn new(conf: &LogConf) -> Self {
        Self {
            display_thread: conf.display_thread,
            display_position: conf.display_position,
        }
    }
    pub fn short_target(target: &str) -> &str {
        target.rsplit_once("::").map_or(target, |(_, t)| t)
    }
}

/// format log message eg:
/// [2024-07-16 16:23:12.626] [main] {curvine_server::master::fs::master.rs:33} INFO - start master success!
impl<S, N> FormatEvent<S, N> for LogFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        LocalTime::new().format_time(&mut writer)?;

        // Format values from the events metadata:
        let normalized_meta = event.normalized_metadata();
        let metadata = normalized_meta.as_ref().unwrap_or_else(|| event.metadata());
        write!(writer, " {}", metadata.level())?;

        if self.display_thread {
            THREAD_INFO.with(|info| write!(writer, " {}", info))?;
        }

        // Format all the spans in the event's span context.
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                write!(writer, " {}", span.metadata().name())?;

                // `FormattedFields` is a formatted representation of the span's
                // fields, which is stored in its extensions by the `fmt` layer's
                // `new_span` method. The fields will have been formatted
                // by the same field formatter that's provided to the event
                // formatter in the `FmtContext`.
                let ext = span.extensions();
                if let Some(fields) = &ext.get::<FormattedFields<N>>() {
                    if !fields.is_empty() {
                        write!(writer, "{{{}}}", fields)?;
                    }
                }
                write!(writer, ": ")?;
            }
        }

        if self.display_position {
            let target = Self::short_target(metadata.target());
            write!(writer, " {}.rs:{} ", target, metadata.line().unwrap_or(0))?;
        }

        // Write fields on the event
        ctx.format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}
