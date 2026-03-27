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

use crate::common::Metrics;
use once_cell::sync::Lazy;

pub static HEAP_TRACE_CAPTURES_TOTAL: Lazy<Option<crate::common::Counter>> = Lazy::new(|| {
    Metrics::new_counter(
        "heap_trace_captures_total",
        "Total heap trace capture attempts",
    )
    .ok()
});

pub fn record_capture_attempt() {
    if let Some(counter) = HEAP_TRACE_CAPTURES_TOTAL.as_ref() {
        counter.inc();
    }
}
