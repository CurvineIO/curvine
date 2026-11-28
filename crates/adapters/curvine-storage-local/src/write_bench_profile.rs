// Copyright 2026 OPPO.
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

//! Process-local diagnostics for the 16-client staged pwrite/shared ring WRITE trial.
//! The harness selects the trial; only its worker child should call [`enable`].

use std::collections::BTreeMap;
use std::sync::{mpsc, Mutex, OnceLock};
use std::time::{Duration, Instant};

pub(super) struct QueuedWrite {
    pub(super) admission: Duration,
    pub(super) enqueued: Instant,
}

pub(super) struct RingReply {
    pub(super) queued: QueuedWrite,
    pub(super) polled: Instant,
    pub(super) transferred: Instant,
    pub(super) ready: Instant,
}

enum Sample {
    WriteCall(Duration, Option<(RingReply, Instant)>),
    Publish(u64),
}

struct Collector {
    tx: mpsc::Sender<Sample>,
    rx: Mutex<mpsc::Receiver<Sample>>,
}

static COLLECTOR: OnceLock<Collector> = OnceLock::new();

/// Enable collection for the rest of this trial child. Call before starting clients.
pub fn enable() {
    COLLECTOR.get_or_init(|| {
        let (tx, rx) = mpsc::channel();
        Collector {
            tx,
            rx: Mutex::new(rx),
        }
    });
}

pub(super) fn start_write(len: usize) -> Option<Instant> {
    (len == crate::uring::CHUNK_BYTES && COLLECTOR.get().is_some()).then(Instant::now)
}

fn record(sample: Sample) {
    if let Some(collector) = COLLECTOR.get() {
        let _ = collector.tx.send(sample);
    }
}

pub(super) fn record_write_call(elapsed: Duration, ring: Option<(RingReply, Instant)>) {
    record(Sample::WriteCall(elapsed, ring));
}

/// Supply the harness's elapsed publish time for one successful write, in nanoseconds.
pub fn record_publish(ns: u64) {
    record(Sample::Publish(ns));
}

/// Drain completed samples. Call after the trial writers have finished; absent keys had no samples.
pub fn take_samples() -> BTreeMap<&'static str, Vec<u64>> {
    let mut samples = BTreeMap::<_, Vec<_>>::new();
    if let Some(collector) = COLLECTOR.get() {
        let mut push = |key, ns| samples.entry(key).or_default().push(ns);
        let ns = |d: Duration| d.as_nanos() as u64;
        for sample in collector.rx.lock().unwrap().try_iter() {
            match sample {
                Sample::WriteCall(d, ring) => {
                    push("write_call", ns(d));
                    if let Some((reply, received)) = ring {
                        push("ring_admission", ns(reply.queued.admission));
                        push(
                            "ring_enqueue_to_poll",
                            ns(reply.polled - reply.queued.enqueued),
                        );
                        push("ring_transfer", ns(reply.transferred - reply.polled));
                        push("ring_reply", ns(received - reply.ready));
                    }
                }
                Sample::Publish(d) => push("publish", d),
            }
        }
    }
    samples
}
