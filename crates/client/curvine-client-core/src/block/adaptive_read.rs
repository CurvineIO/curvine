// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//! Conservative, connection-local control of speculative block reads.

use std::time::{Duration, Instant};

const SAMPLE_COUNT: usize = 32;
const MIN_BYTES_PER_SECOND: u128 = 128 * 1024 * 1024;
pub(super) const MAX_AUTO_BYTES: usize = 4 * 1024 * 1024;
const MAX_AUTO_CHUNKS: usize = 4;

#[derive(Clone, Copy)]
struct Metrics {
    bytes: usize,
    elapsed: Duration,
    p95: Duration,
}

impl Metrics {
    fn bytes_per_second(self) -> f64 {
        self.bytes as f64 / self.elapsed.as_secs_f64()
    }

    fn above_minimum(self) -> bool {
        (self.bytes as u128) * 1_000_000_000 > MIN_BYTES_PER_SECOND * self.elapsed.as_nanos()
    }

    fn qualifies(self, previous: Self) -> bool {
        // Compare rational rates without floating point at the 10% boundary.
        // Extremely large durations/byte counts fall back to floating point;
        // ordinary request timeouts and response sizes fit comfortably in u128.
        let gain = (self.bytes as u128)
            .checked_mul(previous.elapsed.as_nanos())
            .and_then(|value| value.checked_mul(10))
            .zip(
                (previous.bytes as u128)
                    .checked_mul(self.elapsed.as_nanos())
                    .and_then(|value| value.checked_mul(11)),
            )
            .map_or_else(
                || self.bytes_per_second() >= previous.bytes_per_second() * 1.1,
                |(trial, baseline)| trial >= baseline,
            );
        let latency = self.p95.as_nanos() * 10 <= previous.p95.as_nanos() * 11;
        gain && latency
    }
}

#[derive(Default)]
struct Samples {
    first_send: Option<Instant>,
    bytes: usize,
    latency: Vec<Duration>,
}

impl Samples {
    fn push(&mut self, sent: Instant, received: Instant, bytes: usize) -> Option<Metrics> {
        self.first_send = Some(self.first_send.map_or(sent, |first| first.min(sent)));
        self.bytes = self.bytes.saturating_add(bytes);
        self.latency.push(received.saturating_duration_since(sent));
        if self.latency.len() != SAMPLE_COUNT {
            return None;
        }

        // The interval starts when the first request was enqueued, not when the
        // caller starts consuming buffered responses. Consumer pauses can only
        // lower the observed rate and raise latency, making growth conservative.
        let elapsed = received
            .saturating_duration_since(self.first_send.expect("sampled request"))
            .max(Duration::from_nanos(1));
        self.latency.sort_unstable();
        let p95_index = (SAMPLE_COUNT * 95).div_ceil(100) - 1;
        Some(Metrics {
            bytes: self.bytes,
            elapsed,
            p95: self.latency[p95_index],
        })
    }
}

enum Stage {
    Measuring,
    Trial { previous: Metrics },
}

pub(super) struct AdaptiveReadWindow {
    // The stable setting is retained across streams; a trial has to finish
    // before its setting becomes stable.
    stable_window: usize,
    window: usize,
    stage: Stage,
    disabled: bool,
    generation: u64,
    samples: Samples,
}

impl AdaptiveReadWindow {
    pub(super) fn new() -> Self {
        Self {
            stable_window: 1,
            window: 1,
            stage: Stage::Measuring,
            disabled: false,
            generation: 0,
            samples: Samples::default(),
        }
    }

    pub(super) fn max_window(chunk_size: usize) -> usize {
        (MAX_AUTO_BYTES / chunk_size.max(1)).clamp(1, MAX_AUTO_CHUNKS)
    }

    pub(super) fn window(&self, chunk_size: usize) -> usize {
        self.window.min(Self::max_window(chunk_size))
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn observe(
        &mut self,
        generation: u64,
        sent: Instant,
        received: Instant,
        bytes: usize,
        max_window: usize,
    ) {
        if self.disabled
            || self.window == MAX_AUTO_CHUNKS && matches!(self.stage, Stage::Measuring)
            || self.window > max_window
            || generation != self.generation
        {
            return;
        }
        if bytes == 0 {
            // An empty response is not part of a sequential data sample. Do
            // not bridge EOF (or an incomplete trial) with later responses.
            self.end_stream();
            return;
        }
        let Some(metrics) = self.samples.push(sent, received, bytes) else {
            return;
        };

        match self.stage {
            Stage::Measuring => {
                if metrics.above_minimum() && self.stable_window * 2 <= max_window {
                    self.window = self.stable_window * 2;
                    self.stage = Stage::Trial { previous: metrics };
                    log::debug!(
                        "adaptive read: trial window {} (stable {}, {:.1} MiB/s, p95 {:?})",
                        self.window,
                        self.stable_window,
                        metrics.bytes_per_second() / (1024.0 * 1024.0),
                        metrics.p95
                    );
                }
            }
            Stage::Trial { previous } => {
                if !metrics.qualifies(previous) {
                    log::debug!(
                        "adaptive read: rejected window {} ({:.1} vs {:.1} MiB/s, p95 {:?} vs {:?})",
                        self.window,
                        metrics.bytes_per_second() / (1024.0 * 1024.0),
                        previous.bytes_per_second() / (1024.0 * 1024.0),
                        metrics.p95,
                        previous.p95
                    );
                    self.fail();
                    return;
                }
                self.stable_window = self.window;
                log::debug!(
                    "adaptive read: accepted window {} ({:.1} MiB/s, p95 {:?})",
                    self.window,
                    metrics.bytes_per_second() / (1024.0 * 1024.0),
                    metrics.p95
                );
                if self.window * 2 <= max_window && metrics.above_minimum() {
                    self.window *= 2;
                    self.stage = Stage::Trial { previous: metrics };
                    log::debug!("adaptive read: trial window {}", self.window);
                } else {
                    self.stage = Stage::Measuring;
                }
            }
        }
        self.clear_samples();
    }

    /// Keep accepted settings but don't combine samples from different streams
    /// or an unfinished trial with a later stream/EOF rewind.
    pub(super) fn end_stream(&mut self) {
        if self.window != self.stable_window {
            log::debug!(
                "adaptive read: unfinished trial window {} reverted to {}",
                self.window,
                self.stable_window
            );
        }
        self.window = self.stable_window;
        self.stage = Stage::Measuring;
        self.clear_samples();
    }

    pub(super) fn seek(&mut self, eof_rewind: bool) {
        if !eof_rewind {
            if self.window != 1 {
                log::debug!("adaptive read: nonsequential seek reset window to 1");
            }
            self.stable_window = 1;
        }
        self.end_stream();
    }

    pub(super) fn fail(&mut self) {
        if !self.disabled {
            log::debug!("adaptive read: disabled growth for this connection");
        }
        self.stable_window = 1;
        self.window = 1;
        self.stage = Stage::Measuring;
        self.disabled = true;
        self.clear_samples();
    }

    fn clear_samples(&mut self) {
        self.samples = Samples::default();
        self.generation = self.generation.wrapping_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Simulate response delivery at a known aggregate rate and per-request
    // latency without depending on scheduler or network timing.
    fn samples(
        controller: &mut AdaptiveReadWindow,
        origin: Instant,
        interval: Duration,
        latency: Duration,
        max_window: usize,
    ) {
        for i in 0..SAMPLE_COUNT as u32 {
            let sent = origin + interval * i;
            controller.observe(
                controller.generation(),
                sent,
                sent + latency,
                1024 * 1024,
                max_window,
            );
        }
    }

    fn baseline(controller: &mut AdaptiveReadWindow, origin: Instant, max: usize) {
        samples(
            controller,
            origin,
            Duration::from_millis(2),
            Duration::from_millis(1),
            max,
        );
    }

    #[test]
    fn trials_require_32_samples_and_improve_rate_without_increasing_p95() {
        let mut c = AdaptiveReadWindow::new();
        let now = Instant::now();
        for i in 0..31u32 {
            let sent = now + Duration::from_millis(2) * i;
            c.observe(
                c.generation(),
                sent,
                sent + Duration::from_millis(1),
                1024 * 1024,
                4,
            );
        }
        assert_eq!(c.window(1024 * 1024), 1);
        let sent = now + Duration::from_millis(62);
        c.observe(
            c.generation(),
            sent,
            sent + Duration::from_millis(1),
            1024 * 1024,
            4,
        );
        assert_eq!(c.window(1024 * 1024), 2);

        samples(
            &mut c,
            now + Duration::from_secs(1),
            Duration::from_micros(1500),
            Duration::from_millis(1),
            4,
        );
        assert_eq!(c.stable_window, 2);
        assert_eq!(c.window(1024 * 1024), 4);
        samples(
            &mut c,
            now + Duration::from_secs(2),
            Duration::from_micros(1100),
            Duration::from_millis(1),
            4,
        );
        assert_eq!(c.stable_window, 4);
        assert_eq!(c.window(1024 * 1024), 4);
        assert!(!c.disabled);
    }

    #[test]
    fn insufficient_rate_gain_or_excess_p95_disables_growth() {
        for (interval, latency) in [
            (Duration::from_millis(2), Duration::from_millis(1)),
            (Duration::from_micros(1500), Duration::from_micros(1101)),
        ] {
            let now = Instant::now();
            let mut c = AdaptiveReadWindow::new();
            baseline(&mut c, now, 4);
            assert_eq!(c.window(1), 2);
            samples(&mut c, now + Duration::from_secs(1), interval, latency, 4);
            assert!(c.disabled);
            assert_eq!(c.window(1), 1);
            c.seek(false);
            baseline(&mut c, now + Duration::from_secs(2), 4);
            assert_eq!(c.window(1), 1);
        }
    }

    #[test]
    fn baseline_rate_threshold_and_buffered_delivery_are_conservative() {
        let now = Instant::now();
        let mut c = AdaptiveReadWindow::new();
        samples(
            &mut c,
            now,
            Duration::from_millis(10),
            Duration::from_millis(1),
            4,
        );
        assert_eq!(c.window(1), 1);

        // Thirty-two replies may already be buffered when the consumer resumes;
        // they must not be timed only by how quickly the consumer drains them.
        let sent = now + Duration::from_secs(1);
        for i in 0..SAMPLE_COUNT as u32 {
            c.observe(
                c.generation(),
                sent,
                sent + Duration::from_secs(10) + Duration::from_micros(i as u64),
                1024 * 1024,
                4,
            );
        }
        assert_eq!(c.window(1), 1);
    }

    #[test]
    fn trial_accepts_exactly_ten_percent_gain_and_p95_at_the_limit() {
        let now = Instant::now();
        let mut c = AdaptiveReadWindow::new();
        // The 32 MiB baseline takes exactly 110 ms; the trial takes 100 ms.
        samples(
            &mut c,
            now,
            Duration::from_micros(3500),
            Duration::from_micros(1500),
            2,
        );
        samples(
            &mut c,
            now + Duration::from_secs(1),
            Duration::from_micros(3177),
            Duration::from_micros(1513),
            2,
        );
        assert_eq!(c.stable_window, 2);
        assert!(!c.disabled);

        let mut c = AdaptiveReadWindow::new();
        baseline(&mut c, now, 2);
        samples(
            &mut c,
            now + Duration::from_secs(1),
            Duration::from_micros(1500),
            Duration::from_micros(1100),
            2,
        );
        assert_eq!(c.stable_window, 2);
        assert!(!c.disabled);
    }

    #[test]
    fn baseline_must_be_strictly_above_128_mib_per_second() {
        let now = Instant::now();
        let mut c = AdaptiveReadWindow::new();
        // 32 MiB delivered over 250 ms is exactly 128 MiB/s.
        samples(
            &mut c,
            now,
            Duration::from_millis(8),
            Duration::from_millis(2),
            4,
        );
        assert_eq!(c.window(1), 1);
        assert!(!c.disabled);
    }

    #[test]
    fn caps_account_for_outstanding_chunks_and_allow_one_large_chunk() {
        assert_eq!(AdaptiveReadWindow::max_window(1024 * 1024), 4);
        assert_eq!(AdaptiveReadWindow::max_window(2 * 1024 * 1024), 2);
        assert_eq!(AdaptiveReadWindow::max_window(3 * 1024 * 1024), 1);
        assert_eq!(AdaptiveReadWindow::max_window(8 * 1024 * 1024), 1);
        let now = Instant::now();
        let mut c = AdaptiveReadWindow::new();
        baseline(&mut c, now, 1);
        assert_eq!(c.window(1), 1);
        baseline(&mut c, now + Duration::from_secs(1), 2);
        samples(
            &mut c,
            now + Duration::from_secs(2),
            Duration::from_micros(1500),
            Duration::from_millis(1),
            2,
        );
        assert_eq!(c.stable_window, 2);
        assert_eq!(c.window(1), 2);
        assert_eq!(c.window(8 * 1024 * 1024), 1);
    }

    #[test]
    fn rewind_and_stream_reset_keep_stable_window_but_seek_and_failure_reset_it() {
        let now = Instant::now();
        let mut c = AdaptiveReadWindow::new();
        baseline(&mut c, now, 4);
        samples(
            &mut c,
            now + Duration::from_secs(1),
            Duration::from_micros(1500),
            Duration::from_millis(1),
            4,
        );
        assert_eq!(c.window(1), 4); // The next trial has not been accepted.
        c.end_stream();
        assert_eq!(c.window(1), 2);
        c.seek(true);
        assert_eq!(c.window(1), 2);
        c.seek(false);
        assert_eq!(c.window(1), 1);
        baseline(&mut c, now + Duration::from_secs(2), 4);
        assert_eq!(c.window(1), 2);
        c.fail();
        assert_eq!(c.window(1), 1);
        baseline(&mut c, now + Duration::from_secs(3), 4);
        assert_eq!(c.window(1), 1);
    }

    #[test]
    fn stale_and_empty_responses_do_not_count_toward_a_trial() {
        let now = Instant::now();
        let mut c = AdaptiveReadWindow::new();
        let old = c.generation();
        c.end_stream();
        c.observe(
            c.generation(),
            now,
            now + Duration::from_millis(1),
            1024 * 1024,
            4,
        );
        assert_eq!(c.samples.latency.len(), 1);
        for _ in 0..SAMPLE_COUNT {
            c.observe(old, now, now + Duration::from_millis(1), 1024 * 1024, 4);
            c.observe(c.generation(), now, now + Duration::from_millis(1), 0, 4);
        }
        assert_eq!(c.window(1), 1);
        assert!(c.samples.latency.is_empty());
    }
}
