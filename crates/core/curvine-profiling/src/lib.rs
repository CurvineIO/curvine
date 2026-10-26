//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! Opt-in CPU flame-graph sampling.
//!
//! Fully gated:
//!
//! - Without the `profiling` Cargo feature no sampler is compiled in.
//! - With the feature, sampling still requires
//!   [`ProfilingConf::cpu_enabled`]; the HTTP routes are registered from
//!   that same flag.
//!
//! Sampling is process-global: `pprof` keeps one profiler behind a global
//! lock, so a second concurrent session cannot run. The second caller is
//! rejected rather than queued — a queued request would occupy a thread for
//! the duration of the first sample and then report a window the caller
//! never asked for.

use curvine_config::ProfilingConf;

/// Why a sampling request could not be served.
#[derive(Debug, PartialEq, Eq)]
pub enum ProfilingError {
    /// The build has no `profiling` feature, so no sampler exists.
    FeatureDisabled,
    /// `profiling.cpu_enabled` is false.
    NotEnabled,
    /// Another sampling session is already running in this process.
    Busy,
    /// The sampler itself failed (signal handler setup, report build, ...).
    Sampler(String),
    /// The window collected no samples, so no flame graph could be drawn.
    /// This happens when the process is idle for the whole window.
    NoSamples { seconds: u64 },
}

impl std::fmt::Display for ProfilingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FeatureDisabled => write!(
                f,
                "CPU profiling requires a build with the `profiling` feature"
            ),
            Self::NotEnabled => {
                write!(f, "CPU profiling is disabled (profiling.cpu_enabled=false)")
            }
            Self::Busy => write!(f, "another CPU profiling session is already running"),
            Self::Sampler(reason) => write!(f, "CPU profiling failed: {reason}"),
            Self::NoSamples { seconds } => write!(
                f,
                "no CPU samples were collected in {seconds}s; the process was idle. \
                 Retry while the workload is active"
            ),
        }
    }
}

impl std::error::Error for ProfilingError {}

/// Colour scheme for the rendered flame graph.
///
/// The graph is a static SVG served by the sampled process, so it cannot
/// inherit the console's CSS. The caller states its theme instead and the
/// frozen colours are chosen to match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProfileTheme {
    /// Light background for a light console.
    #[default]
    Light,
    /// Dark background for a dark console.
    Dark,
}

impl ProfileTheme {
    /// Parses the `theme` query value; unknown values fall back to `Light`.
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim) {
            Some(v) if v.eq_ignore_ascii_case("dark") => Self::Dark,
            _ => Self::Light,
        }
    }
}

/// A completed sample ready to be returned to the caller.
#[derive(Debug)]
pub struct ProfileOutput {
    /// Rendered SVG flame graph.
    pub svg: Vec<u8>,
    /// Duration actually sampled, which is shorter than requested when the
    /// request was clamped by `profiling.max_duration_secs`.
    pub seconds: u64,
    /// Frequency actually used.
    pub frequency: u32,
}

/// Whether this build can serve samples at all.
pub fn available() -> bool {
    cfg!(all(feature = "profiling", unix))
}

/// Rejects a configuration that asks for sampling this binary cannot serve.
///
/// Callers run this at startup. The alternative — mounting no route and
/// leaving `cpu_enabled = true` in the config — is a silent downgrade: an
/// operator would believe samples are available when the endpoint simply
/// does not exist. Failing here makes the mismatch visible where it can be
/// acted on.
pub fn check_startup(conf: &ProfilingConf) -> Result<(), ProfilingError> {
    if conf.cpu_enabled && !available() {
        return Err(ProfilingError::FeatureDisabled);
    }
    Ok(())
}

/// Samples CPU and returns a rendered SVG flame graph.
///
/// This is a blocking call: it holds an OS thread for the whole window
/// because the sampler is signal-driven and its guard is not known to be
/// `Send`. Async callers must run it on a blocking pool rather than an
/// executor thread.
///
/// The returned `seconds`/`frequency` are the values actually used, so a
/// clamped request is visible to the caller instead of silently differing
/// from what it asked for.
/// Picks inferno's frozen palette for a console theme.
///
/// Both flame graphs (CPU and heap) are static SVGs built by the sampled
/// process, so neither can read the console's CSS variables. These settings
/// are the closest available match; `uicolor` is the search box and the reset
/// button.
///
/// `FlamegraphOptions` and `pprof::flamegraph::Options` are the same underlying
/// `inferno` type, so one helper serves both.
#[cfg(all(any(feature = "profiling", feature = "memory-profiling"), unix))]
fn flamegraph_options(theme: ProfileTheme) -> pprof::flamegraph::Options<'static> {
    use pprof::flamegraph::{color, Options};
    let mut options = Options::default();
    match theme {
        // Light: keep inferno's default warm background, which matches the
        // light console's near-white surfaces.
        ProfileTheme::Light => {
            options.bgcolors = Some(color::BackgroundColor::Yellow);
            options.uicolor = color::Color::new(0, 0, 0);
        }
        // Dark: a flat near-black plate instead of a gradient, with light UI
        // text so the search box stays legible.
        ProfileTheme::Dark => {
            options.bgcolors = Some(color::BackgroundColor::Flat(color::Color::new(
                0x0f, 0x11, 0x15,
            )));
            options.uicolor = color::Color::new(0xe6, 0xe6, 0xe6);
        }
    }
    options
}

pub fn sample(
    conf: &ProfilingConf,
    requested_secs: Option<u64>,
    requested_frequency: Option<u32>,
    theme: ProfileTheme,
) -> Result<ProfileOutput, ProfilingError> {
    if !cfg!(feature = "profiling") {
        return Err(ProfilingError::FeatureDisabled);
    }
    if !conf.cpu_enabled {
        return Err(ProfilingError::NotEnabled);
    }

    let seconds = conf.clamp_duration(requested_secs);
    // `cpu_frequency` is validated to be >= 1 at load time, but a config
    // built by hand (tests, embedding) can still carry 0, and
    // `u32::clamp(1, 0)` panics. Fall back to the default instead of
    // taking the process down over a bad sampling hint.
    let ceiling = conf.cpu_frequency.max(1);
    let frequency = requested_frequency.unwrap_or(ceiling).clamp(1, ceiling);

    imp::sample(conf, seconds, frequency, theme)
}

/// Sampling is Unix-only. Other targets report the feature as absent
/// rather than pretending to sample.
#[cfg(not(all(feature = "profiling", unix)))]
mod imp {
    use super::{ProfileOutput, ProfilingConf, ProfilingError};

    pub fn sample(
        _conf: &ProfilingConf,
        _seconds: u64,
        _frequency: u32,
        _theme: super::ProfileTheme,
    ) -> Result<ProfileOutput, ProfilingError> {
        Err(ProfilingError::FeatureDisabled)
    }
}

#[cfg(all(feature = "profiling", unix))]
mod imp {
    use super::{ProfileOutput, ProfilingConf, ProfilingError};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    /// Upper bound on a rendered profile, so a pathological sample cannot
    /// become an unbounded response body.
    const MAX_OUTPUT_BYTES: usize = 32 * 1024 * 1024;

    /// Below this size the rendered flame graph carries no frames.
    ///
    /// Measured on this codebase (1s window, 200 Hz): a sample with no CPU
    /// activity renders 0 bytes, the smallest sample with any frames
    /// observed (~20ms of load) rendered ~26 KiB. 512 sits inside that gap
    /// with three orders of magnitude of headroom on the "has frames" side.
    const EMPTY_SVG_MAX_BYTES: usize = 512;

    /// Set while a session runs. `pprof` also refuses a second `build()`
    /// while a guard is alive, but that surfaces as an opaque setup error
    /// after the caller has blocked; claiming the slot first lets us answer
    /// "busy" immediately and distinguishably.
    static SESSION_BUSY: AtomicBool = AtomicBool::new(false);

    struct SessionSlot;

    impl SessionSlot {
        fn try_acquire() -> Result<Self, ProfilingError> {
            SESSION_BUSY
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .map(|_| Self)
                .map_err(|_| ProfilingError::Busy)
        }
    }

    impl Drop for SessionSlot {
        fn drop(&mut self) {
            SESSION_BUSY.store(false, Ordering::Release);
        }
    }

    pub fn sample(
        conf: &ProfilingConf,
        seconds: u64,
        frequency: u32,
        theme: super::ProfileTheme,
    ) -> Result<ProfileOutput, ProfilingError> {
        let _slot = SessionSlot::try_acquire()?;

        let blocklist: Vec<&str> = conf.blocklist.iter().map(String::as_str).collect();
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(frequency as i32)
            .blocklist(&blocklist)
            .build()
            .map_err(|e| ProfilingError::Sampler(e.to_string()))?;

        std::thread::sleep(Duration::from_secs(seconds));

        let report = guard
            .report()
            .build()
            .map_err(|e| ProfilingError::Sampler(e.to_string()))?;

        let mut svg = Vec::new();
        let mut options = super::flamegraph_options(theme);
        report
            .flamegraph_with_options(&mut svg, &mut options)
            .map_err(|e| ProfilingError::Sampler(e.to_string()))?;

        // Dropping the guard restores the previous SIGPROF handler. Do it
        // before any early return below so a rejected sample still puts the
        // signal handler back.
        drop(guard);

        // An idle process collects no samples and `pprof` renders an empty
        // document (0 bytes, measured). Returning that as success would be
        // indistinguishable from a broken sampler, so surface the empty
        // window explicitly: callers can retry under load instead of
        // treating it as data.
        if svg.len() <= EMPTY_SVG_MAX_BYTES {
            return Err(ProfilingError::NoSamples { seconds });
        }

        // Note: this checks the rendered size after allocation; it bounds
        // what we hand back, not what the renderer may have materialised.
        // Bounding the render itself would need the per-frame counts, which
        // `pprof` does not expose before rendering.
        if svg.len() > MAX_OUTPUT_BYTES {
            return Err(ProfilingError::Sampler(format!(
                "profile output {} bytes exceeds the {} byte limit",
                svg.len(),
                MAX_OUTPUT_BYTES
            )));
        }

        Ok(ProfileOutput {
            svg,
            seconds,
            frequency,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn available_matches_feature_flag() {
        assert_eq!(available(), cfg!(all(feature = "profiling", unix)));
    }

    #[test]
    fn startup_check_rejects_enabled_config_without_the_feature() {
        let enabled = ProfilingConf {
            cpu_enabled: true,
            ..Default::default()
        };
        match check_startup(&enabled) {
            Ok(()) => assert!(available(), "a build without the sampler must reject it"),
            Err(err) => {
                assert!(!available());
                assert_eq!(err, ProfilingError::FeatureDisabled);
            }
        }
        // A disabled config is always accepted, whatever the build.
        assert!(check_startup(&ProfilingConf::default()).is_ok());
    }

    #[test]
    fn disabled_config_refuses_to_sample() {
        let conf = ProfilingConf::default();
        let err = sample(&conf, Some(1), None, ProfileTheme::Light).unwrap_err();
        // Without the feature the build gate fires first; with it, the
        // config gate does.
        assert!(
            matches!(
                err,
                ProfilingError::FeatureDisabled | ProfilingError::NotEnabled
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn clamps_requested_values_through_to_the_sampler() {
        // A zero-second request keeps the sampling window empty. Under an
        // idle test harness that surfaces as `NoSamples`; under any build
        // without the sampler it surfaces as `FeatureDisabled`. Either way
        // the clamp happens before sampling, which is what this checks.
        let conf = ProfilingConf {
            cpu_enabled: true,
            max_duration_secs: 30,
            cpu_frequency: 500,
            ..Default::default()
        };
        match sample(&conf, Some(9_999), Some(9_999), ProfileTheme::Dark) {
            Ok(output) => {
                assert_eq!(output.seconds, 30);
                assert_eq!(output.frequency, 500);
            }
            Err(err) => assert!(
                matches!(
                    err,
                    ProfilingError::FeatureDisabled
                        | ProfilingError::Sampler(_)
                        | ProfilingError::NoSamples { .. }
                ),
                "unexpected error: {err}"
            ),
        }
    }
}

/// Why a heap-profile request could not be served.
#[derive(Debug, PartialEq, Eq)]
pub enum MemoryProfileError {
    /// The build has no `memory-profiling` feature, so jemalloc was not
    /// compiled with `--enable-prof`.
    FeatureDisabled,
    /// `profiling.memory_enabled` is false.
    NotEnabled,
    /// jemalloc was built with profiling but is not sampling this process —
    /// normally because the process started without the `malloc_conf` symbol.
    NotActivated,
    /// Another dump is already running.
    Busy,
    /// Reading or converting the profile failed.
    Sampler(String),
}

impl std::fmt::Display for MemoryProfileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FeatureDisabled => write!(
                f,
                "heap profiling requires a build with the `memory-profiling` feature"
            ),
            Self::NotEnabled => {
                write!(
                    f,
                    "heap profiling is disabled (profiling.memory_enabled=false)"
                )
            }
            Self::NotActivated => write!(
                f,
                "jemalloc is not sampling this process; start it from a binary built \
                 with `memory-profiling` so `malloc_conf` activates profiling"
            ),
            Self::Busy => write!(f, "another heap profile dump is already running"),
            Self::Sampler(reason) => write!(f, "heap profiling failed: {reason}"),
        }
    }
}

impl std::error::Error for MemoryProfileError {}

/// A rendered heap flame graph.
#[derive(Debug)]
pub struct MemoryProfileOutput {
    /// SVG flame graph of live allocations.
    pub svg: Vec<u8>,
}

/// Whether this build can serve a heap profile at all.
pub fn memory_available() -> bool {
    cfg!(all(feature = "memory-profiling", target_os = "linux"))
}

/// Renders a flame graph of live allocations.
///
/// Sampling happens inside jemalloc as the program allocates; this call only
/// walks the recorded samples, so there is no observation window to wait for.
/// The returned graph is attributed by *allocation site*, which is what makes
/// it useful for a large resident set: the width of a frame is bytes still
/// held, not CPU time.
///
/// Blocking: it holds the process-wide profile lock and writes a temporary
/// file. Async callers must run it on a blocking pool.
pub fn sample_memory(
    conf: &ProfilingConf,
    theme: ProfileTheme,
) -> Result<MemoryProfileOutput, MemoryProfileError> {
    if !cfg!(feature = "memory-profiling") {
        return Err(MemoryProfileError::FeatureDisabled);
    }
    if !conf.memory_enabled {
        return Err(MemoryProfileError::NotEnabled);
    }
    imp_memory::sample_memory(theme)
}

#[cfg(not(all(feature = "memory-profiling", target_os = "linux")))]
mod memory_imp {
    use super::{MemoryProfileError, MemoryProfileOutput, ProfileTheme};

    pub fn sample_memory(_theme: ProfileTheme) -> Result<MemoryProfileOutput, MemoryProfileError> {
        Err(MemoryProfileError::FeatureDisabled)
    }
}

#[cfg(all(feature = "memory-profiling", target_os = "linux"))]
mod memory_imp {
    use super::{MemoryProfileError, MemoryProfileOutput, ProfileTheme};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// One dump at a time: the library serialises on its own lock, but a queued
    /// request would block a blocking-pool thread just to fail behind it.
    static DUMP_BUSY: AtomicBool = AtomicBool::new(false);

    struct DumpSlot;

    impl DumpSlot {
        fn try_acquire() -> Result<Self, MemoryProfileError> {
            DUMP_BUSY
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .map(|_| Self)
                .map_err(|_| MemoryProfileError::Busy)
        }
    }

    impl Drop for DumpSlot {
        fn drop(&mut self) {
            DUMP_BUSY.store(false, Ordering::Release);
        }
    }

    /// Theme-matched plate for the heap graph (inferno 0.12 through
    /// `jemalloc_pprof`). Same colours as the CPU graph's helper, but that
    /// helper's `Options` comes from inferno 0.11 and is not interchangeable.
    fn memory_flamegraph_options(
        theme: ProfileTheme,
    ) -> jemalloc_pprof::FlamegraphOptions<'static> {
        use jemalloc_pprof::FlamegraphOptions;
        let mut options = FlamegraphOptions::default();
        match theme {
            ProfileTheme::Light => {
                options.bgcolors = Some(inferno::flamegraph::color::BackgroundColor::Yellow);
                options.uicolor = inferno::flamegraph::color::Color::new(0, 0, 0);
            }
            ProfileTheme::Dark => {
                options.bgcolors = Some(inferno::flamegraph::color::BackgroundColor::Flat(
                    inferno::flamegraph::color::Color::new(0x0f, 0x11, 0x15),
                ));
                options.uicolor = inferno::flamegraph::color::Color::new(0xe6, 0xe6, 0xe6);
            }
        }
        options
    }

    pub fn sample_memory(theme: ProfileTheme) -> Result<MemoryProfileOutput, MemoryProfileError> {
        let _slot = DumpSlot::try_acquire()?;

        let ctl = jemalloc_pprof::PROF_CTL.as_ref().ok_or_else(|| {
            MemoryProfileError::Sampler("jemalloc profile control unavailable".into())
        })?;
        let mut guard = ctl.blocking_lock();
        if !guard.activated() {
            return Err(MemoryProfileError::NotActivated);
        }

        // jemalloc_pprof depends on inferno 0.12 while pprof uses 0.11, so the
        // two `Options` types are distinct and cannot share a helper. The
        // settings are duplicated deliberately: the plate and UI text follow
        // the console theme, the frame palette keeps inferno's default because
        // it encodes allocation sites rather than aesthetics.
        let mut options = memory_flamegraph_options(theme);
        let svg = guard
            .dump_flamegraph_with_options(&mut options)
            .map_err(|e| MemoryProfileError::Sampler(e.to_string()))?;

        if svg.len() <= 512 {
            return Err(MemoryProfileError::Sampler(
                "heap profile contained no samples; the process has not allocated \
                 enough since start to trigger the sampling interval"
                    .into(),
            ));
        }
        Ok(MemoryProfileOutput { svg })
    }
}

#[cfg(all(feature = "memory-profiling", target_os = "linux"))]
use memory_imp as imp_memory;
#[cfg(not(all(feature = "memory-profiling", target_os = "linux")))]
use memory_imp as imp_memory;
