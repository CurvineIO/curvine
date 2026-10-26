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

use curvine_fs_api::Path;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// Path is normalized by curvine-fs-api, but does not impose a maximum length.
// Never retain an unbounded key: an unusually long path activates the same
// conservative fallback as a full entry table.
const MAX_TRACKED_PATH_BYTES: usize = 4 * 1024;

/// Tracks paths that should continue to use the leader after a metadata write.
///
/// Clones share the same session state. The owner should create one tracker
/// for each client context and share it with that context's clients.
#[derive(Clone)]
pub(crate) struct SessionPathTracker {
    state: Arc<Mutex<TrackerState>>,
    capacity: usize,
    ttl: Duration,
}

#[derive(Default)]
struct TrackerState {
    paths: HashMap<PathKey, Expiry>,
    overflow_until: Option<Expiry>,
    in_flight_paths: HashMap<PathKey, usize>,
    in_flight_overflow: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PathKey(Arc<str>);

#[derive(Clone, Copy)]
enum Expiry {
    At(Instant),
    Never,
}

impl Expiry {
    fn after(now: Instant, ttl: Duration) -> Self {
        now.checked_add(ttl).map_or(Self::Never, Self::At)
    }

    fn is_live_at(self, now: Instant) -> bool {
        match self {
            Self::At(deadline) => now < deadline,
            Self::Never => true,
        }
    }

    fn later(self, other: Self) -> Self {
        match (self, other) {
            (Self::Never, _) | (_, Self::Never) => Self::Never,
            (Self::At(left), Self::At(right)) => Self::At(left.max(right)),
        }
    }
}

impl PathKey {
    fn from_path(path: &Path) -> Self {
        // Metadata RPCs encode Path::path(), which is also the component form
        // used by Path::has_prefix. This intentionally treats cv://authority/x
        // and /x as the same Curvine path and follows Path's normalization for
        // repeated and trailing slashes, relative paths, and dot components.
        Self(Arc::from(path.path()))
    }

    fn matches_or_contains(&self, other: &Self) -> bool {
        Path::has_prefix(&self.0, &other.0) || Path::has_prefix(&other.0, &self.0)
    }
}

impl TrackerState {
    fn prune(&mut self, now: Instant) {
        self.paths.retain(|_, expiry| expiry.is_live_at(now));
        if self
            .overflow_until
            .is_some_and(|expiry| !expiry.is_live_at(now))
        {
            self.overflow_until = None;
        }
    }

    fn mark(&mut self, key: PathKey, expiry: Expiry, capacity: usize) {
        if key.0.len() > MAX_TRACKED_PATH_BYTES {
            self.extend_overflow(expiry);
            return;
        }

        if let Some(existing) = self.paths.get_mut(&key) {
            *existing = expiry;
        } else if self.paths.len() < capacity {
            self.paths.insert(key, expiry);
        } else {
            // Keep the table bounded. Since an untracked live mutation might
            // overlap any later read, pin all reads to the leader until it
            // expires instead of evicting an older path silently.
            self.extend_overflow(expiry);
        }
    }

    fn extend_overflow(&mut self, expiry: Expiry) {
        self.overflow_until = Some(match self.overflow_until {
            Some(existing) => existing.later(expiry),
            None => expiry,
        });
    }

    fn is_sticky(&mut self, key: &PathKey, now: Instant) -> bool {
        self.prune(now);
        if self.in_flight_overflow > 0
            || self
                .overflow_until
                .is_some_and(|expiry| expiry.is_live_at(now))
        {
            return true;
        }

        self.in_flight_paths
            .keys()
            .any(|marked| marked.matches_or_contains(key))
            || self
                .paths
                .iter()
                .any(|(marked, expiry)| expiry.is_live_at(now) && marked.matches_or_contains(key))
    }
}

/// Keeps a mutation's paths sticky for the full time its RPC is in flight,
/// then starts a fresh TTL when the operation returns or is cancelled.
#[must_use = "the mutation guard must stay alive across the RPC await"]
pub(crate) struct SessionMutationGuard {
    tracker: SessionPathTracker,
    paths: Vec<PathKey>,
    overflow: bool,
}

impl Drop for SessionMutationGuard {
    fn drop(&mut self) {
        let now = Instant::now();
        let expiry = Expiry::after(now, self.tracker.ttl);
        let mut state = self.tracker.state.lock().unwrap();
        state.prune(now);

        for key in &self.paths {
            state.mark(key.clone(), expiry, self.tracker.capacity);
            let remove = state.in_flight_paths.get_mut(key).is_some_and(|count| {
                *count = count.saturating_sub(1);
                *count == 0
            });
            if remove {
                state.in_flight_paths.remove(key);
            }
        }

        if self.overflow {
            state.in_flight_overflow = state.in_flight_overflow.saturating_sub(1);
            state.extend_overflow(expiry);
        }
    }
}

impl SessionPathTracker {
    /// Create a tracker with at most `capacity` remembered paths.
    ///
    /// If the table fills or a path is too large to retain, every read stays
    /// sticky until the untracked mutation's TTL ends. A zero capacity therefore
    /// remains correct, at the cost of sending every read to the leader for the
    /// configured TTL after a mutation.
    pub(crate) fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(TrackerState::default())),
            capacity,
            ttl,
        }
    }

    /// Mark one path before sending a metadata mutation.
    #[cfg(test)]
    pub(crate) fn mark_mutation(&self, path: &Path) {
        self.mark_mutations([path]);
    }

    /// Mark several paths under one lock, for batch mutations.
    #[cfg(test)]
    pub(crate) fn mark_mutations<'a>(&self, paths: impl IntoIterator<Item = &'a Path>) {
        self.mark_mutations_at(paths, Instant::now());
    }

    /// Mark both rename subtrees together so reads cannot observe a partial mark.
    #[cfg(test)]
    pub(crate) fn mark_rename(&self, source: &Path, destination: &Path) {
        self.mark_mutations([source, destination]);
    }

    /// Begin tracking an in-flight mutation. Its affected paths stay sticky
    /// without expiring until the returned guard is dropped, which starts a
    /// fresh TTL even when the RPC failed or its future was cancelled.
    pub(crate) fn begin_mutation<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a Path>,
    ) -> SessionMutationGuard {
        let now = Instant::now();
        let expiry = Expiry::after(now, self.ttl);
        let mut state = self.state.lock().unwrap();
        state.prune(now);

        let mut tracked_paths = Vec::new();
        let mut seen = HashSet::new();
        let mut overflow = false;

        for path in paths {
            let path_value = path.path();
            if path_value.len() > MAX_TRACKED_PATH_BYTES {
                state.extend_overflow(expiry);
                overflow = true;
                continue;
            }

            let key = PathKey(Arc::from(path_value));
            if seen.contains(&key) {
                continue;
            }

            if tracked_paths.len() >= self.capacity
                || (!state.in_flight_paths.contains_key(&key)
                    && state.in_flight_paths.len() >= self.capacity)
            {
                // Do not let a large batch turn the in-flight map or guard
                // into an unbounded path cache. The global overflow mark
                // keeps every read on the leader through completion and TTL.
                state.extend_overflow(expiry);
                overflow = true;
                continue;
            }

            state.mark(key.clone(), expiry, self.capacity);
            seen.insert(key.clone());

            if let Some(count) = state.in_flight_paths.get_mut(&key) {
                *count = count.saturating_add(1);
                tracked_paths.push(key);
            } else {
                state.in_flight_paths.insert(key.clone(), 1);
                tracked_paths.push(key);
            }
        }

        if overflow {
            state.in_flight_overflow = state.in_flight_overflow.saturating_add(1);
        }

        SessionMutationGuard {
            tracker: self.clone(),
            paths: tracked_paths,
            overflow,
        }
    }

    /// Whether this path or a directory listing that contains it must use the
    /// leader. Matching uses Path's normalized component-prefix semantics.
    pub(crate) fn is_sticky(&self, path: &Path) -> bool {
        self.is_sticky_at(path, Instant::now())
    }

    #[cfg(test)]
    fn mark_mutations_at<'a>(&self, paths: impl IntoIterator<Item = &'a Path>, now: Instant) {
        let expiry = Expiry::after(now, self.ttl);
        let mut state = self.state.lock().unwrap();
        state.prune(now);
        for path in paths {
            let path_value = path.path();
            if path_value.len() > MAX_TRACKED_PATH_BYTES {
                state.extend_overflow(expiry);
            } else {
                state.mark(PathKey(Arc::from(path_value)), expiry, self.capacity);
            }
        }
    }

    fn is_sticky_at(&self, path: &Path, now: Instant) -> bool {
        self.state
            .lock()
            .unwrap()
            .is_sticky(&PathKey::from_path(path), now)
    }
}

#[cfg(test)]
mod tests {
    use super::SessionPathTracker;
    use curvine_fs_api::Path;
    use std::time::{Duration, Instant};

    fn path(value: &str) -> Path {
        Path::new(value).unwrap()
    }

    #[test]
    fn pins_subtrees_and_ancestor_listings_with_component_boundaries() {
        let tracker = SessionPathTracker::new(16, Duration::from_secs(30));
        tracker.mark_mutation(&path("/users/alice/docs/file"));

        assert!(tracker.is_sticky(&path("/users/alice/docs/file")));
        assert!(tracker.is_sticky(&path("/users/alice/docs/file/chunk")));
        assert!(tracker.is_sticky(&path("/users/alice/docs")));
        assert!(tracker.is_sticky(&path("/")));
        assert!(!tracker.is_sticky(&path("/users/alice2/docs")));
        assert!(!tracker.is_sticky(&path("/users/bob/docs")));
    }

    #[test]
    fn uses_path_normalization_and_preserves_relative_path_behavior() {
        let tracker = SessionPathTracker::new(16, Duration::from_secs(30));
        let normalized = path("relative//directory/file/");
        tracker.mark_mutation(&normalized);

        assert!(tracker.is_sticky(&path("relative/directory/file/child")));
        assert!(!tracker.is_sticky(&path("/relative/directory/file/child")));
        // Path normalizes repeated/trailing slashes but preserves dot components.
        tracker.mark_mutation(&path("relative/../outside"));
        assert!(tracker.is_sticky(&path("relative/../outside/child")));
        assert!(!tracker.is_sticky(&path("outside/child")));

        let empty = path("");
        tracker.mark_mutation(&empty);
        assert!(tracker.is_sticky(&empty));
    }

    #[test]
    fn cv_uri_authority_is_not_a_separate_metadata_path_namespace() {
        let tracker = SessionPathTracker::new(16, Duration::from_secs(30));
        tracker.mark_mutation(&path("cv://cluster-a/data//file/"));

        assert!(tracker.is_sticky(&path("/data/file/child")));
        assert!(tracker.is_sticky(&path("cv://cluster-b/data/file")));
        assert!(!tracker.is_sticky(&path("/data/filesibling")));
    }

    #[test]
    fn rename_marks_both_subtrees_and_their_listing_ancestors() {
        let tracker = SessionPathTracker::new(16, Duration::from_secs(30));
        tracker.mark_rename(&path("/src/tree"), &path("/dst/tree"));

        assert!(tracker.is_sticky(&path("/src/tree/child")));
        assert!(tracker.is_sticky(&path("/dst/tree/child")));
        assert!(tracker.is_sticky(&path("/src")));
        assert!(tracker.is_sticky(&path("/dst")));
        assert!(!tracker.is_sticky(&path("/src-sibling/tree")));
    }

    #[test]
    fn capacity_overflow_pins_all_reads_until_untracked_marks_expire() {
        let tracker = SessionPathTracker::new(1, Duration::from_secs(10));
        let start = Instant::now();
        let first = path("/first");
        let overflow = path("/second");
        let unrelated = path("/unrelated");

        tracker.mark_mutations_at([&first], start);
        tracker.mark_mutations_at([&overflow], start + Duration::from_secs(1));
        assert!(tracker.is_sticky_at(&unrelated, start + Duration::from_secs(10)));
        assert!(!tracker.is_sticky_at(&unrelated, start + Duration::from_secs(11)));
    }

    #[test]
    fn expiry_uses_monotonic_deadlines_and_clones_share_only_their_session() {
        let ttl = Duration::from_secs(5);
        let first_session = SessionPathTracker::new(8, ttl);
        let second_session = SessionPathTracker::new(8, ttl);
        let shared = first_session.clone();
        let target = path("/session/file");
        let start = Instant::now();

        first_session.mark_mutations_at([&target], start);
        assert!(shared.is_sticky_at(&target, start + ttl - Duration::from_nanos(1)));
        assert!(!shared.is_sticky_at(&target, start + ttl));
        assert!(!second_session.is_sticky_at(&target, start + Duration::from_secs(1)));
    }

    #[test]
    fn long_paths_trigger_the_bounded_memory_fallback() {
        let tracker = SessionPathTracker::new(8, Duration::from_secs(30));
        let long_path = format!("/{}", "x".repeat(4 * 1024));
        tracker.mark_mutation(&path(&long_path));

        assert!(tracker.is_sticky(&path("/unrelated")));
    }

    #[test]
    fn in_flight_mutation_survives_ttl_and_drop_starts_a_fresh_ttl() {
        let ttl = Duration::from_millis(10);
        let tracker = SessionPathTracker::new(8, ttl);
        let target = path("/slow/write");
        let guard = tracker.begin_mutation([&target]);

        // Model an RPC that remains pending longer than the configured
        // sticky window. In-flight paths must not expire during the request.
        std::thread::sleep(Duration::from_millis(25));
        assert!(tracker.is_sticky(&path("/slow/write/child")));

        // The guard's drop also covers RPC error and future cancellation.
        // It restarts the TTL after the operation is no longer in flight.
        drop(guard);
        assert!(tracker.is_sticky_at(&target, Instant::now()));
        std::thread::sleep(Duration::from_millis(25));
        assert!(!tracker.is_sticky(&target));
    }

    #[test]
    fn in_flight_capacity_overflow_pins_every_read_until_mutation_finishes() {
        let ttl = Duration::from_millis(10);
        let tracker = SessionPathTracker::new(1, ttl);
        let first = path("/first");
        let overflow = path("/overflow");
        let unrelated = path("/unrelated");
        let first_guard = tracker.begin_mutation([&first]);
        let overflow_guard = tracker.begin_mutation([&overflow]);

        std::thread::sleep(Duration::from_millis(25));
        assert!(tracker.is_sticky(&unrelated));

        drop(overflow_guard);
        assert!(tracker.is_sticky_at(&unrelated, Instant::now()));
        drop(first_guard);
        assert!(tracker.is_sticky_at(&unrelated, Instant::now()));
        std::thread::sleep(Duration::from_millis(25));
        assert!(!tracker.is_sticky(&unrelated));
    }
}
