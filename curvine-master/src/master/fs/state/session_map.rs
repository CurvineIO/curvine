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

use curvine_model::ClientAddress;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Instant;

/// Fallback used when the master configuration was not initialized
/// (for example in tests that build `ClusterConf` without calling `init`).
pub const DEFAULT_CLIENT_SESSION_TIMEOUT_MS: u64 = 60_000;

/// Milliseconds on a process-local monotonic clock.
///
/// Lease deadlines must not follow wall-clock jumps: an NTP step forward of
/// more than `client_session_timeout` would otherwise expire every session at
/// once, and once the reaper exists that becomes a mass lock release. Sessions
/// live only in this process's memory, so a process-local anchor is enough;
/// values are not comparable across processes and must not be persisted or
/// shown as timestamps.
pub fn monotonic_ms() -> u64 {
    static ANCHOR: OnceLock<Instant> = OnceLock::new();
    ANCHOR.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// One live client session, keyed by `ClientAddress::client_name`.
///
/// `started_at`, `last_heartbeat` and `expire_at` are [`monotonic_ms`] values,
/// not Unix timestamps; convert relative to `monotonic_ms()` before displaying.
#[derive(Debug, Clone)]
pub struct ClientSession {
    pub address: ClientAddress,
    pub pid: Option<u32>,
    pub mount_point: Option<String>,
    pub client_version: Option<String>,
    pub heartbeat_interval_ms: u64,
    pub started_at: u64,
    pub last_heartbeat: u64,
    pub expire_at: u64,
}

impl ClientSession {
    pub fn client_id(&self) -> &str {
        &self.address.client_name
    }

    pub fn is_expired_at(&self, now: u64) -> bool {
        now > self.expire_at
    }
}

/// Heartbeat payload used to register or renew a session.
#[derive(Debug, Clone)]
pub struct SessionHeartbeat {
    pub address: ClientAddress,
    pub heartbeat_interval_ms: u64,
    pub pid: Option<u32>,
    pub mount_point: Option<String>,
    pub client_version: Option<String>,
}

/// Leader-local, in-memory registry of client session leases.
///
/// Sessions are intentionally not journaled: renewing them through raft on
/// every heartbeat would be too expensive. A newly elected leader starts with
/// an empty map and clients re-register on their next heartbeat.
///
/// # Lock ordering
///
/// This map must never be acquired while `fs_dir` (or anything taken under it)
/// is held. Paths that need both take a liveness snapshot first and then the
/// namespace lock:
///
/// ```text
/// session snapshot -> fs_dir -> inode_store
/// ```
///
/// [`SessionMap::live_ids`] exists for exactly that: the lock path snapshots the
/// live client ids and releases this map before touching any inode, so the
/// heartbeat handler, the lock path and the session reaper can never take the two
/// locks in opposite orders.
pub struct SessionMap {
    sessions: HashMap<String, ClientSession>,
    session_timeout_ms: u64,
    /// [`monotonic_ms`] instant the current leader epoch started. Consumers that
    /// must not act on a freshly adopted map -- the reaper's grace window, the
    /// session-aware lock validity check -- measure from here instead of from a
    /// session deadline.
    created_at: u64,
    /// Raft term that [`SessionMap::created_at`] was adopted for. A standby keeps
    /// the previous value, and because a promotion always carries a higher term,
    /// the first leader-side observation of the new term starts a new epoch
    /// ([`SessionMap::adopt`]). `None` means the map has never been adopted.
    term: Option<u64>,
}

impl SessionMap {
    pub fn new(session_timeout_ms: u64) -> Self {
        let session_timeout_ms = if session_timeout_ms == 0 {
            DEFAULT_CLIENT_SESSION_TIMEOUT_MS
        } else {
            session_timeout_ms
        };
        Self {
            sessions: HashMap::new(),
            session_timeout_ms,
            created_at: monotonic_ms(),
            term: None,
        }
    }

    pub fn session_timeout_ms(&self) -> u64 {
        self.session_timeout_ms
    }

    /// Start of the current lease epoch: when [`SessionMap::adopt`] first observed
    /// the term this master is serving, or when the last [`SessionMap::clear`] ran.
    ///
    /// It is *not* process start. Consumers that must not act on a freshly adopted
    /// map (the reaper's grace window, the session-aware lock validity check) must
    /// call [`SessionMap::adopt`] with the current raft term before reading this.
    pub fn created_at(&self) -> u64 {
        self.created_at
    }

    /// Adopt the map for `term`, the raft term this master is serving (#1883 review).
    ///
    /// Returns `true` when a new lease epoch started. A term always increases
    /// across promotions, so the first leader-side observation of a new term drops
    /// the previous term's sessions and restarts [`SessionMap::created_at`]; a
    /// standby needs no hook of its own, because it simply keeps observing the old
    /// term until it is promoted.
    pub fn adopt(&mut self, term: u64, now: u64) -> bool {
        if self.term == Some(term) {
            return false;
        }

        self.clear(now);
        self.term = Some(term);
        true
    }

    /// Adopt `term` and renew or register `hb` under one lock, so a heartbeat can
    /// never write a session into an epoch it did not adopt first.
    pub fn heartbeat_in_epoch(&mut self, hb: SessionHeartbeat, term: u64, now: u64) -> bool {
        self.adopt(term, now);
        self.heartbeat(hb, now)
    }

    /// Register a new session or renew an existing one.
    /// Returns `true` when the heartbeat created a new session.
    ///
    /// Crate-private on purpose: outside this crate the only way into the map is
    /// [`SessionMap::heartbeat_in_epoch`], so no caller can register a session
    /// into an epoch it did not adopt first.
    pub(crate) fn heartbeat(&mut self, hb: SessionHeartbeat, now: u64) -> bool {
        let expire_at = now.saturating_add(self.session_timeout_ms);
        let client_id = hb.address.client_name.clone();

        // A lease that lapsed but was not reaped yet must look like a new
        // registration: `new_session` is how the client learns it may no longer
        // own the state (plocks, writer markers) the previous lease covered
        // (#1883). Dropping the entry here keeps the renewal path a plain update.
        if self
            .sessions
            .get(&client_id)
            .is_some_and(|session| session.is_expired_at(now))
        {
            self.sessions.remove(&client_id);
        }

        match self.sessions.get_mut(&client_id) {
            Some(session) => {
                session.address = hb.address;
                session.heartbeat_interval_ms = hb.heartbeat_interval_ms;
                session.pid = hb.pid.or(session.pid);
                session.mount_point = hb.mount_point.or(session.mount_point.take());
                session.client_version = hb.client_version.or(session.client_version.take());
                // Never move the lease backwards, even if callers pass an older `now`.
                session.last_heartbeat = session.last_heartbeat.max(now);
                session.expire_at = session.expire_at.max(expire_at);
                false
            }

            None => {
                let session = ClientSession {
                    address: hb.address,
                    pid: hb.pid,
                    mount_point: hb.mount_point,
                    client_version: hb.client_version,
                    heartbeat_interval_ms: hb.heartbeat_interval_ms,
                    started_at: now,
                    last_heartbeat: now,
                    expire_at,
                };
                self.sessions.insert(client_id, session);
                true
            }
        }
    }

    pub fn get(&self, client_id: &str) -> Option<&ClientSession> {
        self.sessions.get(client_id)
    }

    /// True when the client has a session that has not expired at `now`.
    pub fn is_live(&self, client_id: &str, now: u64) -> bool {
        self.sessions
            .get(client_id)
            .is_some_and(|s| !s.is_expired_at(now))
    }

    /// Client ids whose lease has expired at `now`.
    pub fn expired_at(&self, now: u64) -> Vec<String> {
        self.sessions
            .values()
            .filter(|s| s.is_expired_at(now))
            .map(|s| s.client_id().to_string())
            .collect()
    }

    pub fn remove(&mut self, client_id: &str) -> Option<ClientSession> {
        self.sessions.remove(client_id)
    }

    /// Client ids whose lease is still valid at `now`.
    ///
    /// This is the snapshot the lock path uses: take it, drop this map's guard,
    /// then read `fs_dir`. Expired-but-not-yet-reaped sessions are excluded, so a
    /// client that stopped heartbeating is not treated as alive.
    pub fn live_ids(&self, now: u64) -> HashSet<String> {
        self.sessions
            .values()
            .filter(|s| !s.is_expired_at(now))
            .map(|s| s.client_id().to_string())
            .collect()
    }

    /// Drop every session and restart the lease epoch at `now`.
    ///
    /// [`SessionMap::adopt`] calls this on promotion. Calling it directly on a
    /// step-down is optional: the term is unchanged, and the next promotion adopts
    /// a higher term and clears again. The grace window measures from `created_at`,
    /// i.e. from this call.
    pub fn clear(&mut self, now: u64) {
        self.sessions.clear();
        self.created_at = now;
    }

    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    pub fn sessions(&self) -> impl Iterator<Item = &ClientSession> {
        self.sessions.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: u64 = 60_000;

    fn hb(client: &str) -> SessionHeartbeat {
        SessionHeartbeat {
            address: ClientAddress {
                client_name: client.to_string(),
                hostname: "host-a".to_string(),
                ip_addr: "127.0.0.1".to_string(),
                port: 0,
            },
            heartbeat_interval_ms: 12_000,
            pid: Some(42),
            mount_point: Some("/mnt/cv".to_string()),
            client_version: Some("0.1.0".to_string()),
        }
    }

    #[test]
    fn adopting_a_new_term_drops_sessions_and_restarts_the_grace_window() {
        let mut map = SessionMap::new(TIMEOUT);
        map.heartbeat_in_epoch(hb("c1"), 1, 1_000);
        map.heartbeat_in_epoch(hb("c2"), 1, 1_000);
        let first_epoch = map.created_at();

        // Re-observing the served term keeps the epoch and the sessions.
        assert!(!map.adopt(1, 5_000));
        assert_eq!(map.len(), 2);
        assert_eq!(map.created_at(), first_epoch);

        // A promotion always carries a higher term: previous-term sessions are
        // gone and the grace window starts at the promotion instant.
        assert!(map.adopt(2, 5_000));
        assert!(map.is_empty(), "a new term must not inherit old sessions");
        assert_eq!(map.created_at(), 5_000);

        // Further heartbeats in the same term keep the epoch.
        map.heartbeat_in_epoch(hb("c1"), 2, 6_000);
        assert!(!map.adopt(2, 7_000));
        assert_eq!(map.created_at(), 5_000);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn an_expired_lease_is_renewed_as_a_new_session() {
        let mut map = SessionMap::new(TIMEOUT);
        assert!(map.heartbeat_in_epoch(hb("c1"), 1, 1_000));

        // The lease lapsed and the reaper has not removed the entry yet: the
        // client must learn that it owns nothing from the previous lease.
        let renewed_at = 1_000 + TIMEOUT + 1;
        assert!(map.heartbeat_in_epoch(hb("c1"), 1, renewed_at));
        let session = map.get("c1").unwrap();
        assert_eq!(session.started_at, renewed_at);
        assert_eq!(session.last_heartbeat, renewed_at);
        assert_eq!(session.expire_at, renewed_at + TIMEOUT);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn first_heartbeat_registers_session() {
        let mut map = SessionMap::new(TIMEOUT);
        assert!(map.heartbeat(hb("c1"), 1_000));

        let s = map.get("c1").unwrap();
        assert_eq!(s.started_at, 1_000);
        assert_eq!(s.last_heartbeat, 1_000);
        assert_eq!(s.expire_at, 1_000 + TIMEOUT);
        assert_eq!(s.pid, Some(42));
        assert_eq!(s.mount_point.as_deref(), Some("/mnt/cv"));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn later_heartbeat_renews_without_resetting_start() {
        let mut map = SessionMap::new(TIMEOUT);
        map.heartbeat(hb("c1"), 1_000);

        let mut renew = hb("c1");
        renew.mount_point = None;
        assert!(!map.heartbeat(renew, 13_000));

        let s = map.get("c1").unwrap();
        assert_eq!(s.started_at, 1_000);
        assert_eq!(s.last_heartbeat, 13_000);
        assert_eq!(s.expire_at, 13_000 + TIMEOUT);
        // Optional fields omitted by a later heartbeat are kept.
        assert_eq!(s.mount_point.as_deref(), Some("/mnt/cv"));
    }

    #[test]
    fn renew_never_moves_lease_backwards() {
        let mut map = SessionMap::new(TIMEOUT);
        map.heartbeat(hb("c1"), 10_000);
        map.heartbeat(hb("c1"), 5_000);

        let s = map.get("c1").unwrap();
        assert_eq!(s.last_heartbeat, 10_000);
        assert_eq!(s.expire_at, 10_000 + TIMEOUT);
    }

    #[test]
    fn liveness_and_expiry_follow_timeout() {
        let mut map = SessionMap::new(TIMEOUT);
        map.heartbeat(hb("c1"), 0);
        map.heartbeat(hb("c2"), 30_000);

        assert!(map.is_live("c1", TIMEOUT));
        assert!(!map.is_live("c1", TIMEOUT + 1));
        assert!(!map.is_live("unknown", 0));

        assert!(map.expired_at(TIMEOUT).is_empty());
        assert_eq!(map.expired_at(TIMEOUT + 1), vec!["c1".to_string()]);

        let mut both = map.expired_at(30_000 + TIMEOUT + 1);
        both.sort();
        assert_eq!(both, vec!["c1".to_string(), "c2".to_string()]);
    }

    #[test]
    fn remove_and_clear() {
        let mut map = SessionMap::new(TIMEOUT);
        map.heartbeat(hb("c1"), 0);
        map.heartbeat(hb("c2"), 0);

        assert_eq!(
            map.remove("c1").map(|s| s.address.client_name),
            Some("c1".into())
        );
        assert!(map.remove("c1").is_none());
        assert_eq!(map.len(), 1);

        let epoch_before = map.created_at();
        map.clear(epoch_before + 5_000);
        assert!(map.is_empty());
        assert_eq!(
            map.created_at(),
            epoch_before + 5_000,
            "a step-down restarts the lease epoch for the grace window"
        );
    }

    #[test]
    fn live_ids_excludes_expired_sessions() {
        let mut map = SessionMap::new(TIMEOUT);
        map.heartbeat(hb("c1"), 1_000);
        map.heartbeat(hb("c2"), 40_000);

        let live = map.live_ids(1_000 + TIMEOUT);
        assert!(live.contains("c1"), "a lease is live through its deadline");
        assert!(live.contains("c2"));

        let live = map.live_ids(1_000 + TIMEOUT + 1);
        assert!(!live.contains("c1"), "expired sessions must not be live");
        assert!(live.contains("c2"));
        assert_eq!(map.live_ids(1_000 + TIMEOUT + 1).len(), 1);
    }

    #[test]
    fn monotonic_clock_never_goes_backwards() {
        let a = monotonic_ms();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let b = monotonic_ms();
        assert!(b >= a + 5, "monotonic_ms went from {a} to {b}");

        // A map created now starts its epoch on the same clock.
        let map = SessionMap::new(TIMEOUT);
        assert!(map.created_at() >= b);
        assert!(map.created_at() <= monotonic_ms());
    }

    #[test]
    fn zero_timeout_falls_back_to_default() {
        let map = SessionMap::new(0);
        assert_eq!(map.session_timeout_ms(), DEFAULT_CLIENT_SESSION_TIMEOUT_MS);
    }
}
