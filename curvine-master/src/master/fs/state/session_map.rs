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
use std::collections::HashMap;

/// Fallback used when the master configuration was not initialized
/// (for example in tests that build `ClusterConf` without calling `init`).
pub const DEFAULT_CLIENT_SESSION_TIMEOUT_MS: u64 = 60_000;

/// One live client session, keyed by `ClientAddress::client_name`.
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
pub struct SessionMap {
    sessions: HashMap<String, ClientSession>,
    session_timeout_ms: u64,
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
        }
    }

    pub fn session_timeout_ms(&self) -> u64 {
        self.session_timeout_ms
    }

    /// Register a new session or renew an existing one.
    /// Returns `true` when the heartbeat created a new session.
    pub fn heartbeat(&mut self, hb: SessionHeartbeat, now: u64) -> bool {
        let expire_at = now.saturating_add(self.session_timeout_ms);
        match self.sessions.get_mut(&hb.address.client_name) {
            Some(session) => {
                session.address = hb.address;
                session.heartbeat_interval_ms = hb.heartbeat_interval_ms;
                session.pid = hb.pid.or(session.pid);
                session.mount_point = hb.mount_point.or(session.mount_point.take());
                session.client_version = hb.client_version.or(session.client_version.take());
                // Never move the lease backwards if the master clock jumps.
                session.last_heartbeat = session.last_heartbeat.max(now);
                session.expire_at = session.expire_at.max(expire_at);
                false
            }

            None => {
                let client_id = hb.address.client_name.clone();
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

    /// Drop every session, e.g. when this master steps down from leader.
    pub fn clear(&mut self) {
        self.sessions.clear();
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

        map.clear();
        assert!(map.is_empty());
    }

    #[test]
    fn zero_timeout_falls_back_to_default() {
        let map = SessionMap::new(0);
        assert_eq!(map.session_timeout_ms(), DEFAULT_CLIENT_SESSION_TIMEOUT_MS);
    }
}
