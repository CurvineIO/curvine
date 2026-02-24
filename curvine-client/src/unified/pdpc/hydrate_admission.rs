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

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_STALE_TTL_MS: u64 = 120_000;
const DEFAULT_MAX_ENTRIES: u64 = 200_000;

#[derive(Debug, Clone, Copy)]
struct HydrateTicket {
    started_at: Instant,
    token: u64,
}

#[derive(Debug, Clone)]
pub struct HydrateAdmission {
    in_flight: Arc<DashMap<String, HydrateTicket>>,
    next_token: Arc<AtomicU64>,
    stale_ttl: Duration,
    max_entries: usize,
}

impl HydrateAdmission {
    pub fn new(stale_ttl_ms: u64, max_entries: u64) -> Self {
        Self {
            in_flight: Arc::new(DashMap::new()),
            next_token: Arc::new(AtomicU64::new(1)),
            stale_ttl: Duration::from_millis(stale_ttl_ms.max(1)),
            max_entries: max_entries.max(1) as usize,
        }
    }

    fn next_ticket_token(&self) -> u64 {
        self.next_token.fetch_add(1, Ordering::Relaxed)
    }

    fn sweep_stale(&self, now: Instant) {
        let stale_ttl = self.stale_ttl;
        self.in_flight
            .retain(|_, ticket| now.duration_since(ticket.started_at) <= stale_ttl);
    }

    pub fn try_acquire(&self, key: String) -> Option<HydrateAdmissionGuard> {
        let now = Instant::now();

        let mut at_capacity = self.in_flight.len() >= self.max_entries;
        if at_capacity {
            self.sweep_stale(now);
            at_capacity = self.in_flight.len() >= self.max_entries;
        }

        let token = self.next_ticket_token();
        match self.in_flight.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let ticket = *entry.get();
                if now.duration_since(ticket.started_at) <= self.stale_ttl {
                    return None;
                }
                entry.insert(HydrateTicket {
                    started_at: now,
                    token,
                });
            }
            Entry::Vacant(entry) => {
                if at_capacity {
                    return None;
                }
                entry.insert(HydrateTicket {
                    started_at: now,
                    token,
                });
            }
        }

        Some(HydrateAdmissionGuard {
            key,
            token,
            in_flight: self.in_flight.clone(),
        })
    }

    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }
}

pub struct HydrateAdmissionGuard {
    key: String,
    token: u64,
    in_flight: Arc<DashMap<String, HydrateTicket>>,
}

impl Drop for HydrateAdmissionGuard {
    fn drop(&mut self) {
        if let Some(ticket) = self.in_flight.get(&self.key) {
            if ticket.token == self.token {
                drop(ticket);
                self.in_flight.remove(&self.key);
            }
        }
    }
}

impl Default for HydrateAdmission {
    fn default() -> Self {
        Self::new(DEFAULT_STALE_TTL_MS, DEFAULT_MAX_ENTRIES)
    }
}
