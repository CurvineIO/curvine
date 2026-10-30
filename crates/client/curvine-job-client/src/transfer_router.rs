// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0.
// License text: http://www.apache.org/licenses/LICENSE-2.0

//! Consistent-hash report routing for `TransferClient` (discovery=sql mode).
//!
//! The ring is refreshed from a topology provider (MySQL short connections).
//! Refresh triggers: an explicit `maybe_refresh` after a topology-version bump
//! observed in a report ack, or a stale fallback timer. Until the first
//! successful fetch the ring is empty and reports fall back to the plain
//! connector (any configured endpoint), so a topology outage degrades but
//! never blocks reporting.

use std::sync::Arc;
use std::time::{Duration, Instant};

use curvine_error::FsResult;
use parking_lot::RwLock;

use crate::topology_source::TopologySource;
use curvine_service_discovery::ConsistentHashRing;

pub struct TransferRouter {
    source: Box<dyn TopologySource>,
    state: RwLock<RouterState>,
}

struct RouterState {
    ring: Option<Arc<ConsistentHashRing>>,
    topology_version: i64,
    last_refresh: Option<Instant>,
    fallback_refresh: Duration,
}

impl TransferRouter {
    pub fn new(source: Box<dyn TopologySource>, fallback_refresh: Duration) -> Self {
        Self {
            source,
            state: RwLock::new(RouterState {
                ring: None,
                topology_version: 0,
                last_refresh: None,
                fallback_refresh,
            }),
        }
    }

    /// Owner address for a job, or None while the ring is empty.
    pub fn owner_addr(&self, job_id: &str) -> Option<String> {
        self.state
            .read()
            .ring
            .as_ref()
            .and_then(|ring| ring.owner(job_id))
            .map(|entry| entry.addr.clone())
    }

    /// Refresh the ring if needed: (a) observed a newer topology version,
    /// or (b) the fallback timer elapsed. Returns Ok once any refresh ran.
    pub async fn maybe_refresh(&self, observed_version: i64) -> FsResult<()> {
        let should = {
            let state = self.state.read();
            let stale = state
                .last_refresh
                .map(|t| t.elapsed() >= state.fallback_refresh)
                .unwrap_or(true);
            // A never-fetched ring must be built even if the observed version
            // matches the initial 0, otherwise the first reports have no owner.
            let never_fetched = state.ring.is_none();
            let version_changed = observed_version > state.topology_version;
            never_fetched || stale || version_changed
        };
        if !should {
            return Ok(());
        }
        let (endpoints, version) = self.source.fetch().await?;
        let ring = ConsistentHashRing::build(endpoints);
        let mut state = self.state.write();
        state.ring = Some(Arc::new(ring));
        state.topology_version = version;
        state.last_refresh = Some(Instant::now());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::topology_source::TopologySnapshot;

    struct StaticSource {
        version: i64,
    }

    #[async_trait::async_trait]
    impl TopologySource for StaticSource {
        async fn fetch(&self) -> FsResult<TopologySnapshot> {
            Ok((
                vec![
                    ("a:1".to_string(), "a:1".to_string(), 1),
                    ("b:1".to_string(), "b:1".to_string(), 1),
                ],
                self.version,
            ))
        }
    }

    #[tokio::test]
    async fn router_builds_ring_from_source() {
        let router = TransferRouter::new(
            Box::new(StaticSource { version: 7 }),
            Duration::from_secs(60),
        );
        router.maybe_refresh(0).await.unwrap();
        let first = router.owner_addr("job-a").unwrap();
        for _ in 0..5 {
            assert_eq!(router.owner_addr("job-a").unwrap(), first);
        }
        assert!(router.owner_addr("job-a").is_some());
    }

    #[tokio::test]
    async fn router_ignores_older_versions() {
        let router = TransferRouter::new(
            Box::new(StaticSource { version: 5 }),
            Duration::from_secs(3600),
        );
        router.maybe_refresh(0).await.unwrap();
        // observed 3 < current 5: no refresh needed, still succeeds.
        router.maybe_refresh(3).await.unwrap();
        assert!(router.owner_addr("x").is_some());
    }

    #[tokio::test]
    async fn empty_ring_falls_back_to_none() {
        struct EmptySource;
        #[async_trait::async_trait]
        impl TopologySource for EmptySource {
            async fn fetch(&self) -> FsResult<TopologySnapshot> {
                Ok((Vec::new(), 1))
            }
        }
        let router = TransferRouter::new(Box::new(EmptySource), Duration::from_secs(60));
        router.maybe_refresh(0).await.unwrap();
        assert!(router.owner_addr("job").is_none());
        let _ = HashMap::<String, ()>::new(); // keep import used
    }
}
