// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0.
// License text: http://www.apache.org/licenses/LICENSE-2.0

//! Relays a status/watch request to the transfer instance that owns the job.
//!
//! Only the hash owner holds the job's file-level task state, so a request that
//! lands on any other instance would otherwise report an empty task list. The
//! relay is one hop: the forwarded request is always answered locally, which
//! also keeps two instances with disagreeing rings from ping-ponging.
//!
//! Everything here is best effort. If discovery is off, the topology cannot be
//! resolved, or the owner does not answer, the caller serves the request from
//! its own (store-backed) view instead of failing it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use curvine_error::{FsError, FsResult};
use curvine_rpc::client::ClientFactory;
use curvine_rpc::RpcUtils;
use curvine_service_discovery::{ConsistentHashRing, ServiceKind, ServiceResolver, SqlDiscovery};
use log::warn;
use parking_lot::RwLock;
use prost::Message as PMessage;

const RING_TTL: Duration = Duration::from_secs(3);
const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Forwarder {
    discovery: SqlDiscovery,
    /// This instance's own rpc `host:port`, matching the ring's address form.
    self_addr: String,
    factory: Arc<ClientFactory>,
    ring: RwLock<Option<(Instant, Arc<ConsistentHashRing>)>>,
}

impl Forwarder {
    pub fn new(
        discovery: SqlDiscovery,
        self_addr: impl Into<String>,
        factory: Arc<ClientFactory>,
    ) -> Self {
        Self {
            discovery,
            self_addr: self_addr.into(),
            factory,
            ring: RwLock::new(None),
        }
    }

    /// Owner address of a job, or None when it cannot be resolved (discovery
    /// unavailable), in which case the caller must answer locally.
    fn owner_of(&self, job_id: &str) -> Option<String> {
        let ring = self.ring_snapshot()?;
        let owner = ring.owner(job_id)?.addr.clone();
        Some(owner)
    }

    /// Owner address to relay to, or None when this instance should answer.
    pub fn relay_target(&self, job_id: &str) -> Option<String> {
        let owner = self.owner_of(job_id)?;
        (owner != self.self_addr).then_some(owner)
    }

    fn ring_snapshot(&self) -> Option<Arc<ConsistentHashRing>> {
        if let Some((fetched, ring)) = self.ring.read().as_ref() {
            if fetched.elapsed() < RING_TTL {
                return Some(ring.clone());
            }
        }
        match self.fetch_ring() {
            Ok(ring) => {
                let ring = Arc::new(ring);
                *self.ring.write() = Some((Instant::now(), ring.clone()));
                Some(ring)
            }
            Err(err) => {
                warn!("transfer forwarder could not refresh the topology: {}", err);
                // Fall back to whatever we had; a slightly old ring still
                // relays to the right instance far more often than not.
                self.ring.read().as_ref().map(|(_, ring)| ring.clone())
            }
        }
    }

    fn fetch_ring(&self) -> FsResult<ConsistentHashRing> {
        let kind = ServiceKind::try_new("transfer")
            .map_err(|err| FsError::common(format!("invalid service kind: {err}")))?;
        // The resolver trait is async but its SQL implementation is a plain
        // query; poll it on a short-lived runtime to keep the caller sync.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| FsError::common(format!("forwarder runtime: {err}")))?;
        let snapshot = rt
            .block_on(async { self.discovery.list(kind).await })
            .map_err(|err| FsError::common(format!("transfer topology list failed: {err}")))?;
        Ok(ConsistentHashRing::build(
            snapshot.endpoints.into_iter().map(|endpoint| {
                (
                    endpoint.id,
                    format!("{}:{}", endpoint.host, endpoint.rpc_port),
                    endpoint.start_time_ms,
                )
            }),
        ))
    }

    /// Relays one request to `addr` and returns the owner's own response.
    pub async fn relay<T, R>(
        &self,
        addr: &str,
        code: curvine_fs_api::RpcCode,
        req: T,
    ) -> FsResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        let inet = curvine_net::net::InetAddr::from_str(addr)?;
        let client = self
            .factory
            .get(&inet)
            .await
            .map_err(|err| FsError::common(format!("connect transfer owner {addr}: {err}")))?;
        RpcUtils::proto_rpc(&client, FORWARD_TIMEOUT, code, req).await
    }
}

/// True when the caller opted out of relaying for this request.
pub fn is_forwarded(forwarded: Option<bool>) -> bool {
    forwarded.unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwarded_flag_defaults_to_serving_locally() {
        assert!(!is_forwarded(None));
        assert!(!is_forwarded(Some(false)));
        assert!(is_forwarded(Some(true)));
    }

    /// A ring that resolves a job to this instance must not relay: relaying to
    /// yourself would recurse until the one-hop guard stops it.
    fn ring_for(addrs: &[(&str, &str)]) -> ConsistentHashRing {
        ConsistentHashRing::build(
            addrs
                .iter()
                .map(|(id, addr)| ((*id).to_string(), (*addr).to_string(), 1u64)),
        )
    }

    fn target(ring: &ConsistentHashRing, self_addr: &str, job_id: &str) -> Option<String> {
        // Mirrors Forwarder::relay_target without touching discovery.
        let owner = ring.owner(job_id)?.addr.clone();
        (owner != self_addr).then_some(owner)
    }

    #[test]
    fn relaying_skips_the_owning_instance() {
        let ring = ring_for(&[
            ("a:9000", "a:9000"),
            ("b:9000", "b:9000"),
            ("c:9000", "c:9000"),
        ]);
        for i in 0..50 {
            let job_id = format!("job-{i}");
            let owner = ring.owner(&job_id).unwrap().addr.clone();
            // From the owner: serve locally.
            assert_eq!(target(&ring, &owner, &job_id), None);
            // From any other member: relay to the owner.
            let other = ["a:9000", "b:9000", "c:9000"]
                .into_iter()
                .find(|addr| *addr != owner)
                .unwrap();
            assert_eq!(target(&ring, other, &job_id), Some(owner));
        }
    }

    #[test]
    fn empty_ring_never_relays() {
        let ring = ConsistentHashRing::default();
        assert_eq!(target(&ring, "a:9000", "job-1"), None);
    }
}
