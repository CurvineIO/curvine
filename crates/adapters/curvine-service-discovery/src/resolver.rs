use crate::{DiscoveryError, DiscoveryResult, ServiceEndpoint, ServiceKind, ServiceSnapshot};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

#[async_trait]
pub trait ServiceResolver: Send + Sync {
    async fn list(&self, kind: ServiceKind) -> DiscoveryResult<ServiceSnapshot>;
    async fn watch(&self, kind: ServiceKind) -> DiscoveryResult<ServiceResolverHandle>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServiceWatchEvent {
    Added(ServiceEndpoint),
    Updated(ServiceEndpoint),
    Removed { kind: ServiceKind, id: String },
    Reset(ServiceSnapshot),
}

pub struct ServiceWatch {
    events: mpsc::Receiver<DiscoveryResult<ServiceWatchEvent>>,
    _lifecycle_owner: Option<Arc<dyn Send + Sync>>,
}

impl ServiceWatch {
    pub fn new(events: mpsc::Receiver<DiscoveryResult<ServiceWatchEvent>>) -> Self {
        Self::with_lifecycle_owner(events, None)
    }

    pub fn with_lifecycle_owner(
        events: mpsc::Receiver<DiscoveryResult<ServiceWatchEvent>>,
        lifecycle_owner: Option<Arc<dyn Send + Sync>>,
    ) -> Self {
        Self {
            events,
            _lifecycle_owner: lifecycle_owner,
        }
    }

    pub async fn recv(&mut self) -> Option<DiscoveryResult<ServiceWatchEvent>> {
        self.events.recv().await
    }
}

pub struct ServiceResolverHandle {
    kind: ServiceKind,
    reader: SnapshotReader,
    events: ServiceWatch,
    _lifecycle_owner: Option<Arc<dyn Send + Sync>>,
}

impl ServiceResolverHandle {
    pub fn new(
        kind: ServiceKind,
        reader: SnapshotReader,
        events: mpsc::Receiver<DiscoveryResult<ServiceWatchEvent>>,
    ) -> Self {
        Self::with_lifecycle_owner(kind, reader, events, None)
    }

    pub fn with_lifecycle_owner(
        kind: ServiceKind,
        reader: SnapshotReader,
        events: mpsc::Receiver<DiscoveryResult<ServiceWatchEvent>>,
        lifecycle_owner: Option<Arc<dyn Send + Sync>>,
    ) -> Self {
        Self {
            kind,
            reader,
            events: ServiceWatch::with_lifecycle_owner(events, lifecycle_owner.clone()),
            _lifecycle_owner: lifecycle_owner,
        }
    }

    pub fn kind(&self) -> &ServiceKind {
        &self.kind
    }

    pub fn reader(&self) -> SnapshotReader {
        self.reader.clone()
    }

    pub fn into_parts(self) -> (SnapshotReader, ServiceWatch) {
        (self.reader, self.events)
    }

    pub async fn next_event(&mut self) -> Option<DiscoveryResult<ServiceWatchEvent>> {
        self.events.recv().await
    }
}

#[derive(Clone)]
pub struct SnapshotReader {
    cache: Arc<RwLock<ServiceSnapshot>>,
    allow_stale_cache: bool,
    _lifecycle_owner: Option<Arc<dyn Send + Sync>>,
}

impl SnapshotReader {
    pub fn new(snapshot: ServiceSnapshot, allow_stale_cache: bool) -> Self {
        Self::with_lifecycle_owner(snapshot, allow_stale_cache, None)
    }

    pub fn with_lifecycle_owner(
        snapshot: ServiceSnapshot,
        allow_stale_cache: bool,
        lifecycle_owner: Option<Arc<dyn Send + Sync>>,
    ) -> Self {
        Self {
            cache: Arc::new(RwLock::new(snapshot)),
            allow_stale_cache,
            _lifecycle_owner: lifecycle_owner,
        }
    }

    pub async fn snapshot(&self) -> DiscoveryResult<ServiceSnapshot> {
        let snapshot = self.cache.read().await.clone();
        if snapshot.stale && !self.allow_stale_cache {
            Err(DiscoveryError::StaleCache)
        } else {
            Ok(snapshot)
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn replace_snapshot(&self, snapshot: ServiceSnapshot) {
        *self.cache.write().await = snapshot;
    }

    #[allow(dead_code)]
    pub(crate) async fn cached_snapshot(&self) -> ServiceSnapshot {
        self.cache.read().await.clone()
    }

    #[allow(dead_code)]
    pub(crate) async fn mark_stale(&self) {
        let mut snapshot = self.cache.write().await;
        snapshot.stale = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(stale: bool) -> ServiceSnapshot {
        ServiceSnapshot {
            kind: ServiceKind::try_new("mds").unwrap(),
            revision: 1,
            stale,
            last_update_ms: 1,
            endpoints: Vec::new(),
        }
    }

    #[tokio::test]
    async fn snapshot_reader_rejects_stale_when_disabled() {
        let reader = SnapshotReader::new(snapshot(true), false);
        assert!(matches!(
            reader.snapshot().await,
            Err(DiscoveryError::StaleCache)
        ));
    }

    #[tokio::test]
    async fn snapshot_reader_allows_stale_when_enabled() {
        let reader = SnapshotReader::new(snapshot(true), true);
        assert!(reader.snapshot().await.unwrap().stale);
    }

    #[tokio::test]
    async fn snapshot_reader_replace_snapshot_updates_cache() {
        let reader = SnapshotReader::new(snapshot(false), true);
        let mut updated = snapshot(false);
        updated.revision = 2;

        reader.replace_snapshot(updated).await;

        assert_eq!(reader.snapshot().await.unwrap().revision, 2);
    }

    #[tokio::test]
    async fn snapshot_reader_mark_stale_updates_cache() {
        let reader = SnapshotReader::new(snapshot(false), true);

        reader.mark_stale().await;
        let snapshot = reader.snapshot().await.unwrap();

        assert!(snapshot.stale);
        assert_eq!(snapshot.last_update_ms, 1);
    }

    #[tokio::test]
    async fn resolver_handle_splits_reader_and_events() {
        let (tx, rx) = mpsc::channel(1);
        let reader = SnapshotReader::new(snapshot(false), true);
        let mut handle =
            ServiceResolverHandle::new(ServiceKind::try_new("mds").unwrap(), reader, rx);
        tx.send(Ok(ServiceWatchEvent::Removed {
            kind: ServiceKind::try_new("mds").unwrap(),
            id: "mds-1".to_string(),
        }))
        .await
        .unwrap();

        assert!(matches!(
            handle.next_event().await,
            Some(Ok(ServiceWatchEvent::Removed { .. }))
        ));
    }

    #[tokio::test]
    async fn resolver_events_keep_lifecycle_owner_after_split() {
        let (_tx, rx) = mpsc::channel(1);
        let owner = Arc::new(());
        let reader = SnapshotReader::new(snapshot(false), true);
        let handle = ServiceResolverHandle::with_lifecycle_owner(
            ServiceKind::try_new("mds").unwrap(),
            reader,
            rx,
            Some(owner.clone()),
        );

        assert_eq!(Arc::strong_count(&owner), 3);
        let (reader, events) = handle.into_parts();
        drop(reader);

        assert_eq!(Arc::strong_count(&owner), 2);
        drop(events);
        assert_eq!(Arc::strong_count(&owner), 1);
    }
}
