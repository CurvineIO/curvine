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

use crate::err_box;
use crate::sync::FastDashMap;
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::Mutex;

struct SharedState<T> {
    resource: Option<Arc<T>>,
    refs: usize,
}

impl<T> Default for SharedState<T> {
    fn default() -> Self {
        Self {
            resource: None,
            refs: 0,
        }
    }
}

/// Per-key async shared resource map with reference counting.
///
/// One async mutex per key serializes create/release work. This keeps the
/// implementation small and avoids exposing resources that are being closed.
///
/// The futures passed to `get_or_create`, `with_resource`, and `release` must
/// not call back into this map with the same key; doing so would wait on the
/// same per-key mutex.
pub struct AsyncSharedMap<K, T> {
    inner: FastDashMap<K, Arc<Mutex<SharedState<T>>>>,
}

impl<K: Eq + Hash, T> Default for AsyncSharedMap<K, T> {
    fn default() -> Self {
        Self {
            inner: FastDashMap::default(),
        }
    }
}

impl<K: Eq + Hash + Display + Clone, T> AsyncSharedMap<K, T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn keys(&self) -> Vec<K> {
        self.inner.iter().map(|entry| entry.key().clone()).collect()
    }

    fn get_entry(&self, k: K) -> Arc<Mutex<SharedState<T>>> {
        self.inner
            .entry(k)
            .or_insert_with(|| Arc::new(Mutex::new(SharedState::default())))
            .clone()
    }

    fn is_current_entry(&self, key: &K, entry: &Arc<Mutex<SharedState<T>>>) -> bool {
        self.inner
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(&current, entry))
    }

    pub async fn insert<E>(&self, key: K, resource: Arc<T>) -> Result<Arc<T>, E>
    where
        E: Error + From<String>,
    {
        loop {
            let entry = self.get_entry(key.clone());
            let mut state = entry.lock().await;
            if !self.is_current_entry(&key, &entry) {
                continue;
            }

            if state.resource.is_some() {
                return err_box!("resource already exists for this key {}", key);
            }
            state.refs = 1;
            state.resource = Some(resource.clone());
            return Ok(resource);
        }
    }

    pub async fn get(&self, key: &K) -> Option<Arc<T>> {
        loop {
            let entry = self.inner.get(key)?.clone();
            let state = entry.lock().await;
            if !self.is_current_entry(key, &entry) {
                continue;
            }
            return state.resource.clone();
        }
    }

    pub async fn get_or_create<E, Fut>(&self, key: K, fut: Fut) -> Result<Arc<T>, E>
    where
        E: Error,
        Fut: Future<Output = Result<Arc<T>, E>>,
    {
        loop {
            let entry = self.get_entry(key.clone());
            let mut state = entry.lock().await;
            if !self.is_current_entry(&key, &entry) {
                continue;
            }

            if let Some(resource) = state.resource.clone() {
                state.refs += 1;
                return Ok(resource);
            }

            let resource = match fut.await {
                Ok(resource) => resource,
                Err(e) => {
                    drop(state);
                    self.remove_entry(&key, &entry);
                    return Err(e);
                }
            };

            state.refs = 1;
            state.resource = Some(resource.clone());
            return Ok(resource);
        }
    }

    pub async fn with_resource<E, F, Fut>(&self, key: &K, f: F) -> Result<bool, E>
    where
        E: Error,
        F: FnOnce(Arc<T>) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        loop {
            let entry = match self.inner.get(key) {
                Some(entry) => entry.clone(),
                None => return Ok(false),
            };

            let state = entry.lock().await;
            if !self.is_current_entry(key, &entry) {
                continue;
            }

            let Some(resource) = state.resource.clone() else {
                return Ok(false);
            };

            f(resource).await?;
            return Ok(true);
        }
    }

    fn remove_entry(&self, key: &K, entry: &Arc<Mutex<SharedState<T>>>) {
        self.inner
            .remove_if(key, |_, current| Arc::ptr_eq(current, entry));
    }

    pub fn remove(&self, key: K) {
        self.inner.remove(&key);
    }

    pub async fn release<E, Fut>(&self, key: K, fut: Fut) -> (bool, Result<(), E>)
    where
        E: Error,
        Fut: Future<Output = Result<(), E>>,
    {
        let entry = match self.inner.get(&key) {
            Some(entry) => entry.clone(),
            None => return (true, Ok(())),
        };

        let mut state = entry.lock().await;

        if state.refs == 0 || state.resource.is_none() {
            drop(state);
            self.remove_entry(&key, &entry);
            return (true, Ok(()));
        }

        state.refs -= 1;
        if state.refs > 0 {
            return (false, Ok(()));
        }

        // Hide the resource before cleanup so late users wait for a fresh create.
        state.resource.take();
        state.refs = 0;

        let res = fut.await;
        drop(state);

        self.remove_entry(&key, &entry);

        (true, res)
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncSharedMap;
    use crate::error::StringError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::{oneshot, Barrier};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn get_or_create_runs_single_creator_per_key() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        let creators = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let map = map.clone();
            let creators = creators.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                map.get_or_create(1, async {
                    creators.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    Ok::<_, StringError>(Arc::new(7))
                })
                .await
                .unwrap()
            }));
        }

        for task in tasks {
            assert_eq!(*task.await.unwrap(), 7);
        }
        assert_eq!(creators.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn get_or_create_waits_for_release_cleanup() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        map.insert::<StringError>(1, Arc::new(1)).await.unwrap();

        let (cleanup_started_tx, cleanup_started_rx) = oneshot::channel();
        let (finish_cleanup_tx, finish_cleanup_rx) = oneshot::channel();
        let release_map = map.clone();
        let release_task = tokio::spawn(async move {
            let (released, cleanup_res) = release_map
                .release(1, async {
                    cleanup_started_tx.send(()).unwrap();
                    finish_cleanup_rx.await.unwrap();
                    Ok::<_, StringError>(())
                })
                .await;
            cleanup_res.unwrap();
            released
        });

        cleanup_started_rx.await.unwrap();

        let creators = Arc::new(AtomicUsize::new(0));
        let create_map = map.clone();
        let create_count = creators.clone();
        let create_task = tokio::spawn(async move {
            create_map
                .get_or_create(1, async {
                    create_count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, StringError>(Arc::new(2))
                })
                .await
                .unwrap()
        });

        tokio::task::yield_now().await;
        assert_eq!(creators.load(Ordering::SeqCst), 0);

        finish_cleanup_tx.send(()).unwrap();
        assert!(release_task.await.unwrap());
        assert_eq!(*create_task.await.unwrap(), 2);
        assert_eq!(creators.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn release_closes_only_last_reference() {
        let map = AsyncSharedMap::<u64, u64>::new();
        let first = map
            .get_or_create(1, async { Ok::<_, StringError>(Arc::new(1)) })
            .await
            .unwrap();
        let second = map
            .get_or_create(1, async { Ok::<_, StringError>(Arc::new(2)) })
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        let (released, cleanup_res) = map.release(1, async { Ok::<_, StringError>(()) }).await;
        cleanup_res.unwrap();
        assert!(!released);
        assert!(map.get(&1).await.is_some());
        let (released, cleanup_res) = map.release(1, async { Ok::<_, StringError>(()) }).await;
        cleanup_res.unwrap();
        assert!(released);
        assert!(map.get(&1).await.is_none());
    }

    #[tokio::test]
    async fn create_error_wakes_waiter_and_allows_retry() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        let (fail_started_tx, fail_started_rx) = oneshot::channel();
        let (finish_fail_tx, finish_fail_rx) = oneshot::channel();

        let first_map = map.clone();
        let first = tokio::spawn(async move {
            first_map
                .get_or_create(1, async {
                    fail_started_tx.send(()).unwrap();
                    finish_fail_rx.await.unwrap();
                    Err::<Arc<u64>, _>(StringError::from("create failed"))
                })
                .await
        });

        fail_started_rx.await.unwrap();

        let retries = Arc::new(AtomicUsize::new(0));
        let second_map = map.clone();
        let retry_count = retries.clone();
        let second = tokio::spawn(async move {
            second_map
                .get_or_create(1, async {
                    retry_count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, StringError>(Arc::new(2))
                })
                .await
                .unwrap()
        });

        tokio::task::yield_now().await;
        assert_eq!(retries.load(Ordering::SeqCst), 0);

        finish_fail_tx.send(()).unwrap();
        assert!(first.await.unwrap().is_err());
        assert_eq!(
            *timeout(Duration::from_secs(1), second)
                .await
                .unwrap()
                .unwrap(),
            2
        );
        assert_eq!(retries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelled_create_does_not_leave_entry_busy() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        let (started_tx, started_rx) = oneshot::channel();
        let (_finish_tx, finish_rx) = oneshot::channel::<()>();

        let create_map = map.clone();
        let creating = tokio::spawn(async move {
            create_map
                .get_or_create(1, async {
                    started_tx.send(()).unwrap();
                    finish_rx.await.unwrap();
                    Ok::<_, StringError>(Arc::new(1))
                })
                .await
        });

        started_rx.await.unwrap();
        creating.abort();
        assert!(creating.await.unwrap_err().is_cancelled());

        let resource = timeout(
            Duration::from_secs(1),
            map.get_or_create(1, async { Ok::<_, StringError>(Arc::new(2)) }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(*resource, 2);
    }

    #[tokio::test]
    async fn cancelled_release_does_not_leave_entry_busy() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        map.insert::<StringError>(1, Arc::new(1)).await.unwrap();

        let (started_tx, started_rx) = oneshot::channel();
        let (_finish_tx, finish_rx) = oneshot::channel::<()>();
        let release_map = map.clone();
        let releasing = tokio::spawn(async move {
            release_map
                .release(1, async {
                    started_tx.send(()).unwrap();
                    finish_rx.await.unwrap();
                    Ok::<_, StringError>(())
                })
                .await
        });

        started_rx.await.unwrap();
        releasing.abort();
        assert!(releasing.await.unwrap_err().is_cancelled());

        let resource = timeout(
            Duration::from_secs(1),
            map.get_or_create(1, async { Ok::<_, StringError>(Arc::new(2)) }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(*resource, 2);
    }

    #[tokio::test]
    async fn release_empty_entry_does_not_run_cleanup() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        let (started_tx, started_rx) = oneshot::channel();
        let (_finish_tx, finish_rx) = oneshot::channel::<()>();

        let create_map = map.clone();
        let creating = tokio::spawn(async move {
            create_map
                .get_or_create(1, async {
                    started_tx.send(()).unwrap();
                    finish_rx.await.unwrap();
                    Ok::<_, StringError>(Arc::new(1))
                })
                .await
        });

        started_rx.await.unwrap();
        creating.abort();
        assert!(creating.await.unwrap_err().is_cancelled());

        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        let cleanup_count = cleanup_calls.clone();
        let (released, cleanup_res) = map
            .release(1, async {
                cleanup_count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, StringError>(())
            })
            .await;
        cleanup_res.unwrap();
        assert!(released);
        assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn release_error_wakes_waiter_and_allows_retry() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        map.insert::<StringError>(1, Arc::new(1)).await.unwrap();

        let (cleanup_started_tx, cleanup_started_rx) = oneshot::channel();
        let (finish_cleanup_tx, finish_cleanup_rx) = oneshot::channel();
        let release_map = map.clone();
        let release_task = tokio::spawn(async move {
            release_map
                .release(1, async {
                    cleanup_started_tx.send(()).unwrap();
                    finish_cleanup_rx.await.unwrap();
                    Err::<(), _>(StringError::from("cleanup failed"))
                })
                .await
        });

        cleanup_started_rx.await.unwrap();

        let creators = Arc::new(AtomicUsize::new(0));
        let create_map = map.clone();
        let create_count = creators.clone();
        let create_task = tokio::spawn(async move {
            create_map
                .get_or_create(1, async {
                    create_count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, StringError>(Arc::new(2))
                })
                .await
                .unwrap()
        });

        tokio::task::yield_now().await;
        assert_eq!(creators.load(Ordering::SeqCst), 0);

        finish_cleanup_tx.send(()).unwrap();
        let (released, cleanup_res) = release_task.await.unwrap();
        assert!(released);
        assert!(cleanup_res.is_err());
        assert_eq!(
            *timeout(Duration::from_secs(1), create_task)
                .await
                .unwrap()
                .unwrap(),
            2
        );
        assert_eq!(creators.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn insert_waits_for_create_and_preserves_existing_resource() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        let (create_started_tx, create_started_rx) = oneshot::channel();
        let (finish_create_tx, finish_create_rx) = oneshot::channel();

        let create_map = map.clone();
        let create_task = tokio::spawn(async move {
            create_map
                .get_or_create(1, async {
                    create_started_tx.send(()).unwrap();
                    finish_create_rx.await.unwrap();
                    Ok::<_, StringError>(Arc::new(1))
                })
                .await
                .unwrap()
        });

        create_started_rx.await.unwrap();

        let insert_map = map.clone();
        let insert_task =
            tokio::spawn(async move { insert_map.insert::<StringError>(1, Arc::new(2)).await });

        tokio::task::yield_now().await;
        finish_create_tx.send(()).unwrap();

        assert_eq!(*create_task.await.unwrap(), 1);
        assert!(timeout(Duration::from_secs(1), insert_task)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert_eq!(*map.get(&1).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn with_resource_serializes_with_release() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        map.insert::<StringError>(1, Arc::new(1)).await.unwrap();

        let (with_started_tx, with_started_rx) = oneshot::channel();
        let (finish_with_tx, finish_with_rx) = oneshot::channel();
        let with_map = map.clone();
        let with_task = tokio::spawn(async move {
            with_map
                .with_resource(&1, |resource| async move {
                    assert_eq!(*resource, 1);
                    with_started_tx.send(()).unwrap();
                    finish_with_rx.await.unwrap();
                    Ok::<_, StringError>(())
                })
                .await
                .unwrap()
        });

        with_started_rx.await.unwrap();

        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        let release_map = map.clone();
        let release_count = cleanup_calls.clone();
        let release_task = tokio::spawn(async move {
            let (released, cleanup_res) = release_map
                .release(1, async {
                    release_count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, StringError>(())
                })
                .await;
            cleanup_res.unwrap();
            released
        });

        tokio::task::yield_now().await;
        assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);

        finish_with_tx.send(()).unwrap();
        assert!(with_task.await.unwrap());
        assert!(release_task.await.unwrap());
        assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
    }
}
