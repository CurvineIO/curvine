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

use crate::client::{ClientConf, RpcClient, SyncClient};
use curvine_io::IOResult;
use curvine_net::net::InetAddr;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::FastDashMap;
use std::sync::{Arc, Mutex};

struct ClientPool {
    clients: Vec<Option<RpcClient>>,
    robin_id: usize,
    conn_size: usize,
}

impl ClientPool {
    fn next_id(&mut self) -> usize {
        if self.conn_size <= 1 {
            0
        } else {
            let now = self.robin_id;
            self.robin_id = (self.robin_id + 1) % self.conn_size;
            now
        }
    }
}

impl ClientPool {
    pub fn new(size: usize) -> Self {
        let mut clients = vec![];
        for _ in 0..size {
            clients.push(None);
        }

        Self {
            clients,
            robin_id: 0,
            conn_size: size,
        }
    }
}

// ClientFactory is thread-safe.
pub struct ClientFactory {
    pub(crate) conf: ClientConf,
    pub(crate) rt: Arc<Runtime>,
    pool: FastDashMap<InetAddr, Arc<Mutex<ClientPool>>>,
    #[cfg(test)]
    creation_barrier: Option<Arc<tokio::sync::Barrier>>,
}

impl ClientFactory {
    pub fn new(conf: ClientConf) -> Self {
        let rt = Arc::new(conf.create_runtime());
        Self::with_rt(conf, rt)
    }

    pub fn with_rt(conf: ClientConf, rt: Arc<Runtime>) -> Self {
        ClientFactory {
            conf,
            rt,
            pool: FastDashMap::default(),
            #[cfg(test)]
            creation_barrier: None,
        }
    }

    pub async fn create_raw(&self, addr: &InetAddr) -> IOResult<RpcClient> {
        RpcClient::new(false, self.rt.clone(), addr, &self.conf).await
    }

    pub async fn create_buffer(&self, addr: &InetAddr) -> IOResult<RpcClient> {
        RpcClient::new(true, self.rt.clone(), addr, &self.conf).await
    }

    pub async fn create(&self, addr: &InetAddr, buffer: bool) -> IOResult<RpcClient> {
        RpcClient::new(buffer, self.rt.clone(), addr, &self.conf).await
    }

    pub fn remove(&self, addr: &InetAddr) {
        self.pool.remove(addr);
    }

    /// Remove one failed connection without evicting healthy siblings in the
    /// same address pool.
    pub(crate) fn remove_client(&self, addr: &InetAddr, client: &RpcClient) {
        let Some(pool) = self.pool.get(addr).map(|pool| pool.value().clone()) else {
            return;
        };

        let mut pool = pool.lock().unwrap();
        for slot in &mut pool.clients {
            if slot
                .as_ref()
                .is_some_and(|pooled| pooled.same_connection(client))
            {
                *slot = None;
                break;
            }
        }
    }

    fn new_pool(conn_size: usize) -> Arc<Mutex<ClientPool>> {
        Arc::new(Mutex::new(ClientPool::new(conn_size)))
    }

    // Create or get the connection pool.
    fn get_pool(&self, addr: &InetAddr) -> Arc<Mutex<ClientPool>> {
        if let Some(pool) = self.pool.get(addr) {
            return pool.value().clone();
        }

        self.pool
            .entry(addr.clone())
            .or_insert(Self::new_pool(self.conf.conn_size))
            .clone()
    }

    // Get gets the thread-safe client, so it will not return the raw client.
    pub async fn get(&self, addr: &InetAddr) -> IOResult<RpcClient> {
        let pool = self.get_pool(addr);

        let conn_id = {
            let mut lock = pool.lock().unwrap();
            let conn_id = lock.next_id();
            if let Some(client) = lock.clients[conn_id].clone() {
                if client.is_active() {
                    return Ok(client);
                }
            }
            conn_id
        };

        // Create a new connection.
        let client = self.create_buffer(addr).await?;
        #[cfg(test)]
        if let Some(barrier) = &self.creation_barrier {
            barrier.wait().await;
        }
        let mut lock = pool.lock().unwrap();
        // @todo The overhead of using tokio mutex is too high, and std mutex is used here.However, there is a possibility of repeatedly creating connections, so how to optimize them in the future.
        if let Some(exists) = lock.clients[conn_id].clone() {
            if exists.is_active() {
                return Ok(exists);
            }
        }
        lock.clients[conn_id] = Some(client.clone());
        Ok(client)
    }

    pub fn create_sync(&self, addr: &InetAddr) -> IOResult<SyncClient> {
        let client = self.rt.block_on(RpcClient::with_raw(addr, &self.conf))?;
        Ok(SyncClient::new(self.rt.clone(), client))
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.rt.clone()
    }

    pub fn rt(&self) -> &Runtime {
        &self.rt
    }

    pub fn conf(&self) -> &ClientConf {
        &self.conf
    }
}

impl Default for ClientFactory {
    fn default() -> Self {
        Self::new(ClientConf::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::{Frame, RpcFrame};
    use crate::message::MessageBuilder;
    use std::collections::HashSet;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc as async_mpsc, Barrier};
    use tokio::task::JoinSet;
    use tokio::time::timeout;

    const DEADLINE: Duration = Duration::from_secs(5);

    async fn echo_peer(listener: TcpListener, closed: async_mpsc::UnboundedSender<()>) {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let closed = closed.clone();
            tokio::spawn(async move {
                let mut peer = RpcFrame::with_client(socket, 4096);
                loop {
                    let msg = peer.receive().await.unwrap();
                    if msg.is_empty() {
                        break;
                    }
                    peer.send(msg).await.unwrap();
                }
                closed.send(()).unwrap();
            });
        }
    }

    async fn assert_usable(client: &RpcClient) {
        let request = MessageBuilder::new_rpc(1).build();
        let id = request.req_id();
        let response = timeout(DEADLINE, client.rpc(request))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.req_id(), id);
    }

    #[test]
    fn concurrent_pool_candidates_close_without_harming_retained_clients() {
        let rt = Arc::new(Runtime::new("pool-drop-test", 2, 2));
        rt.block_on(async {
            for conn_size in [1, 3] {
                const CALLERS: usize = 12;
                let conf = ClientConf {
                    conn_size,
                    close_idle: false,
                    rpc_timeout_ms: 60_000,
                    ..Default::default()
                };
                let mut factory = ClientFactory::with_rt(conf, rt.clone());
                // All candidates connect before any can populate a slot.
                factory.creation_barrier = Some(Arc::new(Barrier::new(CALLERS)));
                let mut factory = Arc::new(factory);
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr: InetAddr = listener.local_addr().unwrap().into();
                let (closed, mut closures) = async_mpsc::unbounded_channel();
                let server = tokio::spawn(echo_peer(listener, closed));
                let mut calls = JoinSet::new();
                for _ in 0..CALLERS {
                    let factory = factory.clone();
                    let addr = addr.clone();
                    calls.spawn(async move { factory.get(&addr).await.unwrap() });
                }
                let mut clients = Vec::new();
                timeout(DEADLINE, async {
                    while let Some(result) = calls.join_next().await {
                        clients.push(result.unwrap());
                    }
                })
                .await
                .unwrap();
                Arc::get_mut(&mut factory).unwrap().creation_barrier = None;
                let unique: HashSet<_> =
                    clients.iter().map(|c| c.local_addr().to_string()).collect();
                assert_eq!(unique.len(), conn_size);
                for _ in 0..CALLERS - conn_size {
                    timeout(DEADLINE, closures.recv()).await.unwrap().unwrap();
                }
                for client in &clients {
                    assert_usable(client).await;
                }

                let other_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let other_addr: InetAddr = other_listener.local_addr().unwrap().into();
                let (other_closed, mut other_closures) = async_mpsc::unbounded_channel();
                let other_server = tokio::spawn(echo_peer(other_listener, other_closed));
                let other = factory.get(&other_addr).await.unwrap();

                // Removing the pool must not close externally held clients.
                factory.remove(&addr);
                let replacement = factory.get(&addr).await.unwrap();
                // A late removal of an old connection cannot evict its replacement.
                factory.remove_client(&addr, &clients[0]);
                assert!(factory
                    .get_pool(&addr)
                    .lock()
                    .unwrap()
                    .clients
                    .iter()
                    .any(|slot| slot
                        .as_ref()
                        .is_some_and(|c| c.same_connection(&replacement))));
                for client in &clients {
                    assert_usable(client).await;
                }
                drop(clients);
                for _ in 0..conn_size {
                    timeout(DEADLINE, closures.recv()).await.unwrap().unwrap();
                }
                assert_usable(&replacement).await;
                assert_usable(&other).await;
                drop(factory);
                assert_usable(&replacement).await;
                assert_usable(&other).await;
                drop(replacement);
                timeout(DEADLINE, closures.recv()).await.unwrap().unwrap();
                assert!(closures.try_recv().is_err());
                drop(other);
                timeout(DEADLINE, other_closures.recv())
                    .await
                    .unwrap()
                    .unwrap();
                server.abort();
                let _ = server.await;
                other_server.abort();
                let _ = other_server.await;
            }
        });
    }

    #[test]
    fn remove_client_preserves_healthy_sibling_connections() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr: InetAddr = listener.local_addr().unwrap().into();
        let (release_server, wait_for_release) = mpsc::channel();
        let server = thread::spawn(move || {
            let (_first, _) = listener.accept().unwrap();
            let (_second, _) = listener.accept().unwrap();
            wait_for_release.recv().unwrap();
        });

        let conf = ClientConf {
            io_threads: 1,
            worker_threads: 1,
            conn_size: 2,
            ..Default::default()
        };
        let factory = ClientFactory::new(conf);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let first = factory.get(&addr).await.unwrap();
            let second = factory.get(&addr).await.unwrap();

            factory.remove_client(&addr, &first);
            let pool = factory.get_pool(&addr);
            let pool = pool.lock().unwrap();
            assert!(pool.clients[0].is_none());
            assert!(pool.clients[1]
                .as_ref()
                .is_some_and(|pooled| pooled.same_connection(&second)));
        });

        let _ = release_server.send(());
        server.join().unwrap();
        drop(factory);
        drop(runtime);
    }
}
