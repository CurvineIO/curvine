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
use crate::io::net::InetAddr;
use crate::io::IOResult;
use crate::runtime::{RpcRuntime, Runtime};
use crate::sync::FastDashMap;
use std::sync::{Arc, Mutex};
use crate::{try_option, try_option_ref};

#[cfg(feature = "ucp")]
type UcpRt = Arc<crate::ucp::reactor::UcpRuntime>;

#[cfg(not(feature = "ucp"))]
type UcpRt = ();

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
    pub ucp_rt: Option<UcpRt>,
}

impl ClientFactory {
    pub fn new(conf: ClientConf) -> Self {
        let rt = Arc::new(conf.create_runtime());
        Self::with_rt(conf, rt)
    }

    pub fn with_rt(conf: ClientConf, rt: Arc<Runtime>) -> Self {
        #[cfg(feature = "ucp")] {
            let ucp_rt = if conf.use_ucp {
                let rt = crate::ucp::reactor::UcpRuntime::with_client(&conf).unwrap();
                Some(Arc::new(rt))
            } else {
                None
            };
            ClientFactory {
                conf,
                rt,
                pool: FastDashMap::default(),
                ucp_rt
            }
        }

        #[cfg(not(feature = "ucp"))] {
            ClientFactory {
                conf,
                rt,
                pool: FastDashMap::default(),
                ucp_rt: None,
            }
        }
    }

    pub async fn create_raw(&self, addr: &InetAddr) -> IOResult<RpcClient> {
        #[cfg(feature = "ucp")] {
            if self.conf.use_ucp {
                let rt = try_option_ref!(self.ucp_rt);
                RpcClient::with_ucp(&rt, addr, &self.conf).await
            } else {
                RpcClient::with_raw(addr, &self.conf).await
            }
        }

        #[cfg(not(feature = "ucp"))] {
            RpcClient::new(false, self.rt.clone(), addr, &self.conf).await
        }
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
        let client = self.rt.block_on(self.create_raw(addr))?;
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
    use crate::client::{ClientConf, ClientFactory};
    use crate::runtime::{AsyncRuntime, RpcRuntime};
    use crate::test::SimpleServer;
    use crate::CommonResult;
    use std::sync::Arc;

    #[test]
    fn pool() -> CommonResult<()> {
        let server = SimpleServer::default();
        let addr = server.bind_addr().clone();
        let rt = Arc::new(AsyncRuntime::single());
        server.start(0);

        let rt1 = rt.clone();
        rt.block_on(async move {
            let factory = ClientFactory::with_rt(ClientConf::default(), rt1);
            let c = factory.create(&addr, false).await?;
            println!("c = {}", c.local_addr());
            assert_eq!(factory.pool.len(), 0);

            let c1 = factory.get(&addr).await?;
            let c2 = factory.get(&addr).await?;
            println!("c1 = {}", c1.local_addr());
            println!("c2 = {}", c2.local_addr());
            assert_eq!(factory.pool.len(), 1);
            // is the same connection
            assert_eq!(c1.local_addr(), c2.local_addr());

            c1.set_closed();
            assert!(c2.is_closed());
            let c3 = factory.get(&addr).await?;
            println!("c3 = {}", c3.local_addr());
            assert_eq!(factory.pool.len(), 1);
            // A new connection was created.
            assert_ne!(c1.local_addr(), c3.local_addr());

            Ok(())
        })
    }
}
