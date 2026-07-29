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

mod simple_server;
pub use self::simple_server::SimpleServer;

pub mod file;

#[cfg(test)]
mod client_tests {
    use super::SimpleServer;
    use crate::client::{ClientConf, ClientFactory, RpcClient};
    use crate::io::net::InetAddr;
    use crate::io::IOResult;
    use crate::message::Builder;
    use crate::runtime::{AsyncRuntime, RpcRuntime, Runtime};
    use crate::sys::DataSlice;
    use crate::CommonResult;
    use std::sync::Arc;
    use std::time::Duration;

    async fn wait_for_server(addr: &InetAddr, conf: &ClientConf, rt: Arc<Runtime>) {
        for _ in 0..50 {
            if RpcClient::new(false, rt.clone(), addr, conf).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("Server failed to start within 5 seconds");
    }

    #[test]
    fn client_factory_reuses_active_connection() -> CommonResult<()> {
        let server = SimpleServer::default();
        let addr = server.bind_addr().clone();
        let rt = Arc::new(AsyncRuntime::single());
        server.start(0);

        let conf = ClientConf::default();
        rt.block_on(wait_for_server(&addr, &conf, rt.clone()));

        let rt1 = rt.clone();
        rt.block_on(async move {
            let factory = ClientFactory::with_rt(ClientConf::default(), rt1);
            let client1 = factory.get(&addr).await?;
            let client2 = factory.get(&addr).await?;
            assert_eq!(client1.local_addr(), client2.local_addr());

            client1.set_closed();
            assert!(client2.is_closed());
            let client3 = factory.get(&addr).await?;
            assert_ne!(client1.local_addr(), client3.local_addr());

            Ok(())
        })
    }

    #[test]
    fn rpc_client_supports_buffered_and_raw_calls() -> IOResult<()> {
        let server = SimpleServer::default();
        let addr = server.bind_addr().clone();
        server.start(0);

        let conf = ClientConf::default();
        let rt = Arc::new(conf.create_runtime());
        rt.block_on(wait_for_server(&addr, &conf, rt.clone()));

        rt.block_on(call(&addr, true, &conf, rt.clone()));
        rt.block_on(call(&addr, false, &conf, rt.clone()));

        Ok(())
    }

    async fn call(addr: &InetAddr, buffer: bool, conf: &ClientConf, rt: Arc<Runtime>) {
        let client = RpcClient::new(buffer, rt, addr, conf).await.unwrap();
        assert_eq!(buffer, client.is_buffer());

        let msg = Builder::new_rpc(1).data(DataSlice::from_str("abc")).build();
        let dur = Duration::from_millis(conf.rpc_timeout_ms);
        client
            .retry_rpc(dur, conf.io_retry_policy(), msg)
            .await
            .unwrap();
    }
}
