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

use crate::rpc_stress::StressArgs;
use orpc_rpc::client::{ClientFactory, RpcClient};
use orpc_rpc::io::net::InetAddr;
use orpc_rpc::io::IOResult;
use orpc_rpc::runtime::AsyncRuntime;
use std::sync::Arc;

#[derive(Clone)]
pub struct StressEnv {
    pub rt: Arc<AsyncRuntime>,
    pub args: Arc<StressArgs>,
    pub factory: Arc<ClientFactory>,
}

impl StressEnv {
    pub fn new(args: Arc<StressArgs>) -> Self {
        let rt = Arc::new(AsyncRuntime::new("client", args.client_threads, 1));
        let factory = Arc::new(ClientFactory::default());

        Self { rt, args, factory }
    }

    pub async fn create_client(&self) -> IOResult<RpcClient> {
        let address = InetAddr::new(self.args.hostname.clone(), self.args.port);
        self.factory.create(&address, false).await
    }
}
