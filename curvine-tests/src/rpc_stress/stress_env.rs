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
use curvine_io::{IOError, IOResult};
use curvine_proto::master_service_client::MasterServiceClient;
use curvine_runtime::runtime::AsyncRuntime;
use std::sync::Arc;

#[derive(Clone)]
pub struct StressEnv {
    pub rt: Arc<AsyncRuntime>,
    pub args: Arc<StressArgs>,
}

impl StressEnv {
    pub fn new(args: Arc<StressArgs>) -> Self {
        let rt = Arc::new(AsyncRuntime::new("client", args.client_threads, 1));
        Self { rt, args }
    }

    pub async fn create_client(&self) -> IOResult<MasterServiceClient<tonic::transport::Channel>> {
        let hostname = &self.args.hostname;
        let config = curvine_rpc::grpc::GrpcConfig {
            tls_enabled: self.args.grpc_tls,
            ca_cert_path: self.args.grpc_ca_cert.clone(),
            client_cert_path: self.args.grpc_client_cert.clone(),
            client_key_path: self.args.grpc_client_key.clone(),
            domain_name: self.args.grpc_domain_name.clone(),
            ..Default::default()
        };
        let tls = config.client_tls(hostname).map_err(IOError::new)?;
        let endpoint = curvine_rpc::grpc::TransportConfig::default()
            .endpoint(config.uri(hostname, self.args.port), tls.as_ref())
            .map_err(IOError::create)?;
        let channel = endpoint.connect().await.map_err(IOError::create)?;
        Ok(MasterServiceClient::new(channel)
            .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES))
    }
}
