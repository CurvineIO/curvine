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

use orpc::client::RpcClient;
use orpc::error::ErrorExt;
use orpc::io::IOError;
use orpc::message::MessageBuilder;
use orpc::CommonError;
use prost::Message as PMessage;
use std::time::Duration;

pub struct RpcUtils;

impl RpcUtils {
    pub async fn proto_rpc<T, R, E>(
        client: &RpcClient,
        timeout: Duration,
        code: impl Into<i8>,
        header: T,
    ) -> Result<R, E>
    where
        T: PMessage + Default,
        R: PMessage + Default,
        E: ErrorExt + From<IOError> + From<CommonError>,
    {
        let msg = MessageBuilder::new_rpc(code.into())
            .proto_header(header)
            .build();

        match client.timeout_rpc(timeout, msg).await {
            Ok(v) => {
                v.check_error_ext::<E>()?;
                Ok(v.parse_header()?)
            }

            Err(e) => {
                client.set_closed();
                Err(e.into())
            }
        }
    }
}
