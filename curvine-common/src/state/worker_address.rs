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

use crate::proto::WorkerAddressProto;
use crate::state::WorkerInfo;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkerAddress {
    pub worker_id: u32,
    pub hostname: String,
    pub ip_addr: String,
    pub rpc_port: u32,
    pub web_port: u32,
}

impl WorkerAddress {
    pub fn is_local(&self, hostname: &str) -> bool {
        self.hostname == hostname
    }

    pub fn connect_addr(&self) -> String {
        format!("{}:{}", self.ip_addr, self.rpc_port)
    }
}

impl PartialEq for WorkerAddress {
    fn eq(&self, other: &Self) -> bool {
        self.worker_id == other.worker_id
    }
}

impl Display for WorkerAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "worker_id = {}, addr = {}/{}:{}",
            self.worker_id, self.hostname, self.ip_addr, self.rpc_port
        )
    }
}

impl Into<WorkerAddressProto> for WorkerAddress {
    fn into(self) -> WorkerAddressProto {
        WorkerAddressProto {
            worker_id: self.worker_id,
            hostname: self.hostname,
            ip_addr: self.ip_addr,
            rpc_port: self.rpc_port,
            web_port: self.web_port,
        }
    }
}
