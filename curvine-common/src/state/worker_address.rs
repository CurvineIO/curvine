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

impl From<WorkerAddress> for WorkerAddressProto {
    fn from(val: WorkerAddress) -> Self {
        WorkerAddressProto {
            worker_id: val.worker_id,
            hostname: val.hostname,
            ip_addr: val.ip_addr,
            rpc_port: val.rpc_port,
            web_port: val.web_port,
        }
    }
}

impl From<WorkerAddressProto> for WorkerAddress {
    fn from(proto: WorkerAddressProto) -> Self {
        Self {
            worker_id: proto.worker_id,
            hostname: proto.hostname,
            ip_addr: proto.ip_addr,
            rpc_port: proto.rpc_port,
            web_port: proto.web_port,
        }
    }
}
