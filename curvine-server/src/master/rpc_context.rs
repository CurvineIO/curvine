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

use curvine_common::fs::RpcCode;
use curvine_common::FsResult;
use log::info;
use orpc::common::TimeSpent;
use orpc::io::net::ConnState;
use orpc::message::{Builder, Message};
use orpc::CommonResult;
use prost::Message as PMessage;

pub struct RpcContext<'a> {
    pub msg: &'a Message,
    pub code: RpcCode,
    pub spent: TimeSpent,
    pub audit_src: Option<String>,
    pub audit_dst: Option<String>,
}

impl<'a> RpcContext<'a> {
    pub fn new(msg: &'a Message) -> Self {
        let code = RpcCode::from(msg.code());
        Self {
            msg,
            code,
            spent: TimeSpent::new(),
            audit_src: None,
            audit_dst: None,
        }
    }

    pub fn parse_header<T: PMessage + Default>(&self) -> CommonResult<T> {
        self.msg.parse_header()
    }

    pub fn set_audit<T: Into<String>>(&mut self, src: Option<T>, dst: Option<T>) {
        self.audit_src = src.map(|x| x.into());
        self.audit_dst = dst.map(|x| x.into());
    }

    pub fn response<T: PMessage + Default>(&self, header: T) -> FsResult<Message> {
        let rep_msg = Builder::success_with_header(self.msg, header).build();
        Ok(rep_msg)
    }

    pub fn audit_log(&self, succeeded: bool, used_us: u64, conn_state: Option<&ConnState>) {
        if matches!(
            self.code,
            RpcCode::WorkerHeartbeat | RpcCode::WorkerBlockReport | RpcCode::GetMountTable
        ) {
            return;
        }

        let src = match &self.audit_src {
            None => "",
            Some(v) => v,
        };
        let dst = match &self.audit_dst {
            None => "",
            Some(v) => v,
        };

        let ip = match conn_state {
            None => "",
            Some(v) => &v.remote_addr.hostname,
        };

        info!(target: "audit", "cmd={:?} succeeded={} ip={} src={} dst={} executionTimeUs={}",
            self.code, succeeded, ip, src, dst, used_us);
    }
}
