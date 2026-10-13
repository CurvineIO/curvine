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

use bytes::BytesMut;
use curvine_core_error::CommonResult;
use curvine_error::FsResult;
use curvine_fs_api::RpcCode;
use curvine_model::sanitize_client_context;
use curvine_net::net::ConnState;
use curvine_proto::ClientContextProto;
use curvine_rpc::message::{Builder, Message};
use curvine_runtime::common::TimeSpent;
use log::info;
use prost::Message as PMessage;
use std::borrow::Cow;

fn decode_varint(bytes: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(pos)?;
        value |= u64::from(byte & 0x7f) << shift;
        pos += 1;
        if byte & 0x80 == 0 {
            return Some((value, pos));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

fn extract_client_context(bytes: &[u8]) -> Option<ClientContextProto> {
    let mut pos = 0;
    while pos < bytes.len() {
        let (tag, next) = decode_varint(bytes, pos)?;
        let field_number = tag >> 3;
        let wire_type = tag & 0x07;
        match (field_number, wire_type) {
            (100, 2) => {
                let (len, value_start) = decode_varint(bytes, next)?;
                let len = usize::try_from(len).ok()?;
                let value_end = value_start.checked_add(len)?;
                if value_end > bytes.len() {
                    return None;
                }
                return ClientContextProto::decode(&bytes[value_start..value_end]).ok();
            }
            (_, 0) => pos = next,
            (_, 1) => pos = next.checked_add(8)?,
            (_, 2) => {
                let (len, value_start) = decode_varint(bytes, next)?;
                let len = usize::try_from(len).ok()?;
                pos = value_start.checked_add(len)?;
            }
            (_, 5) => pos = next.checked_add(4)?,
            _ => return None,
        }
        if pos > bytes.len() {
            return None;
        }
    }
    None
}

pub struct RpcContext<'a> {
    pub msg: &'a Message,
    pub code: RpcCode,
    pub spent: TimeSpent,
    pub queue_us: Option<u64>,
    pub audit_src: Option<String>,
    pub audit_dst: Option<String>,
    client_context: Option<ClientContextProto>,
}

impl<'a> RpcContext<'a> {
    pub fn new(msg: &'a Message) -> Self {
        let code = RpcCode::from(msg.code());
        let client_context = msg.header_bytes().and_then(extract_client_context);
        Self {
            msg,
            code,
            spent: TimeSpent::new(),
            queue_us: msg.time.map(|t| t.elapsed().as_micros() as u64),
            audit_src: None,
            audit_dst: None,
            client_context,
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
        let mut buf = BytesMut::with_capacity(header.encoded_len());
        header.encode(&mut buf)?;

        let rep_msg = Builder::success(self.msg).header(buf).build();
        Ok(rep_msg)
    }

    pub fn client_context(&self) -> Option<&ClientContextProto> {
        self.client_context.as_ref()
    }

    pub fn audit_log<T>(&self, res: &FsResult<T>, used_us: u64, conn_state: Option<&ConnState>) {
        if matches!(self.code, RpcCode::WorkerHeartbeat | RpcCode::GetMountTable) {
            return;
        }

        info!(target: "audit", "{}", self.format_audit_log(res, used_us, conn_state));
    }

    fn format_audit_log<T>(
        &self,
        res: &FsResult<T>,
        used_us: u64,
        conn_state: Option<&ConnState>,
    ) -> String {
        let src = self.audit_src.as_deref().unwrap_or("");
        let dst = self.audit_dst.as_deref().unwrap_or("");
        let ip = conn_state
            .map(|s| s.remote_addr.hostname.as_str())
            .unwrap_or("");

        let context_suffix = self.client_context_suffix();
        let err_suffix: Cow<'_, str> = match res.as_ref() {
            Err(e) => Cow::Owned(format!(" err={:?}", e.kind())),
            Ok(_) => Cow::Borrowed(""),
        };
        format!(
            "cmd={} ok={} ip={} src={} dst={} queueUs={} usedUs={}{}{}",
            self.code,
            res.is_ok(),
            ip,
            src,
            dst,
            self.queue_us.unwrap_or(0),
            used_us,
            context_suffix,
            err_suffix,
        )
    }

    fn client_context_suffix(&self) -> String {
        let Some(context) = &self.client_context else {
            return String::new();
        };
        let tags = sanitize_client_context(context.tags.clone());
        if tags.is_empty() {
            return String::new();
        }

        let kv = tags
            .into_iter()
            .map(|(key, value)| format!("{}={}", key, value))
            .collect::<Vec<_>>()
            .join(" ");
        format!(" context={}", kv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_error::FsError;
    use curvine_proto::{AddBlockRequest, AddBlocksBatchRequest, GetBlockLocationsRequest};
    use std::collections::HashMap;

    fn message_with_context(tags: Vec<(&str, &str)>) -> Message {
        let tags = tags
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<_, _>>();
        let request = GetBlockLocationsRequest {
            path: "/data.txt".to_string(),
            client_context: Some(ClientContextProto { tags }),
        };
        Builder::new_rpc(RpcCode::GetBlockLocations)
            .proto_header(request)
            .build()
    }

    #[test]
    fn extracts_client_context_from_raw_header() {
        let msg = message_with_context(vec![("app_id", "job-1"), ("scenario", "etl")]);
        let ctx = RpcContext::new(&msg);

        let context = ctx.client_context().expect("context should be extracted");
        assert_eq!(
            context.tags.get("app_id").map(String::as_str),
            Some("job-1")
        );
        assert_eq!(
            context.tags.get("scenario").map(String::as_str),
            Some("etl")
        );
    }

    #[test]
    fn extracts_context_from_batch_header_without_decoding_each_request() {
        let mut tags = HashMap::new();
        tags.insert("app_id".to_string(), "job-1".to_string());
        let request = AddBlocksBatchRequest {
            requests: vec![AddBlockRequest::default()],
            client_context: Some(ClientContextProto { tags }),
        };
        let msg = Builder::new_rpc(RpcCode::AddBlocksBatch)
            .proto_header(request)
            .build();
        let ctx = RpcContext::new(&msg);

        let context = ctx.client_context().expect("context should be extracted");
        assert_eq!(
            context.tags.get("app_id").map(String::as_str),
            Some("job-1")
        );
    }

    #[test]
    fn empty_context_keeps_original_audit_shape() {
        let msg = message_with_context(vec![]);
        let ctx = RpcContext::new(&msg);

        let line = ctx.format_audit_log(&Ok(()), 123, None);
        assert_eq!(
            line,
            "cmd=GetBlockLocations ok=true ip= src= dst= queueUs=0 usedUs=123"
        );
    }

    #[test]
    fn audit_line_includes_sanitized_context_before_error() {
        let msg = message_with_context(vec![("scenario", "etl"), ("app_id", "job-1")]);
        let ctx = RpcContext::new(&msg);
        let error = FsError::common("boom");

        let line = ctx.format_audit_log(&Err::<(), _>(error), 456, None);

        assert!(line.contains("usedUs=456 context=app_id=job-1 scenario=etl"));
        assert!(line.contains("ok=false"));
        assert!(line.contains(" err="));
    }

    #[test]
    fn sensitive_context_is_dropped_from_audit() {
        let msg = message_with_context(vec![
            ("app_id", "job-1"),
            ("access_token", "should-not-log"),
        ]);
        let ctx = RpcContext::new(&msg);

        let line = ctx.format_audit_log(&Ok(()), 1, None);
        assert!(line.contains("context=app_id=job-1"));
        assert!(!line.contains("access_token"));
    }
}
