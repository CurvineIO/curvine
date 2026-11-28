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

use curvine_config::ClusterConf;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::RpcCode;
use curvine_net::net::{InetAddr, NodeAddr};
use curvine_net::retry::TimeBondedRetryBuilder;
use curvine_proto::{master_service_client::MasterServiceClient, *};
use curvine_rpc::client::ClusterConnector;
use curvine_rpc::message::{Builder, Message};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use dashmap::DashMap;
use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::Duration,
};
use tonic::{transport::Channel, Code, Request, Status};

pub(crate) struct GrpcMasterClient {
    connector: Arc<ClusterConnector>,
    grpc: curvine_rpc::grpc::GrpcConfig,
    transport: curvine_rpc::grpc::TransportConfig,
    channels: DashMap<InetAddr, Vec<Channel>>,
    next_channel: AtomicUsize,
    conn_size: usize,
    retry: TimeBondedRetryBuilder,
    timeout: Duration,
    rt: Weak<Runtime>,
}

impl GrpcMasterClient {
    pub(crate) fn new(
        conf: &ClusterConf,
        rt: Arc<Runtime>,
        connector: Arc<ClusterConnector>,
    ) -> FsResult<Self> {
        let client = conf.client_rpc_conf();
        let transport = curvine_rpc::grpc::TransportConfig {
            connect_timeout: Duration::from_millis(client.conn_timeout_ms),
            request_timeout: Some(Duration::from_millis(client.rpc_timeout_ms)),
            ..Default::default()
        };
        // Validate configured transport and TLS options during client creation.
        for node in conf.master_nodes() {
            let uri = conf.client.grpc.uri(&node.addr.hostname, node.addr.port);
            let tls = conf.client.grpc.client_tls(&node.addr.hostname)?;
            transport
                .endpoint(uri, tls.as_ref())
                .map_err(|error| FsError::common(error.to_string()))?;
        }
        Ok(Self {
            connector,
            grpc: conf.client.grpc.clone(),
            transport,
            channels: DashMap::new(),
            next_channel: AtomicUsize::new(0),
            conn_size: client.conn_size.max(1),
            retry: client.io_retry_builder(),
            timeout: Duration::from_millis(client.rpc_timeout_ms),
            rt: Arc::downgrade(&rt),
        })
    }

    // Keep tonic's typed dispatch state out of each filesystem caller. Nested
    // unified/FUSE metadata calls otherwise exhaust a debug worker's stack.
    pub(crate) fn rpc(&self, msg: Message) -> impl Future<Output = FsResult<Message>> + Send + '_ {
        Box::pin(self.rpc_inner(msg))
    }

    async fn rpc_inner(&self, msg: Message) -> FsResult<Message> {
        let mut last = None;
        let mut retry = self.retry.build();
        let no_retry_budget = self.retry.max_duration().is_zero();
        if let Some(id) = self.connector.leader_id() {
            if let Ok(peer) = self.connector.get_addr(id) {
                let remaining = retry.remaining();
                if !no_retry_budget && remaining.is_zero() {
                    return Err(FsError::common("master RPC retry budget exhausted"));
                }
                let mut timeout = self.timeout;
                if !no_retry_budget {
                    timeout = timeout.min(remaining);
                    if self.connector.peer_snapshot().len() > 1 {
                        timeout = timeout.min(remaining / 2);
                    }
                }
                match self.attempt_node(&peer, &msg, timeout).await {
                    Ok(response) => return Ok(response),
                    Err((false, error)) => return Err(error),
                    Err((true, error)) => last = Some(error),
                }
            }
        }
        self.connector
            .change_leader(ClusterConnector::DEFAULT_LEADER_ID);
        while retry.attempt().await {
            let timeout = if no_retry_budget {
                self.timeout
            } else {
                self.timeout.min(retry.remaining())
            };
            if timeout.is_zero() {
                break;
            }
            let peers = self.connector.peer_snapshot();
            if peers.is_empty() {
                return Err(FsError::common("no master endpoints configured"));
            }
            let attempts = peers
                .into_iter()
                .map(|peer| {
                    let message = &msg;
                    async move {
                        let result = self.attempt_node(&peer, message, timeout).await;
                        (peer, result)
                    }
                })
                .collect::<FuturesUnordered<_>>();
            futures::pin_mut!(attempts);
            while let Some((peer, result)) = attempts.next().await {
                match result {
                    Ok(response) => {
                        self.remember_leader(&peer);
                        return Ok(response);
                    }
                    Err((false, error)) => {
                        self.remember_leader(&peer);
                        return Err(error);
                    }
                    Err((true, error)) => last = Some(error),
                }
            }
        }
        Err(last.unwrap_or_else(|| FsError::common("master RPC retry budget exhausted")))
    }

    fn remember_leader(&self, peer: &NodeAddr) {
        if self
            .connector
            .get_addr(peer.id)
            .is_ok_and(|current| current.as_ref() == peer)
        {
            self.connector.change_leader(peer.id);
        }
    }

    pub(crate) async fn attempt_peer(
        &self,
        peer_id: u64,
        msg: &Message,
        timeout: Duration,
    ) -> Result<Message, (bool, FsError)> {
        let peer = self
            .connector
            .get_addr(peer_id)
            .map_err(|error| (true, error.into()))?;
        self.attempt_node(&peer, msg, timeout).await
    }

    /// Retry a targeted mirror request without changing the leader hint.
    pub(crate) async fn rpc_to_peer(&self, peer_id: u64, msg: Message) -> FsResult<Message> {
        let mut retry = self.retry.build();
        let no_retry_budget = self.retry.max_duration().is_zero();
        let mut last = None;
        while retry.attempt().await {
            let timeout = if no_retry_budget {
                self.timeout
            } else {
                self.timeout.min(retry.remaining())
            };
            if timeout.is_zero() {
                break;
            }
            match self.attempt_peer(peer_id, &msg, timeout).await {
                Ok(response) => return Ok(response),
                Err((false, error)) => return Err(error),
                Err((true, error)) => last = Some(error),
            }
        }
        Err(last.unwrap_or_else(|| FsError::common("master peer RPC retry budget exhausted")))
    }

    fn next_channel_index(&self) -> usize {
        self.next_channel.fetch_add(1, Ordering::Relaxed) % self.conn_size
    }

    async fn channel(&self, peer: &NodeAddr) -> FsResult<Channel> {
        let index = self.next_channel_index();
        if let Some(pool) = self.channels.get(&peer.addr) {
            return Ok(pool[index].clone());
        }
        let uri = self.grpc.uri(&peer.addr.hostname, peer.addr.port);
        let tls = self.grpc.client_tls(&peer.addr.hostname)?;
        let endpoint = self
            .transport
            .endpoint(uri, tls.as_ref())
            .map_err(|error| FsError::common(error.to_string()))?;
        let size = self.conn_size;
        let pool = self
            .rt
            .upgrade()
            .ok_or_else(|| FsError::common("master client runtime stopped"))?
            .spawn(async move {
                (0..size)
                    .map(|_| endpoint.connect_lazy())
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(|error| FsError::common(format!("master channel setup failed: {error}")))?;
        let pool = self.channels.entry(peer.addr.clone()).or_insert(pool);
        Ok(pool[index].clone())
    }

    async fn attempt_node(
        &self,
        peer: &NodeAddr,
        msg: &Message,
        timeout: Duration,
    ) -> Result<Message, (bool, FsError)> {
        if !self
            .connector
            .get_addr(peer.id)
            .is_ok_and(|current| current.as_ref() == peer)
        {
            return Err((
                true,
                FsError::common("master peer is no longer in membership"),
            ));
        }
        let call = async {
            let channel = self.channel(peer).await.map_err(|error| (true, error))?;
            let client = MasterServiceClient::new(channel)
                .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES);
            match Self::invoke(client, msg).await {
                Ok(response) => Ok(response),
                Err(status) => {
                    let transport_retry = status.details().is_empty()
                        && matches!(
                            status.code(),
                            Code::Unavailable
                                | Code::DeadlineExceeded
                                | Code::Cancelled
                                | Code::Internal
                                | Code::Unknown
                        );
                    let error = curvine_error::grpc::from_status(status);
                    Err((transport_retry || error.retry_master(), error))
                }
            }
        };
        match tokio::time::timeout(timeout, call).await {
            Ok(result) => result,
            Err(error) => Err((
                true,
                FsError::io(std::io::Error::new(std::io::ErrorKind::TimedOut, error)),
            )),
        }
    }

    // Preserve tonic Status until retry classification.
    #[allow(clippy::result_large_err)]
    async fn invoke(
        mut client: MasterServiceClient<Channel>,
        msg: &Message,
    ) -> Result<Message, Status> {
        macro_rules! call {
            ($method:ident, $request:ty) => {{
                let header = msg
                    .parse_header::<$request>()
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                let mut request = Request::new(header);
                curvine_rpc::grpc::set_request_id(&mut request, msg.req_id());
                curvine_rpc::grpc::set_request_mode(&mut request, msg.seq_id());
                let response = client.$method(request).await?.into_inner();
                Ok(Builder::success(msg).proto_header(response).build())
            }};
        }
        match RpcCode::from(msg.code()) {
            RpcCode::Mkdir => call!(mkdir, MkdirRequest),
            RpcCode::CreateFile => call!(create_file, CreateFileRequest),
            RpcCode::OpenFile => call!(open_file, OpenFileRequest),
            RpcCode::FileStatus => call!(file_status, GetFileStatusRequest),
            RpcCode::AddBlock => call!(add_block, AddBlockRequest),
            RpcCode::CompleteFile => call!(complete_file, CompleteFileRequest),
            RpcCode::CreateFilesBatch => call!(create_files_batch, CreateFilesBatchRequest),
            RpcCode::AddBlocksBatch => call!(add_blocks_batch, AddBlocksBatchRequest),
            RpcCode::CompleteFilesBatch => call!(complete_files_batch, CompleteFilesBatchRequest),
            RpcCode::Exists => call!(exists, ExistsRequest),
            RpcCode::Delete => call!(delete, DeleteRequest),
            RpcCode::ListTrash => call!(list_trash, ListTrashRequest),
            RpcCode::RestoreTrash => call!(restore_trash, RestoreTrashRequest),
            RpcCode::EmptyTrash => call!(empty_trash, EmptyTrashRequest),
            RpcCode::Free => call!(free, FreeRequest),
            RpcCode::Rename => call!(rename, RenameRequest),
            RpcCode::ListStatus => call!(list_status, ListStatusRequest),
            RpcCode::ListOptions => call!(list_options, ListOptionsRequest),
            RpcCode::GetBlockLocations => call!(get_block_locations, GetBlockLocationsRequest),
            RpcCode::GetFileBlockDetails => {
                call!(get_file_block_details, GetFileBlockDetailsRequest)
            }
            RpcCode::GetFilesystemInfo => call!(get_filesystem_info, GetFilesystemInfoRequest),
            RpcCode::SetAttr => call!(set_attr, SetAttrRequest),
            RpcCode::Symlink => call!(symlink, SymlinkRequest),
            RpcCode::Link => call!(link, LinkRequest),
            RpcCode::ResizeFile => call!(resize_file, FileResizeRequest),
            RpcCode::AssignWorker => call!(assign_worker, AssignWorkerRequest),
            RpcCode::GetLock => call!(get_lock, GetLockRequest),
            RpcCode::SetLock => call!(set_lock, SetLockRequest),
            RpcCode::Mount => call!(mount, MountRequest),
            RpcCode::UnMount => call!(un_mount, UnMountRequest),
            RpcCode::GetMountTable => call!(get_mount_table, GetMountTableRequest),
            RpcCode::GetMountInfo => call!(get_mount_info, GetMountInfoRequest),
            RpcCode::MetricsReport => call!(metrics_report, MetricsReportRequest),
            RpcCode::ClientHeartbeat => call!(client_heartbeat, ClientHeartbeatRequest),
            RpcCode::CreateLeaderSnapshot => {
                call!(create_leader_snapshot, CreateLeaderSnapshotRequest)
            }
            RpcCode::WorkerHeartbeat => call!(worker_heartbeat, WorkerHeartbeatRequest),
            RpcCode::WorkerBlockReport => call!(worker_block_report, BlockReportListRequest),
            RpcCode::ReportBlockReplicationResult => call!(
                report_block_replication_result,
                ReportBlockReplicationRequest
            ),
            RpcCode::RequestReplacementWorker => {
                call!(request_replacement_worker, RequestReplacementWorkerRequest)
            }
            RpcCode::ReportUnderReplicatedBlocks => call!(
                report_under_replicated_blocks,
                ReportUnderReplicatedBlocksRequest
            ),
            RpcCode::GetCvMetadataSnapshotPage => call!(
                get_cv_metadata_snapshot_page,
                GetCvMetadataSnapshotPageRequest
            ),
            _ => Err(Status::unimplemented("unsupported master RPC")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_uses_every_configured_master_connection() {
        let rt = Arc::new(curvine_runtime::runtime::AsyncRuntime::single());
        let conf = ClusterConf::default();
        let connector = Arc::new(ClusterConnector::with_rt(
            conf.client_rpc_conf(),
            rt.clone(),
        ));
        let mut client = GrpcMasterClient::new(&conf, rt, connector).unwrap();
        client.conn_size = 3;
        let indices: Vec<_> = (0..7).map(|_| client.next_channel_index()).collect();
        assert_eq!(indices, [0, 1, 2, 0, 1, 2, 0]);
    }
}
