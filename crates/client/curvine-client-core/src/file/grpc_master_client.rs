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
use curvine_net::retry::TimeBondedRetryBuilder;
use curvine_proto::{master_service_client::MasterServiceClient, *};
use curvine_rpc::message::{Builder, Message};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::Duration,
};
use tonic::{
    transport::{Channel, Endpoint},
    Code, Request, Status,
};

pub(crate) struct GrpcMasterClient {
    endpoints: Vec<Endpoint>,
    channels: tokio::sync::OnceCell<Vec<Vec<Channel>>>,
    preferred: AtomicUsize,
    next_channel: AtomicUsize,
    conn_size: usize,
    retry: TimeBondedRetryBuilder,
    timeout: Duration,
    rt: Weak<Runtime>,
}

impl GrpcMasterClient {
    pub(crate) fn new(conf: &ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        let client = conf.client_rpc_conf();
        let transport = curvine_rpc::grpc::TransportConfig {
            connect_timeout: Duration::from_millis(client.conn_timeout_ms),
            request_timeout: Some(Duration::from_millis(client.rpc_timeout_ms)),
            ..Default::default()
        };
        let endpoints = conf
            .master_nodes()
            .iter()
            .map(|node| {
                let address = conf.client.grpc.uri(&node.addr.hostname, node.addr.port);
                let tls = conf.client.grpc.client_tls(&node.addr.hostname)?;
                transport
                    .endpoint(address, tls.as_ref())
                    .map_err(|e| FsError::common(e.to_string()))
            })
            .collect::<FsResult<Vec<_>>>()?;
        Ok(Self {
            endpoints,
            channels: tokio::sync::OnceCell::new(),
            preferred: AtomicUsize::new(usize::MAX),
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
        let channels = self
            .channels
            .get_or_try_init(|| async {
                let endpoints = self.endpoints.clone();
                let conn_size = self.conn_size;
                self.rt
                    .upgrade()
                    .ok_or_else(|| FsError::common("master client runtime stopped"))?
                    .spawn(async move {
                        endpoints
                            .iter()
                            .map(|endpoint| {
                                (0..conn_size).map(|_| endpoint.connect_lazy()).collect()
                            })
                            .collect()
                    })
                    .await
                    .map_err(|e| FsError::common(format!("master channel setup failed: {e}")))
            })
            .await?;
        if channels.is_empty() {
            return Err(FsError::common("no master endpoints configured"));
        }
        let mut last = None;
        let channel_index = self.next_channel_index();
        let preferred = self.preferred.load(Ordering::Relaxed);
        if preferred < channels.len() {
            match Self::attempt(&channels[preferred][channel_index], &msg, self.timeout).await {
                Ok(response) => return Ok(response),
                Err((false, error)) => return Err(error),
                Err((true, error)) => last = Some(error),
            }
        }
        self.preferred.store(usize::MAX, Ordering::Relaxed);
        let mut retry = self.retry.build();
        while retry.attempt().await {
            let message = &msg;
            let timeout = self.timeout;
            let attempts = channels
                .iter()
                .enumerate()
                .map(|(index, pool)| async move {
                    (
                        index,
                        Self::attempt(&pool[channel_index], message, timeout).await,
                    )
                })
                .collect::<FuturesUnordered<_>>();
            futures::pin_mut!(attempts);
            while let Some((index, result)) = attempts.next().await {
                match result {
                    Ok(response) => {
                        self.preferred.store(index, Ordering::Relaxed);
                        return Ok(response);
                    }
                    Err((false, error)) => {
                        self.preferred.store(index, Ordering::Relaxed);
                        return Err(error);
                    }
                    Err((true, error)) => last = Some(error),
                }
            }
        }
        Err(last.unwrap_or_else(|| FsError::common("master RPC retry budget exhausted")))
    }

    fn next_channel_index(&self) -> usize {
        self.next_channel.fetch_add(1, Ordering::Relaxed) % self.conn_size
    }

    async fn attempt(
        channel: &Channel,
        msg: &Message,
        timeout: Duration,
    ) -> Result<Message, (bool, FsError)> {
        let client = MasterServiceClient::new(channel.clone())
            .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES);
        match tokio::time::timeout(timeout, Self::invoke(client, msg)).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(status)) => {
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
            RpcCode::SubmitJob => call!(submit_job, SubmitJobRequest),
            RpcCode::GetJobStatus => call!(get_job_status, GetJobStatusRequest),
            RpcCode::CancelJob => call!(cancel_job, CancelJobRequest),
            RpcCode::ReportTask => call!(report_task, TaskReportRequest),
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
        let client = GrpcMasterClient {
            endpoints: Vec::new(),
            channels: tokio::sync::OnceCell::new(),
            preferred: AtomicUsize::new(usize::MAX),
            next_channel: AtomicUsize::new(0),
            conn_size: 3,
            retry: TimeBondedRetryBuilder::default(),
            timeout: Duration::from_secs(1),
            rt: Weak::new(),
        };
        let indices: Vec<_> = (0..7).map(|_| client.next_channel_index()).collect();
        assert_eq!(indices, [0, 1, 2, 0, 1, 2, 0]);
    }
}
