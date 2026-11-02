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

use super::MasterMetrics;
use super::MasterService;
use curvine_error::FsError;
use curvine_fs_api::RpcCode;
use curvine_net::net::ConnState;
use curvine_proto::{master_service_server::MasterService as GrpcService, *};
use curvine_rpc::{
    handler::{HandlerService, MessageHandler},
    message::Builder,
};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use prost::Message as ProtoMessage;
use std::{
    fmt::Debug,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tonic::{Request, Response, Status};

/// Typed gRPC front end for the existing master dispatch and leadership gate.
pub(crate) struct MasterGrpcHandler {
    service: MasterService,
    rt: Arc<Runtime>,
    tracker: RequestTracker,
}

#[derive(Default)]
struct TrackerState {
    closing: bool,
    active: usize,
}
#[derive(Default)]
struct TrackerInner {
    state: Mutex<TrackerState>,
    drained: Notify,
}
#[derive(Default, Clone)]
pub(crate) struct RequestTracker(Arc<TrackerInner>);
struct RequestGuard(RequestTracker);
impl Drop for RequestGuard {
    fn drop(&mut self) {
        let mut state = self.0 .0.state.lock().expect("master tracker poisoned");
        state.active -= 1;
        if state.active == 0 {
            self.0 .0.drained.notify_waiters();
        }
    }
}
impl RequestTracker {
    fn register(&self) -> Option<RequestGuard> {
        let mut state = self.0.state.lock().expect("master tracker poisoned");
        if state.closing {
            return None;
        }
        state.active += 1;
        Some(RequestGuard(self.clone()))
    }
    pub(crate) fn shutdown(&self) {
        self.0
            .state
            .lock()
            .expect("master tracker poisoned")
            .closing = true;
    }
    pub(crate) async fn wait_drained(&self) {
        loop {
            let notified = self.0.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.0.state.lock().expect("master tracker poisoned").active == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// Counts a gRPC connection once for its entire HTTP/2 lifetime.
pub(crate) struct TrackedStream {
    stream: TcpStream,
    metrics: &'static MasterMetrics,
}
impl TrackedStream {
    pub(crate) fn new(stream: TcpStream, metrics: &'static MasterMetrics) -> Self {
        metrics.active_connections.inc();
        Self { stream, metrics }
    }
}
impl Drop for TrackedStream {
    fn drop(&mut self) {
        self.metrics.active_connections.dec();
    }
}
impl Connected for TrackedStream {
    type ConnectInfo = TcpConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.stream.connect_info()
    }
}
impl AsyncRead for TrackedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl AsyncWrite for TrackedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl MasterGrpcHandler {
    pub(crate) fn new(service: MasterService, rt: Arc<Runtime>, tracker: RequestTracker) -> Self {
        Self {
            service,
            rt,
            tracker,
        }
    }

    async fn dispatch<Q, R>(
        &self,
        code: RpcCode,
        request: Request<Q>,
    ) -> Result<Response<R>, Status>
    where
        Q: ProtoMessage + Debug + Send + 'static,
        R: ProtoMessage + Default + Send + 'static,
    {
        let id = curvine_rpc::grpc::request_id(&request)?;
        let guard = self
            .tracker
            .register()
            .ok_or_else(|| Status::unavailable("master is shutting down"))?;
        let peer = request
            .remote_addr()
            .map(|address| ConnState::new(address.into(), Default::default()));
        let mut message = Builder::new_rpc(code)
            .req_id(id)
            .proto_header(request.into_inner())
            .build();
        message.set_time();
        let handler = Arc::new(self.service.grpc_message_handler(peer));
        let permit = self
            .service
            .get_limit()
            .global_limit
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("master RPC limiter closed"))?;
        let response = if handler.is_sync(&message) {
            let rt = handler.get_rt(&message).unwrap_or(&self.rt);
            let handler = handler.clone();
            rt.spawn_blocking(move || {
                let _guard = guard;
                let _permit = permit;
                handler.handle(&message)
            })
            .await
            .map_err(|e| Status::internal(format!("master handler failed: {e}")))?
        } else {
            // Keep tracking the handler after the RPC future is cancelled. Async
            // handlers can dispatch work to the control executor that must finish
            // before the master tears down its state.
            self.rt
                .spawn(async move {
                    let _guard = guard;
                    let _permit = permit;
                    handler.async_handle(message).await
                })
                .await
                .map_err(|e| Status::internal(format!("master handler failed: {e}")))?
        }
        .map_err(|e| curvine_error::grpc::to_status(&e))?;
        response
            .check_error_ext::<FsError>()
            .map_err(|e| curvine_error::grpc::to_status(&e))?;
        let result = response
            .parse_header::<R>()
            .map_err(|e| Status::data_loss(format!("invalid master response payload: {e}")))?;
        Ok(Response::new(result))
    }
}

#[tonic::async_trait]
impl GrpcService for MasterGrpcHandler {
    async fn mkdir(
        &self,
        request: Request<MkdirRequest>,
    ) -> Result<Response<MkdirResponse>, Status> {
        self.dispatch(RpcCode::Mkdir, request).await
    }

    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        self.dispatch(RpcCode::CreateFile, request).await
    }

    async fn open_file(
        &self,
        request: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>, Status> {
        self.dispatch(RpcCode::OpenFile, request).await
    }

    async fn file_status(
        &self,
        request: Request<GetFileStatusRequest>,
    ) -> Result<Response<GetFileStatusResponse>, Status> {
        self.dispatch(RpcCode::FileStatus, request).await
    }

    async fn add_block(
        &self,
        request: Request<AddBlockRequest>,
    ) -> Result<Response<LocatedBlockProto>, Status> {
        self.dispatch(RpcCode::AddBlock, request).await
    }

    async fn complete_file(
        &self,
        request: Request<CompleteFileRequest>,
    ) -> Result<Response<CompleteFileResponse>, Status> {
        self.dispatch(RpcCode::CompleteFile, request).await
    }

    async fn create_files_batch(
        &self,
        request: Request<CreateFilesBatchRequest>,
    ) -> Result<Response<CreateFilesBatchResponse>, Status> {
        self.dispatch(RpcCode::CreateFilesBatch, request).await
    }

    async fn add_blocks_batch(
        &self,
        request: Request<AddBlocksBatchRequest>,
    ) -> Result<Response<AddBlocksBatchResponse>, Status> {
        self.dispatch(RpcCode::AddBlocksBatch, request).await
    }

    async fn complete_files_batch(
        &self,
        request: Request<CompleteFilesBatchRequest>,
    ) -> Result<Response<CompleteFilesBatchResponse>, Status> {
        self.dispatch(RpcCode::CompleteFilesBatch, request).await
    }

    async fn exists(
        &self,
        request: Request<ExistsRequest>,
    ) -> Result<Response<ExistsResponse>, Status> {
        self.dispatch(RpcCode::Exists, request).await
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.dispatch(RpcCode::Delete, request).await
    }

    async fn list_trash(
        &self,
        request: Request<ListTrashRequest>,
    ) -> Result<Response<ListTrashResponse>, Status> {
        self.dispatch(RpcCode::ListTrash, request).await
    }

    async fn restore_trash(
        &self,
        request: Request<RestoreTrashRequest>,
    ) -> Result<Response<RestoreTrashResponse>, Status> {
        self.dispatch(RpcCode::RestoreTrash, request).await
    }

    async fn empty_trash(
        &self,
        request: Request<EmptyTrashRequest>,
    ) -> Result<Response<EmptyTrashResponse>, Status> {
        self.dispatch(RpcCode::EmptyTrash, request).await
    }

    async fn free(&self, request: Request<FreeRequest>) -> Result<Response<FreeResponse>, Status> {
        self.dispatch(RpcCode::Free, request).await
    }

    async fn rename(
        &self,
        request: Request<RenameRequest>,
    ) -> Result<Response<RenameResponse>, Status> {
        self.dispatch(RpcCode::Rename, request).await
    }

    async fn list_status(
        &self,
        request: Request<ListStatusRequest>,
    ) -> Result<Response<ListStatusResponse>, Status> {
        self.dispatch(RpcCode::ListStatus, request).await
    }

    async fn list_options(
        &self,
        request: Request<ListOptionsRequest>,
    ) -> Result<Response<ListOptionsResponse>, Status> {
        self.dispatch(RpcCode::ListOptions, request).await
    }

    async fn get_block_locations(
        &self,
        request: Request<GetBlockLocationsRequest>,
    ) -> Result<Response<GetBlockLocationsResponse>, Status> {
        self.dispatch(RpcCode::GetBlockLocations, request).await
    }

    async fn get_file_block_details(
        &self,
        request: Request<GetFileBlockDetailsRequest>,
    ) -> Result<Response<GetFileBlockDetailsResponse>, Status> {
        self.dispatch(RpcCode::GetFileBlockDetails, request).await
    }

    async fn get_filesystem_info(
        &self,
        request: Request<GetFilesystemInfoRequest>,
    ) -> Result<Response<GetFilesystemInfoResponse>, Status> {
        self.dispatch(RpcCode::GetFilesystemInfo, request).await
    }

    async fn set_attr(
        &self,
        request: Request<SetAttrRequest>,
    ) -> Result<Response<SetAttrResponse>, Status> {
        self.dispatch(RpcCode::SetAttr, request).await
    }

    async fn symlink(
        &self,
        request: Request<SymlinkRequest>,
    ) -> Result<Response<SymlinkResponse>, Status> {
        self.dispatch(RpcCode::Symlink, request).await
    }

    async fn link(&self, request: Request<LinkRequest>) -> Result<Response<LinkResponse>, Status> {
        self.dispatch(RpcCode::Link, request).await
    }

    async fn resize_file(
        &self,
        request: Request<FileResizeRequest>,
    ) -> Result<Response<FileResizeResponse>, Status> {
        self.dispatch(RpcCode::ResizeFile, request).await
    }

    async fn assign_worker(
        &self,
        request: Request<AssignWorkerRequest>,
    ) -> Result<Response<AssignWorkerResponse>, Status> {
        self.dispatch(RpcCode::AssignWorker, request).await
    }

    async fn get_lock(
        &self,
        request: Request<GetLockRequest>,
    ) -> Result<Response<GetLockResponse>, Status> {
        self.dispatch(RpcCode::GetLock, request).await
    }

    async fn set_lock(
        &self,
        request: Request<SetLockRequest>,
    ) -> Result<Response<SetLockResponse>, Status> {
        self.dispatch(RpcCode::SetLock, request).await
    }

    async fn mount(
        &self,
        request: Request<MountRequest>,
    ) -> Result<Response<MountResponse>, Status> {
        self.dispatch(RpcCode::Mount, request).await
    }

    async fn un_mount(
        &self,
        request: Request<UnMountRequest>,
    ) -> Result<Response<UnMountResponse>, Status> {
        self.dispatch(RpcCode::UnMount, request).await
    }

    async fn get_mount_table(
        &self,
        request: Request<GetMountTableRequest>,
    ) -> Result<Response<GetMountTableResponse>, Status> {
        self.dispatch(RpcCode::GetMountTable, request).await
    }

    async fn get_mount_info(
        &self,
        request: Request<GetMountInfoRequest>,
    ) -> Result<Response<GetMountInfoResponse>, Status> {
        self.dispatch(RpcCode::GetMountInfo, request).await
    }

    async fn metrics_report(
        &self,
        request: Request<MetricsReportRequest>,
    ) -> Result<Response<MetricsReportResponse>, Status> {
        self.dispatch(RpcCode::MetricsReport, request).await
    }

    async fn client_heartbeat(
        &self,
        request: Request<ClientHeartbeatRequest>,
    ) -> Result<Response<ClientHeartbeatResponse>, Status> {
        self.dispatch(RpcCode::ClientHeartbeat, request).await
    }

    async fn create_leader_snapshot(
        &self,
        request: Request<CreateLeaderSnapshotRequest>,
    ) -> Result<Response<CreateLeaderSnapshotResponse>, Status> {
        self.dispatch(RpcCode::CreateLeaderSnapshot, request).await
    }

    async fn worker_heartbeat(
        &self,
        request: Request<WorkerHeartbeatRequest>,
    ) -> Result<Response<WorkerHeartbeatResponse>, Status> {
        self.dispatch(RpcCode::WorkerHeartbeat, request).await
    }

    async fn worker_block_report(
        &self,
        request: Request<BlockReportListRequest>,
    ) -> Result<Response<BlockReportListResponse>, Status> {
        self.dispatch(RpcCode::WorkerBlockReport, request).await
    }

    async fn report_block_replication_result(
        &self,
        request: Request<ReportBlockReplicationRequest>,
    ) -> Result<Response<ReportBlockReplicationResponse>, Status> {
        self.dispatch(RpcCode::ReportBlockReplicationResult, request)
            .await
    }

    async fn request_replacement_worker(
        &self,
        _request: Request<RequestReplacementWorkerRequest>,
    ) -> Result<Response<RequestReplacementWorkerResponse>, Status> {
        Err(Status::unimplemented(
            "operation is not supported by master",
        ))
    }

    async fn report_under_replicated_blocks(
        &self,
        _request: Request<ReportUnderReplicatedBlocksRequest>,
    ) -> Result<Response<ReportUnderReplicatedBlocksResponse>, Status> {
        Err(Status::unimplemented(
            "operation is not supported by master",
        ))
    }

    async fn submit_job(
        &self,
        request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        self.dispatch(RpcCode::SubmitJob, request).await
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusResponse>, Status> {
        self.dispatch(RpcCode::GetJobStatus, request).await
    }

    async fn cancel_job(
        &self,
        request: Request<CancelJobRequest>,
    ) -> Result<Response<CancelJobResponse>, Status> {
        self.dispatch(RpcCode::CancelJob, request).await
    }

    async fn report_task(
        &self,
        request: Request<TaskReportRequest>,
    ) -> Result<Response<TaskReportResponse>, Status> {
        self.dispatch(RpcCode::ReportTask, request).await
    }

    async fn get_cv_metadata_snapshot_page(
        &self,
        request: Request<GetCvMetadataSnapshotPageRequest>,
    ) -> Result<Response<GetCvMetadataSnapshotPageResponse>, Status> {
        self.dispatch(RpcCode::GetCvMetadataSnapshotPage, request)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::RequestTracker;
    use std::time::Duration;

    #[tokio::test]
    async fn shutdown_waits_for_cancelled_blocking_request() {
        let tracker = RequestTracker::default();
        let guard = tracker.register().unwrap();
        let (release, wait) = std::sync::mpsc::channel();
        let task = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            wait.recv().unwrap();
        });
        drop(task);
        tracker.shutdown();
        assert!(tracker.register().is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), tracker.wait_drained())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), tracker.wait_drained())
            .await
            .unwrap();
    }
}
