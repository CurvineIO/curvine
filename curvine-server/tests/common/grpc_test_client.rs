use curvine_client_core::block::grpc_block_client::GrpcBlockClient;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_rpc::message::Message;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// The test adapter preserves the existing request builders while sending the
// typed block operations over the production gRPC transport.
pub struct WorkerTestClient {
    rt: Arc<Runtime>,
    client: GrpcBlockClient,
}

impl WorkerTestClient {
    pub fn rpc(&self, message: Message) -> CommonResult<Message> {
        Ok(self.rt.block_on(self.client.rpc(message, None))?)
    }

    pub fn rpc_check(&self, message: Message) -> CommonResult<Message> {
        let response = self.rpc(message)?;
        response.check_error_ext::<curvine_error::FsError>()?;
        Ok(response)
    }
}

pub fn worker_client(conf: &ClusterConf) -> CommonResult<WorkerTestClient> {
    let rt = Arc::new(Runtime::single());
    let uri = format!("http://{}:{}", conf.worker.hostname, conf.worker.rpc_port);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match rt.block_on(GrpcBlockClient::connect(
            uri.clone(),
            Duration::from_secs(2),
        )) {
            Ok(client) => return Ok(WorkerTestClient { rt, client }),
            Err(error) if std::time::Instant::now() >= deadline => return Err(error.into()),
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }
    }
}
