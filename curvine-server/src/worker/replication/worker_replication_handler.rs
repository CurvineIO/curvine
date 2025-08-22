use crate::master::RpcContext;
use crate::worker::replication::worker_replication_manager::WorkerReplicationManager;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::SumbitBlockReplicationRequest;
use curvine_common::FsResult;
use log::warn;
use orpc::error::ErrorImpl;
use orpc::handler::MessageHandler;
use orpc::message::Message;

#[derive(Clone)]
struct WorkerReplicationHandler {
    manager: WorkerReplicationManager,
}

impl WorkerReplicationHandler {
    pub fn new(manager: &WorkerReplicationManager) -> Self {
        Self {
            manager: manager.clone(),
        }
    }

    pub fn accept_job(&self, ctx: &mut RpcContext<'_>) -> FsResult<Message> {
        let req: SumbitBlockReplicationRequest = ctx.parse_header()?;
        self.manager.accept_job(req.into())?;
        todo!()
    }
}

impl MessageHandler for WorkerReplicationHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let code = RpcCode::from(msg.code());

        let mut rpc_context = RpcContext::new(msg);
        let ctx = &mut rpc_context;

        let response = match code {
            RpcCode::SubmitBlockReplicationJob => self.accept_job(ctx),
            _ => Err(FsError::Common(ErrorImpl::with_source(
                format!("Unsupported operation: {:?}", code).into(),
            ))),
        };

        // Record the request processing status
        if let Err(ref e) = response {
            warn!("Request {:?} failed: {}", code, e);
        }

        response
    }
}
