use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_rpc::client::{ClientFactory, SyncClient};
use std::thread;
use std::time::{Duration, Instant};

// Tests exercise the same ORPC data endpoint used by production block clients.
pub fn worker_client(conf: &ClusterConf) -> CommonResult<SyncClient> {
    let factory = ClientFactory::new(conf.client_rpc_conf());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match factory.create_sync(&conf.worker_addr()) {
            Ok(client) => return Ok(client),
            Err(error) if Instant::now() >= deadline => return Err(error.into()),
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }
    }
}
