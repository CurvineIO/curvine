use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::common::Logger;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime, Runtime};
use log::info;
use std::sync::Arc;
use tokio::sync::watch;

pub struct Mds {
    conf: ClusterConf,
    rt: Arc<Runtime>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl Mds {
    pub fn with_conf(mut conf: ClusterConf) -> CommonResult<Self> {
        if !conf.mds.enabled {
            return err_box!("mds service is disabled; set mds.enabled=true to start it");
        }
        conf.mds.init()?;
        Logger::init(conf.mds.log.clone());

        let rt = Arc::new(AsyncRuntime::new(
            "mds",
            conf.mds.io_threads,
            conf.mds.worker_threads,
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Ok(Self {
            conf,
            rt,
            shutdown_tx,
            shutdown_rx,
        })
    }

    pub fn conf(&self) -> &ClusterConf {
        &self.conf
    }

    pub async fn start(&mut self) -> CommonResult<()> {
        info!("curvine-mds started: cluster_id={}", self.conf.cluster_id);
        Ok(())
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    async fn wait_for_shutdown(&mut self) -> CommonResult<()> {
        if *self.shutdown_rx.borrow() {
            return Ok(());
        }

        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let mut terminate = signal(SignalKind::terminate())?;
            tokio::select! {
                result = tokio::signal::ctrl_c() => result?,
                _ = terminate.recv() => {},
                result = self.shutdown_rx.changed() => {
                    result.map_err(|error| curvine_core_error::CommonError::from(error.to_string()))?;
                }
            }
        }

        #[cfg(not(unix))]
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            result = self.shutdown_rx.changed() => {
                result.map_err(|error| curvine_core_error::CommonError::from(error.to_string()))?;
            }
        }

        info!("curvine-mds stopped: cluster_id={}", self.conf.cluster_id);
        Ok(())
    }

    pub fn block_on_start(mut self) -> CommonResult<()> {
        let rt = self.rt.clone();
        rt.block_on(async move {
            self.start().await?;
            self.wait_for_shutdown().await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_to_start_when_disabled() {
        let conf = ClusterConf::default();
        assert!(Mds::with_conf(conf).is_err());
    }

    #[test]
    fn starts_and_stops_with_explicit_shutdown() {
        let mut conf = ClusterConf::default();
        conf.mds.enabled = true;
        let mut mds = Mds::with_conf(conf).unwrap();
        mds.shutdown();

        let rt = mds.rt.clone();
        rt.block_on(async move {
            mds.start().await.unwrap();
            mds.wait_for_shutdown().await.unwrap();
        });
    }
}
