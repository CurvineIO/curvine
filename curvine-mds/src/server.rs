use curvine_config::{ClusterConf, KvBackendType};
use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::common::Logger;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime, Runtime};
use log::info;
use std::sync::Arc;
use tokio::sync::watch;

#[cfg(feature = "fdb")]
use crate::FdbBackend;
use crate::{KvBackend, MemoryBackend};
pub struct Mds {
    conf: ClusterConf,
    backend: Arc<dyn KvBackend>,
    rt: Arc<Runtime>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl Mds {
    pub fn with_conf(mut conf: ClusterConf) -> CommonResult<Self> {
        if !conf.mds.enabled {
            return err_box!("mds service is disabled; set mds.enabled=true to start it");
        }
        // Reject kv_backend=fdb before config init() so users get the
        // actionable "rebuild with fdb" message even when other config fields
        // (e.g. an empty fdb_cluster_file) would otherwise fail validation
        // first with a less helpful error.
        #[cfg(not(feature = "fdb"))]
        if conf.mds.kv_backend == KvBackendType::Fdb {
            return err_box!(
                "mds.kv_backend=fdb requires the \"fdb\" feature \
                 (build with --features fdb)"
            );
        }
        conf.mds.init()?;
        Logger::init(conf.mds.log.clone());

        let backend: Arc<dyn KvBackend> = match conf.mds.kv_backend {
            KvBackendType::Memory => Arc::new(MemoryBackend::new()),
            #[cfg(feature = "fdb")]
            KvBackendType::Fdb => Arc::new(
                FdbBackend::open(&conf.mds.fdb_cluster_file, conf.mds.fdb_txn_timeout_ms)
                    .map_err(|error| curvine_core_error::CommonError::from(error.to_string()))?,
            ),
            #[cfg(not(feature = "fdb"))]
            KvBackendType::Fdb => {
                return err_box!(
                    "mds.kv_backend=fdb requires the \"fdb\" feature \
                     (build with --features fdb)"
                );
            }
        };
        let rt = Arc::new(AsyncRuntime::new(
            "mds",
            conf.mds.io_threads,
            conf.mds.worker_threads,
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Ok(Self {
            conf,
            backend,
            rt,
            shutdown_tx,
            shutdown_rx,
        })
    }

    pub fn conf(&self) -> &ClusterConf {
        &self.conf
    }

    pub fn backend(&self) -> &Arc<dyn KvBackend> {
        &self.backend
    }

    pub async fn start(&mut self) -> CommonResult<()> {
        info!(
            "curvine-mds started: cluster_id={}, backend={}",
            self.conf.cluster_id,
            self.backend.name()
        );
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

    fn mds_with_backend(mut conf: ClusterConf, backend: Arc<dyn KvBackend>) -> CommonResult<Mds> {
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
        Ok(Mds {
            conf,
            backend,
            rt,
            shutdown_tx,
            shutdown_rx,
        })
    }

    #[test]
    fn refuses_to_start_when_disabled() {
        let conf = ClusterConf::default();
        assert!(Mds::with_conf(conf).is_err());
    }

    #[test]
    fn starts_and_stops_with_explicit_shutdown() {
        let mut conf = ClusterConf::default();
        conf.mds.enabled = true;
        // Use the memory backend so init() does not require an FDB connection
        // string (this test exercises the lifecycle, not the backend).
        conf.mds.kv_backend = KvBackendType::Memory;
        let mut mds = mds_with_backend(conf, Arc::new(MemoryBackend::new())).unwrap();
        mds.shutdown();

        let rt = mds.rt.clone();
        rt.block_on(async move {
            mds.start().await.unwrap();
            mds.wait_for_shutdown().await.unwrap();
        });
    }

    #[test]
    fn selects_memory_backend_from_config() {
        let mut conf = ClusterConf::default();
        conf.mds.enabled = true;
        conf.mds.kv_backend = KvBackendType::Memory;

        let mds = Mds::with_conf(conf).unwrap();
        assert_eq!(mds.backend().name(), "memory");
    }

    #[cfg(not(feature = "fdb"))]
    #[test]
    fn rejects_fdb_backend_when_feature_disabled() {
        // Default config: kv_backend=Fdb with non-empty fdb_cluster_file.
        // The feature-required error should fire before init() validation.
        let mut conf = ClusterConf::default();
        conf.mds.enabled = true;
        let err = match Mds::with_conf(conf) {
            Ok(_) => panic!("expected fdb feature error when fdb feature is disabled"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("\"fdb\" feature"),
            "expected error mentioning the 'fdb' feature, got: {msg}"
        );

        // Empty fdb_cluster_file: init() would normally reject it, but the
        // early feature check should fire first with the same actionable error.
        let mut conf2 = ClusterConf::default();
        conf2.mds.enabled = true;
        conf2.mds.fdb_cluster_file.clear();
        let err2 = match Mds::with_conf(conf2) {
            Ok(_) => panic!("expected fdb feature error even with empty cluster file"),
            Err(e) => e,
        };
        let msg2 = format!("{err2}");
        assert!(
            msg2.contains("\"fdb\" feature"),
            "expected error mentioning the 'fdb' feature with empty cluster file, got: {msg2}"
        );
    }
}
