use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::common::LogConf;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MdsConf {
    pub enabled: bool,
    pub hostname: String,
    pub rpc_port: u16,
    pub web_port: u16,
    pub io_threads: usize,
    pub worker_threads: usize,
    pub log: LogConf,
}

impl MdsConf {
    pub const DEFAULT_RPC_PORT: u16 = 8998;
    pub const DEFAULT_WEB_PORT: u16 = 9004;

    pub fn init(&mut self) -> CommonResult<()> {
        self.hostname = self.hostname.trim().to_string();
        if self.hostname.is_empty() {
            return err_box!("mds.hostname must not be empty");
        }
        if self.rpc_port == 0 {
            return err_box!("mds.rpc_port must be greater than zero");
        }
        if self.web_port == 0 {
            return err_box!("mds.web_port must be greater than zero");
        }
        if self.rpc_port == self.web_port {
            return err_box!("mds.rpc_port and mds.web_port must be different");
        }
        if self.io_threads == 0 {
            return err_box!("mds.io_threads must be greater than zero");
        }
        if self.worker_threads == 0 {
            return err_box!("mds.worker_threads must be greater than zero");
        }
        Ok(())
    }
}

impl Default for MdsConf {
    fn default() -> Self {
        Self {
            enabled: false,
            hostname: "localhost".to_string(),
            rpc_port: Self::DEFAULT_RPC_PORT,
            web_port: Self::DEFAULT_WEB_PORT,
            io_threads: 4,
            worker_threads: 8,
            log: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_disabled_and_valid() {
        let mut conf = MdsConf::default();
        conf.init().unwrap();

        assert!(!conf.enabled);
        assert_eq!(conf.rpc_port, MdsConf::DEFAULT_RPC_PORT);
        assert_eq!(conf.web_port, MdsConf::DEFAULT_WEB_PORT);
    }

    #[test]
    fn rejects_invalid_ports() {
        let mut conf = MdsConf {
            rpc_port: 0,
            ..Default::default()
        };
        assert!(conf.init().is_err());

        let mut conf = MdsConf {
            web_port: MdsConf::DEFAULT_RPC_PORT,
            ..Default::default()
        };
        assert!(conf.init().is_err());
    }
}
