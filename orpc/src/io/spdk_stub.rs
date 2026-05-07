use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct NvmeTarget {
    pub trtype: String,
    pub traddr: String,
    pub trsvcid: u16,
    pub subnqn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct SpdkConf {
    pub enabled: bool,
    pub app_name: String,
    #[serde(alias = "hugepage", default)]
    pub hugepage_str: String,
    pub reactor_mask: String,
    pub shm_id: i32,
    pub targets: Vec<NvmeTarget>,
    pub io_queue_depth: u32,
    pub io_queue_requests: u32,
    #[serde(alias = "io_timeout", default)]
    pub io_timeout_str: String,
    pub io_retry_count: u32,
    #[serde(alias = "keep_alive_timeout", default)]
    pub keep_alive_timeout_str: String,
    #[serde(alias = "dma_pool_size", default)]
    pub dma_pool_size_str: String,
}

impl SpdkConf {
    pub fn init(&mut self) -> crate::CommonResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct BdevInfo {
    pub name: String,
    pub size_bytes: u64,
    pub block_size: u32,
    pub target_endpoint: String,
}
