use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct NvmeTarget {
    pub trtype: String,
    pub adrfam: String,
    pub traddr: String,
    pub trsvcid: u16,
    pub subnqn: String,
    pub hostnqn: String,
    pub io_queues: u32,
    #[serde(alias = "keep_alive_timeout")]
    pub keep_alive_timeout_str: String,
    #[serde(skip)]
    pub keep_alive_timeout_ms: u64,
    #[serde(alias = "io_timeout")]
    pub io_timeout_str: String,
    #[serde(skip)]
    pub io_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct SpdkConf {
    pub enabled: bool,
    pub app_name: String,
    #[serde(alias = "hugepage")]
    pub hugepage_str: String,
    pub reactor_mask: String,
    pub targets: Vec<NvmeTarget>,
    pub io_queue_depth: u32,
    pub io_queue_requests: u32,
    #[serde(alias = "io_timeout")]
    pub io_timeout_str: String,
    #[serde(skip)]
    pub io_timeout_ms: u64,
    pub io_retry_count: u32,
    #[serde(alias = "keep_alive_timeout")]
    pub keep_alive_timeout_str: String,
    #[serde(skip)]
    pub keep_alive_timeout_ms: u64,
    #[serde(alias = "poll_interval")]
    pub poll_interval_ms: u64,
    pub spin_iter: u32,
    #[serde(alias = "dma_buffer_size")]
    pub dma_buffer_size_str: String,
    #[serde(skip)]
    pub dma_buffer_bytes: u64,
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
    pub io_timeout_ms: u64,
}
