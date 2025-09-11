
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub buffer_size: usize,
    pub enable_tag: bool,
    pub enable_stream: bool,
    pub enable_rma: bool,
    pub progress_interval_us: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            buffer_size: 64 * 1024,
            enable_tag: true,
            enable_stream: true,
            enable_rma: false,
            progress_interval_us: 100,
        }
    }
}