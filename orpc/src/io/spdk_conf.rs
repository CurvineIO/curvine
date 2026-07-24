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

use crate::common::{ByteUnit, DurationUnit};
use crate::{err_box, err_msg, CommonResult};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Remote NVMe-oF target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

impl NvmeTarget {
    pub fn init(&mut self) -> CommonResult<()> {
        if !self.keep_alive_timeout_str.is_empty() {
            let dur = DurationUnit::from_str(&self.keep_alive_timeout_str)?;
            self.keep_alive_timeout_ms = dur.as_millis();
        }
        if !self.io_timeout_str.is_empty() {
            let dur = DurationUnit::from_str(&self.io_timeout_str)?;
            self.io_timeout_ms = dur.as_millis();
        }
        Ok(())
    }

    pub fn new(traddr: &str, trsvcid: u16, subnqn: &str) -> Self {
        Self {
            traddr: traddr.to_string(),
            trsvcid,
            subnqn: subnqn.to_string(),
            ..Default::default()
        }
    }

    pub fn validate(&self) -> CommonResult<()> {
        if self.traddr.is_empty() {
            return err_box!("NvmeTarget: traddr cannot be empty");
        }
        if self.trsvcid == 0 {
            return err_box!("NvmeTarget: trsvcid cannot be 0");
        }
        if self.subnqn.is_empty() {
            return err_box!("NvmeTarget: subnqn cannot be empty");
        }
        let valid_trtype = ["rdma", "tcp"];
        if !valid_trtype.contains(&self.trtype.to_lowercase().as_str()) {
            return err_box!(
                "NvmeTarget: invalid trtype '{}', expected one of {:?}",
                self.trtype,
                valid_trtype
            );
        }
        Ok(())
    }

    pub fn endpoint(&self) -> String {
        format!(
            "{}://{}:{}/{}",
            self.trtype, self.traddr, self.trsvcid, self.subnqn
        )
    }
}

impl Default for NvmeTarget {
    fn default() -> Self {
        Self {
            trtype: "rdma".to_string(),
            adrfam: "ipv4".to_string(),
            traddr: String::new(),
            trsvcid: 4420,
            subnqn: String::new(),
            hostnqn: String::new(),
            io_queues: 0,
            keep_alive_timeout_str: String::new(),
            keep_alive_timeout_ms: 0,
            io_timeout_str: String::new(),
            io_timeout_ms: 0,
        }
    }
}

impl Display for NvmeTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.endpoint())
    }
}

/// SPDK env config. String fields are parsed by `init()`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SpdkConf {
    pub enabled: bool,
    pub app_name: String,
    #[serde(alias = "hugepage")]
    pub hugepage_str: String,
    #[serde(skip)]
    pub hugepage_mb: u32,
    pub reactor_mask: String,
    pub iova_mode: String,
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
    pub fn init(&mut self) -> CommonResult<()> {
        self.hugepage_mb =
            (ByteUnit::from_str(&self.hugepage_str)?.as_byte() / ByteUnit::MB) as u32;

        if !self.io_timeout_str.is_empty() {
            let io_timeout = DurationUnit::from_str(&self.io_timeout_str)?;
            self.io_timeout_ms = io_timeout.as_millis();
        }

        if !self.keep_alive_timeout_str.is_empty() {
            let keep_alive = DurationUnit::from_str(&self.keep_alive_timeout_str)?;
            self.keep_alive_timeout_ms = keep_alive.as_millis();
        }

        if !self.dma_buffer_size_str.is_empty() {
            let dma_buf = ByteUnit::from_str(&self.dma_buffer_size_str)?;
            self.dma_buffer_bytes = dma_buf.as_byte();
        }

        for target in &mut self.targets {
            target.init()?;
        }

        Ok(())
    }

    pub fn validate(&self) -> CommonResult<()> {
        if !self.enabled {
            return Ok(());
        }

        if self.targets.is_empty() {
            return err_box!("SpdkConf: enabled=true but no targets configured");
        }

        let mask = self
            .reactor_mask
            .trim_start_matches("0x")
            .trim_start_matches("0X");
        if u64::from_str_radix(mask, 16).is_err() {
            return err_box!(
                "SpdkConf: invalid reactor_mask '{}', expected hex (e.g. '0x3')",
                self.reactor_mask
            );
        }

        if self.hugepage_mb == 0 {
            return err_box!(
                "SpdkConf: hugepage must be > 0 (got '{}')",
                self.hugepage_str
            );
        }

        if self.io_queue_depth == 0 {
            return err_box!("SpdkConf: io_queue_depth must be > 0");
        }

        if self.io_queue_requests < self.io_queue_depth {
            return err_box!(
                "SpdkConf: io_queue_requests ({}) must be >= io_queue_depth ({})",
                self.io_queue_requests,
                self.io_queue_depth
            );
        }

        if !self.iova_mode.is_empty() && self.iova_mode != "va" && self.iova_mode != "pa" {
            return err_box!(
                "SpdkConf: iova_mode must be 'va', 'pa', or empty (auto-detect), got '{}'",
                self.iova_mode
            );
        }

        for (i, target) in self.targets.iter().enumerate() {
            target.validate().map_err(|e| {
                let msg = format!("SpdkConf: targets[{}]: {}", i, e);
                err_msg!(msg)
            })?;
            let resolved_ka_ms = if target.keep_alive_timeout_ms > 0 {
                target.keep_alive_timeout_ms
            } else {
                self.keep_alive_timeout_ms
            };
            let min_ka_ms = self.poll_interval_ms * 3;
            if resolved_ka_ms < min_ka_ms {
                return err_box!(
                    "SpdkConf: targets[{}]: keep_alive_timeout_ms ({}) must be \
                     >= 3 * poll_interval_ms ({}) as the worst-case idle->active gap spans \
                     ~2 poll intervals, requiring 1 interval margin for safety",
                    i,
                    resolved_ka_ms,
                    min_ka_ms
                );
            }
        }

        if self.poll_interval_ms == 0 {
            return err_box!("SpdkConf: poll_interval_ms must be > 0");
        }
        if self.poll_interval_ms > i32::MAX as u64 {
            return err_box!(
                "SpdkConf: poll_interval_ms ({}) exceeds i32::MAX ({})",
                self.poll_interval_ms,
                i32::MAX
            );
        }

        Ok(())
    }
}

impl Default for SpdkConf {
    fn default() -> Self {
        Self {
            enabled: false,
            app_name: "curvine".to_string(),
            hugepage_str: "1024MB".to_string(),
            hugepage_mb: 1024,
            reactor_mask: "0x1".to_string(),
            iova_mode: String::new(),
            targets: vec![],
            io_queue_depth: 128,
            io_queue_requests: 512,
            io_timeout_str: String::new(),
            io_timeout_ms: 30_000,
            io_retry_count: 4,
            keep_alive_timeout_str: String::new(),
            keep_alive_timeout_ms: 10_000,
            poll_interval_ms: 100,
            spin_iter: 1000,
            dma_buffer_size_str: String::new(),
            dma_buffer_bytes: 1024 * 1024,
        }
    }
}

impl Display for SpdkConf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let iova_mode_label = if self.iova_mode.is_empty() {
            "auto"
        } else {
            self.iova_mode.as_str()
        };
        write!(
            f,
            "SpdkConf(enabled={}, hugepage={}MB, reactor_mask={}, iova_mode={}, targets={})",
            self.enabled,
            self.hugepage_mb,
            self.reactor_mask,
            iova_mode_label,
            self.targets.len()
        )
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
