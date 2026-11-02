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

//! Background checksum scan of the blocks a worker loaded at startup (#1926).
//!
//! An unclean shutdown can leave a block whose data no longer matches its
//! sidecar, for example zeroed pages after a reboot without sync (#1813).
//! Reads already catch that block, but only when someone reads it. The scan
//! verifies every finalized block once after startup and queues each failure
//! for the same heartbeat re-check and removal a failed read uses.

use crate::worker::block::{BlockStore, ScanResult};
use curvine_core_error::CommonResult;
use log::{error, info, warn};
use std::time::{Duration, Instant};

/// Read size of one scan step, also the throttle and stop-check granularity.
const SCAN_READ_LEN: i32 = 1024 * 1024;

/// Counters of one scan pass.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ScanStats {
    pub verified: usize,
    pub mismatched: usize,
    pub no_sidecar: usize,
    pub unverifiable: usize,
    pub skipped: usize,
    pub failed: usize,
    pub bytes: u64,
}

/// Paces reads to `bytes_per_sec` from the start of the scan; 0 is unlimited.
struct Throttle {
    bytes_per_sec: u64,
    start: Instant,
    bytes: u64,
}

impl Throttle {
    fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            start: Instant::now(),
            bytes: 0,
        }
    }

    /// Account `len` bytes and return how long to wait before the next read.
    fn delay_after(&mut self, len: u64) -> Duration {
        self.bytes += len;
        if self.bytes_per_sec == 0 {
            return Duration::ZERO;
        }
        let due = Duration::from_secs_f64(self.bytes as f64 / self.bytes_per_sec as f64);
        due.saturating_sub(self.start.elapsed())
    }
}

pub struct ChecksumScan {
    store: BlockStore,
    bytes_per_sec: u64,
}

impl ChecksumScan {
    pub fn new(store: BlockStore, bytes_per_sec: u64) -> Self {
        Self {
            store,
            bytes_per_sec,
        }
    }

    /// Scan every block in the store once, stopping early when `stopped`
    /// returns true. A mismatch, or a missing sidecar while the policy
    /// requires one, is queued for the heartbeat re-check. Other errors such
    /// as an invalid sidecar are only logged: they do not prove the data is
    /// wrong.
    pub fn run(&self, stopped: impl Fn() -> bool) -> CommonResult<ScanStats> {
        let blocks = self.store.all_blocks()?;
        let mut throttle = Throttle::new(self.bytes_per_sec);
        let mut stats = ScanStats::default();
        info!(
            "start block checksum scan, blocks {}, rate {} bytes/s",
            blocks.len(),
            self.bytes_per_sec
        );

        for meta in blocks {
            if stopped() {
                stats.bytes = throttle.bytes;
                info!("block checksum scan stopped: {:?}", stats);
                return Ok(stats);
            }
            let result = self.store.scan_block(meta.id, SCAN_READ_LEN, |len| {
                let delay = throttle.delay_after(len as u64);
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                !stopped()
            });
            match result {
                Ok(ScanResult::Verified) => stats.verified += 1,
                Ok(ScanResult::Mismatch) => {
                    stats.mismatched += 1;
                    self.store.report_corrupt(meta.id, "startup checksum scan");
                }
                Ok(ScanResult::NoSidecar) => stats.no_sidecar += 1,
                Ok(ScanResult::Unverifiable) => {
                    stats.unverifiable += 1;
                    self.store.report_unverifiable(meta.id);
                }
                Ok(ScanResult::Skipped) => stats.skipped += 1,
                Err(e) => {
                    stats.failed += 1;
                    warn!("checksum scan of block {} failed: {}", meta.id, e);
                }
            }
        }
        stats.bytes = throttle.bytes;

        if stats.mismatched > 0 || stats.unverifiable > 0 || stats.failed > 0 {
            error!("block checksum scan finished: {:?}", stats);
        } else {
            info!("block checksum scan finished: {:?}", stats);
        }
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttle_paces_reads_to_the_rate() {
        let mut unlimited = Throttle::new(0);
        assert_eq!(unlimited.delay_after(u64::MAX / 2), Duration::ZERO);

        let mut throttle = Throttle::new(1024 * 1024);
        let delay = throttle.delay_after(2 * 1024 * 1024);
        assert!(delay > Duration::from_millis(1900), "{delay:?}");
        assert!(delay <= Duration::from_secs(2), "{delay:?}");
    }
}
