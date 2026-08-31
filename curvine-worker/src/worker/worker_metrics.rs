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

use crate::worker::block::BlockStore;
use crate::worker::storage::Dataset;
use curvine_core_error::CommonResult;
use curvine_metrics::{Counter, CounterVec, Gauge, GaugeVec, HistogramVec, Metrics, Metrics as m};
use curvine_sys::SysUtils;
use std::fmt::{Debug, Formatter};

pub struct WorkerMetrics {
    store: BlockStore,

    pub(crate) write_bytes: Counter,
    pub(crate) write_time_us: Counter,
    pub(crate) write_count: Counter,
    pub(crate) write_blocks: CounterVec,

    pub(crate) read_bytes: Counter,
    pub(crate) read_time_us: Counter,
    pub(crate) read_count: Counter,
    pub(crate) read_blocks: CounterVec,

    pub(crate) block_store_stripe_lock_wait_us: HistogramVec,
    pub(crate) block_dataset_write_lock_wait_us: HistogramVec,
    pub(crate) block_dataset_write_lock_hold_us: HistogramVec,
    pub(crate) file_layout_operation_us: HistogramVec,

    pub(crate) capacity: Gauge,
    pub(crate) available: Gauge,
    pub(crate) fs_used: Gauge,
    pub(crate) storage_failed: Gauge,
    pub(crate) num_blocks: Gauge,
    pub(crate) store_total_disks: Gauge,
    pub(crate) num_blocks_to_delete: Gauge,

    /// Per-directory free-space ratio (0.1 = 10%). Pre-reserved, pre-guard.
    pub(crate) disk_free_ratio: GaugeVec,
    /// Writes rejected because storage admission reported insufficient capacity.
    pub(crate) disk_full_rejected_writes: Counter,

    pub(crate) used_memory_bytes: Gauge,
}

impl WorkerMetrics {
    pub fn new(store: BlockStore) -> CommonResult<Self> {
        let wm = Self {
            store,

            write_bytes: m::new_counter("write_bytes", "worker writes total bytes")?,
            write_time_us: m::new_counter("write_time_us", "Microseconds spent writing")?,
            write_count: m::new_counter("write_count", "Number of writes")?,
            write_blocks: m::new_counter_vec("write_blocks", "write_blocks", &["type"])?,

            read_bytes: m::new_counter("read_bytes", "worker read total bytes")?,
            read_time_us: m::new_counter("read_time_us", "Microseconds spent read")?,
            read_count: m::new_counter("read_count", "Number of reads")?,
            read_blocks: m::new_counter_vec("read_blocks", "read_blocks", &["type"])?,

            block_store_stripe_lock_wait_us: m::new_histogram_vec(
                "block_store_stripe_lock_wait_us",
                "Microseconds waiting for a block-store stripe lock",
                &["operation"],
            )?,
            block_dataset_write_lock_wait_us: m::new_histogram_vec(
                "block_dataset_write_lock_wait_us",
                "Microseconds waiting for the block-dataset write lock",
                &["operation"],
            )?,
            block_dataset_write_lock_hold_us: m::new_histogram_vec(
                "block_dataset_write_lock_hold_us",
                "Microseconds holding the block-dataset write lock",
                &["operation"],
            )?,
            file_layout_operation_us: m::new_histogram_vec(
                "file_layout_operation_us",
                "Microseconds spent in file-layout operations outside the dataset lock",
                &["operation"],
            )?,

            capacity: m::new_gauge("capacity", "Total storage capacity")?,
            available: m::new_gauge("available", "Total available space")?,
            fs_used: m::new_gauge("fs_used", "Space used by the file system")?,
            storage_failed: m::new_gauge("failed_disks", "Abnormal storage number")?,
            num_blocks: m::new_gauge("num_blocks", "The total number of blocks")?,
            store_total_disks: m::new_gauge("total_disks", "Total number of storage disks")?,
            num_blocks_to_delete: m::new_gauge(
                "num_blocks_to_delete",
                "Number of blocks pending deletion on the worker",
            )?,

            disk_free_ratio: m::new_gauge_vec(
                "disk_free_ratio",
                "Per-disk free-space ratio in basis points (10000 = 100%, 1000 = 10%); pre-reserved, pre-guard",
                &["dir_id", "dir_path", "storage_type"],
            )?,
            disk_full_rejected_writes: m::new_counter(
                "disk_full_rejected_writes",
                "Writes rejected because the storage layer reported insufficient capacity",
            )?,

            used_memory_bytes: m::new_gauge("used_memory_bytes", "Total memory used")?,
        };

        Ok(wm)
    }

    pub fn text_output(&self) -> CommonResult<String> {
        let state = self.store.read()?;

        self.capacity.set(state.capacity());
        self.available.set(state.available());
        self.fs_used.set(state.fs_used());
        self.num_blocks.set(state.num_blocks() as i64);
        self.num_blocks_to_delete
            .set(state.num_blocks_to_delete() as i64);

        self.storage_failed.set(state.failed_storage_count() as i64);
        self.used_memory_bytes.set(SysUtils::used_memory() as i64);

        self.store_total_disks.set(state.storage_count() as i64);

        for d in state.dir_free_ratios() {
            let dir_id = d.dir_id.to_string();
            // Gauge is i64-only; report the ratio in basis points (ratio * 10000).
            self.disk_free_ratio
                .with_label_values(&[&dir_id, &d.dir_path, d.storage_type.as_str_name()])
                .set((d.free_ratio * 10000.0) as i64);
        }

        #[cfg(feature = "spdk")]
        if let Some(env) = curvine_storage_spdk::SpdkEnv::global_including_shutdown() {
            env.publish_metrics();
        }

        Metrics::text_output()
    }
}

impl Debug for WorkerMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkerMetrics")
    }
}
