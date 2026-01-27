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
use orpc::common::{Counter, CounterVec, Gauge, Metrics as m, Metrics};
use orpc::sys::SysUtils;
use orpc::CommonResult;
use std::fmt::{Debug, Formatter};

pub struct WorkerMetrics {
    store: BlockStore,

    pub(crate) write_bytes: Counter,
    pub(crate) write_time_us: Counter,
    pub(crate) write_count: Counter,
    pub(crate) write_blocks: CounterVec,
    pub(crate) pipeline_establish_total: Counter,
    pub(crate) pipeline_establish_failed: Counter,

    pub(crate) read_bytes: Counter,
    pub(crate) read_time_us: Counter,
    pub(crate) read_count: Counter,
    pub(crate) read_blocks: CounterVec,

    pub(crate) capacity: Gauge,
    pub(crate) available: Gauge,
    pub(crate) fs_used: Gauge,
    pub(crate) storage_failed: Gauge,
    pub(crate) num_blocks: Gauge,
    pub(crate) store_total_disks: Gauge,
    pub(crate) num_blocks_to_delete: Gauge,

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
            pipeline_establish_total: m::new_counter(
                "pipeline_establish_total",
                "Total pipeline establishment attempts",
            )?,
            pipeline_establish_failed: m::new_counter(
                "pipeline_establish_failed",
                "Failed pipeline establishment attempts",
            )?,

            read_bytes: m::new_counter("read_bytes", "worker read total bytes")?,
            read_time_us: m::new_counter("read_time_us", "Microseconds spent read")?,
            read_count: m::new_counter("read_count", "Number of reads")?,
            read_blocks: m::new_counter_vec("read_blocks", "read_blocks", &["type"])?,

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

            used_memory_bytes: m::new_gauge("used_memory_bytes", "Total memory used")?,
        };

        Ok(wm)
    }

    pub fn text_output(&self) -> CommonResult<String> {
        let state = self.store.read();

        self.capacity.set(state.capacity());
        self.available.set(state.available());
        self.fs_used.set(state.fs_used());
        self.num_blocks.set(state.num_blocks() as i64);
        self.num_blocks_to_delete
            .set(state.num_blocks_to_delete() as i64);

        let mut storage_failed = 0;
        for item in state.dir_iter() {
            if item.is_failed() {
                storage_failed += 1;
            }
        }
        self.storage_failed.set(storage_failed);
        self.used_memory_bytes.set(SysUtils::used_memory() as i64);

        let total_disks = state.dir_iter().count();
        self.store_total_disks.set(total_disks as i64);

        Metrics::text_output()
    }
}

impl Debug for WorkerMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkerMetrics")
    }
}
