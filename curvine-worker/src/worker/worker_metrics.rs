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
use std::fmt::{Debug, Formatter};

pub struct WorkerMetrics {
    store: BlockStore,

    pub(crate) write_bytes: CounterVec,
    pub(crate) write_time_us: CounterVec,
    pub(crate) write_count: CounterVec,
    pub(crate) write_blocks: CounterVec,
    pub(crate) slow_write_count: CounterVec,
    pub(crate) write_error_count: CounterVec,

    pub(crate) read_bytes: Counter,
    pub(crate) read_time_us: Counter,
    pub(crate) read_count: Counter,
    pub(crate) read_blocks: CounterVec,
    pub(crate) read_error_count: CounterVec,

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

    /// Health state per directory: 0=healthy, 1=suspect, 2=unusable.
    /// Label by dir so "which disks are degraded" is one query.
    pub(crate) dir_health_state: GaugeVec,
    /// Faults observed per directory, labelled by fault kind so unhealthy
    /// disks can be filtered and stacked by error type.
    pub(crate) dir_fault_total: CounterVec,
    /// State machine transitions, labelled by fault kind: flapping and
    /// per-error-type churn are visible without parsing logs.
    pub(crate) health_transition_total: CounterVec,
    /// Probe results per directory (ok/error), i.e. how often self-check runs.
    pub(crate) health_check_total: CounterVec,
    /// Probe latency; a rising tail means a hung or slow mount.
    pub(crate) health_check_duration_seconds: HistogramVec,
    /// Worker-declared fact: may this node take new writes? 0/1.
    pub(crate) accepting_allocations: Gauge,
    /// Directories currently unusable, for alerting without labels.
    pub(crate) unhealthy_dirs: Gauge,
    /// Admission rejections labelled by reason, so "broken disk" and "full
    /// disk" can be told apart in dashboards and alerts.
    pub(crate) admission_reject_total: CounterVec,
}

impl WorkerMetrics {
    pub fn new(store: BlockStore) -> CommonResult<Self> {
        let wm = Self {
            store,

            write_bytes: m::new_counter_vec(
                "write_bytes",
                "worker writes total bytes",
                &["dir_id"],
            )?,
            write_time_us: m::new_counter_vec(
                "write_time_us",
                "Microseconds spent writing",
                &["dir_id"],
            )?,
            write_count: m::new_counter_vec(
                "write_count",
                "Number of writes",
                &["dir_id"],
            )?,
            write_blocks: m::new_counter_vec("write_blocks", "write_blocks", &["dir_id", "type"])?,
            slow_write_count: m::new_counter_vec(
                "slow_write_count",
                "Writes slower than the io_slow_threshold",
                &["dir_id"],
            )?,
            write_error_count: m::new_counter_vec(
                "write_error_count",
                "Writes that failed with an IO error",
                &["dir_id"],
            )?,

            read_bytes: m::new_counter("read_bytes", "worker read total bytes")?,
            read_time_us: m::new_counter("read_time_us", "Microseconds spent read")?,
            read_count: m::new_counter("read_count", "Number of reads")?,
            read_blocks: m::new_counter_vec("read_blocks", "read_blocks", &["type"])?,
            read_error_count: m::new_counter_vec(
                "read_error_count",
                "Reads that failed with an IO error",
                &["dir_id"],
            )?,

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
            dir_health_state: m::new_gauge_vec(
                "dir_health_state",
                "Per-disk health state: 0=healthy, 1=suspect, 2=unusable",
                &["dir_id", "dir_path", "storage_type"],
            )?,
            dir_fault_total: m::new_counter_vec(
                "dir_fault_total",
                "Probe faults per disk, labelled by fault kind",
                &["dir_id", "dir_path", "kind"],
            )?,
            health_transition_total: m::new_counter_vec(
                "health_transition_total",
                "Health state transitions, labelled by from/to/kind",
                &["from", "to", "kind"],
            )?,
            health_check_total: m::new_counter_vec(
                "health_check_total",
                "Health probe results per disk",
                &["dir_id", "outcome"],
            )?,
            health_check_duration_seconds: m::new_histogram_vec_with_buckets(
                "health_check_duration_seconds",
                "Health probe latency in seconds",
                &["probe"],
                &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
            )?,
            accepting_allocations: m::new_gauge(
                "worker_accepting_allocations",
                "1 when the worker accepts new writes, 0 when every disk is unusable",
            )?,
            unhealthy_dirs: m::new_gauge("worker_unhealthy_dirs", "Data dirs currently unusable")?,
            admission_reject_total: m::new_counter_vec(
                "admission_reject_total",
                "Write admissions rejected, labelled by reason",
                &["reason"],
            )?,
        };

        Ok(wm)
    }

    /// Record one storage probe round.
    pub fn record_probe(&self, report: &curvine_storage_local::ProbeReport) {
        let dir_id = report.dir_id.to_string();
        self.health_check_total
            .with_label_values(&[&dir_id, if report.ok { "ok" } else { "error" }])
            .inc();
        self.health_check_duration_seconds
            .with_label_values(&["storage"])
            .observe(report.elapsed.as_secs_f64());
        self.dir_health_state
            .with_label_values(&[&dir_id, &report.dir_path, report.storage_type.as_str_name()])
            .set(health_state_code(report.state));

        let Some(transition) = report.transition.as_ref() else {
            return;
        };
        let kind = transition.fault.as_str_name();
        self.health_transition_total
            .with_label_values(&[
                transition.from.as_str_name(),
                transition.to.as_str_name(),
                kind,
            ])
            .inc();
        if !transition.is_recovery() {
            self.dir_fault_total
                .with_label_values(&[&dir_id, &report.dir_path, kind])
                .inc();
        }
    }

    /// Record a rejected write admission. `reason` is a stable machine token
    /// (`storage_unavailable`, `disk_out_of_space`), not a message.
    pub fn record_admission_reject(&self, reason: &str) {
        self.disk_full_rejected_writes.inc();
        self.admission_reject_total
            .with_label_values(&[reason])
            .inc();
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

        self.store_total_disks.set(state.storage_count() as i64);

        for dir in state.dir_health() {
            self.dir_health_state
                .with_label_values(&[
                    &dir.dir_id.to_string(),
                    &dir.dir_path,
                    dir.storage_type.as_str_name(),
                ])
                .set(health_state_code(dir.state));
        }

        self.unhealthy_dirs.set(state.failed_storage_count() as i64);
        self.accepting_allocations.set(i64::from(
            state.storage_count() == 0 || state.failed_storage_count() < state.storage_count(),
        ));

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

/// Metric encoding of a health state. Kept explicit (not `as i64`) so the
/// dashboard contract cannot drift with enum ordering.
pub(crate) fn health_state_code(state: curvine_model::HealthState) -> i64 {
    match state {
        curvine_model::HealthState::Healthy => 0,
        curvine_model::HealthState::Suspect => 1,
        curvine_model::HealthState::Unusable => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::health_state_code;
    use curvine_model::HealthState;

    #[test]
    fn health_state_metric_codes_are_stable() {
        assert_eq!(health_state_code(HealthState::Healthy), 0);
        assert_eq!(health_state_code(HealthState::Suspect), 1);
        assert_eq!(health_state_code(HealthState::Unusable), 2);
    }

    #[test]
    fn fault_kind_label_names_are_stable() {
        // Dashboards and saved queries key off these strings.
        use curvine_model::FaultKind;
        assert_eq!(FaultKind::IoError.as_str_name(), "io_error");
        assert_eq!(FaultKind::PathMissing.as_str_name(), "path_missing");
        assert_eq!(FaultKind::ReadOnly.as_str_name(), "read_only");
        assert_eq!(FaultKind::SpaceOut.as_str_name(), "space_out");
    }
}
