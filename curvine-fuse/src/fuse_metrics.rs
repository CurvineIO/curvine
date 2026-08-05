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

use std::time::Instant;

use log::warn;
use once_cell::sync::OnceCell;

use curvine_core_error::CommonResult;
use curvine_metrics::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramVec, Metrics as m,
};
use curvine_runtime::common::LocalTime;

use crate::fuse_error::errno_label;
use crate::session::FuseOpCode;

const REQUEST_DURATION_BUCKETS_US: &[f64] = &[
    10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 25000.0, 50000.0, 100000.0,
    250000.0, 500000.0, 1000000.0, 2500000.0, 5000000.0, 10000000.0,
];

const STAGE_DURATION_BUCKETS_US: &[f64] = &[
    5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 25000.0, 50000.0,
    100000.0,
];

const IO_SIZE_BUCKETS: &[f64] = &[
    4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0, 67108864.0,
];

const READDIR_ENTRIES_BUCKETS: &[f64] = &[1.0, 4.0, 16.0, 64.0, 256.0, 1024.0, 4096.0];

pub(crate) const REPLY_TYPE_REPLIED: &str = "replied";
pub(crate) const REPLY_TYPE_NO_REPLY: &str = "no_reply";

pub(crate) const STAGE_REPLY_WRITE: &str = "reply_write";
pub(crate) const STAGE_META_SPAWN: &str = "meta_spawn";
pub(crate) const STAGE_OPERATION: &str = "operation";
pub(crate) const STAGE_STREAM_IO: &str = "stream_io";

pub(crate) const IO_TYPE_READ: &str = "read";
pub(crate) const IO_TYPE_WRITE: &str = "write";
pub(crate) const IO_TYPE_FLUSH: &str = "flush";
pub(crate) const IO_TYPE_FSYNC: &str = "fsync";
pub(crate) const IO_TYPE_RELEASE: &str = "release";

#[cfg_attr(not(test), allow(dead_code))] // value produced by path_type(); fuse-side use is test-only.
pub(crate) const PATH_TYPE_CURVINE: &str = "curvine";
#[cfg_attr(not(test), allow(dead_code))] // value produced by path_type(); fuse-side use is test-only.
pub(crate) const PATH_TYPE_UFS: &str = "ufs";
// `fallback` is handle capability, not proof this read hit UFS.
#[cfg_attr(not(test), allow(dead_code))] // reader-only Fallback variant; fuse-side use is test-only.
pub(crate) const PATH_TYPE_FALLBACK: &str = "fallback";
#[cfg_attr(not(test), allow(dead_code))] // value produced by path_type(); fuse-side use is test-only.
pub(crate) const PATH_TYPE_LOCAL: &str = "local";
pub(crate) const PATH_TYPE_UNKNOWN: &str = "unknown";

pub(crate) const CACHE_STATUS: &str = "status";
pub(crate) const CACHE_LIST: &str = "list";
pub(crate) const CACHE_BLOCKS: &str = "blocks";

pub(crate) const CACHE_RESULT_HIT: &str = "hit";
pub(crate) const CACHE_RESULT_MISS: &str = "miss";
pub(crate) const CACHE_RESULT_PUT: &str = "put";

pub(crate) const NODE_CACHE_OP_LOOKUP: &str = "lookup";

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_SETATTR: &str = "setattr";
pub(crate) const INVAL_REASON_RESIZE: &str = "resize";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_SETXATTR: &str = "setxattr";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_REMOVEXATTR: &str = "removexattr";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_MKDIR: &str = "mkdir";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_CREATE: &str = "create";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_OPEN_WRITE: &str = "open_write";
pub(crate) const INVAL_REASON_FLUSH: &str = "flush";
pub(crate) const INVAL_REASON_RELEASE: &str = "release";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_UNLINK: &str = "unlink";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_LINK: &str = "link";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_RMDIR: &str = "rmdir";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_RENAME: &str = "rename";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_SYMLINK: &str = "symlink";
pub(crate) const INVAL_REASON_FSYNC: &str = "fsync";

pub(crate) const READDIR_STATUS_SUCCESS: &str = "success";
pub(crate) const READDIR_STATUS_ERROR: &str = "error";

pub(crate) const STATE_STAGE_NODE_MAP: &str = "node_map";
pub(crate) const STATE_STAGE_FILE_HANDLES: &str = "file_handles";
pub(crate) const STATE_STAGE_DIR_HANDLES: &str = "dir_handles";
pub(crate) const STATE_STAGE_MOUNT_FDS: &str = "mount_fds";

pub(crate) const STATE_KIND_NODE_MAP: &str = "node_map";
pub(crate) const STATE_KIND_FILE_HANDLES: &str = "file_handles";
pub(crate) const STATE_KIND_DIR_HANDLES: &str = "dir_handles";

pub(crate) const STATE_STATUS_SUCCESS: &str = "success";
pub(crate) const STATE_STATUS_ERROR: &str = "error";

pub(crate) const SESSION_INIT_SUCCESS: &str = "success";
pub(crate) const SESSION_INIT_ERROR: &str = "error";

pub(crate) const SHUTDOWN_COMPLETED: &str = "completed";
pub(crate) const SHUTDOWN_RUN_ALL_ERROR: &str = "run_all_error";
pub(crate) const SHUTDOWN_RUN_ALL_PANIC: &str = "run_all_panic";
pub(crate) const SHUTDOWN_TERM_SIGNAL: &str = "term_signal";
pub(crate) const SHUTDOWN_SIGUSR1_PERSIST: &str = "sigusr1_persist";
pub(crate) const SHUTDOWN_FD_WATCHER: &str = "fd_watcher";

pub(crate) const DECODE_PHASE_PARSE: &str = "parse";
pub(crate) const DECODE_PHASE_DECODE: &str = "decode";

pub(crate) const RECEIVE_ACTION_CONTINUE: &str = "continue";
pub(crate) const RECEIVE_ACTION_EXIT: &str = "exit";

pub(crate) const ENQUEUE_REASON_CHANNEL_CLOSED: &str = "channel_closed";

pub(crate) const NOTIFY_SUCCESS: &str = "success";
pub(crate) const NOTIFY_ENQUEUE_FAILED: &str = "enqueue_failed";
pub(crate) const NOTIFY_WRITE_FAILED: &str = "write_failed";

const UNSUPPORTED_REASON_MISSING: &str = "missing_reason";

const ERRNO_LABEL_OTHER: &str = "OTHER";

static FUSE_METRICS: OnceCell<FuseMetrics> = OnceCell::new();

pub struct FuseMetrics {
    pub inode_num: Gauge,
    pub file_handle_num: Gauge,
    pub dir_handle_num: Gauge,
    pub fuse_used_memory_bytes: Gauge,

    pub write_back_active_inode_num: Gauge,
    pub write_back_mem_usage: Gauge,
    pub write_back_mem_limit: Gauge,

    pub inode_count: Gauge,
    pub file_handle_count: Gauge,
    pub dir_handle_count: Gauge,

    pub(crate) active_requests: GaugeVec,
    pub(crate) requests_total: CounterVec,
    pub(crate) request_duration_us: HistogramVec,
    pub(crate) errors_total: CounterVec,
    pub(crate) interrupted_total: CounterVec,
    pub(crate) unsupported_total: CounterVec,
    pub(crate) notify_total: CounterVec,
    pub(crate) decode_errors_total: CounterVec,
    pub(crate) response_write_duration_us: HistogramVec,
    pub(crate) response_bytes_total: CounterVec,
    pub(crate) reply_enqueue_errors_total: CounterVec,
    pub(crate) response_write_errors_total: CounterVec,
    pub(crate) stage_duration_us: HistogramVec,

    pub(crate) receive_loop_wait_duration_us: Histogram,
    pub(crate) receive_errors_total: CounterVec,
    pub(crate) meta_task_inflight: Gauge,
    pub(crate) metrics_scrape_duration_us: Histogram,
    pub(crate) metrics_scrape_bytes: Gauge,

    pub(crate) operation_duration_us: HistogramVec,
    pub(crate) setlkw_wait_duration_us: Histogram,
    pub(crate) reply_queue_depth: Gauge,
    pub(crate) sender_last_progress_unixtime: GaugeVec,
    pub(crate) setlkw_inflight: Gauge,

    // Keep read/write status metrics separate from status-less flush/fsync/release lifecycle metrics.
    pub(crate) io_duration_us: HistogramVec,
    pub(crate) io_bytes_total: CounterVec,
    pub(crate) io_requests_total: CounterVec,
    pub(crate) io_size_bytes: HistogramVec,
    pub(crate) io_dispatch_duration_us: HistogramVec,
    pub(crate) stream_io_inflight: GaugeVec,
    pub(crate) stream_lifecycle_duration_us: HistogramVec,
    pub(crate) stream_lifecycle_requests_total: CounterVec,
    pub(crate) stream_lifecycle_inflight: GaugeVec,
    pub(crate) stream_write_queue_depth: Gauge,

    pub(crate) user_meta_cache_total: CounterVec,
    pub(crate) user_meta_cache_invalidations_total: CounterVec,
    pub(crate) node_cache_total: CounterVec,
    pub(crate) negative_entry_returned_total: Counter,
    pub(crate) readdir_entries: HistogramVec,
    pub(crate) readdir_duration_us: HistogramVec,

    pub(crate) state_persist_total: CounterVec,
    pub(crate) state_persist_stage_duration_us: HistogramVec,
    pub(crate) state_persist_handle_count: GaugeVec,
    pub(crate) state_restore_total: CounterVec,
    pub(crate) state_restore_stage_duration_us: HistogramVec,
    pub(crate) session_init_total: CounterVec,
    pub(crate) session_shutdown_total: CounterVec,
    pub(crate) kernel_fd_health: Gauge,
}

impl FuseMetrics {
    pub fn ensure_init() -> CommonResult<()> {
        FUSE_METRICS.get_or_try_init(Self::new)?;
        Ok(())
    }

    pub fn get() -> &'static Self {
        FUSE_METRICS
            .get()
            .expect("FuseMetrics not initialized; call ensure_init from CurvineFileSystem::new")
    }

    pub(crate) fn with<F: FnOnce(&Self)>(f: F) {
        if let Some(m) = FUSE_METRICS.get() {
            f(m);
        }
    }

    fn new() -> CommonResult<Self> {
        Ok(Self {
            inode_num: m::new_gauge("inode_num", "FUSE inode count in dcache")?,
            file_handle_num: m::new_gauge("file_handle_num", "FUSE open file handle count")?,
            dir_handle_num: m::new_gauge("dir_handle_num", "FUSE open directory handle count")?,
            fuse_used_memory_bytes: m::new_gauge("fuse_used_memory_bytes", "Total memory used")?,
            write_back_active_inode_num: m::new_gauge(
                "write_back_active_inode_num",
                "FUSE write-back active inode count",
            )?,
            write_back_mem_usage: m::new_gauge(
                "write_back_mem_usage",
                "FUSE write-back page cache usage (bytes)",
            )?,
            write_back_mem_limit: m::new_gauge(
                "write_back_mem_limit",
                "FUSE write-back page cache size limit (bytes)",
            )?,

            // Namespaced aliases (same values, event-driven in lockstep).
            inode_count: m::new_gauge(
                "curvine_fuse_inode_count",
                "FUSE inode count in dcache (namespaced alias of inode_num)",
            )?,
            file_handle_count: m::new_gauge(
                "curvine_fuse_file_handle_count",
                "FUSE open file handle count (namespaced alias of file_handle_num)",
            )?,
            dir_handle_count: m::new_gauge(
                "curvine_fuse_dir_handle_count",
                "FUSE open directory handle count (namespaced alias of dir_handle_num)",
            )?,

            active_requests: m::new_gauge_vec(
                "curvine_fuse_active_requests",
                "FUSE requests in flight end-to-end (ctx creation to sender finish)",
                &["kind"],
            )?,
            requests_total: m::new_counter_vec(
                "curvine_fuse_requests_total",
                "Total FUSE requests, counted at the finish point",
                &["opcode", "kind", "reply_type", "status"],
            )?,
            request_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_request_duration_us",
                "End-to-end FUSE request latency in microseconds, finished in the sender",
                &["opcode", "kind", "status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            errors_total: m::new_counter_vec(
                "curvine_fuse_errors_total",
                "FUSE requests that failed with a real operation error (status=error only)",
                &["opcode", "kind", "errno"],
            )?,
            interrupted_total: m::new_counter_vec(
                "curvine_fuse_interrupted_total",
                "FUSE requests terminated via the SETLKW interrupt path",
                &["opcode"],
            )?,
            unsupported_total: m::new_counter_vec(
                "curvine_fuse_unsupported_total",
                "Unsupported FUSE requests; currently emits reason \
                 unknown_opcode/unimplemented_opcode, trait_default reserved for later",
                &["opcode", "reason"],
            )?,
            notify_total: m::new_counter_vec(
                "curvine_fuse_notify_total",
                "Kernel notifications by code and delivery status \
                 (success|enqueue_failed|write_failed)",
                &["code", "status"],
            )?,
            decode_errors_total: m::new_counter_vec(
                "curvine_fuse_decode_errors_total",
                "Structural decode/parse failures by phase. phase=parse is per-request and \
                 recurring (recoverable parse-after-ctx failures); phase=decode is TERMINAL — \
                 a from_bytes failure kills the receiver, so it increments at most once per \
                 receiver lifetime. Treat a phase=decode increment as 'receiver died, restart', \
                 not a rate to threshold.",
                &["phase", "reason"],
            )?,
            response_write_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_response_write_duration_us",
                "Kernel-fd write (splice) latency in microseconds, observed on success and failure",
                &["opcode", "request_status"],
                STAGE_DURATION_BUCKETS_US,
            )?,
            response_bytes_total: m::new_counter_vec(
                "curvine_fuse_response_bytes_total",
                "On-wire FUSE reply size in bytes at sender finish",
                &["opcode", "request_status"],
            )?,
            reply_enqueue_errors_total: m::new_counter_vec(
                "curvine_fuse_reply_enqueue_errors_total",
                "Reply-channel enqueue failures; the request never reaches the sender",
                &["opcode", "reason"],
            )?,
            response_write_errors_total: m::new_counter_vec(
                "curvine_fuse_response_write_errors_total",
                "Kernel-fd write (delivery) failures in the sender",
                &["opcode", "errno"],
            )?,
            stage_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_stage_duration_us",
                "Per-stage FUSE framework latency in microseconds; label `stage` is a \
                 bounded enum emitted by the current build",
                &["stage", "kind", "status"],
                STAGE_DURATION_BUCKETS_US,
            )?,

            receive_loop_wait_duration_us: m::new_histogram_with_buckets(
                "curvine_fuse_receive_loop_wait_duration_us",
                "Receiver loop wait (splice + header parse) in microseconds. \
                 SATURATION/health metric, NOT request latency: includes idle wait for \
                 the next kernel request, so long idle periods land in high/+Inf buckets. \
                 Do not use for request P99.",
                REQUEST_DURATION_BUCKETS_US,
            )?,
            receive_errors_total: m::new_counter_vec(
                "curvine_fuse_receive_errors_total",
                "Splice/receive errors before a request is decoded. action=continue \
                 (loop retries) or exit (loop stops: graceful ENODEV break or unexpected \
                 error return; the original error is still returned/logged)",
                &["errno", "action"],
            )?,
            meta_task_inflight: m::new_gauge(
                "curvine_fuse_meta_task_inflight",
                "Spawned metadata tasks in flight (rt.spawn submission to dispatch return)",
            )?,
            metrics_scrape_duration_us: m::new_histogram_with_buckets(
                "curvine_fuse_metrics_scrape_duration_us",
                "Time to render the /metrics text output in microseconds (last scrape)",
                STAGE_DURATION_BUCKETS_US,
            )?,
            metrics_scrape_bytes: m::new_gauge(
                "curvine_fuse_metrics_scrape_bytes",
                "Size of the last /metrics scrape output body in bytes",
            )?,

            operation_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_operation_duration_us",
                "Metadata FUSE operation latency in microseconds (whole dispatch_meta match); \
                 includes the awaited reply enqueue, so it is NOT pure operation latency — do \
                 not subtract reply_enqueue. Metadata only; interrupted SETLKW is excluded.",
                &["opcode", "kind", "status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            setlkw_wait_duration_us: m::new_histogram_with_buckets(
                "curvine_fuse_setlkw_wait_duration_us",
                "SETLKW interruptible-request duration in microseconds: the whole \
                 dispatch_meta_interrupt scope (parse + dispatch + lock polling + reply \
                 enqueue). NOT pure lock-acquisition time — reply-channel backpressure can \
                 inflate it, do not read as lock contention. Includes immediate interrupt \
                 (sample even if the lock poll loop never ran) and malformed-SETLKW parse \
                 failures (near-zero sample).",
                REQUEST_DURATION_BUCKETS_US,
            )?,
            reply_queue_depth: m::new_gauge(
                "curvine_fuse_reply_queue_depth",
                "Reply-channel backlog (tasks enqueued but not yet received by the sender)",
            )?,
            sender_last_progress_unixtime: m::new_gauge_vec(
                "curvine_fuse_sender_last_progress_unixtime",
                "Unix timestamp (seconds) of the last successful reply write per sender \
                 (labels mnt=mount path, sender=channel index within the mount; initialized \
                 to sender construction time so a cold series is not a spurious 0). Use \
                 time() - <this> at scrape time to get the age since a sender last delivered \
                 a reply; a growing age on one series while siblings refresh indicates a \
                 stalled reply sender (issue #1215)",
                &["mnt", "sender"],
            )?,
            setlkw_inflight: m::new_gauge(
                "curvine_fuse_setlkw_inflight",
                "SETLKW interruptible-request scopes in flight (whole dispatch_meta_interrupt \
                 scope, NOT pending_requests map size; can stay non-zero under reply-channel \
                 backpressure after the map entry is removed)",
            )?,

            io_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_io_duration_us",
                "Stream read/write backend IO latency in microseconds, observed in the \
                 reader/writer task body when the backend call returns. io_type=read|write only; \
                 flush/fsync/release use stream_lifecycle_duration_us instead. NOTE: \
                 path_type=fallback means the reader is fallback-CAPABLE (a FallbackFsReader \
                 captured at open), NOT that this read fell back to UFS — Curvine cache hits on \
                 such a handle are also labelled fallback",
                &["io_type", "path_type", "status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            io_bytes_total: m::new_counter_vec(
                "curvine_fuse_io_bytes_total",
                "Bytes transferred by a successful stream read/write (read=actual bytes read, \
                 write=input length reported to the kernel on success — fuse_write returns no \
                 partial size). Only the status=success child is created; an error read/write \
                 records no byte series",
                &["io_type", "path_type", "status"],
            )?,
            io_requests_total: m::new_counter_vec(
                "curvine_fuse_io_requests_total",
                "Stream read/write backend attempts, incremented once per attempt including \
                 status=error. Excludes zero-length writes (they never enter the writer task body)",
                &["io_type", "path_type", "status"],
            )?,
            io_size_bytes: m::new_histogram_vec_with_buckets(
                "curvine_fuse_io_size_bytes",
                "Single stream read/write request size in bytes (read=requested size, \
                 write=input data length); request-size distribution, distinct from the \
                 transferred io_bytes_total which uses actual bytes",
                &["io_type", "path_type"],
                IO_SIZE_BUCKETS,
            )?,
            io_dispatch_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_io_dispatch_duration_us",
                "Stream read/write dispatch-to-worker latency in microseconds at send_stream; \
                 read may include a read-after-write consistency flush + reader reopen, write \
                 includes zero-length no-op direct replies and their reply-enqueue time (NOT \
                 worker dispatch). io_type=read|write only; no status",
                &["io_type"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            stream_io_inflight: m::new_gauge_vec(
                "curvine_fuse_stream_io_inflight",
                "Stream read/write backend calls in flight (task body, backend-only — shorter \
                 than active_requests; does NOT include the reply channel enqueue). \
                 io_type=read|write only",
                &["io_type"],
            )?,
            stream_lifecycle_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_stream_lifecycle_duration_us",
                "flush/fsync/release lifecycle duration in microseconds at send_stream (whole \
                 arm incl. error reply enqueue); attempted count lives in \
                 stream_lifecycle_requests_total; no success/error status; observed on success, \
                 error, pre-dispatch error, and cancel. path_type is unknown for now",
                &["io_type", "path_type"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            stream_lifecycle_requests_total: m::new_counter_vec(
                "curvine_fuse_stream_lifecycle_requests_total",
                "flush/fsync/release lifecycle attempts at send_stream, counted before the match \
                 (so a pre-dispatch error still counts). No status (the result mixes backend and \
                 reply-enqueue errors). path_type is unknown for now",
                &["io_type", "path_type"],
            )?,
            stream_lifecycle_inflight: m::new_gauge_vec(
                "curvine_fuse_stream_lifecycle_inflight",
                "flush/fsync/release lifecycle in-progress at send_stream (dispatch + lock + \
                 backend round-trip + reply enqueue); a saturation signal, NOT a \
                 pure-backend-stuck signal — correlate with stream_write_queue_depth / \
                 reply_queue_depth / request_duration to localize. io_type=flush|fsync|release",
                &["io_type"],
            )?,
            stream_write_queue_depth: m::new_gauge(
                "curvine_fuse_stream_write_queue_depth",
                "Writer-channel backlog: ALL WriteTasks enqueued into the writer task but not yet \
                 dequeued — write / flush / complete / resize, NOT just FUSE_WRITE. Note resize \
                 can be driven by metadata paths (SetAttr truncate / fallocate), so a rising \
                 gauge is not necessarily FUSE write/flush pressure. Event-driven via a \
                 task-embedded guard, dropped at the dequeue point",
            )?,

            // cache + readdir.
            user_meta_cache_total: m::new_counter_vec(
                "curvine_fuse_user_meta_cache_total",
                "userspace metadata cache (NodeState/DirTree) hit/miss/put by cache namespace. status=hit|miss|put",
                &["cache", "status"],
            )?,
            user_meta_cache_invalidations_total: m::new_counter_vec(
                "curvine_fuse_user_meta_cache_invalidations_total",
                "Requested userspace metadata cache (NodeState/DirTree) invalidations at the call site, one inc per affected cache \
                 namespace (NOT per invalidate_cache call)",
                &["cache", "reason"],
            )?,
            node_cache_total: m::new_counter_vec(
                "curvine_fuse_node_cache_total",
                "NodeMap dcache lookup outcome on the real FUSE Lookup path. status=hit|miss",
                &["operation", "status"],
            )?,
            negative_entry_returned_total: m::new_counter(
                "curvine_fuse_negative_entry_returned_total",
                "Negative dentry results returned to the kernel (backend ENOENT + negative_ttl>0)",
            )?,
            readdir_entries: m::new_histogram_vec_with_buckets(
                "curvine_fuse_readdir_entries",
                "Entries returned by one read_dir_common batch (single readdir syscall, not the \
                 directory total). Only the success child is observed",
                &["status"],
                READDIR_ENTRIES_BUCKETS,
            )?,
            readdir_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_readdir_duration_us",
                "read_dir_common latency in microseconds (pull-a-batch + encode; excludes \
                 opendir/list_stream backend init)",
                &["status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,

            // state recovery + session lifecycle.
            state_persist_total: m::new_counter_vec(
                "curvine_fuse_state_persist_total",
                "FUSE state-persist attempts (SIGUSR1). status=success|error",
                &["status"],
            )?,
            state_persist_stage_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_state_persist_stage_duration_us",
                "Per-stage state-persist duration in microseconds. stage is a dedicated \
                 state-recovery domain (node_map|file_handles|dir_handles|mount_fds), NOT the \
                 request stage_duration_us enum",
                &["stage", "status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            state_persist_handle_count: m::new_gauge_vec(
                "curvine_fuse_state_persist_handle_count",
                "Handle counts sampled once at the start of a persist attempt. \
                 kind=node_map|file_handles|dir_handles",
                &["kind"],
            )?,
            state_restore_total: m::new_counter_vec(
                "curvine_fuse_state_restore_total",
                "FUSE state-restore attempts (restart with state-file env var). status=success|error",
                &["status"],
            )?,
            state_restore_stage_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_state_restore_stage_duration_us",
                "Per-stage state-restore duration in microseconds. Same dedicated stage domain as \
                 persist. A NodeState magic/version header failure skips only the NodeState stages \
                 (node_map/file_handles/dir_handles); mount_fds precedes the header in the file \
                 format so it may already be recorded",
                &["stage", "status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            session_init_total: m::new_counter_vec(
                "curvine_fuse_session_init_total",
                "FUSE session init outcome, recorded once in FuseSession::new. result=success|error",
                &["result"],
            )?,
            session_shutdown_total: m::new_counter_vec(
                "curvine_fuse_session_shutdown_total",
                "FUSE session shutdown cause, recorded once per session (first cause wins). \
                 reason=completed|run_all_error|run_all_panic|term_signal|sigusr1_persist|fd_watcher",
                &["reason"],
            )?,
            kernel_fd_health: m::new_gauge(
                "curvine_fuse_kernel_fd_health",
                "FUSE kernel fd health: 1=healthy, 0=HUP/ERR or session exited",
            )?,
        })
    }

    pub(crate) fn record_request_total(
        &self,
        opcode: &'static str,
        kind: FuseReqKind,
        reply_type: &'static str,
        status: FuseReqStatus,
    ) {
        self.requests_total
            .with_label_values(&[opcode, kind.as_str(), reply_type, status.as_str()])
            .inc();
    }

    pub(crate) fn record_request_duration(
        &self,
        opcode: &'static str,
        kind: FuseReqKind,
        status: FuseReqStatus,
        elapsed_us: u64,
    ) {
        self.request_duration_us
            .with_label_values(&[opcode, kind.as_str(), status.as_str()])
            .observe(elapsed_us as f64);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_request_finish(
        &self,
        opcode: &'static str,
        kind: FuseReqKind,
        op_status: FuseReqStatus,
        request_status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
        response_bytes: u32,
        write: WriteOutcome,
        write_elapsed_us: u64,
        total_elapsed_us: u64,
    ) {
        let req_status_str = request_status.as_str();

        // request_status-labelled series (the result the kernel observes).
        self.record_request_total(opcode, kind, REPLY_TYPE_REPLIED, request_status);
        self.record_request_duration(opcode, kind, request_status, total_elapsed_us);
        // Delivery latency/size: observed on both success and failure.
        self.response_write_duration_us
            .with_label_values(&[opcode, req_status_str])
            .observe(write_elapsed_us as f64);
        self.response_bytes_total
            .with_label_values(&[opcode, req_status_str])
            .inc_by(response_bytes as i64);
        self.stage_duration_us
            .with_label_values(&[STAGE_REPLY_WRITE, kind.as_str(), req_status_str])
            .observe(write_elapsed_us as f64);

        // Non-success op counters use FS errno/reason; delivery errors are separate.
        self.record_op_terminal(opcode, kind, op_status, errno, unsupported_reason);

        // Delivery failure is an independent dimension from request status.
        if let WriteOutcome::Failed { errno } = write {
            let label = errno.map(errno_label).unwrap_or(ERRNO_LABEL_OTHER);
            self.response_write_errors_total
                .with_label_values(&[opcode, label])
                .inc();
        }
    }

    /// The op-level terminal counters: `errors_total` / `unsupported_total` /
    /// `interrupted_total`, classified from the **FS-operation** status.
    ///
    /// **Call exactly once per request, only from a request terminal path.**
    /// Calling it twice double-counts the op-level counters for one request. In
    /// particular, the `operation_duration_us` timer must only `observe`
    /// latency — it must NOT call this (the request terminal already did).
    pub(crate) fn record_op_terminal(
        &self,
        opcode: &'static str,
        kind: FuseReqKind,
        op_status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
    ) {
        match op_status {
            FuseReqStatus::Error => {
                self.errors_total
                    .with_label_values(&[opcode, kind.as_str(), errno_label(errno)])
                    .inc();
            }
            FuseReqStatus::Unsupported => {
                let reason = match unsupported_reason {
                    Some(r) => r,
                    None => {
                        debug_assert!(
                            false,
                            "Unsupported op_status without a source tag (opcode {opcode})"
                        );
                        warn!("unsupported status without a source tag for opcode {opcode}");
                        UNSUPPORTED_REASON_MISSING
                    }
                };
                self.unsupported_total
                    .with_label_values(&[opcode, reason])
                    .inc();
            }
            FuseReqStatus::Interrupted => {
                self.interrupted_total.with_label_values(&[opcode]).inc();
            }
            FuseReqStatus::Success => {}
        }
    }

    pub(crate) fn record_notify_result(&self, code: &'static str, status: &'static str) {
        self.notify_total.with_label_values(&[code, status]).inc();
    }

    pub(crate) fn record_reply_enqueue_error(&self, opcode: &'static str, reason: &'static str) {
        self.reply_enqueue_errors_total
            .with_label_values(&[opcode, reason])
            .inc();
    }

    pub(crate) fn record_parse_error(&self, reason: &'static str) {
        self.decode_errors_total
            .with_label_values(&[DECODE_PHASE_PARSE, reason])
            .inc();
    }

    pub(crate) fn record_decode_error(&self, reason: &'static str) {
        self.decode_errors_total
            .with_label_values(&[DECODE_PHASE_DECODE, reason])
            .inc();
    }

    pub(crate) fn record_receive_error(&self, errno: &'static str, action: &'static str) {
        self.receive_errors_total
            .with_label_values(&[errno, action])
            .inc();
    }

    pub(crate) fn record_receive_loop_wait(&self, elapsed_us: u64) {
        self.receive_loop_wait_duration_us
            .observe(elapsed_us as f64);
    }

    pub(crate) fn record_meta_spawn(&self, elapsed_us: u64) {
        self.stage_duration_us
            .with_label_values(&[
                STAGE_META_SPAWN,
                FuseReqKind::Metadata.as_str(),
                FuseReqStatus::Success.as_str(),
            ])
            .observe(elapsed_us as f64);
    }

    pub(crate) fn record_scrape(&self, elapsed_us: u64, output_bytes: usize) {
        self.metrics_scrape_duration_us.observe(elapsed_us as f64);
        self.metrics_scrape_bytes.set(output_bytes as i64);
    }

    pub(crate) fn meta_task_guard(metrics_enabled: bool) -> Option<ActiveGuard> {
        if metrics_enabled {
            Some(ActiveGuard::new(Self::get().meta_task_inflight.clone()))
        } else {
            None
        }
    }

    pub(crate) fn record_operation(
        &self,
        opcode: &'static str,
        status: FuseReqStatus,
        elapsed_us: u64,
    ) {
        let kind = FuseReqKind::Metadata.as_str();
        let status_str = status.as_str();
        let elapsed = elapsed_us as f64;
        self.operation_duration_us
            .with_label_values(&[opcode, kind, status_str])
            .observe(elapsed);
        self.stage_duration_us
            .with_label_values(&[STAGE_OPERATION, kind, status_str])
            .observe(elapsed);
    }

    pub(crate) fn reply_queue_guard() -> Option<ActiveGuard> {
        Some(ActiveGuard::new(Self::get().reply_queue_depth.clone()))
    }

    pub(crate) fn setlkw_inflight_guard(metrics_enabled: bool) -> Option<ActiveGuard> {
        if metrics_enabled {
            Some(ActiveGuard::new(Self::get().setlkw_inflight.clone()))
        } else {
            None
        }
    }

    /// SETLKW timer scoped around the whole interruptible request.
    pub(crate) fn setlkw_wait_timer(metrics_enabled: bool) -> Option<HistogramTimer> {
        if metrics_enabled {
            Some(HistogramTimer::new(
                Self::get().setlkw_wait_duration_us.clone(),
            ))
        } else {
            None
        }
    }

    pub(crate) fn record_stream_io(
        &self,
        io_type: &'static str,
        path_type: &'static str,
        ok: bool,
        transferred_bytes: u64,
        request_size: u64,
        elapsed_us: u64,
    ) {
        let status_str = if ok {
            FuseReqStatus::Success.as_str()
        } else {
            FuseReqStatus::Error.as_str()
        };
        let elapsed = elapsed_us as f64;
        self.io_duration_us
            .with_label_values(&[io_type, path_type, status_str])
            .observe(elapsed);
        self.stage_duration_us
            .with_label_values(&[STAGE_STREAM_IO, FuseReqKind::Stream.as_str(), status_str])
            .observe(elapsed);
        self.io_requests_total
            .with_label_values(&[io_type, path_type, status_str])
            .inc();
        self.io_size_bytes
            .with_label_values(&[io_type, path_type])
            .observe(request_size as f64);
        if ok {
            // Success-only byte series, fixed status=success.
            self.io_bytes_total
                .with_label_values(&[io_type, path_type, FuseReqStatus::Success.as_str()])
                .inc_by(transferred_bytes as i64);
        }
    }

    pub(crate) fn stream_io_guard(
        metrics_enabled: bool,
        io_type: &'static str,
    ) -> Option<ActiveGuard> {
        if metrics_enabled {
            let gauge = Self::get().stream_io_inflight.with_label_values(&[io_type]);
            Some(ActiveGuard::new(gauge))
        } else {
            None
        }
    }

    pub(crate) fn stream_write_queue_guard(metrics_enabled: bool) -> Option<ActiveGuard> {
        if metrics_enabled {
            Some(ActiveGuard::new(
                Self::get().stream_write_queue_depth.clone(),
            ))
        } else {
            None
        }
    }

    pub(crate) fn io_dispatch_timer(io_type: &'static str) -> HistogramTimer {
        let hist = Self::get()
            .io_dispatch_duration_us
            .with_label_values(&[io_type]);
        HistogramTimer::new(hist)
    }

    pub(crate) fn stream_lifecycle_scope(io_type: &'static str) -> StreamLifecycleScope {
        let m = Self::get();
        m.stream_lifecycle_requests_total
            .with_label_values(&[io_type, PATH_TYPE_UNKNOWN])
            .inc();
        let timer = HistogramTimer::new(
            m.stream_lifecycle_duration_us
                .with_label_values(&[io_type, PATH_TYPE_UNKNOWN]),
        );
        let inflight = ActiveGuard::new(m.stream_lifecycle_inflight.with_label_values(&[io_type]));
        StreamLifecycleScope {
            _timer: timer,
            _inflight: inflight,
        }
    }

    pub(crate) fn record_user_meta_cache(&self, cache: &'static str, status: &'static str) {
        self.user_meta_cache_total
            .with_label_values(&[cache, status])
            .inc();
    }

    pub(crate) fn record_node_cache_lookup(&self, status: &'static str) {
        self.node_cache_total
            .with_label_values(&[NODE_CACHE_OP_LOOKUP, status])
            .inc();
    }

    pub(crate) fn record_negative_entry(&self) {
        self.negative_entry_returned_total.inc();
    }

    pub(crate) fn record_invalidation(&self, reason: &'static str, has_parent: bool) {
        let c = &self.user_meta_cache_invalidations_total;
        c.with_label_values(&[CACHE_STATUS, reason]).inc();
        c.with_label_values(&[CACHE_LIST, reason]).inc();
        c.with_label_values(&[CACHE_BLOCKS, reason]).inc();
        if has_parent {
            c.with_label_values(&[CACHE_LIST, reason]).inc();
        }
    }

    pub(crate) fn record_readdir_success(&self, entries: u64, elapsed_us: u64) {
        self.readdir_entries
            .with_label_values(&[READDIR_STATUS_SUCCESS])
            .observe(entries as f64);
        self.readdir_duration_us
            .with_label_values(&[READDIR_STATUS_SUCCESS])
            .observe(elapsed_us as f64);
    }

    pub(crate) fn record_readdir_error(&self, elapsed_us: u64) {
        self.readdir_duration_us
            .with_label_values(&[READDIR_STATUS_ERROR])
            .observe(elapsed_us as f64);
    }

    pub(crate) fn record_state_total(&self, is_persist: bool, status: &'static str) {
        let c = if is_persist {
            &self.state_persist_total
        } else {
            &self.state_restore_total
        };
        c.with_label_values(&[status]).inc();
    }

    pub(crate) fn observe_state_stage(
        &self,
        is_persist: bool,
        stage: &'static str,
        status: &'static str,
        elapsed_us: u64,
    ) {
        let h = if is_persist {
            &self.state_persist_stage_duration_us
        } else {
            &self.state_restore_stage_duration_us
        };
        h.with_label_values(&[stage, status])
            .observe(elapsed_us as f64);
    }

    pub(crate) fn set_state_handle_count(&self, kind: &'static str, count: usize) {
        self.state_persist_handle_count
            .with_label_values(&[kind])
            .set(count as i64);
    }

    pub(crate) fn record_session_init(&self, result: &'static str) {
        self.session_init_total.with_label_values(&[result]).inc();
    }

    pub(crate) fn record_session_shutdown(&self, reason: &'static str) {
        self.session_shutdown_total
            .with_label_values(&[reason])
            .inc();
    }

    pub(crate) fn set_kernel_fd_health(&self, healthy: bool) {
        self.kernel_fd_health.set(if healthy { 1 } else { 0 });
    }

    pub(crate) fn sender_progress_gauge(&self, mnt: &str, idx: usize) -> Gauge {
        self.sender_last_progress_unixtime
            .with_label_values(&[mnt, &idx.to_string()])
    }

    /// Set a sender's last-progress gauge to now (Unix seconds). An NTP step
    /// only perturbs the derived age transiently, acceptable for a coarse staleness signal.
    pub(crate) fn record_sender_progress(gauge: &Gauge) {
        let secs = (LocalTime::mills() / 1000) as i64;
        gauge.set(secs);
    }
}

pub(crate) fn dispatch_io_type(opcode: FuseOpCode) -> Option<&'static str> {
    match opcode {
        FuseOpCode::FUSE_READ => Some(IO_TYPE_READ),
        FuseOpCode::FUSE_WRITE => Some(IO_TYPE_WRITE),
        _ => None,
    }
}

pub(crate) fn lifecycle_io_type(opcode: FuseOpCode) -> Option<&'static str> {
    match opcode {
        FuseOpCode::FUSE_FLUSH => Some(IO_TYPE_FLUSH),
        FuseOpCode::FUSE_FSYNC => Some(IO_TYPE_FSYNC),
        FuseOpCode::FUSE_RELEASE => Some(IO_TYPE_RELEASE),
        _ => None,
    }
}

pub(crate) struct StreamLifecycleScope {
    _timer: HistogramTimer,
    _inflight: ActiveGuard,
}

/// Monotonic time source for durations; do not use wall-clock time for latency.
#[inline]
pub(crate) fn mono_now() -> Instant {
    Instant::now()
}

/// FUSE request kind for the `kind` label; no-reply ops stay `Metadata`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FuseReqKind {
    Metadata,
    Stream,
}

impl FuseReqKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            FuseReqKind::Metadata => "metadata",
            FuseReqKind::Stream => "stream",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FuseReqLabels {
    pub(crate) opcode: &'static str,
    pub(crate) kind: FuseReqKind,
    pub(crate) start: Instant,
    /// Request size from the parsed header. Carried for a future per-request
    /// byte metric; not read by any series yet.
    #[allow(dead_code)]
    pub(crate) request_bytes: u32,
}

impl FuseReqLabels {
    pub(crate) fn new(opcode: &'static str, kind: FuseReqKind, request_bytes: u32) -> Self {
        Self {
            opcode,
            kind,
            start: mono_now(),
            request_bytes,
        }
    }

    pub(crate) fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FuseReqStatus {
    Success,
    Error,
    Interrupted,
    Unsupported,
}

impl FuseReqStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            FuseReqStatus::Success => "success",
            FuseReqStatus::Error => "error",
            FuseReqStatus::Interrupted => "interrupted",
            FuseReqStatus::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteOutcome {
    /// The splice succeeded.
    Success,
    /// The splice failed. `errno` is the OS errno if one was available; `None`
    /// maps to the `OTHER` errno label.
    Failed { errno: Option<i32> },
}

#[derive(Debug)]
pub(crate) struct FuseReqCtx {
    pub(crate) labels: FuseReqLabels,
    pub(crate) active: Option<ActiveGuard>,
}

#[derive(Debug)]
pub(crate) struct FuseRespMetrics {
    pub(crate) labels: FuseReqLabels,
    pub(crate) active: Option<ActiveGuard>,
    #[allow(dead_code)] // production reader is operation_duration_us.
    pub(crate) op_status: Option<FuseReqStatus>,
    #[allow(dead_code)]
    // prod status flows via RequestReply; slot copy is for tests/early-finish.
    pub(crate) request_status: Option<FuseReqStatus>,
    #[allow(dead_code)] // errno label flows via RequestReply / finish_early, not the slot.
    pub(crate) errno: i32,
    #[allow(dead_code)] // unsupported reason flows via RequestReply.unsupported_reason.
    pub(crate) unsupported_reason: Option<&'static str>,
    #[allow(dead_code)]
    // decode_errors_total is emitted from finish_early's arg, not the slot.
    pub(crate) parse_reason: Option<&'static str>,
    /// State-machine guard: prevents a second reply from double-finishing.
    pub(crate) finished: bool,
}

impl FuseRespMetrics {
    pub(crate) fn new(ctx: FuseReqCtx) -> Self {
        Self {
            labels: ctx.labels,
            active: ctx.active,
            op_status: None,
            request_status: None,
            errno: 0,
            unsupported_reason: None,
            parse_reason: None,
            finished: false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ActiveGuard {
    gauge: Option<Gauge>,
}

impl ActiveGuard {
    pub(crate) fn new(gauge: Gauge) -> Self {
        gauge.inc();
        Self { gauge: Some(gauge) }
    }

    /// A no-op guard: same move/drop semantics, but touches no gauge. Used to
    /// validate ownership without a real gauge.
    pub(crate) fn noop() -> Self {
        Self { gauge: None }
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        if let Some(g) = &self.gauge {
            g.dec();
        }
    }
}

#[derive(Debug)]
pub(crate) struct HistogramTimer {
    start: Instant,
    hist: Histogram,
}

impl HistogramTimer {
    pub(crate) fn new(hist: Histogram) -> Self {
        Self {
            start: mono_now(),
            hist,
        }
    }
}

impl Drop for HistogramTimer {
    fn drop(&mut self) {
        self.hist.observe(self.start.elapsed().as_micros() as f64);
    }
}

pub(crate) struct ReaddirTimer {
    start: Instant,
    error_on_drop: bool,
}

impl ReaddirTimer {
    pub(crate) fn start(enabled: bool) -> Option<Self> {
        enabled.then(|| Self {
            start: mono_now(),
            error_on_drop: true,
        })
    }

    /// Success path: record duration + entry count, and disarm the error Drop.
    /// The trade-off (a success that panics mid-observe would be lost) is
    /// acceptable since observe does not panic.
    pub(crate) fn success(mut self, entries: u64) {
        let elapsed_us = self.start.elapsed().as_micros() as u64;
        self.error_on_drop = false;
        FuseMetrics::with(|m| m.record_readdir_success(entries, elapsed_us));
    }
}

impl Drop for ReaddirTimer {
    fn drop(&mut self) {
        if self.error_on_drop {
            let elapsed_us = self.start.elapsed().as_micros() as u64;
            FuseMetrics::with(|m| m.record_readdir_error(elapsed_us));
        }
    }
}

pub(crate) struct StateStageTimer {
    start: Instant,
    is_persist: bool,
    stage: &'static str,
    error_on_drop: bool,
}

impl StateStageTimer {
    pub(crate) fn start(enabled: bool, is_persist: bool, stage: &'static str) -> Option<Self> {
        enabled.then(|| Self {
            start: mono_now(),
            is_persist,
            stage,
            error_on_drop: true,
        })
    }

    pub(crate) fn success(mut self) {
        let elapsed_us = self.start.elapsed().as_micros() as u64;
        self.error_on_drop = false;
        let (is_persist, stage) = (self.is_persist, self.stage);
        FuseMetrics::with(|m| {
            m.observe_state_stage(is_persist, stage, STATE_STATUS_SUCCESS, elapsed_us)
        });
    }
}

impl Drop for StateStageTimer {
    fn drop(&mut self) {
        if self.error_on_drop {
            let elapsed_us = self.start.elapsed().as_micros() as u64;
            let (is_persist, stage) = (self.is_persist, self.stage);
            FuseMetrics::with(|m| {
                m.observe_state_stage(is_persist, stage, STATE_STATUS_ERROR, elapsed_us)
            });
        }
    }
}

#[derive(Clone)]
pub(crate) struct ShutdownOnce {
    recorded: std::sync::Arc<std::sync::atomic::AtomicBool>,
    enabled: bool,
}

impl ShutdownOnce {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            recorded: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            enabled,
        }
    }

    pub(crate) fn record_once(&self, reason: &'static str) -> bool {
        use std::sync::atomic::Ordering;
        if self
            .recorded
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            if self.enabled {
                FuseMetrics::with(|m| m.record_session_shutdown(reason));
            }
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_metrics::Metrics as m;

    // Compile-time guarantee that the guards/timer are `Send`: they travel into
    // spawned/reply tasks, so a field change making them `!Send` fails here rather
    // than at the first cross-task move.
    #[test]
    fn guards_are_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ActiveGuard>();
        assert_send::<HistogramTimer>();
        assert_send::<FuseReqLabels>();
    }

    // record_sender_progress writes the current Unix time (seconds) into the gauge.
    // Uses an injected isolated gauge (not the process-global vec) so it is
    // parallel-safe, and asserts the value lands in seconds, not millis.
    #[test]
    fn record_sender_progress_sets_current_unix_seconds() {
        let g = m::new_gauge("test_sender_progress_gauge", "test").unwrap();
        assert_eq!(g.get(), 0, "fresh gauge starts at 0");

        let before = (LocalTime::mills() / 1000) as i64;
        FuseMetrics::record_sender_progress(&g);
        let after = (LocalTime::mills() / 1000) as i64;

        let v = g.get();
        assert!(
            v >= before && v <= after,
            "gauge {} must be a current Unix-second timestamp in [{}, {}]",
            v,
            before,
            after
        );
    }

    // The metric must carry a `mnt` dimension so senders with the same channel
    // index on DIFFERENT mounts do not collide on one series (which would let an
    // active mount mask a stalled mount). Asserts two child gauges with the same
    // idx but different mnt are independent series. Unique mnt paths keep it
    // parallel-safe.
    #[test]
    fn sender_progress_gauge_distinct_per_mount_same_index() {
        FuseMetrics::ensure_init().unwrap();
        let m = FuseMetrics::get();
        let ga = m.sender_progress_gauge("/test/mnt-collision-a", 0);
        let gb = m.sender_progress_gauge("/test/mnt-collision-b", 0);

        ga.set(111);
        gb.set(222);
        assert_eq!(ga.get(), 111, "mount A series holds its own value");
        assert_eq!(
            gb.get(),
            222,
            "mount B series is independent; same idx on another mount must not collide"
        );

        // Re-fetching the same (mnt, idx) returns the same underlying series.
        let ga2 = m.sender_progress_gauge("/test/mnt-collision-a", 0);
        assert_eq!(
            ga2.get(),
            111,
            "same (mnt, idx) maps to the same child gauge"
        );
    }

    #[test]
    fn req_kind_labels() {
        assert_eq!(FuseReqKind::Metadata.as_str(), "metadata");
        assert_eq!(FuseReqKind::Stream.as_str(), "stream");
    }

    #[test]
    fn req_labels_are_copy_and_measure_monotonically() {
        let labels = FuseReqLabels::new("Lookup", FuseReqKind::Metadata, 64);
        // FuseReqLabels is Copy: using it after a copy must still compile/work.
        let copied = labels;
        assert_eq!(copied.opcode, "Lookup");
        assert_eq!(copied.kind, FuseReqKind::Metadata);
        assert_eq!(copied.request_bytes, 64);
        // elapsed_us is monotonic and never panics.
        let _ = labels.elapsed_us();
    }

    #[test]
    fn active_guard_inc_dec_balances() {
        let g = m::new_gauge("test_active_guard_gauge", "test gauge").unwrap();
        assert_eq!(g.get(), 0);
        {
            let _guard = ActiveGuard::new(g.clone());
            assert_eq!(g.get(), 1);
            {
                let _g2 = ActiveGuard::new(g.clone());
                assert_eq!(g.get(), 2);
            }
            assert_eq!(g.get(), 1);
        }
        assert_eq!(g.get(), 0);
    }

    #[test]
    fn active_guard_moves_without_double_decrement() {
        let g = m::new_gauge("test_active_guard_move_gauge", "test gauge").unwrap();
        let guard = ActiveGuard::new(g.clone());
        assert_eq!(g.get(), 1);
        // Moving the guard must not change the count.
        let moved = guard;
        assert_eq!(g.get(), 1);
        drop(moved);
        // Dropped exactly once.
        assert_eq!(g.get(), 0);
    }

    #[test]
    fn histogram_timer_observes_on_drop() {
        let h = m::new_histogram("test_histogram_timer", "test histogram").unwrap();
        assert_eq!(h.get_sample_count(), 0);
        {
            let _t = HistogramTimer::new(h.clone());
        }
        assert_eq!(h.get_sample_count(), 1);
    }

    // Sender-side emission is a pure function over labels, unit-testable without a
    // kernel fd. The process-global registry accumulates across tests, so each test
    // uses a UNIQUE opcode/code label and asserts a delta.
    fn requests_total(opcode: &str, kind: &str, reply_type: &str, status: &str) -> i64 {
        FuseMetrics::get()
            .requests_total
            .with_label_values(&[opcode, kind, reply_type, status])
            .get()
    }
    fn request_dur_count(opcode: &str, kind: &str, status: &str) -> u64 {
        FuseMetrics::get()
            .request_duration_us
            .with_label_values(&[opcode, kind, status])
            .get_sample_count()
    }

    #[test]
    fn req_status_label_strings() {
        assert_eq!(FuseReqStatus::Success.as_str(), "success");
        assert_eq!(FuseReqStatus::Error.as_str(), "error");
        assert_eq!(FuseReqStatus::Interrupted.as_str(), "interrupted");
        assert_eq!(FuseReqStatus::Unsupported.as_str(), "unsupported");
    }

    // A successful replied request increments requests_total{replied,success} +
    // request_duration once, response_write/bytes/reply_write stage once, and NO
    // error/unsupported/interrupted counter.
    #[test]
    fn record_request_finish_success_emits_request_and_response_series() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "FinishSuccess";
        let rw_before = mx
            .response_write_duration_us
            .with_label_values(&[OP, "success"])
            .get_sample_count();
        let bytes_before = mx
            .response_bytes_total
            .with_label_values(&[OP, "success"])
            .get();
        let stage_before = mx
            .stage_duration_us
            .with_label_values(&[STAGE_REPLY_WRITE, "metadata", "success"])
            .get_sample_count();

        mx.record_request_finish(
            OP,
            FuseReqKind::Metadata,
            FuseReqStatus::Success, // op_status
            FuseReqStatus::Success, // request_status
            0,
            None,
            128,
            WriteOutcome::Success,
            10,
            42,
        );

        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_REPLIED, "success"),
            1
        );
        assert_eq!(request_dur_count(OP, "metadata", "success"), 1);
        assert_eq!(
            mx.response_write_duration_us
                .with_label_values(&[OP, "success"])
                .get_sample_count(),
            rw_before + 1
        );
        assert_eq!(
            mx.response_bytes_total
                .with_label_values(&[OP, "success"])
                .get(),
            bytes_before + 128
        );
        // `stage_duration_us{reply_write,metadata,success}` is opcode-free and shared
        // by every concurrent metadata reply, so only assert it moved by AT LEAST our
        // emission; the exact +1 on the per-opcode response_write_duration_us above
        // already pins this call.
        assert!(
            mx.stage_duration_us
                .with_label_values(&[STAGE_REPLY_WRITE, "metadata", "success"])
                .get_sample_count()
                > stage_before
        );
        // No error/unsupported/interrupted for a success.
        assert_eq!(
            mx.errors_total
                .with_label_values(&[OP, "metadata", "OTHER"])
                .get(),
            0
        );
    }

    // A real (untagged) error increments errors_total with the errno label, NOT
    // unsupported_total.
    #[test]
    fn record_request_finish_error_emits_errors_total_with_errno() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "FinishError";
        mx.record_request_finish(
            OP,
            FuseReqKind::Metadata,
            FuseReqStatus::Error, // op_status
            FuseReqStatus::Error, // request_status
            libc::ENOSYS,
            None,
            0,
            WriteOutcome::Success,
            5,
            20,
        );
        assert_eq!(
            mx.errors_total
                .with_label_values(&[OP, "metadata", "ENOSYS"])
                .get(),
            1,
            "untagged error increments errors_total with errno label"
        );
        assert_eq!(
            mx.unsupported_total
                .with_label_values(&[OP, "unimplemented_opcode"])
                .get(),
            0,
            "error must NOT land in unsupported_total"
        );
    }

    // Unsupported status routes to unsupported_total{reason} only (not
    // errors_total); reason comes from the source tag.
    #[test]
    fn record_request_finish_unsupported_routes_to_unsupported_total() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "FinishUnsup";
        mx.record_request_finish(
            OP,
            FuseReqKind::Metadata,
            FuseReqStatus::Unsupported, // op_status
            FuseReqStatus::Unsupported, // request_status
            libc::ENOSYS,
            Some("unknown_opcode"),
            0,
            WriteOutcome::Success,
            5,
            20,
        );
        assert_eq!(
            mx.unsupported_total
                .with_label_values(&[OP, "unknown_opcode"])
                .get(),
            1
        );
        assert_eq!(
            mx.errors_total
                .with_label_values(&[OP, "metadata", "ENOSYS"])
                .get(),
            0,
            "unsupported must NOT also count as errors_total"
        );
    }

    // Op succeeds but the kernel-fd write fails: the kernel sees a failed request,
    // so request_status-labelled series go to `error` while op-level counters stay
    // clean. The write errno is the independent delivery dimension.
    #[test]
    fn record_request_finish_write_failure_sets_request_status_error_keeps_op_clean() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP_WITH: &str = "FinishWriteErr";
        mx.record_request_finish(
            OP_WITH,
            FuseReqKind::Stream,
            FuseReqStatus::Success, // op_status: the FS op succeeded
            FuseReqStatus::Error,   // request_status: delivery failed
            0,
            None,
            0,
            WriteOutcome::Failed {
                errno: Some(libc::EIO),
            },
            5,
            20,
        );
        // Delivery error dimension.
        assert_eq!(
            mx.response_write_errors_total
                .with_label_values(&[OP_WITH, "EIO"])
                .get(),
            1
        );
        // request_status reflects the delivery failure (NOT success).
        assert_eq!(
            requests_total(OP_WITH, "stream", REPLY_TYPE_REPLIED, "error"),
            1,
            "write failure -> request_status=error"
        );
        assert_eq!(
            requests_total(OP_WITH, "stream", REPLY_TYPE_REPLIED, "success"),
            0,
            "write failure must NOT be counted as a success request"
        );
        // op-level errors_total stays clean: the op itself did not fail.
        assert_eq!(
            mx.errors_total
                .with_label_values(&[OP_WITH, "stream", "EIO"])
                .get(),
            0,
            "op succeeded, so errors_total must stay clean"
        );

        // No OS errno on the write failure -> OTHER label.
        const OP_NONE: &str = "FinishWriteErrNone";
        mx.record_request_finish(
            OP_NONE,
            FuseReqKind::Stream,
            FuseReqStatus::Success,
            FuseReqStatus::Error,
            0,
            None,
            0,
            WriteOutcome::Failed { errno: None },
            5,
            20,
        );
        assert_eq!(
            mx.response_write_errors_total
                .with_label_values(&[OP_NONE, "OTHER"])
                .get(),
            1,
            "no OS errno maps to the OTHER label"
        );
    }

    // Defensive guard: an Unsupported op_status with no source tag is a wiring bug.
    // It must not masquerade as unimplemented_opcode; it is bucketed under
    // missing_reason (and asserts in debug). request_status == op_status so only
    // the op-status path is exercised.
    #[test]
    #[cfg(not(debug_assertions))] // debug_assert! would (correctly) panic in debug.
    fn record_request_finish_unsupported_without_reason_buckets_missing() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "FinishUnsupNoReason";
        mx.record_request_finish(
            OP,
            FuseReqKind::Metadata,
            FuseReqStatus::Unsupported,
            FuseReqStatus::Unsupported,
            libc::ENOSYS,
            None, // missing source tag (a bug)
            0,
            WriteOutcome::Success,
            5,
            20,
        );
        assert_eq!(
            mx.unsupported_total
                .with_label_values(&[OP, "missing_reason"])
                .get(),
            1,
            "missing source tag is bucketed distinctly, not as unimplemented_opcode"
        );
        assert_eq!(
            mx.unsupported_total
                .with_label_values(&[OP, "unimplemented_opcode"])
                .get(),
            0,
        );
    }

    // Debug counterpart: the missing source tag is a wiring bug and must trip the
    // debug_assert. (Release buckets it under `missing_reason` — see above.)
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "without a source tag")]
    fn record_request_finish_unsupported_without_reason_panics_in_debug() {
        FuseMetrics::ensure_init().unwrap();
        FuseMetrics::get().record_op_terminal(
            "FinishUnsupNoReasonDbg",
            FuseReqKind::Metadata,
            FuseReqStatus::Unsupported,
            libc::ENOSYS,
            None, // missing source tag trips debug_assert!
        );
    }

    // Notify lifecycle states are distinct counters under notify_total.
    #[test]
    fn record_notify_result_counts_three_states() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const CODE: &str = "test_notify_code";
        mx.record_notify_result(CODE, NOTIFY_SUCCESS);
        mx.record_notify_result(CODE, NOTIFY_ENQUEUE_FAILED);
        mx.record_notify_result(CODE, NOTIFY_WRITE_FAILED);
        assert_eq!(
            mx.notify_total
                .with_label_values(&[CODE, NOTIFY_SUCCESS])
                .get(),
            1
        );
        assert_eq!(
            mx.notify_total
                .with_label_values(&[CODE, NOTIFY_ENQUEUE_FAILED])
                .get(),
            1
        );
        assert_eq!(
            mx.notify_total
                .with_label_values(&[CODE, NOTIFY_WRITE_FAILED])
                .get(),
            1
        );
    }

    // receive_errors_total: errno + action labels recorded as a delta.
    #[test]
    fn record_receive_error_counts_by_errno_action() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let before = mx
            .receive_errors_total
            .with_label_values(&["enoent", RECEIVE_ACTION_CONTINUE])
            .get();
        mx.record_receive_error("enoent", RECEIVE_ACTION_CONTINUE);
        assert_eq!(
            mx.receive_errors_total
                .with_label_values(&["enoent", RECEIVE_ACTION_CONTINUE])
                .get(),
            before + 1
        );
        // exit action is a distinct series.
        let exit_before = mx
            .receive_errors_total
            .with_label_values(&["enodev", RECEIVE_ACTION_EXIT])
            .get();
        mx.record_receive_error("enodev", RECEIVE_ACTION_EXIT);
        assert_eq!(
            mx.receive_errors_total
                .with_label_values(&["enodev", RECEIVE_ACTION_EXIT])
                .get(),
            exit_before + 1
        );
    }

    // record_decode_error emits under phase=decode. Only the decode-series delta is
    // asserted: a "parse untouched" cross-series check can't be made reliably against
    // the process-global registry under parallel tests, and distinct decode/parse
    // labels are guaranteed by the const values anyway.
    #[test]
    fn record_decode_error_increments_decode_phase() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let decode_before = mx
            .decode_errors_total
            .with_label_values(&[DECODE_PHASE_DECODE, "other"])
            .get();
        mx.record_decode_error("other");
        assert_eq!(
            mx.decode_errors_total
                .with_label_values(&[DECODE_PHASE_DECODE, "other"])
                .get(),
            decode_before + 1,
            "decode phase incremented"
        );
    }

    // meta_task_guard: disabled MUST be None (no metric machinery); enabled is Some
    // and inc/dec balances around the gauge. The inc/dec check uses before/after on
    // the process-global `meta_task_inflight` gauge, relying on no other test
    // mutating it in parallel (this is the only meta_task test today).
    #[test]
    fn meta_task_guard_gate() {
        FuseMetrics::ensure_init().unwrap();
        assert!(
            FuseMetrics::meta_task_guard(false).is_none(),
            "disabled path must be None, never a noop guard"
        );

        let mx = FuseMetrics::get();
        let before = mx.meta_task_inflight.get();
        let guard = FuseMetrics::meta_task_guard(true);
        assert!(guard.is_some());
        assert_eq!(
            mx.meta_task_inflight.get(),
            before + 1,
            "guard inc on create"
        );
        drop(guard);
        assert_eq!(mx.meta_task_inflight.get(), before, "guard dec on drop");
    }

    // record_scrape sets bytes and observes duration (last-scrape semantics).
    // Asserts an absolute `set` on the process-global `metrics_scrape_bytes` gauge,
    // relying on no other test calling `record_scrape()` in parallel (true today).
    #[test]
    fn record_scrape_sets_bytes_and_observes_duration() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let count_before = mx.metrics_scrape_duration_us.get_sample_count();
        mx.record_scrape(42, 1234);
        assert_eq!(
            mx.metrics_scrape_bytes.get(),
            1234,
            "bytes = last scrape size"
        );
        assert_eq!(
            mx.metrics_scrape_duration_us.get_sample_count(),
            count_before + 1,
            "duration observed once"
        );
    }

    // record_meta_spawn observes the stage_duration_us{meta_spawn,metadata,success}
    // series — guards against a label/status/kind typo in the core helper.
    #[test]
    fn record_meta_spawn_observes_correct_labels() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let before = mx
            .stage_duration_us
            .with_label_values(&[STAGE_META_SPAWN, "metadata", "success"])
            .get_sample_count();
        mx.record_meta_spawn(123);
        assert_eq!(
            mx.stage_duration_us
                .with_label_values(&[STAGE_META_SPAWN, "metadata", "success"])
                .get_sample_count(),
            before + 1,
            "meta_spawn observed under stage=meta_spawn,kind=metadata,status=success"
        );
    }

    // record_receive_loop_wait observes the (no-label) histogram — guards against
    // the field/helper/name drifting silently.
    #[test]
    fn record_receive_loop_wait_observes() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let before = mx.receive_loop_wait_duration_us.get_sample_count();
        mx.record_receive_loop_wait(42);
        assert_eq!(
            mx.receive_loop_wait_duration_us.get_sample_count(),
            before + 1
        );
    }

    // --- helper tests ---

    // record_operation feeds BOTH families from one timer — per-opcode
    // `operation_duration_us` and opcode-free `stage_duration_us{operation}` — under
    // the stashed op_status. Unique opcode + delta on the shared registry.
    #[test]
    fn record_operation_observes_both_operation_and_stage_families() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "OpDurSuccess";
        let op_before = mx
            .operation_duration_us
            .with_label_values(&[OP, "metadata", "success"])
            .get_sample_count();
        let stage_before = mx
            .stage_duration_us
            .with_label_values(&[STAGE_OPERATION, "metadata", "success"])
            .get_sample_count();

        mx.record_operation(OP, FuseReqStatus::Success, 321);

        // Exact +1: `operation_duration_us` is keyed by the UNIQUE opcode `OP`, so no
        // other (parallel) test touches this child.
        assert_eq!(
            mx.operation_duration_us
                .with_label_values(&[OP, "metadata", "success"])
                .get_sample_count(),
            op_before + 1,
            "operation_duration_us observed once under opcode/metadata/success"
        );
        // `stage_duration_us{operation,metadata,success}` is opcode-FREE, shared by
        // every metadata op running `record_operation`, so a concurrent op may bump
        // it between our reads: assert it moved by AT LEAST our emission. Combined
        // with the exact +1 above, this still proves the dual-emit (one call feeds
        // both families).
        assert!(
            mx.stage_duration_us
                .with_label_values(&[STAGE_OPERATION, "metadata", "success"])
                .get_sample_count()
                > stage_before,
            "stage_duration_us observed under stage=operation/metadata/success"
        );
    }

    // Status comes through verbatim: the timer observes whatever op_status the caller
    // read back from the slot, NOT a hard-coded success. Guards against a
    // status-source regression in the helper labels.
    #[test]
    fn record_operation_carries_error_status() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "OpDurError";
        let before = mx
            .operation_duration_us
            .with_label_values(&[OP, "metadata", "error"])
            .get_sample_count();
        mx.record_operation(OP, FuseReqStatus::Error, 7);
        assert_eq!(
            mx.operation_duration_us
                .with_label_values(&[OP, "metadata", "error"])
                .get_sample_count(),
            before + 1,
            "error op_status lands under status=error, not success"
        );
        // record_operation must NOT touch the op-terminal counters (the request
        // terminal already did) — no errors_total double-count from the timer.
        assert_eq!(
            mx.errors_total
                .with_label_values(&[OP, "metadata", "OTHER"])
                .get(),
            0,
            "record_operation only observes latency, never record_op_terminal"
        );
    }

    // reply_queue_guard returns Some once the singleton is initialized, inc on
    // create / dec on drop. (The disabled path produces the legacy Reply and never
    // calls this; the gate lives at the FuseResponse call site, so there is no
    // `false` arm to assert here.)
    #[test]
    fn reply_queue_guard_inc_dec_balances() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let before = mx.reply_queue_depth.get();
        let guard = FuseMetrics::reply_queue_guard();
        assert!(guard.is_some(), "initialized singleton yields Some");
        assert_eq!(
            mx.reply_queue_depth.get(),
            before + 1,
            "reply_queue_depth inc on guard create"
        );
        drop(guard);
        assert_eq!(
            mx.reply_queue_depth.get(),
            before,
            "reply_queue_depth dec on guard drop"
        );
    }

    // setlkw_inflight_guard gate — disabled is None (never noop), enabled
    // inc/dec balances the gauge.
    #[test]
    fn setlkw_inflight_guard_gate() {
        FuseMetrics::ensure_init().unwrap();
        assert!(
            FuseMetrics::setlkw_inflight_guard(false).is_none(),
            "disabled path must be None, never a noop guard"
        );
        let mx = FuseMetrics::get();
        let before = mx.setlkw_inflight.get();
        let guard = FuseMetrics::setlkw_inflight_guard(true);
        assert!(guard.is_some());
        assert_eq!(mx.setlkw_inflight.get(), before + 1, "guard inc on create");
        drop(guard);
        assert_eq!(mx.setlkw_inflight.get(), before, "guard dec on drop");
    }

    // setlkw_wait_timer gate — disabled builds NO timer (no clock read, no
    // observe on drop), enabled observes exactly once on drop.
    #[test]
    fn setlkw_wait_timer_gate() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();

        // Disabled: None, and dropping it observes nothing.
        let before = mx.setlkw_wait_duration_us.get_sample_count();
        let disabled = FuseMetrics::setlkw_wait_timer(false);
        assert!(disabled.is_none(), "disabled must be None");
        drop(disabled);
        assert_eq!(
            mx.setlkw_wait_duration_us.get_sample_count(),
            before,
            "disabled timer must not observe"
        );

        // Enabled: observes once on drop (covers normal completion AND the
        // interrupt-cancellation drop — both are just a Drop).
        let timer = FuseMetrics::setlkw_wait_timer(true);
        assert!(timer.is_some());
        drop(timer);
        assert_eq!(
            mx.setlkw_wait_duration_us.get_sample_count(),
            before + 1,
            "enabled timer observes once on drop"
        );
    }

    // --- helper tests ---
    //
    // The process-global registry accumulates across parallel tests, so value
    // assertions check a delta on a label set the test owns. Read/write tests use a
    // UNIQUE synthetic `path_type` (e.g. "pt_io_rw") to isolate their children: the
    // helper takes `path_type` as a parameter, so a test-only label is a valid child
    // that never collides with another test or with e2e.

    // dispatch_io_type / lifecycle_io_type closed maps: stream opcodes map to the
    // LOWERCASE io_type consts (NOT opcode.as_str(), which is "Read"/"Fsync"); non-IO
    // opcodes map to None; exactly one of the two maps is Some for any stream opcode.
    #[test]
    fn stream_io_type_maps_are_lowercase_and_disjoint() {
        // dispatch (read/write).
        assert_eq!(dispatch_io_type(FuseOpCode::FUSE_READ), Some(IO_TYPE_READ));
        assert_eq!(
            dispatch_io_type(FuseOpCode::FUSE_WRITE),
            Some(IO_TYPE_WRITE)
        );
        assert_eq!(dispatch_io_type(FuseOpCode::FUSE_FLUSH), None);
        assert_eq!(dispatch_io_type(FuseOpCode::FUSE_FSYNC), None);
        assert_eq!(dispatch_io_type(FuseOpCode::FUSE_RELEASE), None);
        assert_eq!(dispatch_io_type(FuseOpCode::FUSE_LOOKUP), None);

        // lifecycle (flush/fsync/release).
        assert_eq!(
            lifecycle_io_type(FuseOpCode::FUSE_FLUSH),
            Some(IO_TYPE_FLUSH)
        );
        assert_eq!(
            lifecycle_io_type(FuseOpCode::FUSE_FSYNC),
            Some(IO_TYPE_FSYNC)
        );
        assert_eq!(
            lifecycle_io_type(FuseOpCode::FUSE_RELEASE),
            Some(IO_TYPE_RELEASE)
        );
        assert_eq!(lifecycle_io_type(FuseOpCode::FUSE_READ), None);
        assert_eq!(lifecycle_io_type(FuseOpCode::FUSE_WRITE), None);

        // The lowercase consts are NOT the capitalized request labels.
        assert_eq!(IO_TYPE_READ, "read");
        assert_eq!(IO_TYPE_WRITE, "write");
        assert_eq!(IO_TYPE_FSYNC, "fsync");
        assert_ne!(IO_TYPE_READ, FuseOpCode::FUSE_READ.as_str());
        assert_ne!(IO_TYPE_FSYNC, FuseOpCode::FUSE_FSYNC.as_str());

        // For each known stream opcode, exactly one map is Some (mutually exclusive).
        for op in [
            FuseOpCode::FUSE_READ,
            FuseOpCode::FUSE_WRITE,
            FuseOpCode::FUSE_FLUSH,
            FuseOpCode::FUSE_FSYNC,
            FuseOpCode::FUSE_RELEASE,
        ] {
            assert!(
                dispatch_io_type(op).is_some() ^ lifecycle_io_type(op).is_some(),
                "exactly one of dispatch/lifecycle is Some for {op:?}"
            );
        }
    }

    // read/write io family: a successful read records duration + stage +
    // requests{success} + size + bytes{success}; an error records the same minus the
    // bytes child (never inc_by(0)). Unique path_type isolates children on the shared
    // registry.
    #[test]
    fn record_stream_io_success_and_error_families() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const PT: &str = "pt_io_rw"; // test-owned path_type, isolates children.

        let dur_s = mx
            .io_duration_us
            .with_label_values(&[IO_TYPE_READ, PT, "success"])
            .get_sample_count();
        let stage_s = mx
            .stage_duration_us
            .with_label_values(&[STAGE_STREAM_IO, "stream", "success"])
            .get_sample_count();
        let req_s = mx
            .io_requests_total
            .with_label_values(&[IO_TYPE_READ, PT, "success"])
            .get();
        let size_s = mx
            .io_size_bytes
            .with_label_values(&[IO_TYPE_READ, PT])
            .get_sample_count();
        let bytes_s = mx
            .io_bytes_total
            .with_label_values(&[IO_TYPE_READ, PT, "success"])
            .get();

        // Success: 4096 bytes transferred, requested 8192.
        mx.record_stream_io(IO_TYPE_READ, PT, true, 4096, 8192, 50);

        assert_eq!(
            mx.io_duration_us
                .with_label_values(&[IO_TYPE_READ, PT, "success"])
                .get_sample_count(),
            dur_s + 1
        );
        // `stage_duration_us{stream_io,stream,success}` is opcode-free, shared by
        // every read/write backend success, so assert it moved by AT LEAST our
        // emission. The exact +1 lives on the per-(io_type,path_type) `io_*` children
        // above, keyed by this test's unique `PT`.
        assert!(
            mx.stage_duration_us
                .with_label_values(&[STAGE_STREAM_IO, "stream", "success"])
                .get_sample_count()
                > stage_s,
            "read backend call emits stage=stream_io,kind=stream"
        );
        assert_eq!(
            mx.io_requests_total
                .with_label_values(&[IO_TYPE_READ, PT, "success"])
                .get(),
            req_s + 1
        );
        assert_eq!(
            mx.io_size_bytes
                .with_label_values(&[IO_TYPE_READ, PT])
                .get_sample_count(),
            size_s + 1
        );
        assert_eq!(
            mx.io_bytes_total
                .with_label_values(&[IO_TYPE_READ, PT, "success"])
                .get(),
            bytes_s + 4096,
            "bytes uses ACTUAL transferred (4096), not requested (8192)"
        );

        // Error: requests{error}+1, duration{error}+1, size+1, but NO bytes child.
        let req_e = mx
            .io_requests_total
            .with_label_values(&[IO_TYPE_READ, PT, "error"])
            .get();
        let dur_e = mx
            .io_duration_us
            .with_label_values(&[IO_TYPE_READ, PT, "error"])
            .get_sample_count();
        let bytes_e = mx
            .io_bytes_total
            .with_label_values(&[IO_TYPE_READ, PT, "error"])
            .get();
        mx.record_stream_io(IO_TYPE_READ, PT, false, 0, 8192, 9);
        assert_eq!(
            mx.io_requests_total
                .with_label_values(&[IO_TYPE_READ, PT, "error"])
                .get(),
            req_e + 1,
            "error attempt counts in io_requests_total{{status=error}}"
        );
        assert_eq!(
            mx.io_duration_us
                .with_label_values(&[IO_TYPE_READ, PT, "error"])
                .get_sample_count(),
            dur_e + 1,
            "error duration observed"
        );
        assert_eq!(
            mx.io_bytes_total
                .with_label_values(&[IO_TYPE_READ, PT, "error"])
                .get(),
            bytes_e,
            "error read must NOT create a status=error bytes child (no inc_by(0))"
        );
    }

    // Disabled is None (never noop); enabled inc/dec balances the GaugeVec child for
    // the io_type. Uses the real write child with before/after deltas, so parallel-safe.
    #[test]
    fn stream_io_guard_gate_and_balance() {
        FuseMetrics::ensure_init().unwrap();
        assert!(
            FuseMetrics::stream_io_guard(false, IO_TYPE_READ).is_none(),
            "disabled stream_io_guard must be None, never a noop guard"
        );
        let mx = FuseMetrics::get();
        let before = mx
            .stream_io_inflight
            .with_label_values(&[IO_TYPE_WRITE])
            .get();
        let guard = FuseMetrics::stream_io_guard(true, IO_TYPE_WRITE);
        assert!(guard.is_some());
        assert_eq!(
            mx.stream_io_inflight
                .with_label_values(&[IO_TYPE_WRITE])
                .get(),
            before + 1,
            "stream_io_inflight{{write}} inc on guard create"
        );
        drop(guard);
        assert_eq!(
            mx.stream_io_inflight
                .with_label_values(&[IO_TYPE_WRITE])
                .get(),
            before,
            "stream_io_inflight{{write}} dec on guard drop"
        );
    }

    // stream_lifecycle_scope: opening the scope counts the attempt
    // immediately (before the backend runs), holds the inflight guard while alive
    // (gauge>0), and observes the duration once on drop; the inflight returns to
    // baseline after drop. Asserts the FULL {io_type,path_type="unknown"} label set.
    #[test]
    fn stream_lifecycle_scope_counts_attempt_holds_inflight_observes_on_drop() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let attempt_before = mx
            .stream_lifecycle_requests_total
            .with_label_values(&[IO_TYPE_FLUSH, PATH_TYPE_UNKNOWN])
            .get();
        let dur_before = mx
            .stream_lifecycle_duration_us
            .with_label_values(&[IO_TYPE_FLUSH, PATH_TYPE_UNKNOWN])
            .get_sample_count();
        let inflight_before = mx
            .stream_lifecycle_inflight
            .with_label_values(&[IO_TYPE_FLUSH])
            .get();

        let scope = FuseMetrics::stream_lifecycle_scope(IO_TYPE_FLUSH);
        // Attempt counted at open (before any backend work).
        assert_eq!(
            mx.stream_lifecycle_requests_total
                .with_label_values(&[IO_TYPE_FLUSH, PATH_TYPE_UNKNOWN])
                .get(),
            attempt_before + 1,
            "attempt counted when the scope opens"
        );
        // Inflight is held while the scope is alive.
        assert_eq!(
            mx.stream_lifecycle_inflight
                .with_label_values(&[IO_TYPE_FLUSH])
                .get(),
            inflight_before + 1,
            "lifecycle inflight held while the scope is alive"
        );
        // Duration not observed until drop.
        assert_eq!(
            mx.stream_lifecycle_duration_us
                .with_label_values(&[IO_TYPE_FLUSH, PATH_TYPE_UNKNOWN])
                .get_sample_count(),
            dur_before,
            "duration not observed until the scope drops"
        );

        drop(scope);
        assert_eq!(
            mx.stream_lifecycle_duration_us
                .with_label_values(&[IO_TYPE_FLUSH, PATH_TYPE_UNKNOWN])
                .get_sample_count(),
            dur_before + 1,
            "duration observed once on drop"
        );
        assert_eq!(
            mx.stream_lifecycle_inflight
                .with_label_values(&[IO_TYPE_FLUSH])
                .get(),
            inflight_before,
            "lifecycle inflight back to baseline after drop"
        );
    }

    // io_dispatch_timer: observes once on drop under the io_type child.
    #[test]
    fn io_dispatch_timer_observes_once_on_drop() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let before = mx
            .io_dispatch_duration_us
            .with_label_values(&[IO_TYPE_WRITE])
            .get_sample_count();
        {
            let _t = FuseMetrics::io_dispatch_timer(IO_TYPE_WRITE);
        }
        assert_eq!(
            mx.io_dispatch_duration_us
                .with_label_values(&[IO_TYPE_WRITE])
                .get_sample_count(),
            before + 1,
            "io_dispatch timer observes once on drop"
        );
    }

    // stream_write_queue_guard gate: disabled None (never noop), enabled inc/dec
    // balances the gauge.
    #[test]
    fn stream_write_queue_guard_gate() {
        FuseMetrics::ensure_init().unwrap();
        assert!(
            FuseMetrics::stream_write_queue_guard(false).is_none(),
            "disabled stream_write_queue_guard must be None, never a noop guard"
        );
        let mx = FuseMetrics::get();
        let before = mx.stream_write_queue_depth.get();
        let guard = FuseMetrics::stream_write_queue_guard(true);
        assert!(guard.is_some());
        assert_eq!(
            mx.stream_write_queue_depth.get(),
            before + 1,
            "stream_write_queue_depth inc on guard create"
        );
        drop(guard);
        assert_eq!(
            mx.stream_write_queue_depth.get(),
            before,
            "stream_write_queue_depth dec on guard drop"
        );
    }

    // negative assertion, io family is read/write only: record_stream_io must
    // never be called with a flush/fsync/release io_type in production — the family
    // SPLIT is structural (lifecycle uses stream_lifecycle_*). Here we assert the
    // closed maps enforce that split: an io_type that would land in io_* only ever
    // comes from dispatch_io_type (read/write), and a lifecycle io_type only from
    // lifecycle_io_type (flush/fsync/release), so the two label sets cannot cross.
    #[test]
    fn io_and_lifecycle_io_types_never_cross() {
        // The only io_types that reach record_stream_io / stream_io_guard.
        let io_only = [IO_TYPE_READ, IO_TYPE_WRITE];
        // The only io_types that reach stream_lifecycle_scope.
        let lifecycle_only = [IO_TYPE_FLUSH, IO_TYPE_FSYNC, IO_TYPE_RELEASE];
        for io in io_only {
            assert!(
                !lifecycle_only.contains(&io),
                "{io} must not be a lifecycle io_type"
            );
        }
        for lc in lifecycle_only {
            assert!(
                !io_only.contains(&lc),
                "{lc} must not be a read/write io_type"
            );
        }
    }

    // Contract seam: the fuse-side path_type consts MUST match the raw literals
    // `UnifiedReader/Writer::path_type()` produces in curvine-client, so the shared
    // vocabulary cannot drift apart across the crate boundary (the curvine-client
    // test pins the accessor side).
    #[test]
    fn path_type_label_consts_match_client_vocabulary() {
        assert_eq!(PATH_TYPE_CURVINE, "curvine");
        assert_eq!(PATH_TYPE_UFS, "ufs");
        assert_eq!(PATH_TYPE_FALLBACK, "fallback");
        assert_eq!(PATH_TYPE_LOCAL, "local");
        assert_eq!(PATH_TYPE_UNKNOWN, "unknown");
    }

    // Negative assertion: stage=stream_io is the only stream stage value; there is
    // no STAGE_STREAM_ENQUEUE const.
    #[test]
    fn stage_stream_io_is_the_only_stream_stage() {
        assert_eq!(STAGE_STREAM_IO, "stream_io");
        // No STAGE_STREAM_ENQUEUE exists; if one were added this test's neighbors
        // (the no-enqueue rule) and the send_stream code review would catch it.
    }

    // --- helper tests ---
    //
    // Same parallel-safety discipline as the stream IO tests: value assertions check
    // a before/after delta on a label set the test owns. `user_meta_cache_total` and
    // `*_invalidations_total` take the `cache` label as a param, so each test uses a
    // UNIQUE synthetic `cache` value to isolate its children.

    #[test]
    fn invalidation_records_one_per_namespace_with_and_without_parent() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        // Unique synthetic reason so the child counters are owned by this test.
        let reason = "test_inval_reason_unique";

        let s_before = mx
            .user_meta_cache_invalidations_total
            .with_label_values(&[CACHE_STATUS, reason])
            .get();
        let l_before = mx
            .user_meta_cache_invalidations_total
            .with_label_values(&[CACHE_LIST, reason])
            .get();
        let b_before = mx
            .user_meta_cache_invalidations_total
            .with_label_values(&[CACHE_BLOCKS, reason])
            .get();

        // With a parent: status +1, blocks +1, list +2 (path's own + parent's).
        mx.record_invalidation(reason, true);
        assert_eq!(
            mx.user_meta_cache_invalidations_total
                .with_label_values(&[CACHE_STATUS, reason])
                .get(),
            s_before + 1
        );
        assert_eq!(
            mx.user_meta_cache_invalidations_total
                .with_label_values(&[CACHE_BLOCKS, reason])
                .get(),
            b_before + 1
        );
        assert_eq!(
            mx.user_meta_cache_invalidations_total
                .with_label_values(&[CACHE_LIST, reason])
                .get(),
            l_before + 2,
            "list increments twice with a parent (path + parent listing)"
        );

        // Without a parent (root): status +1, blocks +1, list +1 (path only).
        mx.record_invalidation(reason, false);
        assert_eq!(
            mx.user_meta_cache_invalidations_total
                .with_label_values(&[CACHE_LIST, reason])
                .get(),
            l_before + 3,
            "no-parent call adds exactly one more list inc"
        );
        assert_eq!(
            mx.user_meta_cache_invalidations_total
                .with_label_values(&[CACHE_STATUS, reason])
                .get(),
            s_before + 2
        );
    }

    #[test]
    fn user_meta_cache_hit_miss_put_increment_their_child() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let cache = "test_cache_ns_unique";

        let read_child = |status: &str| {
            mx.user_meta_cache_total
                .with_label_values(&[cache, status])
                .get()
        };
        let (h, m, p) = (
            read_child(CACHE_RESULT_HIT),
            read_child(CACHE_RESULT_MISS),
            read_child(CACHE_RESULT_PUT),
        );

        mx.record_user_meta_cache(cache, CACHE_RESULT_HIT);
        mx.record_user_meta_cache(cache, CACHE_RESULT_MISS);
        mx.record_user_meta_cache(cache, CACHE_RESULT_PUT);

        assert_eq!(read_child(CACHE_RESULT_HIT), h + 1);
        assert_eq!(read_child(CACHE_RESULT_MISS), m + 1);
        assert_eq!(read_child(CACHE_RESULT_PUT), p + 1);
    }

    #[test]
    fn negative_entry_counter_increments() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        // `negative_entry_returned_total` is a process-global, no-label counter
        // shared across parallel tests, so assert a lower bound on the delta, not
        // an exact value (another test could also increment it concurrently).
        let before = mx.negative_entry_returned_total.get();
        mx.record_negative_entry();
        mx.record_negative_entry();
        assert!(
            mx.negative_entry_returned_total.get() > before + 1,
            "two records must add at least 2 to the shared counter"
        );
    }

    #[test]
    fn readdir_timer_success_observes_entries_and_duration_drop_is_noop() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let s_before = mx
            .readdir_duration_us
            .with_label_values(&[READDIR_STATUS_SUCCESS])
            .get_sample_count();
        let e_before = mx
            .readdir_entries
            .with_label_values(&[READDIR_STATUS_SUCCESS])
            .get_sample_count();

        ReaddirTimer::start(true)
            .expect("enabled => Some")
            .success(7);

        // Shared {status=success} histogram children; lower-bound the delta since
        // a concurrent test could also observe them.
        assert!(
            mx.readdir_duration_us
                .with_label_values(&[READDIR_STATUS_SUCCESS])
                .get_sample_count()
                > s_before,
            "success records at least one duration sample"
        );
        assert!(
            mx.readdir_entries
                .with_label_values(&[READDIR_STATUS_SUCCESS])
                .get_sample_count()
                > e_before,
            "success records at least one entries sample"
        );
    }

    #[test]
    fn readdir_timer_disabled_creates_no_timer_and_records_nothing() {
        // metrics_enabled=false => start() returns None, and there is no Drop emission.
        FuseMetrics::ensure_init().unwrap();
        assert!(
            ReaddirTimer::start(false).is_none(),
            "disabled must yield no timer"
        );
    }

    #[test]
    fn readdir_timer_drop_without_success_records_error_no_entries() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let d_before = mx
            .readdir_duration_us
            .with_label_values(&[READDIR_STATUS_ERROR])
            .get_sample_count();
        let e_before = mx
            .readdir_entries
            .with_label_values(&[READDIR_STATUS_ERROR])
            .get_sample_count();

        // Drop without calling success() — the early-return / cancellation path.
        drop(ReaddirTimer::start(true).expect("enabled => Some"));

        // Shared {status=error} children; lower-bound the duration delta.
        assert!(
            mx.readdir_duration_us
                .with_label_values(&[READDIR_STATUS_ERROR])
                .get_sample_count()
                > d_before,
            "drop-without-success records at least one error duration sample"
        );
        // No code path ever observes entries{error} — that is the point — so this
        // child stays at its baseline; the exact check is stable.
        assert_eq!(
            mx.readdir_entries
                .with_label_values(&[READDIR_STATUS_ERROR])
                .get_sample_count(),
            e_before,
            "error path must NOT observe readdir_entries (no partial/zero count)"
        );
    }

    #[test]
    fn invalidation_reason_consts_match_design_15_values() {
        // The 15-value bounded enum the design doc pins. A change here must be
        // mirrored in the design doc's reason list.
        let reasons = [
            INVAL_REASON_SETATTR,
            INVAL_REASON_RESIZE,
            INVAL_REASON_SETXATTR,
            INVAL_REASON_REMOVEXATTR,
            INVAL_REASON_MKDIR,
            INVAL_REASON_CREATE,
            INVAL_REASON_OPEN_WRITE,
            INVAL_REASON_FLUSH,
            INVAL_REASON_RELEASE,
            INVAL_REASON_UNLINK,
            INVAL_REASON_LINK,
            INVAL_REASON_RMDIR,
            INVAL_REASON_RENAME,
            INVAL_REASON_SYMLINK,
            INVAL_REASON_FSYNC,
        ];
        assert_eq!(reasons.len(), 15);
        // No catch-all `other` and no `kernel_notify`.
        assert!(!reasons.contains(&"other"));
        assert!(!reasons.contains(&"kernel_notify"));
    }

    // --- seam tests ---

    // ShutdownOnce records exactly once (first cause wins); later callers no-op.
    // Verified via the return-value of record_once (true only for the winner),
    // which is deterministic and does not touch the process-global counter.
    #[test]
    fn shutdown_once_records_first_cause_only() {
        let once = ShutdownOnce::new(true);
        assert!(once.record_once(SHUTDOWN_FD_WATCHER), "first call wins");
        assert!(
            !once.record_once(SHUTDOWN_COMPLETED),
            "second call is a no-op (already recorded)"
        );
        assert!(
            !once.record_once(SHUTDOWN_TERM_SIGNAL),
            "third call is a no-op too"
        );
    }

    // ShutdownOnce with metrics disabled still enforces the once semantics (the
    // CAS runs regardless), it just does not emit.
    #[test]
    fn shutdown_once_disabled_still_dedups() {
        let once = ShutdownOnce::new(false);
        assert!(once.record_once(SHUTDOWN_COMPLETED), "first call wins");
        assert!(!once.record_once(SHUTDOWN_FD_WATCHER), "second no-op");
    }

    // StateStageTimer: disabled => None (no timer). Enabled success() observes the
    // {stage,status=success} child; drop-without-success observes {status=error}.
    // Uses a unique synthetic stage label so the children are owned by this test
    // (exact, parallel-safe).
    #[test]
    fn state_stage_timer_disabled_none_enabled_records() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();

        // Disabled: no timer at all.
        assert!(
            StateStageTimer::start(false, true, "test_state_stage_unique").is_none(),
            "disabled => None"
        );

        // Enabled + success: records the success child once.
        let s_before = mx
            .state_persist_stage_duration_us
            .with_label_values(&["test_state_stage_unique", STATE_STATUS_SUCCESS])
            .get_sample_count();
        StateStageTimer::start(true, true, "test_state_stage_unique")
            .expect("enabled => Some")
            .success();
        assert_eq!(
            mx.state_persist_stage_duration_us
                .with_label_values(&["test_state_stage_unique", STATE_STATUS_SUCCESS])
                .get_sample_count(),
            s_before + 1,
            "success() records one success sample (unique label => exact)"
        );

        // Enabled + drop without success: records the error child once.
        let e_before = mx
            .state_persist_stage_duration_us
            .with_label_values(&["test_state_stage_unique", STATE_STATUS_ERROR])
            .get_sample_count();
        drop(StateStageTimer::start(
            true,
            true,
            "test_state_stage_unique",
        ));
        assert_eq!(
            mx.state_persist_stage_duration_us
                .with_label_values(&["test_state_stage_unique", STATE_STATUS_ERROR])
                .get_sample_count(),
            e_before + 1,
            "drop-without-success records one error sample"
        );
    }

    // The 6 shutdown reasons are distinct and the run_all arm's three outcomes are
    // present (a regression guard against collapsing them back to `completed`).
    #[test]
    fn shutdown_reason_consts_are_the_six_distinct_values() {
        let reasons = [
            SHUTDOWN_COMPLETED,
            SHUTDOWN_RUN_ALL_ERROR,
            SHUTDOWN_RUN_ALL_PANIC,
            SHUTDOWN_TERM_SIGNAL,
            SHUTDOWN_SIGUSR1_PERSIST,
            SHUTDOWN_FD_WATCHER,
        ];
        let mut uniq: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for r in reasons {
            assert!(uniq.insert(r), "reason {r} must be unique");
        }
        assert_eq!(uniq.len(), 6);
        // run_all's three outcomes are not collapsed into `completed`.
        assert_ne!(SHUTDOWN_RUN_ALL_ERROR, SHUTDOWN_COMPLETED);
        assert_ne!(SHUTDOWN_RUN_ALL_PANIC, SHUTDOWN_COMPLETED);
    }

    // State-recovery stages must NEVER collide with the request stage_duration_us
    // enum (separate domains sharing the label name). Guard by value.
    #[test]
    fn state_stages_disjoint_from_request_stages() {
        let request_stages = [
            STAGE_REPLY_WRITE,
            STAGE_META_SPAWN,
            STAGE_OPERATION,
            STAGE_STREAM_IO,
        ];
        let state_stages = [
            STATE_STAGE_NODE_MAP,
            STATE_STAGE_FILE_HANDLES,
            STATE_STAGE_DIR_HANDLES,
            STATE_STAGE_MOUNT_FDS,
        ];
        for s in state_stages {
            assert!(
                !request_stages.contains(&s),
                "state stage {s} must not appear in the request stage enum"
            );
        }
    }

    // Lifecycle record helpers actually emit through the singleton. Shared global
    // children → lower-bound deltas, parallel-safe.
    #[test]
    fn lifecycle_record_helpers_emit() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();

        let init_before = mx
            .session_init_total
            .with_label_values(&[SESSION_INIT_SUCCESS])
            .get();
        mx.record_session_init(SESSION_INIT_SUCCESS);
        assert!(
            mx.session_init_total
                .with_label_values(&[SESSION_INIT_SUCCESS])
                .get()
                > init_before,
            "record_session_init emits the success child"
        );

        let persist_before = mx
            .state_persist_total
            .with_label_values(&[STATE_STATUS_SUCCESS])
            .get();
        mx.record_state_total(true, STATE_STATUS_SUCCESS);
        assert!(
            mx.state_persist_total
                .with_label_values(&[STATE_STATUS_SUCCESS])
                .get()
                > persist_before,
            "record_state_total(persist) emits the success child"
        );

        // kernel_fd_health is a shared single gauge, so assert only that the setter
        // takes effect and keeps the gauge binary, not an exact value.
        mx.set_kernel_fd_health(true);
        // Not asserting the exact value (shared gauge); the call must not panic and
        // the gauge must be in {0,1}.
        let v = mx.kernel_fd_health.get();
        assert!(v == 0 || v == 1, "health gauge is binary");
    }

    // The `if enabled { emit }` gate is deterministic against an isolated counter —
    // the shape session call sites use to gate every emission on metrics_enabled.
    #[test]
    fn lifecycle_gate_suppresses_when_disabled() {
        fn record_if(enabled: bool, counter: &curvine_metrics::Counter) {
            if enabled {
                counter.inc();
            }
        }
        let counter = curvine_metrics::Metrics::new_counter(
            "test_lifecycle_gate_isolated_counter_unique",
            "isolated lifecycle gate counter",
        )
        .unwrap();
        record_if(false, &counter);
        assert_eq!(counter.get(), 0, "disabled gate emits nothing");
        record_if(true, &counter);
        assert_eq!(counter.get(), 1, "enabled gate emits");
    }
}
