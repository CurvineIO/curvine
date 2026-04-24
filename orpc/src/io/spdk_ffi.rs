#![cfg(feature = "spdk")]
#![allow(non_camel_case_types, non_snake_case, dead_code)]
use std::ffi::{c_char, c_int, c_void};
// Transport type constants
pub const SPDK_NVME_TRANSPORT_RDMA: c_int = 1;
pub const SPDK_NVME_TRANSPORT_TCP: c_int = 3;
// Address family constants
pub const SPDK_NVMF_ADRFAM_IPV4: c_int = 1;
pub const SPDK_NVMF_ADRFAM_IPV6: c_int = 2;
// SPDK structs - opaque byte buffers
pub enum spdk_nvme_ctrlr {}
pub enum spdk_nvme_ns {}
pub enum spdk_nvme_qpair {}
#[repr(C, align(8))]
pub struct spdk_env_opts {
    pub data: [u8; 4096],
}
#[repr(C, align(8))]
pub struct spdk_nvme_transport_id {
    pub data: [u8; 4096],
}
#[repr(C, align(8))]
pub struct spdk_nvme_ctrlr_opts {
    pub data: [u8; 8192],
}
extern "C" {
    // Environment (via C helper)
    pub fn curvine_spdk_env_opts_sizeof() -> usize;
    pub fn curvine_spdk_env_opts_init(opts: *mut spdk_env_opts);
    pub fn curvine_spdk_env_opts_set_name(opts: *mut spdk_env_opts, name: *const c_char);
    pub fn curvine_spdk_env_opts_set_core_mask(opts: *mut spdk_env_opts, mask: *const c_char);
    pub fn curvine_spdk_env_opts_set_shm_id(opts: *mut spdk_env_opts, shm_id: c_int);
    pub fn curvine_spdk_env_opts_set_mem_channel(opts: *mut spdk_env_opts, mem_channel: c_int);
    pub fn curvine_spdk_env_opts_set_mem_size(opts: *mut spdk_env_opts, mem_size: c_int);
    pub fn curvine_spdk_env_init(opts: *mut spdk_env_opts) -> c_int;
    pub fn spdk_env_fini();
    // Transport ID (via C helper)
    pub fn curvine_spdk_trid_sizeof() -> usize;
    pub fn curvine_spdk_trid_set_trtype(trid: *mut spdk_nvme_transport_id, trtype: c_int);
    pub fn curvine_spdk_trid_set_adrfam(trid: *mut spdk_nvme_transport_id, adrfam: c_int);
    pub fn curvine_spdk_trid_set_traddr(trid: *mut spdk_nvme_transport_id, traddr: *const c_char);
    pub fn curvine_spdk_trid_set_trsvcid(trid: *mut spdk_nvme_transport_id, trsvcid: *const c_char);
    pub fn curvine_spdk_trid_set_subnqn(trid: *mut spdk_nvme_transport_id, subnqn: *const c_char);
    // Controller opts (via C helper)
    pub fn curvine_spdk_ctrlr_opts_sizeof() -> usize;
    pub fn curvine_spdk_ctrlr_get_default_opts(opts: *mut spdk_nvme_ctrlr_opts);
    pub fn curvine_spdk_ctrlr_opts_set_num_io_queues(opts: *mut spdk_nvme_ctrlr_opts, num: u32);
    pub fn curvine_spdk_ctrlr_opts_set_keep_alive_timeout_ms(
        opts: *mut spdk_nvme_ctrlr_opts,
        ms: u32,
    );
    pub fn curvine_spdk_ctrlr_opts_set_hostnqn(
        opts: *mut spdk_nvme_ctrlr_opts,
        hostnqn: *const c_char,
    );
    // Connect / namespace
    pub fn curvine_spdk_nvme_connect(
        trid: *mut spdk_nvme_transport_id,
        opts: *mut spdk_nvme_ctrlr_opts,
    ) -> *mut spdk_nvme_ctrlr;
    pub fn spdk_nvme_detach(ctrlr: *mut spdk_nvme_ctrlr) -> c_int;
    pub fn spdk_nvme_ctrlr_get_num_ns(ctrlr: *mut spdk_nvme_ctrlr) -> u32;
    pub fn spdk_nvme_ctrlr_get_ns(ctrlr: *mut spdk_nvme_ctrlr, ns_id: u32) -> *mut spdk_nvme_ns;
    pub fn spdk_nvme_ns_is_active(ns: *mut spdk_nvme_ns) -> bool;
    pub fn spdk_nvme_ns_get_sector_size(ns: *mut spdk_nvme_ns) -> u32;
    pub fn spdk_nvme_ns_get_num_sectors(ns: *mut spdk_nvme_ns) -> u64;
    pub fn curvine_spdk_register_transports();
    // DMA buffer
    pub fn curvine_spdk_dma_malloc(size: u64, align: u64) -> *mut c_void;
    pub fn curvine_spdk_dma_free(buf: *mut c_void);
    // I/O qpair
    pub fn curvine_spdk_alloc_io_qpair(ctrlr: *mut spdk_nvme_ctrlr) -> *mut spdk_nvme_qpair;
    pub fn curvine_spdk_free_io_qpair(qpair: *mut spdk_nvme_qpair);
    // Sync NVMe I/O (unused, to remove)
    pub fn curvine_spdk_ns_read(
        ns: *mut spdk_nvme_ns,
        qpair: *mut spdk_nvme_qpair,
        buf: *mut c_void,
        offset: u64,
        num_bytes: u64,
        timeout_us: u64,
    ) -> c_int;
    pub fn curvine_spdk_ns_write(
        ns: *mut spdk_nvme_ns,
        qpair: *mut spdk_nvme_qpair,
        buf: *mut c_void,
        offset: u64,
        num_bytes: u64,
        timeout_us: u64,
    ) -> c_int;
    pub fn curvine_spdk_ns_flush(
        ns: *mut spdk_nvme_ns,
        qpair: *mut spdk_nvme_qpair,
        timeout_us: u64,
    ) -> c_int;
}

// Async context - opaque C struct (byte buffer)
#[repr(C)]
pub struct curvine_async_ctx {
    pub data: [u8; 64], // oversized to accommodate any SPDK version
}

// Callback type
pub type curvine_async_cb = unsafe extern "C" fn(cb_arg: *mut c_void, status: c_int);

extern "C" {
    // Async NVMe I/O (split submit/poll)
    pub fn curvine_spdk_ns_submit_read(
        ns: *mut spdk_nvme_ns,
        qpair: *mut spdk_nvme_qpair,
        buf: *mut c_void,
        offset: u64,
        num_bytes: u64,
        async_ctx: *mut curvine_async_ctx,
    ) -> c_int;

    pub fn curvine_spdk_ns_submit_write(
        ns: *mut spdk_nvme_ns,
        qpair: *mut spdk_nvme_qpair,
        buf: *mut c_void,
        offset: u64,
        num_bytes: u64,
        async_ctx: *mut curvine_async_ctx,
    ) -> c_int;

    pub fn curvine_spdk_ns_submit_flush(
        ns: *mut spdk_nvme_ns,
        qpair: *mut spdk_nvme_qpair,
        async_ctx: *mut curvine_async_ctx,
    ) -> c_int;

    pub fn curvine_spdk_qpair_poll(qpair: *mut spdk_nvme_qpair, max_completions: c_int) -> c_int;

    pub fn curvine_spdk_async_ctx_sizeof() -> usize;

    pub fn curvine_spdk_async_ctx_init(
        ctx: *mut curvine_async_ctx,
        cb: curvine_async_cb,
        cb_arg: *mut c_void,
    );
}
