#include "spdk/env.h"
#include "spdk/nvme.h"
#include <errno.h>
#include <stddef.h>
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

// env_opts helpers
void curvine_spdk_env_opts_init(struct spdk_env_opts *opts) {
    opts->opts_size = sizeof(*opts);
    spdk_env_opts_init(opts);
}
size_t curvine_spdk_env_opts_sizeof(void) {
    return sizeof(struct spdk_env_opts);
}
void curvine_spdk_env_opts_set_name(struct spdk_env_opts *opts, const char *name) {
    opts->name = name;
}
void curvine_spdk_env_opts_set_core_mask(struct spdk_env_opts *opts, const char *mask) {
    opts->core_mask = mask;
}
void curvine_spdk_env_opts_set_shm_id(struct spdk_env_opts *opts, int shm_id) {
    opts->shm_id = shm_id;
}
void curvine_spdk_env_opts_set_mem_channel(struct spdk_env_opts *opts, int channel) {
    opts->mem_channel = channel;
}
void curvine_spdk_env_opts_set_mem_size(struct spdk_env_opts *opts, int size_mb) {
    opts->mem_size = size_mb;
}
int curvine_spdk_env_init(struct spdk_env_opts *opts) {
    return spdk_env_init(opts);
}

// transport_id helpers
size_t curvine_spdk_trid_sizeof(void) {
    return sizeof(struct spdk_nvme_transport_id);
}
void curvine_spdk_trid_set_trtype(struct spdk_nvme_transport_id *trid, int trtype) {
    trid->trtype = trtype;
}
void curvine_spdk_trid_set_adrfam(struct spdk_nvme_transport_id *trid, int adrfam) {
    trid->adrfam = adrfam;
}
void curvine_spdk_trid_set_traddr(struct spdk_nvme_transport_id *trid, const char *addr) {
    snprintf(trid->traddr, sizeof(trid->traddr), "%s", addr);
}
void curvine_spdk_trid_set_trsvcid(struct spdk_nvme_transport_id *trid, const char *port) {
    snprintf(trid->trsvcid, sizeof(trid->trsvcid), "%s", port);
}
void curvine_spdk_trid_set_subnqn(struct spdk_nvme_transport_id *trid, const char *nqn) {
    snprintf(trid->subnqn, sizeof(trid->subnqn), "%s", nqn);
}

// ctrlr_opts helpers
size_t curvine_spdk_ctrlr_opts_sizeof(void) {
    return sizeof(struct spdk_nvme_ctrlr_opts);
}
void curvine_spdk_ctrlr_get_default_opts(struct spdk_nvme_ctrlr_opts *opts) {
    opts->opts_size = sizeof(*opts);
    spdk_nvme_ctrlr_get_default_ctrlr_opts(opts, sizeof(*opts));
}
void curvine_spdk_ctrlr_opts_set_num_io_queues(struct spdk_nvme_ctrlr_opts *opts, uint32_t num) {
    opts->num_io_queues = num;
}
void curvine_spdk_ctrlr_opts_set_keep_alive_timeout_ms(struct spdk_nvme_ctrlr_opts *opts, uint32_t ms) {
    opts->keep_alive_timeout_ms = ms;
}
void curvine_spdk_ctrlr_opts_set_hostnqn(struct spdk_nvme_ctrlr_opts *opts, const char *nqn) {
    snprintf(opts->hostnqn, sizeof(opts->hostnqn), "%s", nqn);
}

// connect
struct spdk_nvme_ctrlr *curvine_spdk_nvme_connect(struct spdk_nvme_transport_id *trid,
                                                    struct spdk_nvme_ctrlr_opts *opts) {
    return spdk_nvme_connect(trid, opts, sizeof(*opts));
}

// Force transport registration (static link)
extern void __attribute__((weak)) _spdk_nvme_transport_register_tcp(void);
extern void __attribute__((weak)) _spdk_nvme_transport_register_rdma(void);
extern void __attribute__((weak)) _spdk_nvme_transport_register_pcie(void);
void curvine_spdk_register_transports(void) {
    if (_spdk_nvme_transport_register_tcp) _spdk_nvme_transport_register_tcp();
    if (_spdk_nvme_transport_register_rdma) _spdk_nvme_transport_register_rdma();
    if (_spdk_nvme_transport_register_pcie) _spdk_nvme_transport_register_pcie();
}

// DMA buffer
void *curvine_spdk_dma_malloc(uint64_t size, uint64_t align) {
    return spdk_dma_zmalloc(size, align, NULL);
}
void curvine_spdk_dma_free(void *buf) {
    spdk_dma_free(buf);
}

// I/O qpair
struct spdk_nvme_qpair *curvine_spdk_alloc_io_qpair(struct spdk_nvme_ctrlr *ctrlr) {
    struct spdk_nvme_io_qpair_opts opts;
    spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &opts, sizeof(opts));
    return spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, &opts, sizeof(opts));
}
void curvine_spdk_free_io_qpair(struct spdk_nvme_qpair *qpair) {
    if (qpair) spdk_nvme_ctrlr_free_io_qpair(qpair);
}

// I/O context
struct curvine_io_ctx {
    volatile bool done;
    int status;
};
static void curvine_cb(void *arg, const struct spdk_nvme_cpl *cpl) {
    struct curvine_io_ctx *ctx = arg;
    if (spdk_nvme_cpl_is_error(cpl)) {
        ctx->status = -((int)cpl->status.sct * 256 + (int)cpl->status.sc);
        if (ctx->status == 0) ctx->status = -1;
    } else {
        ctx->status = 0;
    }
    ctx->done = true;
}
static int curvine_poll(struct spdk_nvme_qpair *qpair, struct curvine_io_ctx *ctx, uint64_t timeout_us) {
    uint64_t start = 0, hz = 0, ticks = 0;
    if (timeout_us > 0) {
        start = spdk_get_ticks();
        hz = spdk_get_ticks_hz();
        ticks = timeout_us * hz / 1000000ULL;
    }
    while (!ctx->done) {
        int rc = spdk_nvme_qpair_process_completions(qpair, 0);
        if (rc < 0) return rc;
        if (timeout_us > 0 && (spdk_get_ticks() - start) > ticks) return -ETIMEDOUT;
    }
    return ctx->status;
}

// Sync I/O
int curvine_spdk_ns_read(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
                         void *buf, uint64_t offset, uint64_t nbytes, uint64_t timeout_us) {
    uint32_t ss = spdk_nvme_ns_get_sector_size(ns);
    struct curvine_io_ctx ctx = { .done = false, .status = 0 };
    int rc = spdk_nvme_ns_cmd_read(ns, qpair, buf, offset / ss, nbytes / ss, curvine_cb, &ctx, 0);
    return rc ? rc : curvine_poll(qpair, &ctx, timeout_us);
}
int curvine_spdk_ns_write(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
                          void *buf, uint64_t offset, uint64_t nbytes, uint64_t timeout_us) {
    uint32_t ss = spdk_nvme_ns_get_sector_size(ns);
    struct curvine_io_ctx ctx = { .done = false, .status = 0 };
    int rc = spdk_nvme_ns_cmd_write(ns, qpair, buf, offset / ss, nbytes / ss, curvine_cb, &ctx, 0);
    return rc ? rc : curvine_poll(qpair, &ctx, timeout_us);
}
int curvine_spdk_ns_flush(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, uint64_t timeout_us) {
    struct curvine_io_ctx ctx = { .done = false, .status = 0 };
    int rc = spdk_nvme_ns_cmd_flush(ns, qpair, curvine_cb, &ctx);
    return rc ? rc : curvine_poll(qpair, &ctx, timeout_us);
}

// Async I/O
typedef void (*curvine_async_cb)(void *arg, int status);
struct curvine_async_ctx {
    curvine_async_cb cb;
    void *cb_arg;
};
static void curvine_async_cb_fn(void *arg, const struct spdk_nvme_cpl *cpl) {
    struct curvine_async_ctx *ctx = arg;
    int s = 0;
    if (spdk_nvme_cpl_is_error(cpl)) {
        s = -((int)cpl->status.sct * 256 + (int)cpl->status.sc);
        if (s == 0) s = -1;
    }
    ctx->cb(ctx->cb_arg, s);
}
int curvine_spdk_ns_submit_read(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
                               void *buf, uint64_t offset, uint64_t nbytes, struct curvine_async_ctx *ctx) {
    uint32_t ss = spdk_nvme_ns_get_sector_size(ns);
    return spdk_nvme_ns_cmd_read(ns, qpair, buf, offset / ss, nbytes / ss, curvine_async_cb_fn, ctx, 0);
}
int curvine_spdk_ns_submit_write(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
                                  void *buf, uint64_t offset, uint64_t nbytes, struct curvine_async_ctx *ctx) {
    uint32_t ss = spdk_nvme_ns_get_sector_size(ns);
    return spdk_nvme_ns_cmd_write(ns, qpair, buf, offset / ss, nbytes / ss, curvine_async_cb_fn, ctx, 0);
}
int curvine_spdk_ns_submit_flush(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
                                  struct curvine_async_ctx *ctx) {
    return spdk_nvme_ns_cmd_flush(ns, qpair, curvine_async_cb_fn, ctx);
}
int curvine_spdk_qpair_poll(struct spdk_nvme_qpair *qpair, int max_completions) {
    return spdk_nvme_qpair_process_completions(qpair, max_completions);
}
size_t curvine_spdk_async_ctx_sizeof(void) {
    return sizeof(struct curvine_async_ctx);
}
void curvine_spdk_async_ctx_init(struct curvine_async_ctx *ctx, curvine_async_cb cb, void *cb_arg) {
    ctx->cb = cb;
    ctx->cb_arg = cb_arg;
}
