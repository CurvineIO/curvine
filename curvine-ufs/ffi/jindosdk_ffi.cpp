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

// C wrapper implementation for JindoSDK C library
// This file provides a simplified C-compatible interface to JindoSDK C API

#include "jindosdk_ffi.h"
#include <string>
#include <map>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <chrono>
#include <algorithm>
#include <atomic>
#include <cstdio>
#include <memory>
#include <mutex>
#include <vector>

// Include JindoSDK C API headers
#include "jdo_api.h"
#include "jdo_data_types.h"
#include "jdo_common.h"
#include "jdo_error.h"
#include "jdo_defines.h"
#include "jdo_options.h"
#include "jdo_file_status.h"
#include "jdo_list_dir_result.h"
#include "jdo_content_summary.h"

// Thread-local error message storage
thread_local std::string g_last_error;

static constexpr size_t kDefaultFilesystemStoreShards = 16;
static constexpr size_t kMaxFilesystemStoreShards = 64;
static constexpr int64_t kFilesystemStoreShardWaitTimeoutMs = 3 * 1000;
static constexpr int64_t kFilesystemCloseWaitTimeoutMs = 30 * 1000;

struct JindoFileSystemWrapper;

// =========================
// Async helper definitions
// =========================

static inline JindoStatus map_error_code_to_status(int32_t code) {
    // Best-effort mapping for a few common error codes.
    // See /usr/local/include/jindosdk/jdo_error.h for the full list.
    if (code == 3001 /* JDO_FILE_NOT_FOUND_ERROR */
        || code == 30001 /* JDO_PARENT_NOT_FOUND_ERROR */) {
        return JINDO_STATUS_FILE_NOT_FOUND;
    }
    return JINDO_STATUS_ERROR;
}

static inline bool is_reusable_operation_ctx(JindoStatus status) {
    return status == JINDO_STATUS_OK || status == JINDO_STATUS_FILE_NOT_FOUND;
}

struct AsyncBase {
    JdoOperationCall_t call {nullptr};
    JdoOptions_t options {nullptr};
    JdoHandleCtx_t handle_ctx {nullptr};
    struct JindoStoreShard* store_shard {nullptr};
    JindoFileSystemWrapper* fs_wrapper {nullptr};
    void* userdata {nullptr};
};

struct AsyncStatusCtx : public AsyncBase {
    JindoStatusCallback cb {nullptr};
    char* s1 {nullptr};
    char* s2 {nullptr};
    char* s3 {nullptr};
};

struct AsyncI64Ctx : public AsyncBase {
    JindoI64ResultCallback cb {nullptr};
};

static inline void free_async_strings(AsyncStatusCtx* c) {
    if (!c) return;
    if (c->s1) free(c->s1);
    if (c->s2) free(c->s2);
    if (c->s3) free(c->s3);
}

// Config wrapper - stores configuration as key-value pairs
struct JindoConfigWrapper {
    std::map<std::string, std::string> string_configs;
    std::map<std::string, bool> bool_configs;
};

struct JindoStoreShard {
    std::timed_mutex mutex;
    JdoStore_t store {nullptr};
    JdoHandleCtx_t idle_ctx {nullptr};
};

// FileSystem wrapper - stores JindoSDK handles
struct JindoFileSystemWrapper {
    std::string uri;
    std::string user;
    std::map<std::string, std::string> string_configs;
    std::map<std::string, bool> bool_configs;
    std::vector<std::unique_ptr<JindoStoreShard>> store_shards;
    std::atomic<size_t> next_shard {0};
    std::atomic<bool> initialized {false};
    std::atomic<bool> closing {false};
};

static size_t filesystem_store_shard_count() {
    const char* raw = std::getenv("CURVINE_OSS_HDFS_STORE_POOL_SIZE");
    if (!raw || raw[0] == '\0') {
        return kDefaultFilesystemStoreShards;
    }

    char* end = nullptr;
    unsigned long long parsed = std::strtoull(raw, &end, 10);
    if (end == raw || parsed == 0) {
        return kDefaultFilesystemStoreShards;
    }

    return std::min(static_cast<size_t>(parsed), kMaxFilesystemStoreShards);
}

static JdoStore_t create_store(JindoFileSystemWrapper* wrapper) {
    if (!wrapper) {
        g_last_error = "Filesystem not initialized";
        return nullptr;
    }

    JdoOptions_t options = jdo_createOptions();
    if (!options) {
        g_last_error = "Failed to create options";
        return nullptr;
    }

    for (const auto& kv : wrapper->string_configs) {
        jdo_setOption(options, kv.first.c_str(), kv.second.c_str());
    }

    for (const auto& kv : wrapper->bool_configs) {
        jdo_setOption(options, kv.first.c_str(), kv.second ? "true" : "false");
    }

    JdoStore_t store = jdo_createStore(options, wrapper->uri.c_str());
    jdo_freeOptions(options);

    if (!store) {
        g_last_error = "Failed to create store";
        return nullptr;
    }

    return store;
}

static bool ensure_store_shard_locked(JindoFileSystemWrapper* wrapper, JindoStoreShard* shard) {
    if (!wrapper || !shard) {
        g_last_error = "Filesystem not initialized";
        return false;
    }

    if (shard->store) {
        return true;
    }

    shard->store = create_store(wrapper);
    return shard->store != nullptr;
}

static JdoHandleCtx_t create_handle_ctx(JindoFileSystemWrapper* wrapper, JindoStoreShard* shard) {
    if (!wrapper || !shard || !shard->store) {
        g_last_error = "Filesystem not initialized";
        return nullptr;
    }

    JdoHandleCtx_t ctx = jdo_createHandleCtx1(shard->store);
    if (!ctx) {
        g_last_error = "Failed to create handle context";
        return nullptr;
    }

    jdo_init(ctx, wrapper->user.c_str());
    int32_t error_code = jdo_getHandleCtxErrorCode(ctx);
    if (error_code != 0) {
        const char* error_msg = jdo_getHandleCtxErrorMsg(ctx);
        g_last_error = error_msg ? error_msg : "Unknown error";
        jdo_freeHandleCtx(ctx);
        return nullptr;
    }

    return ctx;
}

static bool prepare_store_shard_locked(
        JindoFileSystemWrapper* wrapper,
        JindoStoreShard* shard,
        AsyncBase* c) {
    if (wrapper->closing.load(std::memory_order_acquire)
            || !wrapper->initialized.load(std::memory_order_acquire)) {
        g_last_error = "Filesystem is closing";
        return false;
    }

    if (!ensure_store_shard_locked(wrapper, shard)) {
        return false;
    }

    JdoHandleCtx_t ctx = shard->idle_ctx;
    shard->idle_ctx = nullptr;
    if (!ctx) {
        ctx = create_handle_ctx(wrapper, shard);
    }
    if (!ctx) {
        return false;
    }

    c->fs_wrapper = wrapper;
    c->store_shard = shard;
    c->handle_ctx = ctx;
    return true;
}

static bool acquire_operation_ctx(JindoFileSystemWrapper* wrapper, AsyncBase* c) {
    if (!wrapper || !c || wrapper->store_shards.empty()) {
        g_last_error = "Filesystem not initialized";
        return false;
    }

    size_t shard_count = wrapper->store_shards.size();
    size_t start = wrapper->next_shard.fetch_add(1, std::memory_order_relaxed) % shard_count;
    for (size_t i = 0; i < shard_count; ++i) {
        auto* shard = wrapper->store_shards[(start + i) % shard_count].get();
        if (!shard->mutex.try_lock()) {
            continue;
        }
        bool ok = prepare_store_shard_locked(wrapper, shard, c);
        if (!ok) {
            shard->mutex.unlock();
        }
        return ok;
    }

    auto* shard = wrapper->store_shards[start].get();
    if (!shard->mutex.try_lock_for(
            std::chrono::milliseconds(kFilesystemStoreShardWaitTimeoutMs))) {
        g_last_error = "Timed out waiting for OSS-HDFS store shard";
        return false;
    }

    bool ok = prepare_store_shard_locked(wrapper, shard, c);
    if (!ok) {
        shard->mutex.unlock();
    }
    return ok;
}

static void release_operation_ctx(AsyncBase* c, bool reusable) {
    if (!c || !c->handle_ctx) {
        return;
    }

    auto* wrapper = c->fs_wrapper;
    auto* shard = c->store_shard;
    JdoHandleCtx_t ctx = c->handle_ctx;
    c->handle_ctx = nullptr;
    c->store_shard = nullptr;

    if (shard) {
        bool keep = reusable
                && wrapper
                && !wrapper->closing.load(std::memory_order_acquire)
                && shard->idle_ctx == nullptr;
        if (keep) {
            shard->idle_ctx = ctx;
        } else {
            jdo_freeHandleCtx(ctx);
        }
        shard->mutex.unlock();
    } else {
        jdo_freeHandleCtx(ctx);
    }
}

static inline void finish_async_base(AsyncBase* c, bool reusable) {
    if (!c) return;
    if (c->call) {
        jdo_freeOperationCall(c->call);
        c->call = nullptr;
    }
    if (c->options) {
        jdo_freeOptions(c->options);
        c->options = nullptr;
    }
    if (c->handle_ctx) {
        release_operation_ctx(c, reusable);
    }
}

static inline void free_io_async_base(AsyncBase* c) {
    if (!c) return;
    if (c->call) jdo_freeOperationCall(c->call);
    if (c->options) jdo_freeOptions(c->options);
}

static bool prepare_async_context(JindoFileSystemWrapper* wrapper, AsyncBase* c) {
    if (!c) {
        g_last_error = "Invalid async context";
        return false;
    }

    if (!acquire_operation_ctx(wrapper, c)) {
        return false;
    }

    c->options = jdo_createOptions();
    if (!c->options) {
        g_last_error = "Failed to create options";
        finish_async_base(c, true);
        return false;
    }

    return true;
}

static JindoStatus status_from_bool_result(
        JdoHandleCtx_t ctx,
        bool ok,
        const char** err,
        int32_t* code) {
    *err = nullptr;
    *code = jdo_getHandleCtxErrorCode(ctx);
    if (*code != 0 || !ok) {
        *err = jdo_getHandleCtxErrorMsg(ctx);
        return map_error_code_to_status(*code);
    }

    return JINDO_STATUS_OK;
}

static JindoFileInfo* copy_file_status(
        JdoFileStatus_t file_status,
        JindoStatus* status,
        const char** err) {
    if (*status != JINDO_STATUS_OK || !file_status) {
        return nullptr;
    }

    auto* out = (JindoFileInfo*)malloc(sizeof(JindoFileInfo));
    if (!out) {
        *status = JINDO_STATUS_ERROR;
        *err = "malloc failed";
        return nullptr;
    }

    memset(out, 0, sizeof(JindoFileInfo));
    const char* file_path = jdo_getFileStatusPath(file_status);
    const char* owner = jdo_getFileStatusUser(file_status);
    const char* group = jdo_getFileStatusGroup(file_status);
    out->path = file_path ? strdup(file_path) : nullptr;
    out->user = owner ? strdup(owner) : nullptr;
    out->group = group ? strdup(group) : nullptr;
    out->type = jdo_getFileStatusType(file_status);
    out->perm = jdo_getFileStatusPerm(file_status);
    out->length = jdo_getFileStatusSize(file_status);
    out->mtime = jdo_getFileStatusMtime(file_status);
    out->atime = jdo_getFileStatusAtime(file_status);
    return out;
}

static JindoListResult* copy_list_result(
        JdoListDirResult_t list_result,
        JindoStatus* status,
        const char** err) {
    if (*status != JINDO_STATUS_OK || !list_result) {
        return nullptr;
    }

    auto* out = (JindoListResult*)malloc(sizeof(JindoListResult));
    if (!out) {
        *status = JINDO_STATUS_ERROR;
        *err = "malloc failed";
        return nullptr;
    }

    memset(out, 0, sizeof(JindoListResult));
    int64_t count = jdo_getListDirResultSize(list_result);
    if (count <= 0) {
        out->count = 0;
        return out;
    }

    out->count = (size_t)count;
    out->file_infos = (JindoFileInfo*)malloc(sizeof(JindoFileInfo) * count);
    if (!out->file_infos) {
        free(out);
        *status = JINDO_STATUS_ERROR;
        *err = "malloc failed";
        return nullptr;
    }

    memset(out->file_infos, 0, sizeof(JindoFileInfo) * count);
    for (int64_t i = 0; i < count; i++) {
        JdoFileStatus_t file_status = jdo_getListDirFileStatus(list_result, i);
        if (!file_status) {
            for (int64_t j = 0; j < i; j++) {
                if (out->file_infos[j].path) free(out->file_infos[j].path);
                if (out->file_infos[j].user) free(out->file_infos[j].user);
                if (out->file_infos[j].group) free(out->file_infos[j].group);
            }
            free(out->file_infos);
            free(out);
            *status = JINDO_STATUS_ERROR;
            *err = "listDir returned a null file status";
            return nullptr;
        }

        const char* file_path = jdo_getFileStatusPath(file_status);
        const char* owner = jdo_getFileStatusUser(file_status);
        const char* group = jdo_getFileStatusGroup(file_status);
        out->file_infos[i].path = file_path ? strdup(file_path) : nullptr;
        out->file_infos[i].user = owner ? strdup(owner) : nullptr;
        out->file_infos[i].group = group ? strdup(group) : nullptr;
        out->file_infos[i].type = jdo_getFileStatusType(file_status);
        out->file_infos[i].perm = jdo_getFileStatusPerm(file_status);
        out->file_infos[i].length = jdo_getFileStatusSize(file_status);
        out->file_infos[i].mtime = jdo_getFileStatusMtime(file_status);
        out->file_infos[i].atime = jdo_getFileStatusAtime(file_status);
    }

    return out;
}

// Writer/Reader wrapper - stores IO context and handle context
struct JindoIOWrapper {
    JdoIOContext_t io_ctx;
    JdoHandleCtx_t handle_ctx;
    JdoStore_t store;  // Reference to store for context creation
};

extern "C" {

// Config functions
JindoConfigHandle jindo_config_new() {
    try {
        return new JindoConfigWrapper();
    } catch (...) {
        g_last_error = "Failed to create config";
        return nullptr;
    }
}

void jindo_config_set_string(JindoConfigHandle config, const char* key, const char* value) {
    if (!config || !key || !value) return;
    try {
        auto* cfg = reinterpret_cast<JindoConfigWrapper*>(config);
        cfg->string_configs[key] = value;
    } catch (const std::exception& e) {
        g_last_error = e.what();
    }
}

void jindo_config_set_bool(JindoConfigHandle config, const char* key, bool value) {
    if (!config || !key) return;
    try {
        auto* cfg = reinterpret_cast<JindoConfigWrapper*>(config);
        cfg->bool_configs[key] = value;
    } catch (const std::exception& e) {
        g_last_error = e.what();
    }
}

void jindo_config_free(JindoConfigHandle config) {
    if (config) {
        delete reinterpret_cast<JindoConfigWrapper*>(config);
    }
}

// FileSystem functions
JindoFileSystemHandle jindo_filesystem_new() {
    try {
        auto* wrapper = new JindoFileSystemWrapper();
        wrapper->initialized.store(false, std::memory_order_release);
        wrapper->closing.store(false, std::memory_order_release);
        wrapper->next_shard.store(0, std::memory_order_release);
        return wrapper;
    } catch (...) {
        g_last_error = "Failed to create filesystem";
        return nullptr;
    }
}

JindoStatus jindo_filesystem_init(
    JindoFileSystemHandle fs,
    const char* bucket,
    const char* user,
    JindoConfigHandle config
) {
    if (!fs || !bucket || !user || !config) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        auto* cfg = reinterpret_cast<JindoConfigWrapper*>(config);
        
        // Build URI
        wrapper->uri = std::string(bucket);
        wrapper->user = std::string(user);
        wrapper->string_configs = cfg->string_configs;
        wrapper->bool_configs = cfg->bool_configs;
        wrapper->closing.store(false, std::memory_order_release);
        wrapper->initialized.store(false, std::memory_order_release);
        wrapper->next_shard.store(0, std::memory_order_release);

        wrapper->store_shards.clear();
        size_t shard_count = filesystem_store_shard_count();
        wrapper->store_shards.reserve(shard_count);
        for (size_t i = 0; i < shard_count; ++i) {
            wrapper->store_shards.emplace_back(new JindoStoreShard());
        }
        
        // Validate one shard eagerly. The remaining shards are created lazily on first use.
        auto* first_shard = wrapper->store_shards[0].get();
        first_shard->mutex.lock();
        if (!ensure_store_shard_locked(wrapper, first_shard)) {
            first_shard->mutex.unlock();
            wrapper->store_shards.clear();
            return JINDO_STATUS_ERROR;
        }

        JdoHandleCtx_t init_ctx = create_handle_ctx(wrapper, first_shard);
        if (!init_ctx) {
            if (first_shard->store) {
                jdo_destroyStore(first_shard->store);
                first_shard->store = nullptr;
            }
            first_shard->mutex.unlock();
            wrapper->store_shards.clear();
            return JINDO_STATUS_ERROR;
        }
        
        // Check for errors
        int32_t error_code = jdo_getHandleCtxErrorCode(init_ctx);
        if (error_code != 0) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(init_ctx);
            g_last_error = error_msg ? error_msg : "Unknown error";
            jdo_freeHandleCtx(init_ctx);
            if (first_shard->store) {
                jdo_destroyStore(first_shard->store);
                first_shard->store = nullptr;
            }
            first_shard->mutex.unlock();
            wrapper->store_shards.clear();
            return JINDO_STATUS_ERROR;
        }
        jdo_freeHandleCtx(init_ctx);
        first_shard->mutex.unlock();
        
        wrapper->initialized.store(true, std::memory_order_release);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

void jindo_filesystem_free(JindoFileSystemHandle fs) {
    if (!fs) return;
    auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);

    wrapper->closing.store(true, std::memory_order_release);
    wrapper->initialized.store(false, std::memory_order_release);

    std::vector<JindoStoreShard*> locked_shards;
    locked_shards.reserve(wrapper->store_shards.size());
    auto timeout = std::chrono::milliseconds(kFilesystemCloseWaitTimeoutMs);
    for (auto& shard_owner : wrapper->store_shards) {
        auto* shard = shard_owner.get();
        if (!shard->mutex.try_lock_for(timeout)) {
            // Keep wrapper/stores alive for in-flight native operations. This avoids use-after-free
            // at shutdown without blocking Rust Drop indefinitely.
            g_last_error =
                "Timed out waiting for OSS-HDFS store shard while closing filesystem; leaving wrapper allocated";
            std::fprintf(stderr, "curvine oss-hdfs: %s\n", g_last_error.c_str());
            for (auto* locked : locked_shards) {
                locked->mutex.unlock();
            }
            return;
        }
        locked_shards.push_back(shard);
    }

    for (auto* shard : locked_shards) {
        if (shard->idle_ctx) {
            jdo_freeHandleCtx(shard->idle_ctx);
            shard->idle_ctx = nullptr;
        }
        if (shard->store) {
            jdo_destroyStore(shard->store);
            shard->store = nullptr;
        }
        shard->mutex.unlock();
    }
    delete wrapper;
}

// Filesystem metadata/open operations keep the async ABI used by Rust, but intentionally
// call JindoSDK's synchronous APIs internally. Production cores showed native crashes in
// Jindo async metadata entry points, especially jdo_getFileStatusAsync.

JindoStatus jindo_filesystem_mkdir_async(JindoFileSystemHandle fs, const char* path, bool recursive, JindoStatusCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(wrapper, &c)) {
            return JINDO_STATUS_ERROR;
        }

        bool ok = jdo_mkdir(c.handle_ctx, path, recursive, 0755, c.options);
        const char* err = nullptr;
        int32_t code = 0;
        JindoStatus status = status_from_bool_result(c.handle_ctx, ok, &err, &code);
        cb(status, err, userdata);
        finish_async_base(&c, is_reusable_operation_ctx(status));
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_rename_async(JindoFileSystemHandle fs, const char* oldpath, const char* newpath, JindoStatusCallback cb, void* userdata) {
    if (!fs || !oldpath || !newpath || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(wrapper, &c)) {
            return JINDO_STATUS_ERROR;
        }

        bool ok = jdo_rename(c.handle_ctx, oldpath, newpath, c.options);
        const char* err = nullptr;
        int32_t code = 0;
        JindoStatus status = status_from_bool_result(c.handle_ctx, ok, &err, &code);
        cb(status, err, userdata);
        finish_async_base(&c, is_reusable_operation_ctx(status));
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_remove_async(JindoFileSystemHandle fs, const char* path, bool recursive, JindoStatusCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(wrapper, &c)) {
            return JINDO_STATUS_ERROR;
        }

        bool ok = jdo_remove(c.handle_ctx, path, recursive, c.options);
        const char* err = nullptr;
        int32_t code = 0;
        JindoStatus status = status_from_bool_result(c.handle_ctx, ok, &err, &code);
        cb(status, err, userdata);
        finish_async_base(&c, is_reusable_operation_ctx(status));
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_exists_async(JindoFileSystemHandle fs, const char* path, JindoBoolResultCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(wrapper, &c)) {
            return JINDO_STATUS_ERROR;
        }

        bool exists = jdo_exists(c.handle_ctx, path, c.options);
        const char* err = nullptr;
        int32_t code = jdo_getHandleCtxErrorCode(c.handle_ctx);
        JindoStatus status = JINDO_STATUS_OK;
        if (code != 0) {
            err = jdo_getHandleCtxErrorMsg(c.handle_ctx);
            status = map_error_code_to_status(code);
            if (status == JINDO_STATUS_FILE_NOT_FOUND) {
                status = JINDO_STATUS_OK;
                exists = false;
            } else {
                status = JINDO_STATUS_ERROR;
            }
        }

        cb(status, exists, err, userdata);
        finish_async_base(&c, is_reusable_operation_ctx(status));
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_get_file_info_async(JindoFileSystemHandle fs, const char* path, JindoFileInfoResultCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(wrapper, &c)) {
            return JINDO_STATUS_ERROR;
        }

        JdoFileStatus_t file_status = jdo_getFileStatus(c.handle_ctx, path, c.options);
        const char* err = nullptr;
        int32_t code = jdo_getHandleCtxErrorCode(c.handle_ctx);
        JindoStatus status = JINDO_STATUS_OK;
        if (code != 0 || !file_status) {
            err = jdo_getHandleCtxErrorMsg(c.handle_ctx);
            status = map_error_code_to_status(code);
            if (status == JINDO_STATUS_ERROR && !file_status) {
                status = JINDO_STATUS_FILE_NOT_FOUND;
            }
        }

        JindoFileInfo* out = copy_file_status(file_status, &status, &err);
        if (file_status) {
            jdo_freeFileStatus(file_status);
        }

        cb(status, out, err, userdata);
        finish_async_base(&c, is_reusable_operation_ctx(status));
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

void jindo_file_info_free(JindoFileInfo* info) {
    if (!info) return;
    if (info->path) free(info->path);
    if (info->user) free(info->user);
    if (info->group) free(info->group);
}

JindoStatus jindo_filesystem_list_dir_async(JindoFileSystemHandle fs, const char* path, bool recursive, JindoListResultCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(wrapper, &c)) {
            return JINDO_STATUS_ERROR;
        }

        JdoListDirResult_t list_result = jdo_listDir(c.handle_ctx, path, recursive, c.options);
        const char* err = nullptr;
        int32_t code = jdo_getHandleCtxErrorCode(c.handle_ctx);
        JindoStatus status = JINDO_STATUS_OK;
        if (code != 0 || !list_result) {
            err = jdo_getHandleCtxErrorMsg(c.handle_ctx);
            status = map_error_code_to_status(code);
            if (!list_result && code == 0) {
                status = JINDO_STATUS_ERROR;
            }
        }

        JindoListResult* out = copy_list_result(list_result, &status, &err);
        if (list_result) {
            jdo_freeListDirResult(list_result);
        }

        cb(status, out, err, userdata);
        finish_async_base(&c, is_reusable_operation_ctx(status));
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

void jindo_list_result_free(JindoListResult* result) {
    if (!result) return;
    if (result->file_infos) {
        for (size_t i = 0; i < result->count; i++) {
            if (result->file_infos[i].path) free(result->file_infos[i].path);
            if (result->file_infos[i].user) free(result->file_infos[i].user);
            if (result->file_infos[i].group) free(result->file_infos[i].group);
        }
        free(result->file_infos);
    }
    result->count = 0;
}

JindoStatus jindo_filesystem_get_content_summary_async(JindoFileSystemHandle fs, const char* path, bool recursive, JindoContentSummaryResultCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(wrapper, &c)) {
            return JINDO_STATUS_ERROR;
        }

        JdoContentSummary_t summary = jdo_getContentSummary(c.handle_ctx, path, recursive, c.options);
        const char* err = nullptr;
        int32_t code = jdo_getHandleCtxErrorCode(c.handle_ctx);
        JindoStatus status = JINDO_STATUS_OK;
        if (code != 0 || !summary) {
            err = jdo_getHandleCtxErrorMsg(c.handle_ctx);
            status = map_error_code_to_status(code);
            if (!summary && code == 0) {
                status = JINDO_STATUS_ERROR;
            }
        }

        JindoContentSummary out;
        JindoContentSummary* out_ptr = nullptr;
        if (status == JINDO_STATUS_OK && summary) {
            out.file_count = jdo_getContentSummaryFileCount(summary);
            out.dir_count = jdo_getContentSummaryDirectoryCount(summary);
            out.file_size = jdo_getContentSummaryFileSize(summary);
            out_ptr = &out;
        }

        if (summary) {
            jdo_freeContentSummary(summary);
        }

        cb(status, out_ptr, err, userdata);
        finish_async_base(&c, is_reusable_operation_ctx(status));
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

// OSS-HDFS specific metadata operations.

JindoStatus jindo_filesystem_set_permission_async(JindoFileSystemHandle fs, const char* path, int16_t perm, JindoStatusCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(wrapper, &c)) {
            return JINDO_STATUS_ERROR;
        }

        bool ok = jdo_setPermission(c.handle_ctx, path, perm, c.options);
        const char* err = nullptr;
        int32_t code = 0;
        JindoStatus status = status_from_bool_result(c.handle_ctx, ok, &err, &code);
        cb(status, err, userdata);
        finish_async_base(&c, is_reusable_operation_ctx(status));
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_set_owner_async(JindoFileSystemHandle fs, const char* path, const char* user, const char* group, JindoStatusCallback cb, void* userdata) {
    if (!fs || !path || !user || !group || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(wrapper, &c)) {
            return JINDO_STATUS_ERROR;
        }

        bool ok = jdo_setOwner(c.handle_ctx, path, user, group, c.options);
        const char* err = nullptr;
        int32_t code = 0;
        JindoStatus status = status_from_bool_result(c.handle_ctx, ok, &err, &code);
        cb(status, err, userdata);
        finish_async_base(&c, is_reusable_operation_ctx(status));
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

// Writer functions.

void jindo_writer_free(JindoWriterHandle writer) {
    if (!writer) return;
    auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(writer);
    jdo_freeHandleCtx(io_wrapper->handle_ctx);
    jdo_freeIOContext(io_wrapper->io_ctx);
    delete io_wrapper;
}

// Reader functions.

void jindo_reader_free(JindoReaderHandle reader) {
    if (!reader) return;
    auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(reader);
    jdo_freeHandleCtx(io_wrapper->handle_ctx);
    jdo_freeIOContext(io_wrapper->io_ctx);
    delete io_wrapper;
}

static void jindo_internal_io_status_cb(JdoHandleCtx_t ctx, bool ok, void* ud) {
    auto* c = reinterpret_cast<AsyncStatusCtx*>(ud);
    const char* err = nullptr;
    JindoStatus st = JINDO_STATUS_OK;
    int32_t code = jdo_getHandleCtxErrorCode(ctx);
    if (code != 0 || !ok) {
        err = jdo_getHandleCtxErrorMsg(ctx);
        st = map_error_code_to_status(code);
    }

    if (c && c->cb) {
        c->cb(st, err, c->userdata);
    }

    if (c) {
        free_io_async_base(c);
        free_async_strings(c);
        delete c;
    }
}

JindoStatus jindo_filesystem_open_writer_async(JindoFileSystemHandle fs, const char* path, JindoOpenWriterCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* fsw = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(fsw, &c)) {
            return JINDO_STATUS_ERROR;
        }

        JdoIOContext_t io_ctx = jdo_open(
            c.handle_ctx,
            path,
            JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_OVERWRITE,
            0644,
            c.options
        );

        const char* err = nullptr;
        int32_t code = jdo_getHandleCtxErrorCode(c.handle_ctx);
        JindoStatus status = JINDO_STATUS_OK;
        if (code != 0 || !io_ctx) {
            err = jdo_getHandleCtxErrorMsg(c.handle_ctx);
            status = map_error_code_to_status(code);
            cb(status, nullptr, err, userdata);
            if (io_ctx) {
                jdo_freeIOContext(io_ctx);
            }
            finish_async_base(&c, false);
            return JINDO_STATUS_OK;
        }

        JdoHandleCtx_t handle_ctx = jdo_createHandleCtx2(c.store_shard->store, io_ctx);
        if (!handle_ctx) {
            err = "Failed to create handle context for IO";
            cb(JINDO_STATUS_ERROR, nullptr, err, userdata);
            jdo_freeIOContext(io_ctx);
            finish_async_base(&c, false);
            return JINDO_STATUS_OK;
        }

        auto* io_wrapper = new JindoIOWrapper();
        io_wrapper->io_ctx = io_ctx;
        io_wrapper->handle_ctx = handle_ctx;
        io_wrapper->store = c.store_shard->store;

        cb(JINDO_STATUS_OK, io_wrapper, nullptr, userdata);
        finish_async_base(&c, true);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_open_writer_append_async(JindoFileSystemHandle fs, const char* path, JindoOpenWriterCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* fsw = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(fsw, &c)) {
            return JINDO_STATUS_ERROR;
        }

        JdoIOContext_t io_ctx = jdo_open(
            c.handle_ctx,
            path,
            JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_APPEND,
            0644,
            c.options
        );

        const char* err = nullptr;
        int32_t code = jdo_getHandleCtxErrorCode(c.handle_ctx);
        JindoStatus status = JINDO_STATUS_OK;
        if (code != 0 || !io_ctx) {
            err = jdo_getHandleCtxErrorMsg(c.handle_ctx);
            status = map_error_code_to_status(code);
            cb(status, nullptr, err, userdata);
            if (io_ctx) {
                jdo_freeIOContext(io_ctx);
            }
            finish_async_base(&c, false);
            return JINDO_STATUS_OK;
        }

        JdoHandleCtx_t handle_ctx = jdo_createHandleCtx2(c.store_shard->store, io_ctx);
        if (!handle_ctx) {
            err = "Failed to create handle context for IO";
            cb(JINDO_STATUS_ERROR, nullptr, err, userdata);
            jdo_freeIOContext(io_ctx);
            finish_async_base(&c, false);
            return JINDO_STATUS_OK;
        }

        auto* io_wrapper = new JindoIOWrapper();
        io_wrapper->io_ctx = io_ctx;
        io_wrapper->handle_ctx = handle_ctx;
        io_wrapper->store = c.store_shard->store;

        cb(JINDO_STATUS_OK, io_wrapper, nullptr, userdata);
        finish_async_base(&c, true);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_open_reader_async(JindoFileSystemHandle fs, const char* path, JindoOpenReaderCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* fsw = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        AsyncBase c;
        if (!prepare_async_context(fsw, &c)) {
            return JINDO_STATUS_ERROR;
        }

        JdoIOContext_t io_ctx = jdo_open(
            c.handle_ctx,
            path,
            JDO_OPEN_FLAG_READ_ONLY,
            0,
            c.options
        );

        const char* err = nullptr;
        int32_t code = jdo_getHandleCtxErrorCode(c.handle_ctx);
        JindoStatus status = JINDO_STATUS_OK;
        if (code != 0 || !io_ctx) {
            err = jdo_getHandleCtxErrorMsg(c.handle_ctx);
            status = map_error_code_to_status(code);
            cb(status, nullptr, err, userdata);
            if (io_ctx) {
                jdo_freeIOContext(io_ctx);
            }
            finish_async_base(&c, false);
            return JINDO_STATUS_OK;
        }

        JdoHandleCtx_t handle_ctx = jdo_createHandleCtx2(c.store_shard->store, io_ctx);
        if (!handle_ctx) {
            err = "Failed to create handle context for IO";
            cb(JINDO_STATUS_ERROR, nullptr, err, userdata);
            jdo_freeIOContext(io_ctx);
            finish_async_base(&c, false);
            return JINDO_STATUS_OK;
        }

        auto* io_wrapper = new JindoIOWrapper();
        io_wrapper->io_ctx = io_ctx;
        io_wrapper->handle_ctx = handle_ctx;
        io_wrapper->store = c.store_shard->store;

        cb(JINDO_STATUS_OK, io_wrapper, nullptr, userdata);
        finish_async_base(&c, true);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

// =========================
// Async IO operations
// =========================

static void jindo_internal_i64_cb(JdoHandleCtx_t ctx, int64_t value, void* ud) {
    auto* c = reinterpret_cast<AsyncI64Ctx*>(ud);
    const char* err = nullptr;
    JindoStatus st = JINDO_STATUS_OK;
    int32_t code = jdo_getHandleCtxErrorCode(ctx);
    if (code != 0) {
        err = jdo_getHandleCtxErrorMsg(ctx);
        st = map_error_code_to_status(code);
    }

    if (c && c->cb) c->cb(st, value, err, c->userdata);

    if (c) {
        free_io_async_base(c);
        delete c;
    }
}

JindoStatus jindo_writer_write_async(JindoWriterHandle writer, const uint8_t* data, size_t len, JindoI64ResultCallback cb, void* userdata) {
    if (!writer || !data || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* io = reinterpret_cast<JindoIOWrapper*>(writer);
        auto* c = new AsyncI64Ctx();
        c->cb = cb;
        c->userdata = userdata;
        c->options = jdo_createOptions();
        if (!c->options) {
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setInt64Callback(c->options, jindo_internal_i64_cb);
        jdo_setCallbackContext(c->options, c);
        c->call = jdo_writeAsync(io->handle_ctx, reinterpret_cast<const char*>(data), (int64_t)len, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(io->handle_ctx);
            g_last_error = em ? em : "writeAsync failed";
            jdo_freeOptions(c->options);
            delete c;
            return JINDO_STATUS_ERROR;
        }
        jdo_perform(io->handle_ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_writer_flush_async(JindoWriterHandle writer, JindoStatusCallback cb, void* userdata) {
    if (!writer || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* io = reinterpret_cast<JindoIOWrapper*>(writer);
        auto* c = new AsyncStatusCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->options = jdo_createOptions();
        if (!c->options) {
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        // bool-returning async op: normalize completion into JindoStatus via shared callback
        jdo_setBoolCallback(c->options, jindo_internal_io_status_cb);
        jdo_setCallbackContext(c->options, c);
        c->call = jdo_flushAsync(io->handle_ctx, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(io->handle_ctx);
            g_last_error = em ? em : "flushAsync failed";
            jdo_freeOptions(c->options);
            delete c;
            return JINDO_STATUS_ERROR;
        }
        jdo_perform(io->handle_ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_writer_tell_async(JindoWriterHandle writer, JindoI64ResultCallback cb, void* userdata) {
    if (!writer || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* io = reinterpret_cast<JindoIOWrapper*>(writer);
        auto* c = new AsyncI64Ctx();
        c->cb = cb;
        c->userdata = userdata;
        c->options = jdo_createOptions();
        if (!c->options) {
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setInt64Callback(c->options, jindo_internal_i64_cb);
        jdo_setCallbackContext(c->options, c);
        c->call = jdo_tellAsync(io->handle_ctx, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(io->handle_ctx);
            g_last_error = em ? em : "tellAsync failed";
            jdo_freeOptions(c->options);
            delete c;
            return JINDO_STATUS_ERROR;
        }
        jdo_perform(io->handle_ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_writer_close_async(JindoWriterHandle writer, JindoStatusCallback cb, void* userdata) {
    if (!writer || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* io = reinterpret_cast<JindoIOWrapper*>(writer);
        auto* c = new AsyncStatusCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->options = jdo_createOptions();
        if (!c->options) {
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        // bool-returning async op: normalize completion into JindoStatus via shared callback
        jdo_setBoolCallback(c->options, jindo_internal_io_status_cb);
        jdo_setCallbackContext(c->options, c);
        c->call = jdo_closeAsync(io->handle_ctx, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(io->handle_ctx);
            g_last_error = em ? em : "closeAsync failed";
            jdo_freeOptions(c->options);
            delete c;
            return JINDO_STATUS_ERROR;
        }
        jdo_perform(io->handle_ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_read_async(JindoReaderHandle reader, size_t n, uint8_t* scratch, JindoI64ResultCallback cb, void* userdata) {
    if (!reader || !scratch || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* io = reinterpret_cast<JindoIOWrapper*>(reader);
        auto* c = new AsyncI64Ctx();
        c->cb = cb;
        c->userdata = userdata;
        c->options = jdo_createOptions();
        if (!c->options) {
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setInt64Callback(c->options, jindo_internal_i64_cb);
        jdo_setCallbackContext(c->options, c);
        c->call = jdo_readAsync(io->handle_ctx, reinterpret_cast<char*>(scratch), (int64_t)n, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(io->handle_ctx);
            g_last_error = em ? em : "readAsync failed";
            jdo_freeOptions(c->options);
            delete c;
            return JINDO_STATUS_ERROR;
        }
        jdo_perform(io->handle_ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_pread_async(JindoReaderHandle reader, int64_t offset, size_t n, uint8_t* scratch, JindoI64ResultCallback cb, void* userdata) {
    if (!reader || !scratch || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* io = reinterpret_cast<JindoIOWrapper*>(reader);
        auto* c = new AsyncI64Ctx();
        c->cb = cb;
        c->userdata = userdata;
        c->options = jdo_createOptions();
        if (!c->options) {
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setInt64Callback(c->options, jindo_internal_i64_cb);
        jdo_setCallbackContext(c->options, c);
        c->call = jdo_preadAsync(io->handle_ctx, reinterpret_cast<char*>(scratch), (int64_t)n, offset, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(io->handle_ctx);
            g_last_error = em ? em : "preadAsync failed";
            jdo_freeOptions(c->options);
            delete c;
            return JINDO_STATUS_ERROR;
        }
        jdo_perform(io->handle_ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_seek_async(JindoReaderHandle reader, int64_t offset, JindoStatusCallback cb, void* userdata) {
    if (!reader || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* io = reinterpret_cast<JindoIOWrapper*>(reader);
        struct SeekCtx {
            JdoOperationCall_t call {nullptr};
            JdoOptions_t options {nullptr};
            JindoStatusCallback cb {nullptr};
            void* userdata {nullptr};
        };

        JdoInt64Callback seek_cb = [](JdoHandleCtx_t ctx, int64_t value, void* ud) {
            auto* c = reinterpret_cast<SeekCtx*>(ud);
            const char* err = nullptr;
            JindoStatus st = JINDO_STATUS_OK;
            int32_t code = jdo_getHandleCtxErrorCode(ctx);
            if (code != 0 || value < 0) {
                err = jdo_getHandleCtxErrorMsg(ctx);
                st = map_error_code_to_status(code);
            }

            if (c && c->cb) c->cb(st, err, c->userdata);
            if (c) {
                if (c->call) jdo_freeOperationCall(c->call);
                if (c->options) jdo_freeOptions(c->options);
                delete c;
            }
        };

        auto* c = new SeekCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->options = jdo_createOptions();
        if (!c->options) {
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setInt64Callback(c->options, seek_cb);
        jdo_setCallbackContext(c->options, c);
        c->call = jdo_seekAsync(io->handle_ctx, offset, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(io->handle_ctx);
            g_last_error = em ? em : "seekAsync failed";
            jdo_freeOptions(c->options);
            delete c;
            return JINDO_STATUS_ERROR;
        }
        jdo_perform(io->handle_ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_tell_async(JindoReaderHandle reader, JindoI64ResultCallback cb, void* userdata) {
    if (!reader || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* io = reinterpret_cast<JindoIOWrapper*>(reader);
        auto* c = new AsyncI64Ctx();
        c->cb = cb;
        c->userdata = userdata;
        c->options = jdo_createOptions();
        if (!c->options) {
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setInt64Callback(c->options, jindo_internal_i64_cb);
        jdo_setCallbackContext(c->options, c);
        c->call = jdo_tellAsync(io->handle_ctx, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(io->handle_ctx);
            g_last_error = em ? em : "tellAsync failed";
            jdo_freeOptions(c->options);
            delete c;
            return JINDO_STATUS_ERROR;
        }
        jdo_perform(io->handle_ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_get_file_length_async(JindoReaderHandle reader, JindoI64ResultCallback cb, void* userdata) {
    if (!reader || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* io = reinterpret_cast<JindoIOWrapper*>(reader);
        auto* c = new AsyncI64Ctx();
        c->cb = cb;
        c->userdata = userdata;
        c->options = jdo_createOptions();
        if (!c->options) {
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setInt64Callback(c->options, jindo_internal_i64_cb);
        jdo_setCallbackContext(c->options, c);
        c->call = jdo_getFileLengthAsync(io->handle_ctx, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(io->handle_ctx);
            g_last_error = em ? em : "getFileLengthAsync failed";
            jdo_freeOptions(c->options);
            delete c;
            return JINDO_STATUS_ERROR;
        }
        jdo_perform(io->handle_ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_close_async(JindoReaderHandle reader, JindoStatusCallback cb, void* userdata) {
    if (!reader || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* io = reinterpret_cast<JindoIOWrapper*>(reader);
        auto* c = new AsyncStatusCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->options = jdo_createOptions();
        if (!c->options) {
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        // bool-returning async op: normalize completion into JindoStatus via shared callback
        jdo_setBoolCallback(c->options, jindo_internal_io_status_cb);
        jdo_setCallbackContext(c->options, c);
        c->call = jdo_closeAsync(io->handle_ctx, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(io->handle_ctx);
            g_last_error = em ? em : "closeAsync failed";
            jdo_freeOptions(c->options);
            delete c;
            return JINDO_STATUS_ERROR;
        }
        jdo_perform(io->handle_ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

const char* jindo_get_last_error(void) {
    return g_last_error.c_str();
}

void jindo_free(void* p) {
    if (p) free(p);
}

#ifdef CURVINE_OSS_HDFS_FFI_TEST
size_t jindo_test_default_filesystem_store_shards(void) {
    return kDefaultFilesystemStoreShards;
}

size_t jindo_test_max_filesystem_store_shards(void) {
    return kMaxFilesystemStoreShards;
}

int64_t jindo_test_filesystem_store_shard_wait_timeout_ms(void) {
    return kFilesystemStoreShardWaitTimeoutMs;
}

bool jindo_test_is_reusable_operation_ctx(JindoStatus status) {
    return is_reusable_operation_ctx(status);
}
#endif

} // extern "C"
