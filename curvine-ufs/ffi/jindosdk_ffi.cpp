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

// =========================
// Async helper definitions
// =========================

static inline JindoStatus map_error_code_to_status(int32_t code) {
    // Best-effort mapping for a few common error codes.
    // See /usr/local/include/jindosdk/jdo_error.h for the full list.
    if (code == 3001 /* JDO_FILE_NOT_FOUND_ERROR */) {
        return JINDO_STATUS_FILE_NOT_FOUND;
    }
    return JINDO_STATUS_ERROR;
}

struct AsyncBase {
    JdoOperationCall_t call {nullptr};
    JdoOptions_t options {nullptr};
    void* userdata {nullptr};
};

struct AsyncStatusCtx : public AsyncBase {
    JindoStatusCallback cb {nullptr};
    char* s1 {nullptr};
    char* s2 {nullptr};
    char* s3 {nullptr};
};

struct AsyncBoolCtx : public AsyncBase {
    JindoBoolResultCallback cb {nullptr};
    char* s1 {nullptr};
};

struct AsyncFileInfoCtx : public AsyncBase {
    JindoFileInfoResultCallback cb {nullptr};
    char* s1 {nullptr};
};

struct AsyncListDirCtx : public AsyncBase {
    JindoListResultCallback cb {nullptr};
    char* s1 {nullptr};
};

struct AsyncContentSummaryCtx : public AsyncBase {
    JindoContentSummaryResultCallback cb {nullptr};
    char* s1 {nullptr};
};

struct AsyncI64Ctx : public AsyncBase {
    JindoI64ResultCallback cb {nullptr};
};

struct AsyncOpenWriterCtx : public AsyncBase {
    JindoOpenWriterCallback cb {nullptr};
    char* path {nullptr};
};

struct AsyncOpenReaderCtx : public AsyncBase {
    JindoOpenReaderCallback cb {nullptr};
    char* path {nullptr};
};

static inline void free_async_strings(AsyncStatusCtx* c) {
    if (!c) return;
    if (c->s1) free(c->s1);
    if (c->s2) free(c->s2);
    if (c->s3) free(c->s3);
}
static inline void free_async_strings(AsyncBoolCtx* c) {
    if (!c) return;
    if (c->s1) free(c->s1);
}
static inline void free_async_strings(AsyncFileInfoCtx* c) {
    if (!c) return;
    if (c->s1) free(c->s1);
}
static inline void free_async_strings(AsyncListDirCtx* c) {
    if (!c) return;
    if (c->s1) free(c->s1);
}
static inline void free_async_strings(AsyncContentSummaryCtx* c) {
    if (!c) return;
    if (c->s1) free(c->s1);
}
static inline void free_async_strings(AsyncOpenWriterCtx* c) {
    if (!c) return;
    if (c->path) free(c->path);
}
static inline void free_async_strings(AsyncOpenReaderCtx* c) {
    if (!c) return;
    if (c->path) free(c->path);
}

// Config wrapper - stores configuration as key-value pairs
struct JindoConfigWrapper {
    std::map<std::string, std::string> string_configs;
    std::map<std::string, bool> bool_configs;
};

// FileSystem wrapper - stores JindoSDK handles
struct JindoFileSystemWrapper {
    JdoStore_t store;
    JdoHandleCtx_t ctx;
    std::string uri;
    bool initialized;
};

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
        wrapper->store = nullptr;
        wrapper->ctx = nullptr;
        wrapper->initialized = false;
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
        
        // Create JdoOptions and set configuration
        JdoOptions_t options = jdo_createOptions();
        if (!options) {
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        
        // Set string configurations
        for (const auto& kv : cfg->string_configs) {
            jdo_setOption(options, kv.first.c_str(), kv.second.c_str());
        }
        
        // Set bool configurations
        for (const auto& kv : cfg->bool_configs) {
            jdo_setOption(options, kv.first.c_str(), kv.second ? "true" : "false");
        }
        
        // Create store
        wrapper->store = jdo_createStore(options, wrapper->uri.c_str());
        jdo_freeOptions(options);
        
        if (!wrapper->store) {
            g_last_error = "Failed to create store";
            return JINDO_STATUS_ERROR;
        }
        
        // Create handle context
        wrapper->ctx = jdo_createHandleCtx1(wrapper->store);
        if (!wrapper->ctx) {
            g_last_error = "Failed to create handle context";
            jdo_destroyStore(wrapper->store);
            wrapper->store = nullptr;
            return JINDO_STATUS_ERROR;
        }
        
        // Initialize
        jdo_init(wrapper->ctx, user);
        
        // Check for errors
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "Unknown error";
            jdo_freeHandleCtx(wrapper->ctx);
            jdo_destroyStore(wrapper->store);
            wrapper->ctx = nullptr;
            wrapper->store = nullptr;
            return JINDO_STATUS_ERROR;
        }
        
        wrapper->initialized = true;
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

void jindo_filesystem_free(JindoFileSystemHandle fs) {
    if (!fs) return;
    auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
    if (wrapper->ctx) {
        jdo_freeHandleCtx(wrapper->ctx);
    }
    if (wrapper->store) {
        jdo_destroyStore(wrapper->store);
    }
    delete wrapper;
}

// Directory operations (async-only)

// ======
// Async: mkdir/rename/remove (status-only)
// ======

void jindo_internal_bool_to_status_cb(JdoHandleCtx_t ctx, bool ok, void* ud) {
    auto* c = reinterpret_cast<AsyncStatusCtx*>(ud);
    const char* err = nullptr;
    JindoStatus st = JINDO_STATUS_OK;
    int32_t code = jdo_getHandleCtxErrorCode(ctx);
    if (code != 0 || !ok) {
        err = jdo_getHandleCtxErrorMsg(ctx);
        st = map_error_code_to_status(code);
        if (st == JINDO_STATUS_FILE_NOT_FOUND) {
            // For status-only operations we don't distinguish NotFound.
            st = JINDO_STATUS_ERROR;
        }
    }

    if (c && c->cb) {
        c->cb(st, err, c->userdata);
    }

    if (c) {
        if (c->call) jdo_freeOperationCall(c->call);
        if (c->options) jdo_freeOptions(c->options);
        free_async_strings(c);
        delete c;
    }
}

JindoStatus jindo_filesystem_mkdir_async(JindoFileSystemHandle fs, const char* path, bool recursive, JindoStatusCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncStatusCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->s1 = strdup(path);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setBoolCallback(c->options, jindo_internal_bool_to_status_cb);
        jdo_setCallbackContext(c->options, c);

        c->call = jdo_mkdirAsync(wrapper->ctx, c->s1, recursive, 0755, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = em ? em : "mkdirAsync failed";
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(wrapper->ctx, c->call);
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
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncStatusCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->s1 = strdup(oldpath);
        c->s2 = strdup(newpath);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setBoolCallback(c->options, jindo_internal_bool_to_status_cb);
        jdo_setCallbackContext(c->options, c);

        c->call = jdo_renameAsync(wrapper->ctx, c->s1, c->s2, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = em ? em : "renameAsync failed";
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(wrapper->ctx, c->call);
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
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncStatusCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->s1 = strdup(path);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setBoolCallback(c->options, jindo_internal_bool_to_status_cb);
        jdo_setCallbackContext(c->options, c);

        c->call = jdo_removeAsync(wrapper->ctx, c->s1, recursive, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = em ? em : "removeAsync failed";
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(wrapper->ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

// ======
// Async: exists (bool result)
// ======

void jindo_internal_exists_cb(JdoHandleCtx_t ctx, bool exists, void* ud) {
    auto* c = reinterpret_cast<AsyncBoolCtx*>(ud);
    const char* err = nullptr;
    JindoStatus st = JINDO_STATUS_OK;
    int32_t code = jdo_getHandleCtxErrorCode(ctx);
    if (code != 0) {
        err = jdo_getHandleCtxErrorMsg(ctx);
        st = map_error_code_to_status(code);
        if (st == JINDO_STATUS_FILE_NOT_FOUND) {
            // exists semantics: not-found should be (OK, false) if SDK reports it as an error.
            st = JINDO_STATUS_OK;
            exists = false;
        } else {
            st = JINDO_STATUS_ERROR;
        }
    }

    if (c && c->cb) {
        c->cb(st, exists, err, c->userdata);
    }

    if (c) {
        if (c->call) jdo_freeOperationCall(c->call);
        if (c->options) jdo_freeOptions(c->options);
        free_async_strings(c);
        delete c;
    }
}

JindoStatus jindo_filesystem_exists_async(JindoFileSystemHandle fs, const char* path, JindoBoolResultCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncBoolCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->s1 = strdup(path);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setBoolCallback(c->options, jindo_internal_exists_cb);
        jdo_setCallbackContext(c->options, c);

        c->call = jdo_existsAsync(wrapper->ctx, c->s1, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = em ? em : "existsAsync failed";
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(wrapper->ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

// File info operations (async-only)
// ======
// Async: getFileStatus (file info result)
// ======

void jindo_internal_file_status_cb(JdoHandleCtx_t ctx, JdoFileStatus_t file_status, void* ud) {
    auto* c = reinterpret_cast<AsyncFileInfoCtx*>(ud);
    const char* err = nullptr;
    JindoStatus st = JINDO_STATUS_OK;
    int32_t code = jdo_getHandleCtxErrorCode(ctx);
    if (code != 0 || !file_status) {
        err = jdo_getHandleCtxErrorMsg(ctx);
        st = map_error_code_to_status(code);
        if (st == JINDO_STATUS_ERROR) {
            // If SDK didn't give an error code but returned null, treat as not-found.
            if (!file_status) st = JINDO_STATUS_FILE_NOT_FOUND;
        }
    }

    JindoFileInfo* out = nullptr;
    if (st == JINDO_STATUS_OK && file_status) {
        out = (JindoFileInfo*)malloc(sizeof(JindoFileInfo));
        if (out) {
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
        } else {
            st = JINDO_STATUS_ERROR;
            err = "malloc failed";
        }
    }

    // Free SDK object now that we've copied everything we need.
    if (file_status) {
        jdo_freeFileStatus(file_status);
    }

    if (c && c->cb) {
        c->cb(st, out, err, c->userdata);
    }

    if (c) {
        if (c->call) jdo_freeOperationCall(c->call);
        if (c->options) jdo_freeOptions(c->options);
        free_async_strings(c);
        delete c;
    }
}

JindoStatus jindo_filesystem_get_file_info_async(JindoFileSystemHandle fs, const char* path, JindoFileInfoResultCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncFileInfoCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->s1 = strdup(path);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setFileStatusCallback(c->options, jindo_internal_file_status_cb);
        jdo_setCallbackContext(c->options, c);

        c->call = jdo_getFileStatusAsync(wrapper->ctx, c->s1, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = em ? em : "getFileStatusAsync failed";
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(wrapper->ctx, c->call);
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

// ======
// Async: listDir (list result)
// ======

void jindo_internal_list_dir_cb(JdoHandleCtx_t ctx, JdoListDirResult_t list_result, void* ud) {
    auto* c = reinterpret_cast<AsyncListDirCtx*>(ud);
    const char* err = nullptr;
    JindoStatus st = JINDO_STATUS_OK;
    int32_t code = jdo_getHandleCtxErrorCode(ctx);
    if (code != 0 || !list_result) {
        err = jdo_getHandleCtxErrorMsg(ctx);
        st = map_error_code_to_status(code);
        if (st == JINDO_STATUS_FILE_NOT_FOUND) {
            // listDir: treat not-found as error (matches common fs semantics for listing a missing directory)
            st = JINDO_STATUS_ERROR;
        }
        if (!list_result && code == 0) {
            st = JINDO_STATUS_ERROR;
        }
    }

    JindoListResult* out = nullptr;
    if (st == JINDO_STATUS_OK && list_result) {
        out = (JindoListResult*)malloc(sizeof(JindoListResult));
        if (out) {
            memset(out, 0, sizeof(JindoListResult));
            int64_t count = jdo_getListDirResultSize(list_result);
            out->count = (size_t)count;
            if (count > 0) {
                out->file_infos = (JindoFileInfo*)malloc(sizeof(JindoFileInfo) * count);
                if (!out->file_infos) {
                    free(out);
                    out = nullptr;
                    st = JINDO_STATUS_ERROR;
                    err = "malloc failed";
                } else {
                    memset(out->file_infos, 0, sizeof(JindoFileInfo) * count);
                    for (int64_t i = 0; i < count; i++) {
                        JdoFileStatus_t file_status = jdo_getListDirFileStatus(list_result, i);
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
                }
            }
        } else {
            st = JINDO_STATUS_ERROR;
            err = "malloc failed";
        }
    }

    if (list_result) {
        jdo_freeListDirResult(list_result);
    }

    if (c && c->cb) {
        c->cb(st, out, err, c->userdata);
    }

    if (c) {
        if (c->call) jdo_freeOperationCall(c->call);
        if (c->options) jdo_freeOptions(c->options);
        free_async_strings(c);
        delete c;
    }
}

JindoStatus jindo_filesystem_list_dir_async(JindoFileSystemHandle fs, const char* path, bool recursive, JindoListResultCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncListDirCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->s1 = strdup(path);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setListDirResultCallback(c->options, jindo_internal_list_dir_cb);
        jdo_setCallbackContext(c->options, c);

        c->call = jdo_listDirAsync(wrapper->ctx, c->s1, recursive, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = em ? em : "listDirAsync failed";
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(wrapper->ctx, c->call);
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

// ======
// Async: getContentSummary (summary result)
// ======

void jindo_internal_content_summary_cb(JdoHandleCtx_t ctx, JdoContentSummary_t summary, void* ud) {
    auto* c = reinterpret_cast<AsyncContentSummaryCtx*>(ud);
    const char* err = nullptr;
    JindoStatus st = JINDO_STATUS_OK;
    int32_t code = jdo_getHandleCtxErrorCode(ctx);
    if (code != 0 || !summary) {
        err = jdo_getHandleCtxErrorMsg(ctx);
        st = map_error_code_to_status(code);
        if (st == JINDO_STATUS_FILE_NOT_FOUND) st = JINDO_STATUS_ERROR;
        if (!summary && code == 0) st = JINDO_STATUS_ERROR;
    }

    JindoContentSummary* out = nullptr;
    if (st == JINDO_STATUS_OK && summary) {
        out = (JindoContentSummary*)malloc(sizeof(JindoContentSummary));
        if (out) {
            out->file_count = jdo_getContentSummaryFileCount(summary);
            out->dir_count = jdo_getContentSummaryDirectoryCount(summary);
            out->file_size = jdo_getContentSummaryFileSize(summary);
        } else {
            st = JINDO_STATUS_ERROR;
            err = "malloc failed";
        }
    }

    if (summary) {
        jdo_freeContentSummary(summary);
    }

    if (c && c->cb) {
        c->cb(st, out, err, c->userdata);
    }

    if (c) {
        if (c->call) jdo_freeOperationCall(c->call);
        if (c->options) jdo_freeOptions(c->options);
        free_async_strings(c);
        delete c;
    }
}

JindoStatus jindo_filesystem_get_content_summary_async(JindoFileSystemHandle fs, const char* path, bool recursive, JindoContentSummaryResultCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncContentSummaryCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->s1 = strdup(path);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setContentSummaryCallback(c->options, jindo_internal_content_summary_cb);
        jdo_setCallbackContext(c->options, c);

        c->call = jdo_getContentSummaryAsync(wrapper->ctx, c->s1, recursive, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = em ? em : "getContentSummaryAsync failed";
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(wrapper->ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

// OSS-HDFS specific operations (async-only)

// ======
// Async: setPermission / setOwner (status-only)
// Reuse the same bool->status callback as mkdir/rename/remove.
// ======

JindoStatus jindo_filesystem_set_permission_async(JindoFileSystemHandle fs, const char* path, int16_t perm, JindoStatusCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncStatusCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->s1 = strdup(path);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setBoolCallback(c->options, jindo_internal_bool_to_status_cb);
        jdo_setCallbackContext(c->options, c);

        c->call = jdo_setPermissionAsync(wrapper->ctx, c->s1, perm, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = em ? em : "setPermissionAsync failed";
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(wrapper->ctx, c->call);
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
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncStatusCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->s1 = strdup(path);
        c->s2 = strdup(user);
        c->s3 = strdup(group);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }
        jdo_setBoolCallback(c->options, jindo_internal_bool_to_status_cb);
        jdo_setCallbackContext(c->options, c);

        c->call = jdo_setOwnerAsync(wrapper->ctx, c->s1, c->s2, c->s3, c->options);
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = em ? em : "setOwnerAsync failed";
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(wrapper->ctx, c->call);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

// Writer functions (async open/write/flush/tell/close are implemented below)

void jindo_writer_free(JindoWriterHandle writer) {
    if (!writer) return;
    auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(writer);
    jdo_freeHandleCtx(io_wrapper->handle_ctx);
    jdo_freeIOContext(io_wrapper->io_ctx);
    delete io_wrapper;
}

// Reader functions (async open/read/pread/seek/tell/get_file_length/close are implemented below)

void jindo_reader_free(JindoReaderHandle reader) {
    if (!reader) return;
    auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(reader);
    jdo_freeHandleCtx(io_wrapper->handle_ctx);
    jdo_freeIOContext(io_wrapper->io_ctx);
    delete io_wrapper;
}

// =========================
// Async open (writer/reader)
// =========================

static void jindo_internal_open_io_cb(JdoHandleCtx_t ctx, JdoIOContext_t io_ctx, void* ud, bool is_writer, JindoFileSystemWrapper* fsw) {
    if (!ud) {
        if (io_ctx) jdo_freeIOContext(io_ctx);
        return;
    }

    const char* err = nullptr;
    int32_t code = jdo_getHandleCtxErrorCode(ctx);
    if (code != 0 || !io_ctx) {
        err = jdo_getHandleCtxErrorMsg(ctx);
        JindoStatus st = map_error_code_to_status(code);
        if (st == JINDO_STATUS_FILE_NOT_FOUND) st = JINDO_STATUS_ERROR;

        if (is_writer) {
            auto* c = reinterpret_cast<AsyncOpenWriterCtx*>(ud);
            if (c->cb) c->cb(st, nullptr, err, c->userdata);
            if (c->call) jdo_freeOperationCall(c->call);
            if (c->options) jdo_freeOptions(c->options);
            free_async_strings(c);
            delete c;
        } else {
            auto* c = reinterpret_cast<AsyncOpenReaderCtx*>(ud);
            if (c->cb) c->cb(st, nullptr, err, c->userdata);
            if (c->call) jdo_freeOperationCall(c->call);
            if (c->options) jdo_freeOptions(c->options);
            free_async_strings(c);
            delete c;
        }
        if (io_ctx) jdo_freeIOContext(io_ctx);
        return;
    }

    // Create handle context for this IO context
    JdoHandleCtx_t handle_ctx = jdo_createHandleCtx2(fsw->store, io_ctx);
    if (!handle_ctx) {
        err = "Failed to create handle context for IO";
        if (is_writer) {
            auto* c = reinterpret_cast<AsyncOpenWriterCtx*>(ud);
            if (c->cb) c->cb(JINDO_STATUS_ERROR, nullptr, err, c->userdata);
            if (c->call) jdo_freeOperationCall(c->call);
            if (c->options) jdo_freeOptions(c->options);
            free_async_strings(c);
            delete c;
        } else {
            auto* c = reinterpret_cast<AsyncOpenReaderCtx*>(ud);
            if (c->cb) c->cb(JINDO_STATUS_ERROR, nullptr, err, c->userdata);
            if (c->call) jdo_freeOperationCall(c->call);
            if (c->options) jdo_freeOptions(c->options);
            free_async_strings(c);
            delete c;
        }
        jdo_freeIOContext(io_ctx);
        return;
    }

    auto* io_wrapper = new JindoIOWrapper();
    io_wrapper->io_ctx = io_ctx;
    io_wrapper->handle_ctx = handle_ctx;
    io_wrapper->store = fsw->store;

    if (is_writer) {
        auto* c = reinterpret_cast<AsyncOpenWriterCtx*>(ud);
        if (c->cb) c->cb(JINDO_STATUS_OK, io_wrapper, nullptr, c->userdata);
        if (c->call) jdo_freeOperationCall(c->call);
        if (c->options) jdo_freeOptions(c->options);
        free_async_strings(c);
        delete c;
    } else {
        auto* c = reinterpret_cast<AsyncOpenReaderCtx*>(ud);
        if (c->cb) c->cb(JINDO_STATUS_OK, io_wrapper, nullptr, c->userdata);
        if (c->call) jdo_freeOperationCall(c->call);
        if (c->options) jdo_freeOptions(c->options);
        free_async_strings(c);
        delete c;
    }
}

struct OpenCtxWrapper {
    void* inner; // AsyncOpenWriterCtx* or AsyncOpenReaderCtx*
    JindoFileSystemWrapper* fsw;
    bool is_writer;
};

static void jindo_internal_open_io_trampoline(JdoHandleCtx_t ctx, JdoIOContext_t io_ctx, void* ud) {
    auto* w = reinterpret_cast<OpenCtxWrapper*>(ud);
    if (!w) {
        if (io_ctx) jdo_freeIOContext(io_ctx);
        return;
    }
    if (w->is_writer) {
        jindo_internal_open_io_cb(ctx, io_ctx, w->inner, true, w->fsw);
        delete w;
    } else {
        jindo_internal_open_io_cb(ctx, io_ctx, w->inner, false, w->fsw);
        delete w;
    }
}

JindoStatus jindo_filesystem_open_writer_async(JindoFileSystemHandle fs, const char* path, JindoOpenWriterCallback cb, void* userdata) {
    if (!fs || !path || !cb) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    try {
        auto* fsw = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!fsw->initialized || !fsw->ctx || !fsw->store) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncOpenWriterCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->path = strdup(path);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }

        auto* wrap = new OpenCtxWrapper();
        wrap->inner = c;
        wrap->fsw = fsw;
        wrap->is_writer = true;

        jdo_setIOContextCallback(c->options, jindo_internal_open_io_trampoline);
        jdo_setCallbackContext(c->options, wrap);

        c->call = jdo_openAsync(
            fsw->ctx,
            c->path,
            JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_OVERWRITE,
            0644,
            c->options
        );
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(fsw->ctx);
            g_last_error = em ? em : "openAsync failed";
            delete wrap;
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(fsw->ctx, c->call);
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
        if (!fsw->initialized || !fsw->ctx || !fsw->store) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncOpenWriterCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->path = strdup(path);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }

        auto* wrap = new OpenCtxWrapper();
        wrap->inner = c;
        wrap->fsw = fsw;
        wrap->is_writer = true;

        jdo_setIOContextCallback(c->options, jindo_internal_open_io_trampoline);
        jdo_setCallbackContext(c->options, wrap);

        c->call = jdo_openAsync(
            fsw->ctx,
            c->path,
            JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_APPEND,
            0644,
            c->options
        );
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(fsw->ctx);
            g_last_error = em ? em : "openAsync append failed";
            delete wrap;
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(fsw->ctx, c->call);
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
        if (!fsw->initialized || !fsw->ctx || !fsw->store) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }

        auto* c = new AsyncOpenReaderCtx();
        c->cb = cb;
        c->userdata = userdata;
        c->path = strdup(path);
        c->options = jdo_createOptions();
        if (!c->options) {
            free_async_strings(c);
            delete c;
            g_last_error = "Failed to create options";
            return JINDO_STATUS_ERROR;
        }

        auto* wrap = new OpenCtxWrapper();
        wrap->inner = c;
        wrap->fsw = fsw;
        wrap->is_writer = false;

        jdo_setIOContextCallback(c->options, jindo_internal_open_io_trampoline);
        jdo_setCallbackContext(c->options, wrap);

        c->call = jdo_openAsync(
            fsw->ctx,
            c->path,
            JDO_OPEN_FLAG_READ_ONLY,
            0,
            c->options
        );
        if (!c->call) {
            const char* em = jdo_getHandleCtxErrorMsg(fsw->ctx);
            g_last_error = em ? em : "openAsync reader failed";
            delete wrap;
            jdo_freeOptions(c->options);
            c->options = nullptr;
            free_async_strings(c);
            delete c;
            return JINDO_STATUS_ERROR;
        }

        jdo_perform(fsw->ctx, c->call);
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
        if (st == JINDO_STATUS_FILE_NOT_FOUND) st = JINDO_STATUS_ERROR;
        if (st == JINDO_STATUS_ERROR) st = JINDO_STATUS_ERROR;
    }

    if (c && c->cb) c->cb(st, value, err, c->userdata);

    if (c) {
        if (c->call) jdo_freeOperationCall(c->call);
        if (c->options) jdo_freeOptions(c->options);
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
        jdo_setBoolCallback(c->options, jindo_internal_bool_to_status_cb);
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
        jdo_setBoolCallback(c->options, jindo_internal_bool_to_status_cb);
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
                if (st == JINDO_STATUS_FILE_NOT_FOUND) st = JINDO_STATUS_ERROR;
                st = JINDO_STATUS_ERROR;
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
        jdo_setBoolCallback(c->options, jindo_internal_bool_to_status_cb);
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

} // extern "C"
