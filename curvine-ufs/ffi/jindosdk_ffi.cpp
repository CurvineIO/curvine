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

// Directory operations
JindoStatus jindo_filesystem_mkdir(JindoFileSystemHandle fs, const char* path, bool recursive) {
    if (!fs || !path) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        bool result = jdo_mkdir(wrapper->ctx, path, recursive, 0755, nullptr);
        
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0 || !result) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "mkdir failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_rename(JindoFileSystemHandle fs, const char* oldpath, const char* newpath) {
    if (!fs || !oldpath || !newpath) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        bool result = jdo_rename(wrapper->ctx, oldpath, newpath, nullptr);
        
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0 || !result) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "rename failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_remove(JindoFileSystemHandle fs, const char* path, bool recursive) {
    if (!fs || !path) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        bool result = jdo_remove(wrapper->ctx, path, recursive, nullptr);
        
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0 || !result) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "remove failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_exists(JindoFileSystemHandle fs, const char* path, bool* exists) {
    if (!fs || !path || !exists) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        *exists = jdo_exists(wrapper->ctx, path, nullptr);
        
        // exists() doesn't fail even if file doesn't exist
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

// File info operations
JindoStatus jindo_filesystem_get_file_info(JindoFileSystemHandle fs, const char* path, JindoFileInfo* info) {
    if (!fs || !path || !info) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        JdoFileStatus_t file_status = jdo_getFileStatus(wrapper->ctx, path, nullptr);
        
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0 || !file_status) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "getFileStatus failed";
            return JINDO_STATUS_FILE_NOT_FOUND;
        }
        
        // Extract file info
        const char* file_path = jdo_getFileStatusPath(file_status);
        const char* owner = jdo_getFileStatusUser(file_status);
        const char* group = jdo_getFileStatusGroup(file_status);
        
        info->path = file_path ? strdup(file_path) : nullptr;
        info->user = owner ? strdup(owner) : nullptr;
        info->group = group ? strdup(group) : nullptr;
        info->type = jdo_getFileStatusType(file_status);  // 1=dir, 2=file, etc.
        info->perm = jdo_getFileStatusPerm(file_status);
        info->length = jdo_getFileStatusSize(file_status);
        info->mtime = jdo_getFileStatusMtime(file_status);
        info->atime = jdo_getFileStatusAtime(file_status);
        
        jdo_freeFileStatus(file_status);
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

JindoStatus jindo_filesystem_list_dir(
    JindoFileSystemHandle fs,
    const char* path,
    bool recursive,
    JindoListResult* result
) {
    if (!fs || !path || !result) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        JdoListDirResult_t list_result = jdo_listDir(wrapper->ctx, path, recursive, nullptr);
        
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0 || !list_result) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "listDir failed";
            return JINDO_STATUS_ERROR;
        }
        
        int64_t count = jdo_getListDirResultSize(list_result);
        result->count = count;
        result->file_infos = (JindoFileInfo*)malloc(sizeof(JindoFileInfo) * count);
        
        for (int64_t i = 0; i < count; i++) {
            JdoFileStatus_t file_status = jdo_getListDirFileStatus(list_result, i);
            
            const char* file_path = jdo_getFileStatusPath(file_status);
            const char* owner = jdo_getFileStatusUser(file_status);
            const char* group = jdo_getFileStatusGroup(file_status);
            
            result->file_infos[i].path = file_path ? strdup(file_path) : nullptr;
            result->file_infos[i].user = owner ? strdup(owner) : nullptr;
            result->file_infos[i].group = group ? strdup(group) : nullptr;
            result->file_infos[i].type = jdo_getFileStatusType(file_status);
            result->file_infos[i].perm = jdo_getFileStatusPerm(file_status);
            result->file_infos[i].length = jdo_getFileStatusSize(file_status);
            result->file_infos[i].mtime = jdo_getFileStatusMtime(file_status);
            result->file_infos[i].atime = jdo_getFileStatusAtime(file_status);
        }
        
        jdo_freeListDirResult(list_result);
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

JindoStatus jindo_filesystem_get_content_summary(
    JindoFileSystemHandle fs,
    const char* path,
    bool recursive,
    JindoContentSummary* summary
) {
    if (!fs || !path || !summary) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        JdoContentSummary_t content_summary = jdo_getContentSummary(wrapper->ctx, path, recursive, nullptr);
        
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0 || !content_summary) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "getContentSummary failed";
            return JINDO_STATUS_ERROR;
        }
        
        summary->file_count = jdo_getContentSummaryFileCount(content_summary);
        summary->dir_count = jdo_getContentSummaryDirectoryCount(content_summary);
        summary->file_size = jdo_getContentSummaryFileSize(content_summary);
        
        jdo_freeContentSummary(content_summary);
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

// OSS-HDFS specific operations
JindoStatus jindo_filesystem_set_permission(JindoFileSystemHandle fs, const char* path, int16_t perm) {
    if (!fs || !path) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        bool result = jdo_setPermission(wrapper->ctx, path, perm, nullptr);
        
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0 || !result) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "setPermission failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_set_owner(JindoFileSystemHandle fs, const char* path, const char* user, const char* group) {
    if (!fs || !path || !user || !group) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        bool result = jdo_setOwner(wrapper->ctx, path, user, group, nullptr);
        
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0 || !result) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "setOwner failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

// Writer functions
JindoStatus jindo_filesystem_open_writer(JindoFileSystemHandle fs, const char* path, JindoWriterHandle* writer) {
    if (!fs || !path || !writer) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx || !wrapper->store) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        // Open file for writing
        JdoIOContext_t io_ctx = jdo_open(wrapper->ctx, path, 
                                          JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_OVERWRITE, 
                                          0644, nullptr);
        
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0 || !io_ctx) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "open writer failed";
            return JINDO_STATUS_ERROR;
        }
        
        // Create handle context for this IO context
        JdoHandleCtx_t handle_ctx = jdo_createHandleCtx2(wrapper->store, io_ctx);
        if (!handle_ctx) {
            g_last_error = "Failed to create handle context for writer";
            jdo_freeIOContext(io_ctx);
            return JINDO_STATUS_ERROR;
        }
        
        auto* io_wrapper = new JindoIOWrapper();
        io_wrapper->io_ctx = io_ctx;
        io_wrapper->handle_ctx = handle_ctx;
        io_wrapper->store = wrapper->store;
        
        *writer = io_wrapper;
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_filesystem_open_writer_append(JindoFileSystemHandle fs, const char* path, JindoWriterHandle* writer) {
    if (!fs || !path || !writer) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx || !wrapper->store) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        // Open file for appending (create if not exists, append if exists)
        JdoIOContext_t io_ctx = jdo_open(wrapper->ctx, path, 
                                          JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_APPEND, 
                                          0644, nullptr);
        
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0 || !io_ctx) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "open writer for append failed";
            return JINDO_STATUS_ERROR;
        }
        
        // Create handle context for this IO context
        JdoHandleCtx_t handle_ctx = jdo_createHandleCtx2(wrapper->store, io_ctx);
        if (!handle_ctx) {
            g_last_error = "Failed to create handle context for writer";
            jdo_freeIOContext(io_ctx);
            return JINDO_STATUS_ERROR;
        }
        
        auto* io_wrapper = new JindoIOWrapper();
        io_wrapper->io_ctx = io_ctx;
        io_wrapper->handle_ctx = handle_ctx;
        io_wrapper->store = wrapper->store;
        
        *writer = io_wrapper;
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_writer_write(JindoWriterHandle writer, const uint8_t* data, size_t len) {
    if (!writer || !data) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(writer);
        int64_t written = jdo_write(io_wrapper->handle_ctx, reinterpret_cast<const char*>(data), len, nullptr);
        if (written < 0 || (size_t)written != len) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(io_wrapper->handle_ctx);
            g_last_error = error_msg ? error_msg : "write failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_writer_flush(JindoWriterHandle writer) {
    if (!writer) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(writer);
        bool result = jdo_flush(io_wrapper->handle_ctx, nullptr);
        if (!result) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(io_wrapper->handle_ctx);
            g_last_error = error_msg ? error_msg : "flush failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_writer_tell(JindoWriterHandle writer, int64_t* offset) {
    if (!writer || !offset) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(writer);
        *offset = jdo_tell(io_wrapper->handle_ctx, nullptr);
        if (*offset < 0) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(io_wrapper->handle_ctx);
            g_last_error = error_msg ? error_msg : "tell failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_writer_close(JindoWriterHandle writer) {
    if (!writer) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(writer);
        bool result = jdo_close(io_wrapper->handle_ctx, nullptr);
        if (!result) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(io_wrapper->handle_ctx);
            g_last_error = error_msg ? error_msg : "close writer failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

void jindo_writer_free(JindoWriterHandle writer) {
    if (!writer) return;
    auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(writer);
    jdo_freeHandleCtx(io_wrapper->handle_ctx);
    jdo_freeIOContext(io_wrapper->io_ctx);
    delete io_wrapper;
}

// Reader functions
JindoStatus jindo_filesystem_open_reader(JindoFileSystemHandle fs, const char* path, JindoReaderHandle* reader) {
    if (!fs || !path || !reader) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* wrapper = reinterpret_cast<JindoFileSystemWrapper*>(fs);
        if (!wrapper->initialized || !wrapper->ctx || !wrapper->store) {
            g_last_error = "Filesystem not initialized";
            return JINDO_STATUS_ERROR;
        }
        
        // Open file for reading
        JdoIOContext_t io_ctx = jdo_open(wrapper->ctx, path, JDO_OPEN_FLAG_READ_ONLY, 0, nullptr);
        
        int32_t error_code = jdo_getHandleCtxErrorCode(wrapper->ctx);
        if (error_code != 0 || !io_ctx) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(wrapper->ctx);
            g_last_error = error_msg ? error_msg : "open reader failed";
            return JINDO_STATUS_ERROR;
        }
        
        // Create handle context for this IO context
        JdoHandleCtx_t handle_ctx = jdo_createHandleCtx2(wrapper->store, io_ctx);
        if (!handle_ctx) {
            g_last_error = "Failed to create handle context for reader";
            jdo_freeIOContext(io_ctx);
            return JINDO_STATUS_ERROR;
        }
        
        auto* io_wrapper = new JindoIOWrapper();
        io_wrapper->io_ctx = io_ctx;
        io_wrapper->handle_ctx = handle_ctx;
        io_wrapper->store = wrapper->store;
        
        *reader = io_wrapper;
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_read(JindoReaderHandle reader, size_t n, uint8_t* scratch, size_t* actual_read) {
    if (!reader || !scratch || !actual_read) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(reader);
        int64_t bytes_read = jdo_read(io_wrapper->handle_ctx, reinterpret_cast<char*>(scratch), n, nullptr);
        if (bytes_read < 0) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(io_wrapper->handle_ctx);
            g_last_error = error_msg ? error_msg : "read failed";
            return JINDO_STATUS_ERROR;
        }
        
        *actual_read = bytes_read;
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_pread(JindoReaderHandle reader, int64_t offset, size_t n, uint8_t* scratch, size_t* actual_read) {
    if (!reader || !scratch || !actual_read) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(reader);
        int64_t bytes_read = jdo_pread(io_wrapper->handle_ctx, reinterpret_cast<char*>(scratch), n, offset, nullptr);
        if (bytes_read < 0) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(io_wrapper->handle_ctx);
            g_last_error = error_msg ? error_msg : "pread failed";
            return JINDO_STATUS_ERROR;
        }
        
        *actual_read = bytes_read;
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_seek(JindoReaderHandle reader, int64_t offset) {
    if (!reader) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(reader);
        int64_t result = jdo_seek(io_wrapper->handle_ctx, offset, nullptr);
        if (result < 0) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(io_wrapper->handle_ctx);
            g_last_error = error_msg ? error_msg : "seek failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_tell(JindoReaderHandle reader, int64_t* offset) {
    if (!reader || !offset) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(reader);
        *offset = jdo_tell(io_wrapper->handle_ctx, nullptr);
        if (*offset < 0) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(io_wrapper->handle_ctx);
            g_last_error = error_msg ? error_msg : "tell failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_get_file_length(JindoReaderHandle reader, int64_t* length) {
    if (!reader || !length) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(reader);
        
        // Get current position
        int64_t current_pos = jdo_tell(io_wrapper->handle_ctx, nullptr);
        if (current_pos < 0) {
            g_last_error = "tell failed";
            return JINDO_STATUS_ERROR;
        }
        
        // Seek to end to get file length
        // In JindoSDK, seek returns the new position
        int64_t end_pos = jdo_seek(io_wrapper->handle_ctx, INT64_MAX, nullptr);  // Seek to a very large offset
        if (end_pos < 0) {
            g_last_error = "seek to end failed";
            return JINDO_STATUS_ERROR;
        }
        
        *length = end_pos;
        
        // Seek back to original position
        jdo_seek(io_wrapper->handle_ctx, current_pos, nullptr);
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

JindoStatus jindo_reader_close(JindoReaderHandle reader) {
    if (!reader) {
        g_last_error = "Invalid parameters";
        return JINDO_STATUS_ERROR;
    }
    
    try {
        auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(reader);
        bool result = jdo_close(io_wrapper->handle_ctx, nullptr);
        if (!result) {
            const char* error_msg = jdo_getHandleCtxErrorMsg(io_wrapper->handle_ctx);
            g_last_error = error_msg ? error_msg : "close reader failed";
            return JINDO_STATUS_ERROR;
        }
        
        return JINDO_STATUS_OK;
    } catch (const std::exception& e) {
        g_last_error = e.what();
        return JINDO_STATUS_ERROR;
    }
}

void jindo_reader_free(JindoReaderHandle reader) {
    if (!reader) return;
    auto* io_wrapper = reinterpret_cast<JindoIOWrapper*>(reader);
    jdo_freeHandleCtx(io_wrapper->handle_ctx);
    jdo_freeIOContext(io_wrapper->io_ctx);
    delete io_wrapper;
}

const char* jindo_get_last_error(void) {
    return g_last_error.c_str();
}

} // extern "C"
