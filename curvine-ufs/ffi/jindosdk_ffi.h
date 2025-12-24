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

// C FFI bindings for JindoSDK C++ library
// This header provides C-compatible interface to JindoSDK

#ifndef JINDOSDK_FFI_H
#define JINDOSDK_FFI_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>  // for size_t

// Opaque handle types
typedef void* JindoFileSystemHandle;
typedef void* JindoWriterHandle;
typedef void* JindoReaderHandle;
typedef void* JindoConfigHandle;

// Status codes
typedef enum {
    JINDO_STATUS_OK = 0,
    JINDO_STATUS_ERROR = 1,
    JINDO_STATUS_FILE_NOT_FOUND = 2,
    JINDO_STATUS_IO_ERROR = 3,
} JindoStatus;

// File info structure
typedef struct {
    char* path;
    char* user;
    char* group;
    int8_t type;  // 1=dir, 2=file, 3=symlink, 4=mount
    int16_t perm;
    int64_t length;
    int64_t mtime;
    int64_t atime;
} JindoFileInfo;

// List result structure
typedef struct {
    JindoFileInfo* file_infos;
    size_t count;
} JindoListResult;

// Content summary structure
typedef struct {
    int64_t file_count;
    int64_t dir_count;
    int64_t file_size;
} JindoContentSummary;

// =========================
// Async callback signatures
// =========================
// NOTE: For async APIs below, `err` is a UTF-8 C string (may be NULL). The pointer is only
// guaranteed to be valid for the duration of the callback invocation (Rust must copy it).
typedef void (*JindoStatusCallback)(JindoStatus status, const char* err, void* userdata);
typedef void (*JindoBoolResultCallback)(JindoStatus status, bool value, const char* err, void* userdata);
typedef void (*JindoFileInfoResultCallback)(JindoStatus status, JindoFileInfo* info, const char* err, void* userdata);
typedef void (*JindoListResultCallback)(JindoStatus status, JindoListResult* result, const char* err, void* userdata);
typedef void (*JindoContentSummaryResultCallback)(JindoStatus status, const JindoContentSummary* summary, const char* err, void* userdata);
typedef void (*JindoI64ResultCallback)(JindoStatus status, int64_t value, const char* err, void* userdata);
typedef void (*JindoOpenWriterCallback)(JindoStatus status, JindoWriterHandle writer, const char* err, void* userdata);
typedef void (*JindoOpenReaderCallback)(JindoStatus status, JindoReaderHandle reader, const char* err, void* userdata);

// Config functions
JindoConfigHandle jindo_config_new(void);
void jindo_config_set_string(JindoConfigHandle config, const char* key, const char* value);
void jindo_config_set_bool(JindoConfigHandle config, const char* key, bool value);
void jindo_config_free(JindoConfigHandle config);

// FileSystem functions
JindoFileSystemHandle jindo_filesystem_new(void);
JindoStatus jindo_filesystem_init(
    JindoFileSystemHandle fs,
    const char* bucket,
    const char* user,
    JindoConfigHandle config
);
void jindo_filesystem_free(JindoFileSystemHandle fs);

// Async directory/meta operations (sync APIs removed; async only)
JindoStatus jindo_filesystem_mkdir_async(JindoFileSystemHandle fs, const char* path, bool recursive, JindoStatusCallback cb, void* userdata);
JindoStatus jindo_filesystem_rename_async(JindoFileSystemHandle fs, const char* oldpath, const char* newpath, JindoStatusCallback cb, void* userdata);
JindoStatus jindo_filesystem_remove_async(JindoFileSystemHandle fs, const char* path, bool recursive, JindoStatusCallback cb, void* userdata);
JindoStatus jindo_filesystem_exists_async(JindoFileSystemHandle fs, const char* path, JindoBoolResultCallback cb, void* userdata);

// Async file info / list / summary (sync APIs removed; async only)
JindoStatus jindo_filesystem_get_file_info_async(JindoFileSystemHandle fs, const char* path, JindoFileInfoResultCallback cb, void* userdata);
JindoStatus jindo_filesystem_list_dir_async(JindoFileSystemHandle fs, const char* path, bool recursive, JindoListResultCallback cb, void* userdata);
JindoStatus jindo_filesystem_get_content_summary_async(JindoFileSystemHandle fs, const char* path, bool recursive, JindoContentSummaryResultCallback cb, void* userdata);
void jindo_file_info_free(JindoFileInfo* info);
void jindo_list_result_free(JindoListResult* result);

// OSS-HDFS specific operations
JindoStatus jindo_filesystem_set_permission_async(JindoFileSystemHandle fs, const char* path, int16_t perm, JindoStatusCallback cb, void* userdata);
JindoStatus jindo_filesystem_set_owner_async(JindoFileSystemHandle fs, const char* path, const char* user, const char* group, JindoStatusCallback cb, void* userdata);

// Writer functions (async only)
JindoStatus jindo_filesystem_open_writer_async(JindoFileSystemHandle fs, const char* path, JindoOpenWriterCallback cb, void* userdata);
JindoStatus jindo_filesystem_open_writer_append_async(JindoFileSystemHandle fs, const char* path, JindoOpenWriterCallback cb, void* userdata);
JindoStatus jindo_writer_write_async(JindoWriterHandle writer, const uint8_t* data, size_t len, JindoI64ResultCallback cb, void* userdata);
JindoStatus jindo_writer_flush_async(JindoWriterHandle writer, JindoStatusCallback cb, void* userdata);
JindoStatus jindo_writer_tell_async(JindoWriterHandle writer, JindoI64ResultCallback cb, void* userdata);
JindoStatus jindo_writer_close_async(JindoWriterHandle writer, JindoStatusCallback cb, void* userdata);
void jindo_writer_free(JindoWriterHandle writer);

// Reader functions (async only)
JindoStatus jindo_filesystem_open_reader_async(JindoFileSystemHandle fs, const char* path, JindoOpenReaderCallback cb, void* userdata);
JindoStatus jindo_reader_read_async(JindoReaderHandle reader, size_t n, uint8_t* scratch, JindoI64ResultCallback cb, void* userdata);
JindoStatus jindo_reader_pread_async(JindoReaderHandle reader, int64_t offset, size_t n, uint8_t* scratch, JindoI64ResultCallback cb, void* userdata);
JindoStatus jindo_reader_seek_async(JindoReaderHandle reader, int64_t offset, JindoStatusCallback cb, void* userdata);
JindoStatus jindo_reader_tell_async(JindoReaderHandle reader, JindoI64ResultCallback cb, void* userdata);
JindoStatus jindo_reader_get_file_length_async(JindoReaderHandle reader, JindoI64ResultCallback cb, void* userdata);
JindoStatus jindo_reader_close_async(JindoReaderHandle reader, JindoStatusCallback cb, void* userdata);
void jindo_reader_free(JindoReaderHandle reader);

// Error handling
const char* jindo_get_last_error(void);

// Generic heap free helper (frees pointers allocated by this shim with malloc).
void jindo_free(void* p);

#ifdef __cplusplus
}
#endif

#endif // JINDOSDK_FFI_H

