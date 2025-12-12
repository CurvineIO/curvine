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

// Directory operations
JindoStatus jindo_filesystem_mkdir(JindoFileSystemHandle fs, const char* path, bool recursive);
JindoStatus jindo_filesystem_rename(JindoFileSystemHandle fs, const char* oldpath, const char* newpath);
JindoStatus jindo_filesystem_remove(JindoFileSystemHandle fs, const char* path, bool recursive);
JindoStatus jindo_filesystem_exists(JindoFileSystemHandle fs, const char* path, bool* exists);

// File info operations
JindoStatus jindo_filesystem_get_file_info(JindoFileSystemHandle fs, const char* path, JindoFileInfo* info);
void jindo_file_info_free(JindoFileInfo* info);

JindoStatus jindo_filesystem_list_dir(
    JindoFileSystemHandle fs,
    const char* path,
    bool recursive,
    JindoListResult* result
);
void jindo_list_result_free(JindoListResult* result);

JindoStatus jindo_filesystem_get_content_summary(
    JindoFileSystemHandle fs,
    const char* path,
    bool recursive,
    JindoContentSummary* summary
);

// OSS-HDFS specific operations
JindoStatus jindo_filesystem_set_permission(JindoFileSystemHandle fs, const char* path, int16_t perm);
JindoStatus jindo_filesystem_set_owner(JindoFileSystemHandle fs, const char* path, const char* user, const char* group);

// Writer functions
JindoStatus jindo_filesystem_open_writer(JindoFileSystemHandle fs, const char* path, JindoWriterHandle* writer);
JindoStatus jindo_filesystem_open_writer_append(JindoFileSystemHandle fs, const char* path, JindoWriterHandle* writer);
JindoStatus jindo_writer_write(JindoWriterHandle writer, const uint8_t* data, size_t len);
JindoStatus jindo_writer_flush(JindoWriterHandle writer);
JindoStatus jindo_writer_tell(JindoWriterHandle writer, int64_t* offset);
JindoStatus jindo_writer_close(JindoWriterHandle writer);
void jindo_writer_free(JindoWriterHandle writer);

// Reader functions
JindoStatus jindo_filesystem_open_reader(JindoFileSystemHandle fs, const char* path, JindoReaderHandle* reader);
JindoStatus jindo_reader_read(JindoReaderHandle reader, size_t n, uint8_t* scratch, size_t* actual_read);
JindoStatus jindo_reader_pread(JindoReaderHandle reader, int64_t offset, size_t n, uint8_t* scratch, size_t* actual_read);
JindoStatus jindo_reader_seek(JindoReaderHandle reader, int64_t offset);
JindoStatus jindo_reader_tell(JindoReaderHandle reader, int64_t* offset);
JindoStatus jindo_reader_get_file_length(JindoReaderHandle reader, int64_t* length);
JindoStatus jindo_reader_close(JindoReaderHandle reader);
void jindo_reader_free(JindoReaderHandle reader);

// Error handling
const char* jindo_get_last_error(void);

#ifdef __cplusplus
}
#endif

#endif // JINDOSDK_FFI_H

