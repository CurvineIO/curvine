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

#![allow(clippy::missing_safety_doc)]

use crate::java::{JavaFilesystem, SUCCESS};
use crate::{java_err, java_err2, LibFsReader, LibFsWriter};
use jni::objects::{JLongArray, JObject, JString};
use jni::sys::{jarray, jboolean, jint, jlong};
use jni::JNIEnv;
use orpc::sys::DataSlice;
use orpc::sys::{FFIUtils, RawVec};

// It's too troublesome to parse object members by jni. Here the configuration will be passed through the json string.
#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_newFilesystem(
    mut env: JNIEnv,
    _this: JObject,
    conf: JString,
) -> jlong {
    let fs = java_err!(env, JavaFilesystem::new(&mut env, conf));
    FFIUtils::into_raw_ptr(fs)
}

// Create a file.
#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_create(
    mut env: JNIEnv,
    _this: JObject,
    fs_ptr: *const JavaFilesystem,
    path: JString,
    overwrite: jboolean,
) -> jlong {
    let fs = &*fs_ptr;
    let writer = java_err!(env, fs.create(&mut env, path, overwrite));
    FFIUtils::into_raw_ptr(writer)
}

#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_append(
    mut env: JNIEnv,
    _this: JObject,
    fs_ptr: *const JavaFilesystem,
    path: JString,
    tmp: JLongArray,
) -> jlong {
    let fs = &*fs_ptr;
    let writer = java_err!(env, fs.append(&mut env, path));

    let arr = [writer.pos()];
    env.set_long_array_region(tmp, 0, &arr).unwrap();

    FFIUtils::into_raw_ptr(writer)
}

// Write data.Java passes the direct buffer memory address and length.
#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_write(
    mut env: JNIEnv,
    _this: JObject,
    writer_ptr: *mut LibFsWriter,
    buf: jlong,
    len: jint,
) -> jlong {
    let writer = &mut *writer_ptr;

    let raw_vec = RawVec::from_raw(buf as *mut u8, len as usize);
    let buf = DataSlice::MemSlice(raw_vec);
    java_err!(env, writer.write(buf));

    SUCCESS
}

// flush operation
#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_flush(
    mut env: JNIEnv,
    _this: JObject,
    writer_ptr: *mut LibFsWriter,
) -> jlong {
    let writer = &mut *writer_ptr;
    java_err!(env, writer.flush());
    SUCCESS
}

// Close the file.
#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_closeWriter(
    mut env: JNIEnv,
    _this: JObject,
    writer_ptr: *mut LibFsWriter,
) -> jlong {
    let writer = &mut *writer_ptr;
    java_err!(env, writer.complete(), FFIUtils::free_raw_ptr(writer_ptr));
    SUCCESS
}

// Open the file for reading.
#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_open(
    mut env: JNIEnv,
    _this: JObject,
    fs_ptr: *const JavaFilesystem,
    path: JString,
    tmp: JLongArray,
) -> jlong {
    let fs = &*fs_ptr;
    let reader = java_err!(env, fs.open(&mut env, path));
    // Returns the file length.
    let arr = [reader.len()];
    env.set_long_array_region(tmp, 0, &arr).unwrap();

    FFIUtils::into_raw_ptr(reader)
}

// Read data.
#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_read(
    mut env: JNIEnv,
    _this: JObject,
    reader_ptr: *mut LibFsReader,
    tmp: JLongArray,
) -> jlong {
    let reader = &mut *reader_ptr;
    let bytes = java_err!(env, reader.read());

    // Write the memory address and length into the buf.
    let arr = [bytes.as_ptr() as jlong, bytes.len() as jlong];
    env.set_long_array_region(tmp, 0, &arr).unwrap();

    SUCCESS
}

#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_seek(
    mut env: JNIEnv,
    _this: JObject,
    reader_ptr: *mut LibFsReader,
    pos: jlong,
) -> jlong {
    let reader = &mut *reader_ptr;
    java_err!(env, reader.seek(pos));
    SUCCESS
}

#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_closeReader(
    mut env: JNIEnv,
    _this: JObject,
    reader_ptr: *mut LibFsReader,
) -> jlong {
    let reader = &mut *reader_ptr;
    java_err!(env, reader.complete(), FFIUtils::free_raw_ptr(reader_ptr));
    SUCCESS
}

#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_closeFilesystem(
    _env: JNIEnv,
    _this: JObject,
    fs_ptr: *mut JavaFilesystem,
) -> jlong {
    FFIUtils::free_raw_ptr(fs_ptr);
    SUCCESS
}

// Create a directory.
#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_mkdir(
    mut env: JNIEnv,
    _this: JObject,
    fs_ptr: *mut JavaFilesystem,
    path: JString,
    create_parent: jboolean,
) -> jlong {
    let fs = &*fs_ptr;
    let _ = java_err!(env, fs.mkdir(&mut env, path, create_parent));
    SUCCESS
}

// Get file status.
#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_getFileStatus(
    mut env: JNIEnv,
    _this: JObject,
    fs_ptr: *mut JavaFilesystem,
    path: JString,
) -> jarray {
    let fs = &*fs_ptr;
    java_err2!(env, fs.get_file_status(&mut env, path))
}

#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_listStatus(
    mut env: JNIEnv,
    _this: JObject,
    fs_ptr: *mut JavaFilesystem,
    path: JString,
) -> jarray {
    let fs = &*fs_ptr;
    java_err2!(env, fs.list_status(&mut env, path))
}

#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_rename(
    mut env: JNIEnv,
    _this: JObject,
    fs_ptr: *mut JavaFilesystem,
    src: JString,
    dst: JString,
) -> jlong {
    let fs = &*fs_ptr;
    let _ = java_err!(env, fs.rename(&mut env, src, dst));
    SUCCESS
}

#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_delete(
    mut env: JNIEnv,
    _this: JObject,
    fs_ptr: *mut JavaFilesystem,
    path: JString,
    recursive: jboolean,
) -> jlong {
    let fs = &*fs_ptr;
    java_err!(env, fs.delete(&mut env, path, recursive));
    SUCCESS
}

#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_getMasterInfo(
    mut env: JNIEnv,
    _this: JObject,
    fs_ptr: *mut JavaFilesystem,
) -> jarray {
    let fs = &*fs_ptr;
    java_err2!(env, fs.get_master_info(&mut env))
}

#[no_mangle]
pub unsafe extern "C" fn Java_io_curvine_CurvineNative_getMountPoint(
    mut env: JNIEnv,
    _this: JObject,
    fs_ptr: *mut JavaFilesystem,
    path: JString,
) -> jarray {
    let fs = &*fs_ptr;
    java_err2!(env, fs.get_mount_point(&mut env, path))
}
