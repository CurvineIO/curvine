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

#![allow(unused_imports)]

use bytes::BytesMut;
use orpc::common::Utils;
use orpc::sys;
use orpc::sys::CacheManager;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

#[cfg(target_os = "linux")]
#[test]
fn get_raw_fd() {
    let test_file = Utils::test_sub_dir("libc-read_ahead.log");
    // Normalize the path to handle .. components
    let test_file_path = Path::new(&test_file);
    let parent = test_file_path.parent().unwrap();
    // Ensure testing directory exists
    create_dir_all(parent).unwrap_or_else(|e| {
        panic!("Failed to create directory: {:?}, error: {}", parent, e);
    });

    let writer = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(test_file_path)
        .unwrap_or_else(|e| {
            panic!("Failed to open file: {:?}, error: {}", test_file_path, e);
        });

    let fd = sys::get_raw_io(&writer);
    println!("fd = {}", fd.unwrap());
}

#[test]
fn read_ahead() {
    let test_file = Utils::test_sub_dir("libc-read_ahead.log");
    // Normalize the path to handle .. components
    let test_file_path = Path::new(&test_file);
    let parent = test_file_path.parent().unwrap();
    // Ensure testing directory exists
    create_dir_all(parent).unwrap_or_else(|e| {
        panic!("Failed to create directory: {:?}, error: {}", parent, e);
    });

    let mut writer = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(test_file_path)
        .unwrap_or_else(|e| {
            panic!("Failed to open file: {:?}, error: {}", test_file_path, e);
        });

    for _ in 0..10000 {
        let s = "a".repeat(1024);
        writer.write_all(s.as_bytes()).unwrap();
    }

    drop(writer);

    let cache_manager = CacheManager::default();
    let mut reader = File::open(test_file_path).unwrap();
    let file_size = reader.metadata().unwrap().len();
    println!("file_size {}", file_size);
    let mut buf = BytesMut::zeroed(64 * 1024);

    let mut cur_pos: u64 = 0;
    let mut last_task = None;

    while cur_pos < file_size {
        last_task = cache_manager.read_ahead(&reader, cur_pos as i64, file_size as i64, last_task);
        let chunk_size = (64 * 1024).min(file_size - cur_pos) as usize;
        buf.resize(chunk_size, 0);
        let mut b = buf.split_to(chunk_size);

        reader.read_exact(&mut b).unwrap();
        cur_pos += chunk_size as u64;
    }

    println!("cur_pos {}", cur_pos)
}

#[cfg(target_os = "linux")]
#[test]
fn tmpfs_test() {
    assert!(sys::is_tmpfs("/run").unwrap());
    assert!(!sys::is_tmpfs("/").unwrap());
}
