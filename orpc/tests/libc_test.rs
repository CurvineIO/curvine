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
use orpc::common::{FileUtils, Utils};
use orpc::io::{IOResult, LocalFile};
use orpc::sys;
use orpc::sys::CacheManager;
use std::fs::{create_dir_all, remove_file, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

#[cfg(target_os = "linux")]
#[test]
fn get_raw_fd() {
    let test_file = Utils::test_file();
    let test_file_path = Path::new(&test_file);
    let parent = test_file_path.parent().unwrap();
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

    remove_file(test_file_path).unwrap();
}

#[test]
fn read_ahead() {
    let test_file = Utils::temp_file();
    let test_file_path = Path::new(&test_file);
    let parent = test_file_path.parent().unwrap();
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

    println!("cur_pos {}", cur_pos);

    remove_file(test_file_path).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn tmpfs_test() {
    assert!(sys::is_tmpfs("/run").unwrap());
    assert!(!sys::is_tmpfs("/").unwrap());
}

#[test]
fn resize_truncate_extend_with_hole() {
    // Test case 1: truncate extends file size, creating holes (sparse file)
    let test_file = Utils::test_file();

    // Create file and write some data
    let mut file = LocalFile::with_write(&test_file, true).unwrap();
    file.write_all(b"hello world").unwrap();
    file.flush().unwrap();

    let initial_len = file.len();
    let initial_actual = file.actual_size().unwrap();
    println!(
        "Initial: len={}, actual_size={}",
        initial_len, initial_actual
    );

    // Use truncate to extend to larger size (creates holes)
    let new_len = 1024 * 1024; // 1MB
    file.resize(true, 0, new_len, 0).unwrap();

    let final_len = file.len();
    let final_actual = file.actual_size().unwrap();
    println!(
        "After truncate extend: len={}, actual_size={}",
        final_len, final_actual
    );

    // Verify: logical size should increase, but actual size should be small (holes created)
    assert_eq!(
        final_len, new_len,
        "Logical size should be extended to {}",
        new_len
    );
    assert!(
        final_actual < final_len as u64,
        "Actual size should be less than logical size (sparse file)"
    );
    assert!(
        final_actual <= initial_actual + 4096,
        "Actual size should be close to initial size (hole created)"
    );

    // Verify file content is still preserved
    let mut file = LocalFile::with_read(&test_file, 0).unwrap();
    let mut buf = vec![0u8; 11];
    file.read_all(&mut buf).unwrap();
    assert_eq!(buf, b"hello world", "Original content should be preserved");

    remove_file(test_file).unwrap();
}

#[test]
fn resize_truncate_shrink() {
    // Test case 2: truncate shrinks file size
    let test_file = Utils::test_file();

    // Create file and write data
    let mut file = LocalFile::with_write(&test_file, true).unwrap();
    let data = "x".repeat(10240); // 10KB
    file.write_all(data.as_bytes()).unwrap();
    file.flush().unwrap();

    let initial_len = file.len();
    let initial_actual = file.actual_size().unwrap();
    println!(
        "Initial: len={}, actual_size={}",
        initial_len, initial_actual
    );

    // Use truncate to shrink file
    let new_len = 1024; // 1KB
    file.resize(true, 0, new_len, 0).unwrap();

    let final_len = file.len();
    let final_actual = file.actual_size().unwrap();
    println!(
        "After truncate shrink: len={}, actual_size={}",
        final_len, final_actual
    );

    // Verify: both logical size and actual size should decrease
    assert_eq!(
        final_len, new_len,
        "Logical size should be shrunk to {}",
        new_len
    );
    assert!(
        final_actual <= final_len as u64,
        "Actual size should be <= logical size"
    );
    assert!(
        final_actual < initial_actual,
        "Actual size should be reduced"
    );

    // Verify file content is truncated
    let mut file = LocalFile::with_read(&test_file, 0).unwrap();
    let mut buf = vec![0u8; new_len as usize];
    file.read_all(&mut buf).unwrap();
    assert_eq!(
        buf.len(),
        new_len as usize,
        "File should be truncated to {} bytes",
        new_len
    );

    remove_file(test_file).unwrap();
}

#[test]
fn resize_allocate_preallocate() {
    // Test case 3: allocate pre-allocates file space, both logical and actual size change
    let test_file = Utils::test_file();

    // Create file and write some data
    let mut file = LocalFile::with_write(&test_file, true).unwrap();
    file.write_all(b"hello").unwrap();
    file.flush().unwrap();

    let initial_len = file.len();
    let initial_actual = file.actual_size().unwrap();
    println!(
        "Initial: len={}, actual_size={}",
        initial_len, initial_actual
    );

    // Use fallocate to pre-allocate space (DEFAULT mode = 0)
    // fallocate allocates space in the range from offset to offset+len
    // Default mode changes file size to max(current_size, offset+len)
    let allocate_off = 0;
    let allocate_len = 1024 * 1024; // 1MB
    file.resize(false, allocate_off, allocate_len, 0).unwrap();

    let final_len = file.len();
    let final_actual = file.actual_size().unwrap();
    println!(
        "After fallocate: len={}, actual_size={}",
        final_len, final_actual
    );

    // Verify: logical size should equal allocate_off + allocate_len (fallocate default mode extends file size)
    assert_eq!(
        final_len,
        allocate_off + allocate_len,
        "Logical size should be extended to offset + len"
    );
    // Verify: actual size should equal or close to logical size (space is pre-allocated)
    assert!(
        final_actual >= allocate_len as u64,
        "Actual size should be >= allocated size (space pre-allocated)"
    );
    assert!(
        final_actual >= initial_actual,
        "Actual size should be increased"
    );

    // Verify file content is still preserved
    let mut file = LocalFile::with_read(&test_file, 0).unwrap();
    let mut buf = vec![0u8; 5];
    file.read_all(&mut buf).unwrap();
    assert_eq!(buf, b"hello", "Original content should be preserved");

    remove_file(test_file).unwrap();
}
