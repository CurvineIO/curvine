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

use crate::common::LocalTime;
use crate::runtime::Runtime;
use crate::CommonResult;
use md5::{Digest, Md5};
use rand::prelude::ThreadRng;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::de::DeserializeOwned;
use std::backtrace::Backtrace;
use std::io::Cursor;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{env, panic, process, thread};
use uuid::Uuid;

pub struct Utils;

impl Utils {
    // Create a 64-bit request id
    pub fn req_id() -> i64 {
        let mut lsb: i64 = 0;
        for b in Uuid::new_v4().as_bytes() {
            lsb = (lsb << 8) | *b as i64
        }
        lsb
    }

    pub fn new_rt<T: AsRef<str>>(name: T, threads: usize) -> Runtime {
        Runtime::new(name, threads, threads)
    }

    pub fn rand_id() -> u64 {
        let mut rng = rand::thread_rng();
        rng.gen::<u64>()
    }

    pub fn rand_rng() -> ThreadRng {
        rand::thread_rng()
    }

    pub fn shuffle<T>(vec: &mut [T]) {
        let mut rng = Self::rand_rng();
        vec.shuffle(&mut rng)
    }

    pub fn murmur3(bytes: &[u8]) -> u32 {
        murmur3::murmur3_32(&mut Cursor::new(bytes), 104729).unwrap()
    }

    pub fn crc32(buf: &[u8]) -> u32 {
        crc32fast::hash(buf)
    }

    pub fn rand_str(len: usize) -> String {
        let mut str = String::with_capacity(len);
        for i in 0..len {
            let c = rand::thread_rng().gen_range(b'a'..b'z' + 1);
            if i == len - 1 {
                str.push('\n');
            } else {
                str.push(c as char);
            }
        }
        str
    }

    pub fn uuid() -> String {
        Uuid::new_v4().to_string()
    }

    pub fn machine_id() -> u32 {
        let str = format!(
            "cache-machine_id-{}-{}",
            Self::rand_id(),
            LocalTime::nanos()
        );

        murmur3::murmur3_32(&mut Cursor::new(str.as_bytes()), 104729).unwrap()
    }

    pub fn recover_panic<F, R>(f: F) -> thread::Result<R>
    where
        F: FnOnce() -> R,
    {
        panic::catch_unwind(AssertUnwindSafe(f))
    }

    pub fn sleep(time_ms: u64) {
        thread::sleep(Duration::from_millis(time_ms));
    }

    // Get the number of CPU cores
    pub fn cpu_nums() -> usize {
        thread::available_parallelism()
            .map(|x| x.get())
            .unwrap_or(0)
    }

    // Get the current working directory.
    pub fn cur_dir() -> String {
        let path = env::current_dir().unwrap_or(PathBuf::from("./"));
        format!("{}", path.display())
    }

    pub fn cur_dir_sub<T: AsRef<Path>>(sub: T) -> String {
        let mut path = env::current_dir().unwrap_or(PathBuf::from("."));
        path.push(sub);

        format!("{}", path.display())
    }

    // Return a temporary file name
    pub fn temp_file() -> String {
        let mut path = env::temp_dir();
        path.push(format!("temp-{}", Self::rand_id()));
        format!("{}", path.display())
    }

    // Returns a test file name.Located in the testing directory.
    pub fn test_file() -> String {
        let mut path = env::current_dir().unwrap_or(PathBuf::from("."));
        path.push(format!("../testing/test-{}", Self::rand_id()));
        format!("{}", path.display())
    }

    pub fn test_sub_dir<T: AsRef<Path>>(sub: T) -> String {
        let mut path = env::current_dir().unwrap_or(PathBuf::from("."));
        path.push("../testing");
        path.push(sub);
        format!("{}", path.display())
    }

    // Set the panic capture handler, which requires setting the profile panic to abort
    // After the program triggers panic, prints the log and exits the program.
    pub fn set_panic_exit_hook() {
        panic::set_hook(Box::new(|panic_info| {
            let message = match panic_info.payload().downcast_ref::<&str>() {
                Some(s) => *s,
                None => "Box<Any>",
            };

            let location = panic_info.location().unwrap();
            let backtrace = Backtrace::capture();
            println!(
                "{} Panic occurred at {}:{}: {}, backtrace {:?}",
                LocalTime::now_datetime(),
                location.file(),
                location.line(),
                message,
                backtrace
            );

            process::exit(1);
        }));
    }

    // Calculate the number of worker threads based on the number of CPUs
    pub fn worker_threads(io_threads: usize) -> usize {
        let cpus = Self::cpu_nums();
        io_threads.max(2 * cpus)
    }

    pub fn read_toml_conf<T: DeserializeOwned>(path: impl AsRef<Path>) -> CommonResult<T> {
        let content = std::fs::read_to_string(path)?;
        let conf = toml::from_str::<T>(&content)?;
        Ok(conf)
    }

    pub fn md5(source: impl AsRef<str>) -> String {
        let mut hasher = Md5::new();
        hasher.update(source.as_ref().as_bytes());
        let hash = hasher.finalize();
        format!("{:x}", hash)
    }

    pub fn thread_id() -> String {
        format!("{:?}", thread::current().id())
    }

    pub fn thread_name() -> String {
        format!("{}", thread::current().name().unwrap_or(""))
    }
}

#[cfg(test)]
mod tests {
    use crate::common::Utils;

    #[test]
    pub fn uuid() {
        println!("uuid = {}", Utils::uuid())
    }

    #[test]
    pub fn test_md5() {
        assert_eq!(
            Utils::md5("hello world"),
            "5eb63bbbe01eeed093cb22bb8f5acdc3"
        );
        assert_eq!(Utils::md5(""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(
            Utils::md5("The quick brown fox jumps over the lazy dog"),
            "9e107d9d372bb6826bd81d3542a419d6"
        );
        assert_eq!(Utils::md5("hello"), "5d41402abc4b2a76b9719d911017c592");

        let input = "test string";
        let hash1 = Utils::md5(input);
        let hash2 = Utils::md5(input);
        assert_eq!(hash1, hash2);

        assert_eq!(Utils::md5("any string").len(), 32);

        assert_ne!(Utils::md5("hello"), Utils::md5("world"));

        println!("MD5 tests passed!");
        println!("Examples:");
        println!("  MD5('hello world') = {}", Utils::md5("hello world"));
        println!("  MD5('hello') = {}", Utils::md5("hello"));
        println!("  MD5('') = {}", Utils::md5(""));
    }

    #[test]
    fn cur_dir() {
        let dir = Utils::cur_dir_sub("meta");
        println!("{}", dir)
    }
}
