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
//

use super::{BenchMode, CurvineBenchRunner};
use curvine_common::fs::Path;
use orpc::{err_box, CommonResult};
use std::path::PathBuf;
use std::time::Duration;

const LIST_DIR_WALK_SPAN: usize = 8;

impl CurvineBenchRunner {
    pub(crate) fn paths(&self, prefix: &str, count: usize) -> Vec<String> {
        (0..count)
            .map(|index| join_path(&self.tmp_path, &format!("{}.{}", prefix, index)))
            .collect()
    }

    pub(crate) fn workset_paths(&self, count: usize) -> Vec<String> {
        self.workset_paths_for(&self.config.base_path, count)
    }

    pub(crate) fn workset_paths_for(&self, base: &str, count: usize) -> Vec<String> {
        let files_per_dir = self.config.files_per_dir.max(1);
        (0..count)
            .map(|index| workset_path(base, index, files_per_dir))
            .collect()
    }

    pub(crate) fn workset_dirs_for(&self, base: &str, file_count: usize) -> Vec<String> {
        workset_dirs(base, file_count, self.config.files_per_dir)
    }

    pub(crate) fn op_path(&self, prefix: &str, worker_id: usize, index: usize) -> String {
        join_path(
            &self.tmp_path,
            &format!("{}.{}.{}", prefix, worker_id, index),
        )
    }
}

pub(crate) fn workset_dirs(base: &str, file_count: usize, files_per_dir: usize) -> Vec<String> {
    let dir_count = workset_dir_count(file_count, files_per_dir.max(1));
    (0..dir_count)
        .map(|index| workset_dir(base, index))
        .collect()
}

pub(crate) fn select_list_dir<'a>(
    fallback: &'a str,
    worker_id: usize,
    index: usize,
    list_dirs: &'a [String],
) -> &'a str {
    if list_dirs.is_empty() {
        return fallback;
    }
    let walk = (index / LIST_DIR_WALK_SPAN).wrapping_add(worker_id);
    let dir_index = walk % list_dirs.len();
    &list_dirs[dir_index]
}

pub fn infer_mode(path: &str) -> CommonResult<BenchMode> {
    if path.starts_with("file://") {
        return Ok(BenchMode::Fuse);
    }
    match Path::from_str(path) {
        Ok(path) if path.scheme() == Some("cv") => Ok(BenchMode::Client),
        Ok(path) if path.scheme().is_none() && path.full_path().starts_with('/') => {
            Ok(BenchMode::Client)
        }
        Ok(path) if path.scheme().is_none() => {
            err_box!("--mode auto does not accept relative paths; use file://... or pass --mode fuse explicitly")
        }
        Ok(path) => err_box!(
            "--mode auto only supports cv://, file://, or absolute paths; got scheme '{}'",
            path.scheme().unwrap_or_default()
        ),
        Err(err) => Err(err),
    }
}

pub fn join_path(base: &str, child: &str) -> String {
    if base.starts_with("file://") {
        let path = base.trim_start_matches("file://");
        return PathBuf::from(path)
            .join(child)
            .to_string_lossy()
            .to_string();
    }
    format!("{}/{}", base.trim_end_matches('/'), child)
}

fn workset_dir(base: &str, dir_index: usize) -> String {
    join_path(base, &format!("part-{dir_index:06}"))
}

fn workset_dir_count(file_count: usize, files_per_dir: usize) -> usize {
    file_count.div_ceil(files_per_dir.max(1))
}

pub fn workset_path(base: &str, index: usize, files_per_dir: usize) -> String {
    let files_per_dir = files_per_dir.max(1);
    let dir_index = index / files_per_dir;
    join_path(&workset_dir(base, dir_index), &format!("item-{index:012}"))
}

pub fn parse_duration(value: &str) -> CommonResult<Duration> {
    let value = value.trim();
    if value.is_empty() {
        return err_box!("Duration must not be empty");
    }

    let (number, multiplier) = if let Some(v) = value.strip_suffix("ms") {
        (v, 1)
    } else if let Some(v) = value.strip_suffix('s') {
        (v, 1_000)
    } else if let Some(v) = value.strip_suffix('m') {
        (v, 60_000)
    } else if let Some(v) = value.strip_suffix('h') {
        (v, 3_600_000)
    } else {
        (value, 1_000)
    };

    let amount = number
        .parse::<u64>()
        .map_err(|e| format!("Invalid duration '{}': {}", value, e))?;
    Ok(Duration::from_millis(amount * multiplier))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("100ms").unwrap(), Duration::from_millis(100));
        assert_eq!(parse_duration("2s").unwrap(), Duration::from_secs(2));
        assert_eq!(parse_duration("3m").unwrap(), Duration::from_secs(180));
    }

    #[test]
    fn join_paths() {
        assert_eq!(join_path("cv://default/a", "b"), "cv://default/a/b");
        assert_eq!(join_path("cv://default/a/", "b"), "cv://default/a/b");
    }

    #[test]
    fn workset_paths_are_sharded() {
        assert_eq!(
            workset_path("cv://default/bench/ws", 0, 10_000),
            "cv://default/bench/ws/part-000000/item-000000000000"
        );
        assert_eq!(
            workset_path("cv://default/bench/ws", 10_000, 10_000),
            "cv://default/bench/ws/part-000001/item-000000010000"
        );
        assert_eq!(workset_dir_count(10_000, 10_000), 1);
        assert_eq!(workset_dir_count(10_001, 10_000), 2);
    }

    #[test]
    fn list_sampling_uses_shard_directories() {
        let list_dirs = vec![
            "cv://default/bench/ws/part-000000".to_string(),
            "cv://default/bench/ws/part-000001".to_string(),
        ];
        assert_eq!(
            select_list_dir("cv://default/bench/tmp", 0, 0, &list_dirs),
            "cv://default/bench/ws/part-000000"
        );
        assert_eq!(
            select_list_dir("cv://default/bench/tmp", 0, LIST_DIR_WALK_SPAN, &list_dirs),
            "cv://default/bench/ws/part-000001"
        );
        assert_eq!(
            select_list_dir("cv://default/bench/tmp", 1, 0, &list_dirs),
            "cv://default/bench/ws/part-000001"
        );
        assert_eq!(
            select_list_dir("cv://default/bench/tmp", 0, 0, &[]),
            "cv://default/bench/tmp"
        );
    }

    #[test]
    fn infer_access_mode() {
        assert_eq!(infer_mode("cv://default/a").unwrap(), BenchMode::Client);
        assert_eq!(infer_mode("file:///tmp/a").unwrap(), BenchMode::Fuse);
        assert_eq!(
            infer_mode("/definitely-not-existing-curvine-path").unwrap(),
            BenchMode::Client
        );
        assert!(infer_mode("relative/path").is_err());
        assert!(infer_mode("./bench/path").is_err());
        assert!(infer_mode("s3://bucket/path").is_err());
    }
}
