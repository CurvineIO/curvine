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

use crate::common::Utils;
use crate::CommonResult;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::fs;
use std::path::Path;

const LATEST_SUMMARY_FILE: &str = "heap-profile-latest-summary.json";

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct HeapProfileSummary {
    pub runtime_enabled: bool,
    pub sample_interval_bytes: usize,
    pub capture_count: u64,
    pub last_capture_epoch_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HeapTraceArtifactKind {
    Profile,
    Flamegraph,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeapTraceArtifact {
    pub kind: HeapTraceArtifactKind,
    pub media_type: String,
    pub file_name: String,
    pub payload: Vec<u8>,
}

impl HeapTraceArtifact {
    pub fn new(
        kind: HeapTraceArtifactKind,
        media_type: impl Into<String>,
        file_name: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            kind,
            media_type: media_type.into(),
            file_name: file_name.into(),
            payload,
        }
    }
}

pub fn next_profile_id() -> String {
    Utils::uuid()
}

pub fn write_summary(path: &Path, summary: &HeapProfileSummary) -> CommonResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let payload = summary_to_json(summary);
    fs::write(path, &payload)?;

    if let Some(parent) = path.parent() {
        fs::write(parent.join(LATEST_SUMMARY_FILE), payload)?;
    }

    Ok(())
}

pub fn prune_old_artifacts(dir: &Path, keep_last: usize) -> CommonResult<()> {
    if !dir.exists() {
        return Ok(());
    }

    let mut entries = fs::read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_type()
                .map(|kind| kind.is_file())
                .unwrap_or(false)
        })
        .filter(|entry| {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            file_name.as_ref() != LATEST_SUMMARY_FILE && is_profile_related(file_name.as_ref())
        })
        .collect::<Vec<_>>();

    if entries.len() <= keep_last {
        return Ok(());
    }

    entries.sort_by_key(|entry| {
        let modified = entry.metadata().and_then(|meta| meta.modified()).ok();
        let file_name = entry.file_name().to_string_lossy().into_owned();
        (Reverse(modified), Reverse(file_name))
    });

    for entry in entries.into_iter().skip(keep_last) {
        fs::remove_file(entry.path())?;
    }

    Ok(())
}

fn summary_to_json(summary: &HeapProfileSummary) -> Vec<u8> {
    let last_capture_epoch_ms = summary
        .last_capture_epoch_ms
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string());

    format!(
        concat!(
            "{{\n",
            "  \"runtime_enabled\": {},\n",
            "  \"sample_interval_bytes\": {},\n",
            "  \"capture_count\": {},\n",
            "  \"last_capture_epoch_ms\": {}\n",
            "}}\n"
        ),
        summary.runtime_enabled,
        summary.sample_interval_bytes,
        summary.capture_count,
        last_capture_epoch_ms
    )
    .into_bytes()
}

fn is_profile_related(file_name: &str) -> bool {
    file_name == LATEST_SUMMARY_FILE
        || file_name.contains("profile")
        || file_name.contains("summary")
        || file_name.ends_with(".pb.gz")
        || file_name.ends_with(".svg")
}
