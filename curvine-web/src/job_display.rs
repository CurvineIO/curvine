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

use chrono::{TimeZone, Utc};
use curvine_model::JobSourceType;

/// Human-readable job label: `job_{actor}_{YYYYMMDD-HHmmss}_{slug}`.
pub fn build_job_display_name(
    source_type: JobSourceType,
    source_path: &str,
    create_time_ms: i64,
    submitter: Option<&str>,
) -> String {
    let actor = job_actor(source_type, submitter);
    let timestamp = format_job_timestamp_ms(create_time_ms);
    let slug = job_path_slug(source_path);
    format!("job_{}_{}_{}", actor, timestamp, slug)
}

pub fn format_job_timestamp_ms(ms: i64) -> String {
    if ms <= 0 {
        return "00000000-000000".to_string();
    }
    let secs = ms / 1000;
    let nanos = ((ms % 1000) * 1_000_000) as u32;
    match Utc.timestamp_opt(secs, nanos) {
        chrono::LocalResult::Single(dt) => dt.format("%Y%m%d-%H%M%S").to_string(),
        _ => "00000000-000000".to_string(),
    }
}

fn job_actor(source_type: JobSourceType, submitter: Option<&str>) -> String {
    if let Some(name) = submitter.filter(|value| !value.is_empty()) {
        return sanitize_actor_name(name);
    }
    match source_type {
        JobSourceType::FsModeAuto => "ufsloader".to_string(),
        JobSourceType::Manual => "user".to_string(),
    }
}

fn sanitize_actor_name(name: &str) -> String {
    let mut out = String::new();
    for c in name.trim().to_lowercase().chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c);
        } else if !out.is_empty() && !out.ends_with('_') {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        return String::new();
    }
    trimmed.chars().take(16).collect()
}

pub fn job_path_slug(source_path: &str) -> String {
    let trimmed = source_path.trim();
    let without_scheme = trimmed
        .split_once("://")
        .map(|(_, path)| path)
        .unwrap_or(trimmed)
        .trim_matches('/');
    let segments: Vec<&str> = without_scheme
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if segments.is_empty() {
        return "root".to_string();
    }
    let tail = if segments.len() >= 2 {
        format!(
            "{}-{}",
            segments[segments.len() - 2],
            segments[segments.len() - 1]
        )
    } else {
        segments[segments.len() - 1].to_string()
    };
    sanitize_slug(&tail)
}

fn sanitize_slug(raw: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for c in raw.to_lowercase().chars() {
        let mapped = if c.is_ascii_alphanumeric() { c } else { '-' };
        if mapped == '-' {
            if !prev_dash && !out.is_empty() {
                out.push('-');
                prev_dash = true;
            }
        } else {
            out.push(mapped);
            prev_dash = false;
        }
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        return "path".to_string();
    }
    if trimmed.len() > 24 {
        trimmed[..24].trim_end_matches('-').to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_auto_export_display_name() {
        let name = build_job_display_name(
            JobSourceType::FsModeAuto,
            "/acc-hdfs/starrocks/test-01/8d833642/db",
            1_719_763_852_000,
            None,
        );
        assert!(name.starts_with("job_ufsloader_"));
        assert!(name.ends_with("_8d833642-db"));
    }

    #[test]
    fn formats_manual_display_name_with_submitter() {
        let name = build_job_display_name(
            JobSourceType::Manual,
            "/mnt/data/mytable",
            1_719_763_852_000,
            Some("liujifeng"),
        );
        assert!(name.starts_with("job_liujifeng_"));
        assert!(name.ends_with("_data-mytable"));
    }

    #[test]
    fn manual_without_submitter_uses_user_actor() {
        let name = build_job_display_name(
            JobSourceType::Manual,
            "/data/table",
            1_719_763_852_000,
            None,
        );
        assert!(name.starts_with("job_user_"));
    }

    #[test]
    fn auto_export_uses_submitter_when_attributed() {
        let name = build_job_display_name(
            JobSourceType::FsModeAuto,
            "/multi-fs-b/data.txt",
            1_719_763_852_000,
            Some("liujifeng"),
        );
        assert!(name.starts_with("job_liujifeng_"));
    }

    #[test]
    fn slug_handles_empty_path() {
        assert_eq!(job_path_slug(""), "root");
    }
}
