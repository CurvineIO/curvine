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

use curvine_core_error::{err_box, CommonResult};
use curvine_ufs_api::{CosnConf, S3Conf};
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

pub async fn handle_rpc_result<T, E: Display>(operation: impl Future<Output = Result<T, E>>) -> T {
    match operation.await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("❌ Error: {}", e);
            std::process::exit(1);
        }
    }
}

pub fn validate_path_and_configs(
    path: &str,
    configs: &HashMap<String, String>,
) -> CommonResult<()> {
    let scheme = extract_scheme(path);

    match scheme.as_deref() {
        Some("s3") => {
            validate_s3_path(path)?;
            S3Conf::validate(configs)?;
            Ok(())
        }
        Some("cosn") => {
            CosnConf::with_map(configs.clone())?;
            Ok(())
        }
        Some(_) => Ok(()), // No special validation for other schemes
        None => err_box!("Unrecognized path format: {}", path),
    }
}

pub fn validate_s3_path(path: &str) -> Result<(), String> {
    let (bucket, _) = extract_s3_bucket_and_key(path)
        .ok_or_else(|| format!("Invalid S3 path format: {}", path))?;

    if bucket.is_empty() {
        return Err("S3 bucket name cannot be empty".to_string());
    }

    if !bucket
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.')
    {
        return Err(format!(
            "S3 bucket name contains invalid characters: {}",
            bucket
        ));
    }

    Ok(())
}

pub fn extract_s3_bucket_and_key(path: &str) -> Option<(String, String)> {
    let path = path.strip_prefix("s3://")?;

    let slash_pos = path.find('/');

    match slash_pos {
        Some(pos) => {
            let bucket = path[..pos].to_string();
            let key = path[pos + 1..].to_string();
            Some((bucket, key))
        }
        None => Some((path.to_string(), String::new())),
    }
}

pub fn enrich_s3_configs(path: &str, configs: &mut HashMap<String, String>) {
    if !configs.contains_key("s3.bucket_name") {
        if let Some((bucket, _)) = extract_s3_bucket_and_key(path) {
            configs.insert("s3.bucket_name".to_string(), bucket.clone());
            println!("bucket name: {}", bucket);
        }
    }
}

pub fn enrich_hdfs_configs(path: &str, configs: &mut HashMap<String, String>) {
    if let Some((namenode, hdfs_path)) = extract_hdfs_namenode_and_path(path) {
        // Auto-set hdfs.namenode if not provided
        if !configs.contains_key("hdfs.namenode") {
            configs.insert("hdfs.namenode".to_string(), namenode.clone());
            println!("Auto-detected HDFS namenode: {}", namenode);
        }

        // Auto-set hdfs.root if not provided
        if !configs.contains_key("hdfs.root") && !hdfs_path.is_empty() {
            configs.insert("hdfs.root".to_string(), hdfs_path);
            println!(
                "Auto-detected HDFS root path: {}",
                configs.get("hdfs.root").unwrap()
            );
        }
    }
}

pub fn extract_hdfs_namenode_and_path(path: &str) -> Option<(String, String)> {
    if !path.starts_with("hdfs://") {
        return None;
    }

    let path = path.strip_prefix("hdfs://")?;

    // Find the first slash after the authority
    let slash_pos = path.find('/');

    match slash_pos {
        Some(pos) => {
            let authority = path[..pos].to_string();
            let hdfs_path = path[pos..].to_string(); // Include the leading slash
            let namenode = format!("hdfs://{}", authority);
            Some((namenode, hdfs_path))
        }
        None => {
            // No path component, just authority
            let namenode = format!("hdfs://{}", path);
            Some((namenode, "/".to_string()))
        }
    }
}

pub fn extract_scheme(path: &str) -> Option<String> {
    path.find("://").map(|pos| path[..pos].to_lowercase())
}

/// One message for every rejected interval, so callers never have to guess
/// which units are supported.
const DURATION_ERROR: &str =
    "Unable to parse time interval: expected a number followed by s, m, or h (e.g. 5s, 2m, 1h)";

pub fn parse_duration(interval_str: &str) -> Result<Duration, String> {
    let interval_str = interval_str.trim();

    if interval_str.is_empty() {
        return Ok(Duration::from_secs(5)); // Default 5 seconds
    }

    // Slice at a char boundary, not at len - 1: a multi-byte unit such as
    // "秒" used to leave the slice inside the code point and panic.
    let (num_part, unit) = match interval_str.char_indices().last() {
        Some((index, unit)) => (&interval_str[..index], unit),
        None => return Err(DURATION_ERROR.to_string()),
    };

    let num = num_part
        .parse::<u64>()
        .map_err(|_| DURATION_ERROR.to_string())?;
    let secs = match unit {
        's' => num,
        'm' => num
            .checked_mul(60)
            .ok_or_else(|| DURATION_ERROR.to_string())?,
        'h' => num
            .checked_mul(3600)
            .ok_or_else(|| DURATION_ERROR.to_string())?,
        _ => return Err(DURATION_ERROR.to_string()),
    };
    Ok(Duration::from_secs(secs))
}

pub fn format_duration(duration: &Duration) -> String {
    let total_secs = duration.as_secs();

    if total_secs < 60 {
        return format!("{} seconds", total_secs);
    }

    let mins = total_secs / 60;
    let secs = total_secs % 60;

    if mins < 60 {
        if secs == 0 {
            return format!("{} minutes", mins);
        } else {
            return format!("{} minutes {} seconds", mins, secs);
        }
    }

    let hours = mins / 60;
    let mins = mins % 60;

    if mins == 0 && secs == 0 {
        format!("{} hours", hours)
    } else if secs == 0 {
        format!("{} hours {} minutes", hours, mins)
    } else {
        format!("{} hours {} minutes {} seconds", hours, mins, secs)
    }
}

pub fn bytes_to_string(size: i64) -> String {
    const KIB: i64 = 1_i64 << 10;
    const MIB: i64 = 1_i64 << 20;
    const GIB: i64 = 1_i64 << 30;
    const TIB: i64 = 1_i64 << 40;
    const PIB: i64 = 1_i64 << 50;
    const EIB: i64 = 1_i64 << 60;

    let (value, unit) = if size >= EIB * 2 {
        (size as f64 / EIB as f64, "EB")
    } else if size >= PIB * 2 {
        (size as f64 / PIB as f64, "PB")
    } else if size >= TIB * 2 {
        (size as f64 / TIB as f64, "TB")
    } else if size >= GIB * 2 {
        (size as f64 / GIB as f64, "GB")
    } else if size >= MIB * 2 {
        (size as f64 / MIB as f64, "MB")
    } else if size >= KIB * 2 {
        (size as f64 / KIB as f64, "KB")
    } else {
        (size as f64, "B")
    };

    format!("{value:.1}{unit}")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::{
        bytes_to_string, enrich_hdfs_configs, enrich_s3_configs, extract_hdfs_namenode_and_path,
        extract_s3_bucket_and_key, extract_scheme, format_duration, parse_duration,
    };

    #[test]
    fn parse_duration_accepts_the_supported_units_and_the_empty_default() {
        assert_eq!(parse_duration("5s"), Ok(Duration::from_secs(5)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Ok(Duration::from_secs(3600)));
        assert_eq!(parse_duration("0s"), Ok(Duration::from_secs(0)));
        assert_eq!(parse_duration(""), Ok(Duration::from_secs(5)));
        assert_eq!(parse_duration("  2m  "), Ok(Duration::from_secs(120)));
    }

    #[test]
    fn parse_duration_rejects_bad_input_with_one_helpful_message() {
        for input in ["5", "s", "10x", "1.5h", "-1s", "5秒", "分"] {
            let error = parse_duration(input).expect_err("invalid interval must be rejected");
            assert!(error.contains("s, m, or h"), "{input}: {error}");
        }
    }

    #[test]
    fn parse_duration_does_not_panic_on_a_multibyte_unit() {
        // "5秒" ends in a three-byte character. The old byte-offset slice
        // landed inside it and panicked with "not a char boundary".
        assert!(parse_duration("5秒").is_err());
    }

    #[test]
    fn parse_duration_reports_an_overflow_instead_of_panicking() {
        assert!(parse_duration("18446744073709551615h").is_err());
    }

    #[test]
    fn format_duration_renders_seconds_minutes_and_hours() {
        assert_eq!(format_duration(&Duration::from_secs(0)), "0 seconds");
        assert_eq!(format_duration(&Duration::from_secs(59)), "59 seconds");
        assert_eq!(format_duration(&Duration::from_secs(60)), "1 minutes");
        assert_eq!(
            format_duration(&Duration::from_secs(90)),
            "1 minutes 30 seconds"
        );
        assert_eq!(format_duration(&Duration::from_secs(3600)), "1 hours");
        assert_eq!(
            format_duration(&Duration::from_secs(3660)),
            "1 hours 1 minutes"
        );
    }

    #[test]
    fn bytes_to_string_formats_report_capacity_values() {
        assert_eq!(bytes_to_string(0), "0.0B");
        assert_eq!(bytes_to_string(1), "1.0B");
        assert_eq!(bytes_to_string(1024), "1024.0B");
        assert_eq!(bytes_to_string(2047), "2047.0B");
        assert_eq!(bytes_to_string(2048), "2.0KB");
        assert_eq!(bytes_to_string(3 * (1_i64 << 20)), "3.0MB");
        assert_eq!(bytes_to_string(5 * (1_i64 << 30)), "5.0GB");
        assert_eq!(bytes_to_string(7 * (1_i64 << 40)), "7.0TB");
        assert_eq!(bytes_to_string(9 * (1_i64 << 50)), "9.0PB");
        assert_eq!(bytes_to_string(2 * (1_i64 << 60)), "2.0EB");
        assert_eq!(bytes_to_string(i64::MAX), "8.0EB");
    }

    #[test]
    fn extract_s3_bucket_and_key_parses_bucket_and_key() {
        assert_eq!(
            extract_s3_bucket_and_key("s3://bucket"),
            Some(("bucket".to_string(), String::new()))
        );
        assert_eq!(
            extract_s3_bucket_and_key("s3://bucket/a/b.txt"),
            Some(("bucket".to_string(), "a/b.txt".to_string()))
        );
        assert_eq!(
            extract_s3_bucket_and_key("s3://bucket/"),
            Some(("bucket".to_string(), String::new()))
        );
    }

    #[test]
    fn extract_s3_bucket_and_key_rejects_non_s3_paths() {
        assert_eq!(extract_s3_bucket_and_key("file:///tmp/a"), None);
        assert_eq!(extract_s3_bucket_and_key("bucket/key"), None);
    }

    #[test]
    fn extract_s3_bucket_and_key_preserves_unusual_keys() {
        assert_eq!(
            extract_s3_bucket_and_key("s3://bucket/a://b c"),
            Some(("bucket".to_string(), "a://b c".to_string()))
        );
    }

    #[test]
    fn extract_hdfs_namenode_and_path_parses_authority_and_root() {
        assert_eq!(
            extract_hdfs_namenode_and_path("hdfs://nn:8020"),
            Some(("hdfs://nn:8020".to_string(), "/".to_string()))
        );
        assert_eq!(
            extract_hdfs_namenode_and_path("hdfs://nn:8020/a/b"),
            Some(("hdfs://nn:8020".to_string(), "/a/b".to_string()))
        );
        assert_eq!(
            extract_hdfs_namenode_and_path("hdfs://nn:8020/"),
            Some(("hdfs://nn:8020".to_string(), "/".to_string()))
        );
        assert_eq!(
            extract_hdfs_namenode_and_path("hdfs://"),
            Some(("hdfs://".to_string(), "/".to_string()))
        );
    }

    #[test]
    fn extract_hdfs_namenode_and_path_rejects_non_hdfs_paths() {
        assert_eq!(extract_hdfs_namenode_and_path("s3://bucket/key"), None);
    }

    #[test]
    fn extract_scheme_normalizes_and_handles_missing_scheme() {
        assert_eq!(extract_scheme("s3://bucket"), Some("s3".to_string()));
        assert_eq!(extract_scheme("S3://bucket"), Some("s3".to_string()));
        assert_eq!(extract_scheme("hdfs://nn"), Some("hdfs".to_string()));
        assert_eq!(extract_scheme("file:///tmp/a"), Some("file".to_string()));
        assert_eq!(extract_scheme("cv://cluster"), Some("cv".to_string()));
        assert_eq!(extract_scheme("bucket/key"), None);
    }

    #[test]
    fn enrich_s3_configs_fills_missing_bucket_without_overwriting() {
        let mut configs = HashMap::new();
        enrich_s3_configs("s3://bucket/a/b.txt", &mut configs);
        assert_eq!(
            configs.get("s3.bucket_name").map(String::as_str),
            Some("bucket")
        );

        let mut existing = HashMap::from([("s3.bucket_name".to_string(), "kept".to_string())]);
        enrich_s3_configs("s3://other/a", &mut existing);
        assert_eq!(
            existing.get("s3.bucket_name").map(String::as_str),
            Some("kept")
        );
    }

    #[test]
    fn enrich_hdfs_configs_fills_missing_values_without_overwriting() {
        let mut configs = HashMap::new();
        enrich_hdfs_configs("hdfs://nn:8020/a/b", &mut configs);
        assert_eq!(
            configs.get("hdfs.namenode").map(String::as_str),
            Some("hdfs://nn:8020")
        );
        assert_eq!(configs.get("hdfs.root").map(String::as_str), Some("/a/b"));

        let mut existing = HashMap::from([
            ("hdfs.namenode".to_string(), "kept".to_string()),
            ("hdfs.root".to_string(), "/kept".to_string()),
        ]);
        enrich_hdfs_configs("hdfs://other:8020/a", &mut existing);
        assert_eq!(
            existing.get("hdfs.namenode").map(String::as_str),
            Some("kept")
        );
        assert_eq!(existing.get("hdfs.root").map(String::as_str), Some("/kept"));
    }

    #[test]
    fn enrich_helpers_ignore_invalid_paths() {
        let mut configs = HashMap::new();
        enrich_s3_configs("file:///tmp/a", &mut configs);
        enrich_hdfs_configs("s3://bucket/key", &mut configs);
        assert!(configs.is_empty());
    }
}
