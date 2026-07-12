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

use curvine_ufs::S3Conf;
use orpc::{err_box, CommonResult};
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
            S3Conf::with_map(configs.clone())?;
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

pub fn parse_duration(interval_str: &str) -> Result<Duration, String> {
    let interval_str = interval_str.trim();

    if interval_str.is_empty() {
        return Ok(Duration::from_secs(5)); // Default 5 seconds
    }

    let last_char = interval_str.chars().last().unwrap_or('s');
    let num_part = &interval_str[0..interval_str.len() - 1];

    let num = num_part
        .parse::<u64>()
        .map_err(|_| format!("Unable to parse time interval: {}", interval_str))?;

    match last_char {
        's' => Ok(Duration::from_secs(num)),
        'm' => Ok(Duration::from_secs(num * 60)),
        'h' => Ok(Duration::from_secs(num * 3600)),
        _ => Err(format!("Unsupported time unit: {}", last_char)),
    }
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
    use super::bytes_to_string;

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
}
