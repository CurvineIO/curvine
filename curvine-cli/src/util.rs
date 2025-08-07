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

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use orpc::CommonResult;
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

/// Parse configuration string
#[allow(unused)]
pub fn parse_config(config_str: Option<&str>) -> CommonResult<HashMap<String, String>> {
    let mut configs = HashMap::new();

    if let Some(config) = config_str {
        for pair in config.split(',') {
            let parts: Vec<&str> = pair.split('=').collect();
            if parts.len() == 2 {
                configs.insert(parts[0].trim().to_string(), parts[1].trim().to_string());
            } else {
                return Err(format!("Invalid config format: {}", pair).into());
            }
        }
    }

    Ok(configs)
}

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
) -> Result<(), String> {
    let scheme = extract_scheme(path);

    match scheme.as_deref() {
        Some("s3") => {
            validate_s3_path(path)?;
            validate_s3_configs(configs)
        }
        Some("minio") => validate_minio_configs(path),
        Some(_) => Ok(()), // No special validation for other schemes
        None => Err(format!("Unrecognized path format: {}", path)),
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

    if !configs.contains_key("conn_timeout") {
        configs.insert("conn_timeout".to_string(), "3".to_string());
    }

    if !configs.contains_key("retry_times") {
        configs.insert("retry_times".to_string(), "3".to_string());
    }

    if !configs.contains_key("read_timeout") {
        configs.insert("read_timeout".to_string(), "5".to_string());
    }
}

pub fn extract_scheme(path: &str) -> Option<String> {
    path.find("://").map(|pos| path[..pos].to_lowercase())
}

pub fn validate_s3_configs(configs: &HashMap<String, String>) -> Result<(), String> {
    if !configs.contains_key("s3.region_name") {
        return Err("Missing required S3 parameter: s3.region_name".to_string());
    }

    if let Some(endpoint) = configs.get("s3.endpoint_url") {
        if endpoint.is_empty() {
            return Err("s3.endpoint_url cannot be empty".to_string());
        }

        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err("s3.endpoint_url must start with http:// or https://".to_string());
        }
    }

    let has_access = configs.contains_key("s3.credentials.access");
    let has_secret = configs.contains_key("s3.credentials.secret");

    if has_access != has_secret {
        let missing = if has_access {
            "s3.credentials.secret"
        } else {
            "s3.credentials.access"
        };

        return Err(format!(
            "S3 authentication information incomplete: access provided but missing {}",
            missing
        ));
    }

    if has_access && has_secret {
        let access = configs.get("s3.credentials.access").unwrap();
        let secret = configs.get("s3.credentials.secret").unwrap();

        if access.is_empty() {
            return Err("s3.credentials.access cannot be empty".to_string());
        }

        if secret.is_empty() {
            return Err("s3.credentials.secret cannot be empty".to_string());
        }
    }

    Ok(())
}

pub fn validate_minio_configs(path: &str) -> Result<(), String> {
    let file_path = path.strip_prefix("file://").unwrap_or(path);

    if !Path::new(file_path).exists() {
        return Err(format!("Local file does not exist: {}", file_path));
    }

    Ok(())
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

pub fn bytes_to_string(size: &BigInt) -> String {
    let eib = BigInt::from(1i64 << 60);
    let pib = BigInt::from(1i64 << 50);
    let tib = BigInt::from(1i64 << 40);
    let gib = BigInt::from(1i64 << 30);
    let mib = BigInt::from(1i64 << 20);
    let kib = BigInt::from(1i64 << 10);

    let eib_threshold = &BigInt::from(1i64 << 11) * &eib;

    if size >= &eib_threshold {
        let bd = BigDecimal::from_str(&size.to_string()).unwrap().round(3);
        format!("{} B", bd)
    } else {
        let (value, unit) = if size >= &(&eib * 2) {
            (
                BigDecimal::from_str(&size.to_string()).unwrap()
                    / BigDecimal::from_str(&eib.to_string()).unwrap(),
                "EB",
            )
        } else if size >= &(&pib * 2) {
            (
                BigDecimal::from_str(&size.to_string()).unwrap()
                    / BigDecimal::from_str(&pib.to_string()).unwrap(),
                "PB",
            )
        } else if size >= &(&tib * 2) {
            (
                BigDecimal::from_str(&size.to_string()).unwrap()
                    / BigDecimal::from_str(&tib.to_string()).unwrap(),
                "TB",
            )
        } else if size >= &(&gib * 2) {
            (
                BigDecimal::from_str(&size.to_string()).unwrap()
                    / BigDecimal::from_str(&gib.to_string()).unwrap(),
                "GB",
            )
        } else if size >= &(&mib * 2) {
            (
                BigDecimal::from_str(&size.to_string()).unwrap()
                    / BigDecimal::from_str(&mib.to_string()).unwrap(),
                "MB",
            )
        } else if size >= &(&kib * 2) {
            (
                BigDecimal::from_str(&size.to_string()).unwrap()
                    / BigDecimal::from_str(&kib.to_string()).unwrap(),
                "KB",
            )
        } else {
            (BigDecimal::from_str(&size.to_string()).unwrap(), "B")
        };

        format!("{:.1}{}", value, unit)
    }
}
