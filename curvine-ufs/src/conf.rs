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

use orpc::common::DurationUnit;
use orpc::{err_box, CommonResult};
use std::collections::HashMap;
use std::ops::Deref;
use std::time::Duration;

pub struct ConfMap(HashMap<String, String>);

impl ConfMap {
    pub fn new(map: HashMap<String, String>) -> Self {
        Self(map)
    }

    pub fn get_string(&self, key: &str) -> CommonResult<String> {
        if let Some(v) = self.0.get(key) {
            Ok(v.clone())
        } else {
            err_box!("{} cannot be empty", key)
        }
    }

    pub fn get_bool(&self, key: &str) -> CommonResult<bool> {
        let value = self.get_string(key)?;
        match value.trim().to_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => err_box!("Invalid boolean string"),
        }
    }

    pub fn get_u32(&self, key: &str) -> CommonResult<u32> {
        let value = self.get_string(key)?;
        value
            .trim()
            .parse::<u32>()
            .map_err(|_| format!("Invalid u32 value for key '{}': {}", key, value).into())
    }

    pub fn get_u64(&self, key: &str) -> CommonResult<u64> {
        let value = self.get_string(key)?;
        value
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("Invalid u64 value for key '{}': {}", key, value).into())
    }
}

#[derive(Debug, Clone)]
pub struct S3Conf {
    pub endpoint_url: String,
    pub access_key: String,
    pub secret_key: String,
    pub region_name: String,
    pub force_path_style: bool,

    pub retry_times: u32,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,

    pub properties: HashMap<String, String>,
}

/// Unified OpenDAL configuration for timeout and retry settings
/// Uses the same default values as S3Conf for consistency
#[derive(Debug, Clone)]
pub struct OpendalConf {
    pub retry_times: u32,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub retry_interval_ms: u64,
    pub retry_max_delay_ms: u64,
}

impl S3Conf {
    pub const ENDPOINT: &'static str = "s3.endpoint_url";
    pub const ACCESS_KEY: &'static str = "s3.credentials.access";
    pub const SECRET_KEY: &'static str = "s3.credentials.secret";
    pub const REGION_NAME: &'static str = "s3.region_name";
    pub const FORCE_PATH_STYLE: &'static str = "s3.force.path.style";
    pub const RETRY_TIMES: &'static str = "s3.retry_times";
    pub const CONNECT_TIMEOUT: &'static str = "s3.connect_timeout";
    pub const READ_TIMEOUT: &'static str = "s3.read_timeout";

    pub const DEFAULT_RETRY_TIMES: u32 = 3;
    pub const DEFAULT_CONNECT_TIMEOUT: &'static str = "30s";
    pub const DEFAULT_READ_TIMEOUT: &'static str = "120s";

    pub fn with_map(properties: HashMap<String, String>) -> CommonResult<Self> {
        let map = ConfMap::new(properties);

        let endpoint_url = map.get_string(Self::ENDPOINT)?;
        if !endpoint_url.starts_with("http://") && !endpoint_url.starts_with("https://") {
            return err_box!("s3.endpoint_url must start with http:// or https://");
        }

        let access_key = map.get_string(Self::ACCESS_KEY)?;
        let secret_key = map.get_string(Self::SECRET_KEY)?;

        let region_name = if endpoint_url.contains("amazonaws.com") {
            map.get_string(Self::REGION_NAME)?
        } else {
            map.get_string(Self::REGION_NAME)
                .unwrap_or("undefined".to_string())
        };

        let force_path_style = map.get_bool(Self::FORCE_PATH_STYLE).unwrap_or(false);

        let retry_times = map
            .get_u32(Self::RETRY_TIMES)
            .unwrap_or(Self::DEFAULT_RETRY_TIMES);

        let connect_timeout = map
            .get_string(Self::CONNECT_TIMEOUT)
            .unwrap_or(Self::DEFAULT_CONNECT_TIMEOUT.to_string());
        let connect_timeout = DurationUnit::from_str(&connect_timeout)?.as_duration();

        let read_timeout = map
            .get_string(Self::READ_TIMEOUT)
            .unwrap_or(Self::DEFAULT_READ_TIMEOUT.to_string());
        let read_timeout = DurationUnit::from_str(&read_timeout)?.as_duration();

        Ok(Self {
            endpoint_url,
            access_key,
            secret_key,
            region_name,
            force_path_style,
            retry_times,
            connect_timeout,
            read_timeout,
            properties: map.0,
        })
    }
}

impl OpendalConf {
    // Configuration keys
    pub const RETRY_TIMES: &'static str = "opendal.retry_times";
    pub const CONNECT_TIMEOUT: &'static str = "opendal.connect_timeout";
    pub const READ_TIMEOUT: &'static str = "opendal.read_timeout";
    pub const RETRY_INTERVAL_MS: &'static str = "opendal.retry_interval_ms";
    pub const RETRY_MAX_DELAY_MS: &'static str = "opendal.retry_max_delay_ms";

    // Default values - using S3Conf defaults for consistency
    pub const DEFAULT_RETRY_TIMES: u32 = S3Conf::DEFAULT_RETRY_TIMES; // 3
    pub const DEFAULT_CONNECT_TIMEOUT: &'static str = S3Conf::DEFAULT_CONNECT_TIMEOUT; // "30s"
    pub const DEFAULT_READ_TIMEOUT: &'static str = S3Conf::DEFAULT_READ_TIMEOUT; // "120s"
    pub const DEFAULT_RETRY_INTERVAL_MS: u64 = 1000; // 1 second
    pub const DEFAULT_RETRY_MAX_DELAY_MS: u64 = 10000; // 10 seconds

    /// Create OpendalConf from configuration map
    pub fn from_map(properties: &HashMap<String, String>) -> CommonResult<Self> {
        let map = ConfMap::new(properties.clone());

        let retry_times = map
            .get_u32(Self::RETRY_TIMES)
            .unwrap_or(Self::DEFAULT_RETRY_TIMES);

        let connect_timeout_str = map
            .get_string(Self::CONNECT_TIMEOUT)
            .unwrap_or(Self::DEFAULT_CONNECT_TIMEOUT.to_string());
        let connect_timeout = DurationUnit::from_str(&connect_timeout_str)?.as_duration();

        let read_timeout_str = map
            .get_string(Self::READ_TIMEOUT)
            .unwrap_or(Self::DEFAULT_READ_TIMEOUT.to_string());
        let read_timeout = DurationUnit::from_str(&read_timeout_str)?.as_duration();

        let retry_interval_ms = map
            .get_u64(Self::RETRY_INTERVAL_MS)
            .unwrap_or(Self::DEFAULT_RETRY_INTERVAL_MS);

        let retry_max_delay_ms = map
            .get_u64(Self::RETRY_MAX_DELAY_MS)
            .unwrap_or(Self::DEFAULT_RETRY_MAX_DELAY_MS);

        Ok(Self {
            retry_times,
            connect_timeout,
            read_timeout,
            retry_interval_ms,
            retry_max_delay_ms,
        })
    }

    pub fn total_timeout_ms(&self) -> u64 {
        self.connect_timeout.as_millis() as u64 + self.read_timeout.as_millis() as u64
    }
}

impl Deref for S3Conf {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.properties
    }
}

/// OSS-HDFS configuration (JindoSDK).
///
/// Notes:
/// - The underlying JindoSDK configuration keys are prefixed with `fs.oss.*`.
/// - This config struct is used by the `oss://` UFS implementation (`oss_hdfs` module).
#[derive(Debug, Clone)]
pub struct OssHdfsConf {
    pub endpoint_url: String,
    pub access_key: String,
    pub secret_key: String,
    pub region_name: Option<String>,
    pub data_endpoint: Option<String>,
    pub second_level_domain_enable: bool,
    pub data_lake_storage_enable: bool,
    pub properties: HashMap<String, String>,
}

impl OssHdfsConf {
    // Configuration keys
    pub const ENDPOINT: &'static str = "fs.oss.endpoint";
    pub const ACCESS_KEY_ID: &'static str = "fs.oss.accessKeyId";
    pub const ACCESS_KEY_SECRET: &'static str = "fs.oss.accessKeySecret";
    pub const REGION: &'static str = "fs.oss.region";
    pub const DATA_ENDPOINT: &'static str = "fs.oss.data.endpoint";
    pub const SECOND_LEVEL_DOMAIN_ENABLE: &'static str = "fs.oss.second.level.domain.enable";
    pub const DATA_LAKE_STORAGE_ENABLE: &'static str = "fs.oss.data.lake.storage.enable";

    // Default values
    pub const DEFAULT_SECOND_LEVEL_DOMAIN_ENABLE: bool = true;
    pub const DEFAULT_DATA_LAKE_STORAGE_ENABLE: bool = true;

    /// Create OssHdfsConf from configuration map
    pub fn with_map(properties: HashMap<String, String>) -> CommonResult<Self> {
        let map = ConfMap::new(properties);

        let endpoint_url = map.get_string(Self::ENDPOINT)?;
        let access_key = map.get_string(Self::ACCESS_KEY_ID)?;
        let secret_key = map.get_string(Self::ACCESS_KEY_SECRET)?;
        let region_name = map.get_string(Self::REGION).ok();
        let data_endpoint = map.get_string(Self::DATA_ENDPOINT).ok();
        let second_level_domain_enable = map
            .get_bool(Self::SECOND_LEVEL_DOMAIN_ENABLE)
            .unwrap_or(Self::DEFAULT_SECOND_LEVEL_DOMAIN_ENABLE);
        let data_lake_storage_enable = map
            .get_bool(Self::DATA_LAKE_STORAGE_ENABLE)
            .unwrap_or(Self::DEFAULT_DATA_LAKE_STORAGE_ENABLE);

        Ok(Self {
            endpoint_url,
            access_key,
            secret_key,
            region_name,
            data_endpoint,
            second_level_domain_enable,
            data_lake_storage_enable,
            properties: map.0,
        })
    }
}

impl Deref for OssHdfsConf {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.properties
    }
}
