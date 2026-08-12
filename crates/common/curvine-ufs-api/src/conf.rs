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
use curvine_runtime::common::DurationUnit;
use std::collections::HashMap;
use std::ops::Deref;
use std::time::Duration;
use url::Url;

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
            _ => err_box!("{} must be true or false", key),
        }
    }

    pub fn get_bool_or(&self, key: &str, default: bool) -> CommonResult<bool> {
        if self.0.contains_key(key) {
            self.get_bool(key)
        } else {
            Ok(default)
        }
    }

    pub fn get_u32(&self, key: &str) -> CommonResult<u32> {
        let value = self.get_string(key)?;
        value
            .trim()
            .parse::<u32>()
            .map_err(|_| format!("Invalid u32 value for key '{}': {}", key, value).into())
    }

    pub fn get_u32_or(&self, key: &str, default: u32) -> CommonResult<u32> {
        if self.0.contains_key(key) {
            self.get_u32(key)
        } else {
            Ok(default)
        }
    }

    pub fn get_u64(&self, key: &str) -> CommonResult<u64> {
        let value = self.get_string(key)?;
        value
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("Invalid u64 value for key '{}': {}", key, value).into())
    }

    pub fn get_u64_or(&self, key: &str, default: u64) -> CommonResult<u64> {
        if self.0.contains_key(key) {
            self.get_u64(key)
        } else {
            Ok(default)
        }
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
    /// Per-request read buffer size (bytes) for the object reader. Also the size
    /// of each concurrent range chunk when `read_concurrent > 1`. Internal
    /// constant (not user-tunable); defaults to [`Self::DEFAULT_READ_CHUNK_SIZE`].
    pub read_chunk_size: usize,
    /// Number of concurrent range requests issued against a single object while
    /// reading. Internal constant (not user-tunable); defaults to
    /// [`Self::DEFAULT_READ_CONCURRENT`]. Parallelism for large loads is driven
    /// instead by the outer fan-out (`load_task.parallel_streams`).
    pub read_concurrent: usize,
}

impl S3Conf {
    pub const ENDPOINT: &'static str = "s3.endpoint_url";
    pub const ACCESS_KEY: &'static str = "s3.credentials.access";
    pub const SECRET_KEY: &'static str = "s3.credentials.secret";
    pub const REGION_NAME: &'static str = "s3.region_name";
    pub const LEGACY_ACCESS_KEY: &'static str = "s3.access_key";
    pub const LEGACY_SECRET_KEY: &'static str = "s3.secret_key";
    pub const LEGACY_REGION_NAME: &'static str = "s3.region";
    pub const FORCE_PATH_STYLE: &'static str = "s3.force.path.style";
    pub const LIST_OBJECTS_VERSION: &'static str = "s3.list_objects_version";
    pub const RETRY_TIMES: &'static str = "s3.retry_times";
    pub const CONNECT_TIMEOUT: &'static str = "s3.connect_timeout";
    pub const READ_TIMEOUT: &'static str = "s3.read_timeout";

    pub const DEFAULT_RETRY_TIMES: u32 = 3;
    pub const DEFAULT_CONNECT_TIMEOUT: &'static str = "30s";
    pub const DEFAULT_READ_TIMEOUT: &'static str = "120s";

    pub fn validate_region(properties: &HashMap<String, String>) -> CommonResult<()> {
        Self::required_property(properties, Self::REGION_NAME)
    }

    pub fn validate(properties: &HashMap<String, String>) -> CommonResult<()> {
        let conf = Self::with_map(properties.clone())?;
        Self::uses_list_objects_v1(&conf.properties)?;
        OpendalConf::from_map(&conf.properties)?;
        Ok(())
    }

    /// Convert the S3 property names used by older clients into the mount schema.
    pub fn canonicalize_properties(
        mut properties: HashMap<String, String>,
    ) -> CommonResult<HashMap<String, String>> {
        for (legacy, canonical) in [
            (Self::LEGACY_ACCESS_KEY, Self::ACCESS_KEY),
            (Self::LEGACY_SECRET_KEY, Self::SECRET_KEY),
            (Self::LEGACY_REGION_NAME, Self::REGION_NAME),
        ] {
            let Some(value) = properties.remove(legacy) else {
                continue;
            };

            if let Some(canonical_value) = properties.get(canonical) {
                if canonical_value != &value {
                    return err_box!(
                        "conflicting S3 configuration: both {} and {} are set",
                        legacy,
                        canonical
                    );
                }
            } else {
                properties.insert(canonical.to_string(), value);
            }
        }
        Ok(properties)
    }

    pub fn uses_list_objects_v1(properties: &HashMap<String, String>) -> CommonResult<bool> {
        match properties
            .get(Self::LIST_OBJECTS_VERSION)
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("") | Some("v2") => Ok(false),
            Some("v1") => Ok(true),
            Some(value) => err_box!(
                "s3.list_objects_version must be 'v1' or 'v2', got '{}'",
                value
            ),
        }
    }

    fn required_property(properties: &HashMap<String, String>, key: &str) -> CommonResult<()> {
        if properties
            .get(key)
            .is_some_and(|value| !value.trim().is_empty())
        {
            Ok(())
        } else {
            err_box!("{} is required", key)
        }
    }

    pub fn with_map(properties: HashMap<String, String>) -> CommonResult<Self> {
        let map = ConfMap::new(Self::canonicalize_properties(properties)?);

        Self::required_property(&map.0, Self::ENDPOINT)?;
        let endpoint_url = map.get_string(Self::ENDPOINT)?;
        let endpoint = Url::parse(&endpoint_url)
            .map_err(|_| "s3.endpoint_url must be an absolute http(s) URL with a host")?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
            return err_box!("s3.endpoint_url must be an absolute http(s) URL with a host");
        }

        Self::required_property(&map.0, Self::ACCESS_KEY)?;
        let access_key = map.get_string(Self::ACCESS_KEY)?;
        Self::required_property(&map.0, Self::SECRET_KEY)?;
        let secret_key = map.get_string(Self::SECRET_KEY)?;

        Self::validate_region(&map.0)?;
        let region_name = map.get_string(Self::REGION_NAME)?;

        let force_path_style = map.get_bool_or(Self::FORCE_PATH_STYLE, false)?;

        let retry_times = map.get_u32_or(Self::RETRY_TIMES, Self::DEFAULT_RETRY_TIMES)?;

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
    pub const DEFAULT_READ_CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16 MiB
    pub const DEFAULT_READ_CONCURRENT: usize = 2; // 2 parallel range requests per object

    /// Create OpendalConf from configuration map
    pub fn from_map(properties: &HashMap<String, String>) -> CommonResult<Self> {
        let map = ConfMap::new(properties.clone());

        let retry_times = map.get_u32_or(Self::RETRY_TIMES, Self::DEFAULT_RETRY_TIMES)?;

        let connect_timeout_str = map
            .get_string(Self::CONNECT_TIMEOUT)
            .unwrap_or(Self::DEFAULT_CONNECT_TIMEOUT.to_string());
        let connect_timeout = DurationUnit::from_str(&connect_timeout_str)?.as_duration();

        let read_timeout_str = map
            .get_string(Self::READ_TIMEOUT)
            .unwrap_or(Self::DEFAULT_READ_TIMEOUT.to_string());
        let read_timeout = DurationUnit::from_str(&read_timeout_str)?.as_duration();

        let retry_interval_ms =
            map.get_u64_or(Self::RETRY_INTERVAL_MS, Self::DEFAULT_RETRY_INTERVAL_MS)?;

        let retry_max_delay_ms =
            map.get_u64_or(Self::RETRY_MAX_DELAY_MS, Self::DEFAULT_RETRY_MAX_DELAY_MS)?;

        // read_chunk_size and read_concurrent are intentionally NOT user-tunable
        // via mount properties: benchmarking showed the defaults saturate the
        // link, and the outer parallel-load fan-out (load_task.parallel_streams)
        // is the knob that matters. Kept as internal constants.
        //
        // NOTE: this is a silent behavior change. These used to be per-mount
        // tunables (legacy keys `opendal.read_chunk_size_in_bytes` /
        // `opendal.read_concurrent`). Existing mounts that still carry those keys
        // are now ignored -- not rejected -- so the values below always win.
        let read_chunk_size = Self::DEFAULT_READ_CHUNK_SIZE.max(1);
        let read_concurrent = Self::DEFAULT_READ_CONCURRENT.max(1);

        Ok(Self {
            retry_times,
            connect_timeout,
            read_timeout,
            retry_interval_ms,
            retry_max_delay_ms,
            read_chunk_size,
            read_concurrent,
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
/// - User-facing configuration keys use `oss.*` format (without `fs.` prefix).
/// - JindoSDK internal configuration keys use `fs.oss.*` format (with `fs.` prefix).
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
    // User-facing configuration keys (without fs. prefix)
    pub const USER_ENDPOINT: &'static str = "oss.endpoint";
    pub const USER_ACCESS_KEY_ID: &'static str = "oss.accessKeyId";
    pub const USER_ACCESS_KEY_SECRET: &'static str = "oss.accessKeySecret";
    pub const USER_REGION: &'static str = "oss.region";
    pub const USER_DATA_ENDPOINT: &'static str = "oss.data.endpoint";
    pub const USER_SECOND_LEVEL_DOMAIN_ENABLE: &'static str = "oss.second.level.domain.enable";
    pub const USER_DATA_LAKE_STORAGE_ENABLE: &'static str = "oss.data.lake.storage.enable";

    // JindoSDK internal configuration keys (with fs. prefix)
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
    /// Uses user-facing configuration keys (oss.*) for reading from config map
    pub fn with_map(properties: HashMap<String, String>) -> CommonResult<Self> {
        let map = ConfMap::new(properties);

        let endpoint_url = map.get_string(Self::USER_ENDPOINT)?;
        let access_key = map.get_string(Self::USER_ACCESS_KEY_ID)?;
        let secret_key = map.get_string(Self::USER_ACCESS_KEY_SECRET)?;
        let region_name = map.get_string(Self::USER_REGION).ok();
        let data_endpoint = map.get_string(Self::USER_DATA_ENDPOINT).ok();
        let second_level_domain_enable = map
            .get_bool(Self::USER_SECOND_LEVEL_DOMAIN_ENABLE)
            .unwrap_or(Self::DEFAULT_SECOND_LEVEL_DOMAIN_ENABLE);
        let data_lake_storage_enable = map
            .get_bool(Self::USER_DATA_LAKE_STORAGE_ENABLE)
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

#[cfg(test)]
mod opendal_conf_tests {
    use super::OpendalConf;
    use std::collections::HashMap;

    #[test]
    fn defaults_when_unset() {
        let conf = OpendalConf::from_map(&HashMap::new()).unwrap();
        assert_eq!(conf.read_chunk_size, OpendalConf::DEFAULT_READ_CHUNK_SIZE);
        assert_eq!(conf.read_concurrent, OpendalConf::DEFAULT_READ_CONCURRENT);
        // Locks in the tuned defaults (16 MiB chunk, 2 concurrent range GETs).
        assert_eq!(conf.read_chunk_size, 16 * 1024 * 1024);
        assert_eq!(conf.read_concurrent, 2);
    }

    #[test]
    fn read_tuning_is_not_user_overridable() {
        // read_chunk_size / read_concurrent are internal constants now: any mount
        // property with those (legacy) keys must be ignored, not applied.
        let mut props = HashMap::new();
        props.insert(
            "opendal.read_chunk_size_in_bytes".to_string(),
            "1024".to_string(),
        );
        props.insert("opendal.read_concurrent".to_string(), "32".to_string());
        let conf = OpendalConf::from_map(&props).unwrap();
        assert_eq!(conf.read_chunk_size, OpendalConf::DEFAULT_READ_CHUNK_SIZE);
        assert_eq!(conf.read_concurrent, OpendalConf::DEFAULT_READ_CONCURRENT);
    }
}

#[cfg(test)]
mod s3_conf_tests {
    use super::S3Conf;
    use std::collections::HashMap;

    #[test]
    fn requires_non_empty_region() {
        let mut properties = HashMap::new();

        assert!(S3Conf::validate_region(&properties).is_err());

        properties.insert(S3Conf::REGION_NAME.to_string(), "  ".to_string());
        assert!(S3Conf::validate_region(&properties).is_err());

        properties.insert(S3Conf::REGION_NAME.to_string(), "cn-test-1".to_string());
        assert!(S3Conf::validate_region(&properties).is_ok());
    }

    #[test]
    fn rejects_empty_credentials() {
        let mut properties = HashMap::from([
            (
                S3Conf::ENDPOINT.to_string(),
                "http://s3.example.com".to_string(),
            ),
            (S3Conf::ACCESS_KEY.to_string(), " ".to_string()),
            (S3Conf::SECRET_KEY.to_string(), "secret-key".to_string()),
            (S3Conf::REGION_NAME.to_string(), "cn-test-1".to_string()),
        ]);

        let err = S3Conf::with_map(properties.clone()).expect_err("empty access key must fail");
        assert!(err.to_string().contains(S3Conf::ACCESS_KEY));

        properties.insert(S3Conf::ACCESS_KEY.to_string(), "access-key".to_string());
        properties.insert(S3Conf::SECRET_KEY.to_string(), " ".to_string());
        let err = S3Conf::with_map(properties).expect_err("empty secret key must fail");
        assert!(err.to_string().contains(S3Conf::SECRET_KEY));
    }

    #[test]
    fn canonicalizes_legacy_s3_properties() {
        let properties = HashMap::from([
            (
                S3Conf::ENDPOINT.to_string(),
                "http://s3.example.com".to_string(),
            ),
            (
                S3Conf::LEGACY_ACCESS_KEY.to_string(),
                "access-key".to_string(),
            ),
            (
                S3Conf::LEGACY_SECRET_KEY.to_string(),
                "secret-key".to_string(),
            ),
            (
                S3Conf::LEGACY_REGION_NAME.to_string(),
                "cn-test-1".to_string(),
            ),
        ]);

        let conf = S3Conf::with_map(properties).expect("legacy S3 configuration must work");
        assert_eq!(conf.access_key, "access-key");
        assert_eq!(conf.secret_key, "secret-key");
        assert_eq!(conf.region_name, "cn-test-1");
        assert!(!conf.properties.contains_key(S3Conf::LEGACY_ACCESS_KEY));
        assert!(!conf.properties.contains_key(S3Conf::LEGACY_SECRET_KEY));
        assert!(!conf.properties.contains_key(S3Conf::LEGACY_REGION_NAME));
    }

    #[test]
    fn rejects_conflicting_legacy_and_canonical_property() {
        let properties = HashMap::from([
            (
                S3Conf::LEGACY_REGION_NAME.to_string(),
                "cn-test-1".to_string(),
            ),
            (S3Conf::REGION_NAME.to_string(), "cn-test-2".to_string()),
        ]);

        let err = S3Conf::canonicalize_properties(properties)
            .expect_err("conflicting S3 property names must fail");
        assert!(err.to_string().contains(S3Conf::LEGACY_REGION_NAME));
        assert!(err.to_string().contains(S3Conf::REGION_NAME));
    }

    #[test]
    fn list_objects_v2_is_the_default() {
        let mut properties = HashMap::new();
        assert!(!S3Conf::uses_list_objects_v1(&properties).unwrap());

        properties.insert(S3Conf::LIST_OBJECTS_VERSION.to_string(), "  ".to_string());
        assert!(!S3Conf::uses_list_objects_v1(&properties).unwrap());

        properties.insert(S3Conf::LIST_OBJECTS_VERSION.to_string(), "V2".to_string());
        assert!(!S3Conf::uses_list_objects_v1(&properties).unwrap());
    }

    #[test]
    fn list_objects_v1_is_supported() {
        let properties =
            HashMap::from([(S3Conf::LIST_OBJECTS_VERSION.to_string(), " v1 ".to_string())]);

        assert!(S3Conf::uses_list_objects_v1(&properties).unwrap());
    }

    #[test]
    fn rejects_invalid_list_objects_version() {
        let properties =
            HashMap::from([(S3Conf::LIST_OBJECTS_VERSION.to_string(), "v3".to_string())]);

        let err = S3Conf::uses_list_objects_v1(&properties)
            .expect_err("invalid ListObjects version must fail");
        assert!(err.to_string().contains(S3Conf::LIST_OBJECTS_VERSION));
    }

    #[test]
    fn rejects_invalid_optional_values() {
        let mut properties = HashMap::from([
            (
                S3Conf::ENDPOINT.to_string(),
                "http://s3.example.com".to_string(),
            ),
            (S3Conf::ACCESS_KEY.to_string(), "access-key".to_string()),
            (S3Conf::SECRET_KEY.to_string(), "secret-key".to_string()),
            (S3Conf::REGION_NAME.to_string(), "cn-test-1".to_string()),
            (S3Conf::FORCE_PATH_STYLE.to_string(), "maybe".to_string()),
        ]);

        let err = S3Conf::validate(&properties).expect_err("invalid bool must fail");
        assert!(err.to_string().contains(S3Conf::FORCE_PATH_STYLE));

        properties.insert(S3Conf::FORCE_PATH_STYLE.to_string(), "true".to_string());
        properties.insert("opendal.retry_times".to_string(), "many".to_string());
        let err = S3Conf::validate(&properties).expect_err("invalid retry count must fail");
        assert!(err.to_string().contains("opendal.retry_times"));
    }

    #[test]
    fn rejects_endpoint_without_host() {
        let properties = HashMap::from([
            (S3Conf::ENDPOINT.to_string(), "http://".to_string()),
            (S3Conf::ACCESS_KEY.to_string(), "access-key".to_string()),
            (S3Conf::SECRET_KEY.to_string(), "secret-key".to_string()),
            (S3Conf::REGION_NAME.to_string(), "cn-test-1".to_string()),
        ]);

        let err = S3Conf::validate(&properties).expect_err("endpoint without host must fail");
        assert!(err.to_string().contains(S3Conf::ENDPOINT));
    }
}
