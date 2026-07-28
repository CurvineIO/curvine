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

use curvine_common::fs::Path;
use curvine_ufs::OssHdfsConf;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Create test configuration from environment variables
pub fn create_test_conf() -> Option<HashMap<String, String>> {
    let endpoint = std::env::var("OSS_ENDPOINT").ok()?;
    let access_key_id = std::env::var("OSS_ACCESS_KEY_ID").ok()?;
    let access_key_secret = std::env::var("OSS_ACCESS_KEY_SECRET").ok()?;
    let region = std::env::var("OSS_REGION").ok()?;

    let mut config = HashMap::new();
    let endpoint_clone = endpoint.clone();
    config.insert(OssHdfsConf::USER_ENDPOINT.to_string(), endpoint);
    config.insert(OssHdfsConf::USER_ACCESS_KEY_ID.to_string(), access_key_id);
    config.insert(
        OssHdfsConf::USER_ACCESS_KEY_SECRET.to_string(),
        access_key_secret,
    );
    config.insert(OssHdfsConf::USER_REGION.to_string(), region);

    // Use OSS_DATA_ENDPOINT if provided, otherwise derive from endpoint
    let data_endpoint = std::env::var("OSS_DATA_ENDPOINT").unwrap_or_else(|_| {
        // Try to derive from endpoint if not explicitly set
        // This is a fallback, but still better than hardcoding credentials
        endpoint_clone.replace(".oss-dls.", "-vpc.oss.")
    });
    config.insert(OssHdfsConf::USER_DATA_ENDPOINT.to_string(), data_endpoint);

    Some(config)
}

/// Get test bucket from environment or use default
pub fn get_test_bucket() -> String {
    std::env::var("OSS_BUCKET").unwrap_or_else(|_| "test-bucket".to_string())
}

/// Create a unique test path with timestamp
pub fn create_test_path(bucket: &str, prefix: &str) -> Path {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let path = format!("oss://{}/{}_{}", bucket, prefix, timestamp);
    Path::new(&path).unwrap()
}
