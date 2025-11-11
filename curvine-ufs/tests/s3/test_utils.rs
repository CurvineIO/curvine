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

use std::collections::HashMap;

// S3 test environment configuration
pub fn get_s3_test_config() -> HashMap<String, String> {
    let mut config = HashMap::new();

    // Configuration of MinIO as S3 compatible service
    config.insert(
        "s3.endpoint_url".to_string(),
        "http://s3v2.dg-access-test.wanyol.com".to_string(),
    );
    config.insert("s3.region_name".to_string(), "cn-south-1".to_string());
    config.insert("s3.bucket_name".to_string(), "test-bucket".to_string());
    config.insert(
        "s3.credentials.access".to_string(),
        "T6A4jOFA9TssTrn2K1A6pFDT-xwFiFQbfS2JxZ5D".to_string(),
    );
    config.insert(
        "s3.credentials.secret".to_string(),
        "4ewVYsv5MFn2KPd6oYmKRQgMCz22LSlqF0Zl2KZz".to_string(),
    );
    config.insert("s3.path_style".to_string(), "true".to_string());

    config
}
