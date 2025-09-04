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

use orpc::{err_box, CommonResult};
use std::collections::HashMap;
use std::ops::Deref;

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
}

#[derive(Debug, Clone)]
pub struct S3Conf {
    pub endpoint_url: String,
    pub access_key: String,
    pub secret_key: String,
    pub region_name: String,
    pub force_path_style: bool,

    pub properties: HashMap<String, String>,
}

impl S3Conf {
    pub const ENDPOINT: &'static str = "s3.endpoint_url";
    pub const ACCESS_KEY: &'static str = "s3.credentials.access";
    pub const SECRET_KEY: &'static str = "s3.credentials.secret";
    pub const REGION_NAME: &'static str = "s3.region_name";
    pub const FORCE_PATH_STYLE: &'static str = "s3.force.path.style";

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

        Ok(Self {
            endpoint_url,
            access_key,
            secret_key,
            region_name,
            force_path_style,
            properties: map.0,
        })
    }
}

impl Deref for S3Conf {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.properties
    }
}
