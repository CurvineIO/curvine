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

// The user manages the configuration of different UFS
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Various configurations for connecting to UFS
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UfsConf {
    config: HashMap<String, String>,
}

impl UfsConf {
    pub fn new() -> Self {
        UfsConf {
            config: HashMap::new(),
        }
    }

    pub fn with_map(config: HashMap<String, String>) -> Self {
        Self { config }
    }

    pub fn get_config(&self) -> &HashMap<String, String> {
        &self.config
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.config.get(key).map(|v| v.as_str())
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.config.contains_key(key)
    }
}

/// Builder for UfsConf
pub struct UfsConfBuilder {
    config: HashMap<String, String>,
}

impl UfsConfBuilder {
    pub fn new() -> Self {
        UfsConfBuilder {
            config: HashMap::new(),
        }
    }

    pub fn add_config<K, V>(&mut self, key: K, value: V) -> &Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.config.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> UfsConf {
        UfsConf {
            config: self.config,
        }
    }
}

impl Default for UfsConfBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// Add this method to UfsConf to create a builder
impl UfsConf {
    // Add this method to existing impl block
    pub fn builder() -> UfsConfBuilder {
        UfsConfBuilder::new()
    }
}
