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

#![allow(unused)]

use curvine_common::conf::UfsConf;
use std::collections::HashMap;

pub struct UFSContext {
    conf: UfsConf,
    scheme: String,
    mount_id: String,
}

impl UFSContext {
    pub fn new(scheme: String, conf: UfsConf) -> Self {
        UFSContext {
            scheme,
            conf,
            mount_id: "".to_string(),
        }
    }

    //TODO Then change to fixed configuration items
    pub fn s3a_config(&self) -> &HashMap<String, String> {
        self.conf.get_config()
    }

    pub fn conf(&self) -> &UfsConf {
        &self.conf
    }

    pub fn get_scheme(&self) -> Option<&str> {
        Some(self.scheme.as_str())
    }
}
