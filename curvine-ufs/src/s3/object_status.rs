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

use crate::FOLDER_SUFFIX;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use orpc::common::LocalTime;

pub struct ObjectStatus {
    pub key: String,
    pub is_dir: bool,
    pub len: i64,
    pub mtime: i64,
}

impl ObjectStatus {
    pub fn new(key: impl Into<String>, len: i64, mtime: i64, is_dir: bool) -> Self {
        ObjectStatus {
            key: key.into(),
            is_dir,
            len,
            mtime,
        }
    }

    pub fn create_root() -> Self {
        ObjectStatus::new(FOLDER_SUFFIX, 0, LocalTime::mills() as i64, true)
    }

    pub fn from_head_object(key: impl Into<String>, output: &HeadObjectOutput) -> Self {
        let key = key.into();
        let is_dir = key.ends_with(FOLDER_SUFFIX);
        ObjectStatus {
            key,
            is_dir,
            len: output.content_length.unwrap_or(0),
            mtime: output
                .last_modified()
                .map(|x| x.to_millis().unwrap_or(0))
                .unwrap_or(0),
        }
    }

    pub fn is_dir(&self) -> bool {
        self.is_dir
    }

    pub fn is_file(&self) -> bool {
        !self.is_dir
    }
}
