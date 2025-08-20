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

use crate::err_ufs;
use crate::FOLDER_SUFFIX;
use curvine_common::fs::Path;
use curvine_common::FsResult;

pub struct UfsUtils;

impl UfsUtils {
    pub fn get_bucket_key(path: &Path) -> FsResult<(&str, &str)> {
        let bucket = match path.authority() {
            Some(v) => v,
            None => return err_ufs!("UFS URI missing bucket name: {}", path.full_path()),
        };

        let key = match path.path().strip_prefix('/') {
            Some(v) => v,
            None => return err_ufs!("Path {} invalid", path.full_path()),
        };

        Ok((bucket, key))
    }

    // Create a directory object; the key of the directory object in S3 ends with /
    pub fn dir_key(key: &str) -> String {
        if key.is_empty() {
            "".to_string()
        } else if key.ends_with(FOLDER_SUFFIX) {
            key.to_string()
        } else {
            format!("{}{}", key, FOLDER_SUFFIX)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::UfsUtils;

    #[test]
    fn get_bucket_key() {
        let path = "s3://bucket/path".into();
        let (bucket, key) = UfsUtils::get_bucket_key(&path).unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "path");

        let path = "s3://bucket/".into();
        let (bucket, key) = UfsUtils::get_bucket_key(&path).unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "");
    }
}
