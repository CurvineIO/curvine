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
use orpc::common::FastHashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct RouteIndexSnapshot<T> {
    cv_map: FastHashMap<String, Arc<T>>,
    ufs_map: FastHashMap<String, Arc<T>>,
}

impl<T> Default for RouteIndexSnapshot<T> {
    fn default() -> Self {
        Self {
            cv_map: FastHashMap::new(),
            ufs_map: FastHashMap::new(),
        }
    }
}

impl<T> RouteIndexSnapshot<T> {
    pub fn from_maps(
        cv_map: &FastHashMap<String, Arc<T>>,
        ufs_map: &FastHashMap<String, Arc<T>>,
    ) -> Self {
        Self {
            cv_map: cv_map.clone(),
            ufs_map: ufs_map.clone(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.cv_map.is_empty() && self.ufs_map.is_empty()
    }

    pub fn lookup(&self, path: &Path) -> Option<Arc<T>> {
        if path.is_cv() {
            self.lookup_local(path.path())
        } else {
            self.lookup_uri(path.full_path())
        }
    }

    fn lookup_local(&self, path: &str) -> Option<Arc<T>> {
        let mut best = self.cv_map.get(Path::SEPARATOR).cloned();

        for (idx, ch) in path.char_indices() {
            if idx > 0 && ch == '/' {
                if let Some(value) = self.cv_map.get(&path[..idx]) {
                    best = Some(value.clone());
                }
            }
        }

        if let Some(value) = self.cv_map.get(path) {
            best = Some(value.clone());
        }

        best
    }

    fn lookup_uri(&self, path: &str) -> Option<Arc<T>> {
        let Some(root_slash_idx) = Self::uri_root_slash(path) else {
            return self.ufs_map.get(path).cloned();
        };

        let mut best = self.ufs_map.get(&path[..=root_slash_idx]).cloned();

        for (idx, ch) in path.char_indices() {
            if idx > root_slash_idx && ch == '/' {
                if let Some(value) = self.ufs_map.get(&path[..idx]) {
                    best = Some(value.clone());
                }
            }
        }

        if path.len() > root_slash_idx + 1 {
            if let Some(value) = self.ufs_map.get(path) {
                best = Some(value.clone());
            }
        }

        best
    }

    fn uri_root_slash(path: &str) -> Option<usize> {
        let scheme_end = path.find("://")?;
        let authority_start = scheme_end + 3;
        let root_offset = path[authority_start..].find('/')?;
        Some(authority_start + root_offset)
    }
}
