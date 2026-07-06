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

use curvine_common::conf::MasterConf;
use curvine_common::error::FsError;
use curvine_common::FsResult;
use mini_moka::sync::{Cache, CacheBuilder};
use num_enum::{FromPrimitive, IntoPrimitive};
use orpc::common::DurationUnit;
use orpc::err_ext;
use orpc::error::ErrorExt;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

// Caching the client request, in the case of retry with the same request ID,
// the previous response is returned, which is thread-safe.
#[repr(i8)]
#[derive(Debug, IntoPrimitive, FromPrimitive, Copy, Clone, PartialEq)]
pub enum OperationStatus {
    #[num_enum(default)]
    Init,

    Success,
    Failed,
}

#[derive(Clone)]
pub struct FsRetryCache(Arc<Cache<i64, OperationStatus>>);

// Use lock, single thread cache.Ensure safety.Memory operation, this lock has basically no impact on performance.
impl FsRetryCache {
    pub fn new(capacity: u64, ttl: Duration) -> Self {
        let cache = CacheBuilder::new(capacity).time_to_live(ttl).build();
        Self(Arc::new(cache))
    }

    pub fn with_conf(conf: &MasterConf) -> FsResult<Option<FsRetryCache>> {
        if conf.retry_cache_enable {
            let ttl = DurationUnit::from_str(&conf.retry_cache_ttl)
                .map_err(FsError::from)
                .map_err(|e| {
                    e.ctx(format!(
                        "invalid master retry_cache_ttl: {}",
                        conf.retry_cache_ttl
                    ))
                })?
                .as_duration();
            let cache = Self::new(conf.retry_cache_size, ttl);
            Ok(Some(cache))
        } else {
            Ok(None)
        }
    }

    // Check whether it is a retry request.
    pub fn check_is_retry(&self, req_id: i64) -> FsResult<bool> {
        match self.get(&req_id) {
            None => {
                // First request.
                self.insert(req_id, OperationStatus::Init);
                Ok(false)
            }

            Some(status) => match status {
                OperationStatus::Init => err_ext!(FsError::in_progress(req_id)),
                OperationStatus::Success => Ok(true),
                OperationStatus::Failed => Ok(false),
            },
        }
    }

    pub fn set_status(&self, req_id: i64, success: bool) {
        let status = if success {
            OperationStatus::Success
        } else {
            OperationStatus::Failed
        };
        self.insert(req_id, status)
    }
}

impl Deref for FsRetryCache {
    type Target = Cache<i64, OperationStatus>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
