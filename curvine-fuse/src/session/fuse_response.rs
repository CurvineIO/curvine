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

use crate::raw::fuse_abi::{fuse_notify_inval_inode_out, fuse_out_header};
use crate::session::{FuseNotifyCode, FuseOpCode, FuseTask};
use crate::{FuseError, FuseResult, FuseUtils};
use crate::{FUSE_NOTIFY_UNIQUE, FUSE_OUT_HEADER_LEN, FUSE_SUCCESS};
use log::{info, warn};
use orpc::io::IOResult;
use orpc::sync::channel::AsyncSender;
use orpc::sys::DataSlice;
use orpc::ternary;
use std::fmt::Debug;
use std::io::IoSlice;
use std::vec;
use tokio_util::bytes::BytesMut;

pub struct ResponseData {
    pub header: fuse_out_header,
    pub data: Vec<DataSlice>,
}

impl ResponseData {
    pub fn new(header: fuse_out_header, data: Vec<DataSlice>) -> Self {
        Self { header, data }
    }

    pub fn len(&self) -> u32 {
        self.header.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_iovec(&self) -> IOResult<(usize, Vec<IoSlice<'_>>)> {
        let mut iovec: Vec<IoSlice<'_>> = Vec::with_capacity(self.data.len() + 1);

        // write header
        let header_bytes = FuseUtils::struct_as_bytes(&self.header);
        iovec.push(IoSlice::new(header_bytes));

        // write data
        for data in &self.data {
            iovec.push(IoSlice::new(data.as_slice()));
        }
        Ok((self.header.len as usize, iovec))
    }

    fn create(unique: u64, error: i32, data: Vec<DataSlice>) -> Self {
        let data_len = data.iter().map(|x| x.len()).sum::<usize>();
        let error = ternary!(unique == FUSE_NOTIFY_UNIQUE, error, -error);

        // The fuse error code is the negative number of the os error code.
        let header = fuse_out_header {
            len: (FUSE_OUT_HEADER_LEN + data_len) as u32,
            error,
            unique,
        };

        Self::new(header, data)
    }
}

// Send fuse response to the mount point
#[derive(Clone)]
pub struct FuseResponse {
    pub(crate) unique: u64,
    pub(crate) sender: AsyncSender<FuseTask>,
    pub(crate) debug: bool,
}

impl FuseResponse {
    pub fn new(unique: u64, sender: AsyncSender<FuseTask>, debug: bool) -> Self {
        Self {
            unique,
            sender,
            debug,
        }
    }

    pub fn unique(&self) -> u64 {
        self.unique
    }

    pub async fn send_rep<T: Debug, E: Into<FuseError> + Debug>(
        &self,
        res: Result<T, E>,
    ) -> IOResult<()> {
        let data = match res {
            Ok(v) => {
                if self.debug {
                    info!("send_rep unique {}, res: {:?}", self.unique, v);
                }

                let data = if size_of::<T>() == 0 {
                    vec![]
                } else {
                    vec![DataSlice::buffer(FuseUtils::struct_as_buf(&v))]
                };
                ResponseData::create(self.unique, FUSE_SUCCESS, data)
            }

            Err(e) => {
                let e = e.into();
                if self.debug || e.errno != libc::ENOENT {
                    warn!("send_rep unique {}: {:?}", self.unique, e);
                }
                ResponseData::create(self.unique, e.errno, vec![])
            }
        };

        self.sender.send(FuseTask::Reply(data)).await
    }

    pub async fn send_notify<T: Debug>(&self, code: FuseNotifyCode, res: T) -> IOResult<bool> {
        if self.debug {
            info!("send_notify code {:?}, res: {:?}", code, res);
        }

        #[cfg(target_os = "linux")]
        {
            let data = vec![DataSlice::buffer(FuseUtils::struct_as_buf(&res))];
            let data = ResponseData::create(FUSE_NOTIFY_UNIQUE, code.into(), data);

            self.sender.send(FuseTask::Reply(data)).await?;
            Ok(true)
        }

        #[cfg(not(target_os = "linux"))]
        {
            Ok(false)
        }
    }

    pub async fn send_buf(&self, res: FuseResult<BytesMut>) -> IOResult<()> {
        let data = match res {
            Ok(v) => {
                if self.debug {
                    info!("send_buf unique {}, data len: {}", self.unique, v.len());
                }
                ResponseData::create(self.unique, FUSE_SUCCESS, vec![DataSlice::Buffer(v)])
            }

            Err(e) => {
                if self.debug || e.errno != libc::ENOENT {
                    warn!("send_buf unique {}: {}", self.unique, e);
                }
                ResponseData::create(self.unique, e.errno, vec![])
            }
        };

        self.sender.send(FuseTask::Reply(data)).await
    }

    pub async fn send_data(&self, res: FuseResult<Vec<DataSlice>>) -> IOResult<()> {
        let data = match res {
            Ok(v) => {
                if self.debug {
                    let len = v.iter().map(|x| x.len()).sum::<usize>();
                    info!("send_data unique {}, data len: {}", self.unique, len);
                }
                ResponseData::create(self.unique, FUSE_SUCCESS, v)
            }

            Err(e) => {
                if self.debug || e.errno != libc::ENOENT {
                    warn!("send_data unique {}: {}", self.unique, e);
                }
                ResponseData::create(self.unique, e.errno, vec![])
            }
        };

        self.sender.send(FuseTask::Reply(data)).await
    }

    pub fn send_none(&self, _: FuseResult<()>) -> IOResult<()> {
        Ok(())
    }

    // notify kernel cache invalidation
    pub async fn send_inode_out(&self, ino: u64, off: i64, len: i64) -> IOResult<bool> {
        let arg = fuse_notify_inval_inode_out { ino, off, len };
        self.send_notify(FuseNotifyCode::FUSE_NOTIFY_INVAL_INODE, arg)
            .await
    }
}
