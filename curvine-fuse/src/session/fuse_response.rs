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

use crate::raw::fuse_abi::fuse_out_header;
use crate::{FuseResult, FuseUtils};
use crate::{FUSE_OUT_HEADER_LEN, FUSE_SUCCESS};
use log::{error, info, warn};
use orpc::io::IOResult;
use orpc::sync::channel::AsyncSender;
use orpc::sys::DataSlice;
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
}

// Send fuse response to the mount point
#[derive(Clone)]
pub struct FuseResponse {
    pub(crate) unique: u64,
    pub(crate) sender: AsyncSender<ResponseData>,
    pub(crate) debug: bool,
}

impl FuseResponse {
    pub fn new(unique: u64, sender: AsyncSender<ResponseData>, debug: bool) -> Self {
        Self {
            unique,
            sender,
            debug,
        }
    }

    pub fn unique(&self) -> u64 {
        self.unique
    }

    async fn send(&self, errno: i32, data: Vec<DataSlice>) -> IOResult<isize> {
        let data_len = data.iter().map(|x| x.len()).sum::<usize>();

        // The fuse error code is the negative number of the os error code.
        let header = fuse_out_header {
            len: (FUSE_OUT_HEADER_LEN + data_len) as u32,
            error: -errno,
            unique: self.unique,
        };

        let len = header.len;
        self.sender.send(ResponseData::new(header, data)).await?;
        Ok(len as isize)
    }

    /// Build a response without data blocks.
    pub async fn send_empty(&self) -> IOResult<isize> {
        self.send(FUSE_SUCCESS, vec![]).await
    }

    /// Send a data.
    pub async fn send_rep<T: Debug>(&self, res: FuseResult<T>) -> IOResult<isize> {
        match res {
            Ok(v) => {
                if self.debug {
                    info!("send_rep unique {}, res: {:?}", self.unique, v);
                }
                let data = DataSlice::Buffer(FuseUtils::struct_as_buf(&v));
                self.send(FUSE_SUCCESS, vec![data]).await
            }

            Err(e) => {
                if e.errno != libc::ENOENT {
                    warn!("send_error unique {}: {}", self.unique, e);
                }
                self.send(e.errno, vec![]).await
            }
        }
    }

    pub async fn send_buf(&self, res: FuseResult<BytesMut>) -> IOResult<isize> {
        match res {
            Ok(v) => {
                if self.debug {
                    info!("send_buf unique {}, data len: {}", self.unique, v.len());
                }
                self.send(FUSE_SUCCESS, vec![DataSlice::Buffer(v)]).await
            }

            Err(e) => {
                if e.errno != libc::ENOENT {
                    warn!("send_error unique {}: {}", self.unique, e);
                }
                self.send(e.errno, vec![]).await
            }
        }
    }

    pub async fn send_data(&self, res: FuseResult<Vec<DataSlice>>) -> IOResult<isize> {
        match res {
            Ok(v) => {
                if self.debug {
                    let len = v.iter().map(|x| x.len()).sum::<usize>();
                    info!("send_data unique {}, data len: {}", self.unique, len);
                }
                self.send(FUSE_SUCCESS, v).await
            }

            Err(e) => {
                warn!("send_error unique {}: {}", self.unique, e);
                self.send(e.errno, vec![]).await
            }
        }
    }

    // Requests that do not need to be responded to.
    pub fn send_none<T: Debug>(&self, res: FuseResult<T>) -> IOResult<isize> {
        match res {
            Ok(v) => {
                if self.debug {
                    info!("send_none unique: {}, res: {:?}", self.unique, v);
                }
            }

            Err(e) => {
                error!("Request({}) processing failed: {}", self.unique, e);
            }
        }
        Ok(0)
    }
}
