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

use crate::error::CommonErrorExt;
use crate::handler::{HandlerService, MessageHandler};
use crate::io::net::ConnState;
use crate::io::LocalFile;
use crate::message::*;
use crate::sys::{CacheManager, DataSlice, ReadAheadTask};
use crate::test::file::dir_location::DirLocation;
use crate::{err_box, CommonResultExt};
use num_enum::{FromPrimitive, IntoPrimitive};
use std::sync::Arc;

#[repr(i8)]
#[derive(Debug, Copy, Clone, IntoPrimitive, FromPrimitive)]
pub enum RpcCode {
    #[num_enum(default)]
    Write = 1,
    Read = 2,
}

pub struct FileHandler {
    location: Arc<DirLocation>,
    os_cache: Arc<CacheManager>,
    file: Option<LocalFile>,
    chunk_size: i32,
    last_task: Option<ReadAheadTask>,
}

impl FileHandler {
    pub fn new(location: Arc<DirLocation>, os_cache: Arc<CacheManager>, chunk_size: i32) -> Self {
        FileHandler {
            location,
            os_cache,
            file: None,
            chunk_size,
            last_task: None,
        }
    }

    pub fn do_open(&mut self, msg: &Message, for_write: bool) -> CommonResultExt<Message> {
        let block_id = if let Some(ref v) = msg.header {
            let mut bytes = [0; 8];
            bytes.copy_from_slice(&v[0..8]);
            u64::from_be_bytes(bytes)
        } else {
            0
        };

        self.file = Some(self.location.get_or_create(block_id, for_write)?);
        Ok(msg.success_with_data(None, DataSlice::Empty))
    }

    pub fn do_write(&mut self, msg: &Message) -> CommonResultExt<Message> {
        match msg.request_status() {
            RequestStatus::Open => self.do_open(msg, true),

            RequestStatus::Running => {
                if let Some(ref mut f) = self.file {
                    f.write_region(&msg.data)?;
                    Ok(msg.success_with_data(None, DataSlice::Empty))
                } else {
                    err_box!("file not initialized")
                }
            }

            _ => err_box!("unknown request"),
        }
    }

    pub fn do_read(&mut self, msg: &Message) -> CommonResultExt<Message> {
        match msg.request_status() {
            RequestStatus::Open => self.do_open(msg, false),

            RequestStatus::Running => {
                if let Some(ref mut f) = self.file {
                    self.last_task = f.read_ahead(&self.os_cache, self.last_task.take());
                    let region = f.read_region(false, self.chunk_size)?;
                    Ok(msg.success_with_data(None, region))
                } else {
                    err_box!("file not initialized")
                }
            }

            _ => err_box!("unknown request"),
        }
    }
}

impl MessageHandler for FileHandler {
    type Error = CommonErrorExt;

    fn handle(&mut self, msg: &Message) -> CommonResultExt<Message> {
        let code = RpcCode::from(msg.protocol.code);
        match code {
            RpcCode::Write => self.do_write(msg),
            RpcCode::Read => self.do_read(msg),
        }
    }
}

pub struct FileService {
    location: Arc<DirLocation>,
    os_cache: Arc<CacheManager>,
}

impl FileService {
    pub fn new(dirs: Vec<String>) -> Self {
        let location = Arc::new(DirLocation::new(dirs));
        let os_cache = Arc::new(CacheManager::default());
        FileService { location, os_cache }
    }
}

impl HandlerService for FileService {
    type Item = FileHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        FileHandler::new(self.location.clone(), self.os_cache.clone(), 64 * 1024)
    }
}
