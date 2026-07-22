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
use crate::sync::FastMutex;
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
    chunk_size: i32,
    state: FastMutex<FileHandlerState>,
}

struct FileHandlerState {
    file: Option<LocalFile>,
    last_task: Option<ReadAheadTask>,
}

impl FileHandler {
    pub fn new(location: Arc<DirLocation>, os_cache: Arc<CacheManager>, chunk_size: i32) -> Self {
        FileHandler {
            location,
            os_cache,
            chunk_size,
            state: FastMutex::new(FileHandlerState {
                file: None,
                last_task: None,
            }),
        }
    }

    fn do_open(
        &self,
        state: &mut FileHandlerState,
        msg: &Message,
        for_write: bool,
    ) -> CommonResultExt<Message> {
        let block_id = if let Some(ref v) = msg.header {
            let mut bytes = [0; 8];
            bytes.copy_from_slice(&v[0..8]);
            u64::from_be_bytes(bytes)
        } else {
            0
        };

        state.file = Some(self.location.get_or_create(block_id, for_write)?);
        Ok(msg.success_with_data(None, DataSlice::Empty))
    }

    pub fn do_write(&self, msg: &Message) -> CommonResultExt<Message> {
        let mut state = self.state.lock();
        match msg.request_status() {
            RequestStatus::Open => self.do_open(&mut state, msg, true),

            RequestStatus::Running => {
                if let Some(ref mut f) = state.file {
                    f.write_region(&msg.data)?;
                    Ok(msg.success_with_data(None, DataSlice::Empty))
                } else {
                    err_box!("file not initialized")
                }
            }

            _ => err_box!("unknown request"),
        }
    }

    pub fn do_read(&self, msg: &Message) -> CommonResultExt<Message> {
        let mut state = self.state.lock();
        match msg.request_status() {
            RequestStatus::Open => self.do_open(&mut state, msg, false),

            RequestStatus::Running => {
                let last_task = state.last_task.take();
                if let Some(ref mut f) = state.file {
                    let next_task = f.read_ahead(&self.os_cache, last_task);
                    let region = f.read_region(true, self.chunk_size)?;
                    state.last_task = next_task;
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

    fn handle(&self, msg: &Message) -> CommonResultExt<Message> {
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
