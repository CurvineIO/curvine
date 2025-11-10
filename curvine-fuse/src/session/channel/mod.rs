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

use crate::fs::FileSystem;
use crate::session::FuseMnt;
use crate::{FuseResult, FuseUtils};
use curvine_common::conf::FuseConf;
use orpc::runtime::Runtime;
use orpc::sync::channel::AsyncChannel;
use std::sync::Arc;

mod fuse_receiver;
pub use self::fuse_receiver::FuseReceiver;

mod fuse_sender;
pub use self::fuse_sender::FuseSender;

pub struct FuseChannel<T> {
    pub senders: Vec<FuseSender<T>>,
    pub receivers: Vec<FuseReceiver<T>>,
}

impl<T: FileSystem> FuseChannel<T> {
    pub fn new(fs: Arc<T>, rt: Arc<Runtime>, mnt: &FuseMnt, conf: &FuseConf) -> FuseResult<Self> {
        let buf_size = FuseUtils::get_fuse_buf_size();

        let mut receivers = Vec::with_capacity(conf.mnt_per_task);
        let mut senders = Vec::with_capacity(conf.mnt_per_task);
        for _ in 0..conf.mnt_per_task {
            let (tx, rx) = AsyncChannel::new(conf.fuse_channel_size).split();
            let fd = mnt.create_async_task_fd(conf.clone_fd)?;

            let sender =
                FuseSender::new(fs.clone(), rt.clone(), fd.clone(), rx, buf_size, conf.debug)?;

            let receiver = FuseReceiver::new(fs.clone(), rt.clone(), fd, tx, buf_size, conf.debug)?;

            senders.push(sender);
            receivers.push(receiver);
        }

        Ok(Self { senders, receivers })
    }
}
