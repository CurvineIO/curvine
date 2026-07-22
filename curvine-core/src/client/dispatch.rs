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

use crate::err_box;
use crate::handler::RpcCodec;
use crate::io::IOResult;
use crate::message::{BoxMessage, Message, RefMessage};
use crate::sync::FastDashMap;
use futures::stream::{SplitSink, SplitStream};
use log::warn;
use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::codec::Framed;

// Return to the Promise. The call uses it to wait for a return value.
pub type Promise = oneshot::Receiver<IOResult<Message>>;

// Callback, after the executor has processed it, he will initiate a notification through it, and the message has been processed.
pub type Callback = oneshot::Sender<IOResult<Message>>;

// Read the message side, pay attention to the return Message type, which is a decoded message.
pub type ReadHalf = SplitStream<Framed<TcpStream, RpcCodec>>;

// Message write end.Note: A byte message is written.
pub type WriteHalf = SplitSink<Framed<TcpStream, RpcCodec>, BoxMessage>;

pub struct Envelope {
    pub msg: BoxMessage,
    pub cb: Callback,
}

impl Envelope {
    pub fn new<M: RefMessage>(msg: M, cb: Callback) -> Self {
        Self {
            msg: msg.into_box(),
            cb,
        }
    }

    pub fn is_canceled(&self) -> bool {
        self.cb.is_closed()
    }

    pub fn send(self, msg: IOResult<Message>) -> IOResult<()> {
        match self.cb.send(msg) {
            Ok(_) => Ok(()),
            Err(_) => err_box!("Send fail for {}", self.msg.req_id()),
        }
    }

    pub fn send_with_log(self, msg: IOResult<Message>) {
        if let Err(e) = self.cb.send(msg) {
            warn!("Send fail for {}: {:?}", self.msg.req_id(), e.err())
        }
    }
}

pub struct CallMap(FastDashMap<i64, Callback>);

impl CallMap {
    pub fn new() -> Self {
        Self(FastDashMap::default())
    }

    pub fn insert(&self, req_id: i64, cb: Callback) {
        let _ = self.0.insert(req_id, cb);
    }

    pub fn remove(&self, id: i64) -> Option<Callback> {
        self.0.remove(&id).map(|x| x.1)
    }

    pub fn clear(&self) -> HashMap<i64, Callback> {
        let mut map = HashMap::new();
        let list = self.0.iter().map(|x| *x.key()).collect::<Vec<_>>();

        for id in list {
            if let Some(value) = self.0.remove(&id) {
                map.insert(value.0, value.1);
            }
        }

        map
    }

    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    pub fn not_empty(&self) -> bool {
        !self.is_empty()
    }
}

impl Default for CallMap {
    fn default() -> Self {
        Self::new()
    }
}
