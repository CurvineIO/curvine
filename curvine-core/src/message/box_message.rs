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

use crate::message::{Message, RefMessage};
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug)]
pub enum BoxMessage {
    Msg(Message),
    Arc(Arc<Message>),
}

impl BoxMessage {
    pub fn arc(msg: Message) -> Self {
        Self::Arc(Arc::new(msg))
    }

    pub fn msg(msg: Message) -> Self {
        Self::Msg(msg)
    }
}

impl RefMessage for BoxMessage {
    fn as_ref(&self) -> &Message {
        match self {
            BoxMessage::Msg(m) => m,
            BoxMessage::Arc(m) => m,
        }
    }

    fn as_mut(&mut self) -> &mut Message {
        panic!("BoxMessage cannot obtain a mutable reference")
    }

    fn into_box(self) -> BoxMessage {
        self
    }
}

impl Clone for BoxMessage {
    fn clone(&self) -> Self {
        match self {
            BoxMessage::Msg(_) => panic!("Message not support clone"),
            BoxMessage::Arc(m) => BoxMessage::Arc(m.clone()),
        }
    }
}

impl Deref for BoxMessage {
    type Target = Message;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}
