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

mod rpc_message;
pub use self::rpc_message::*;

mod ref_message;
pub use self::ref_message::RefMessage;

mod message_builder;
pub use self::message_builder::MessageBuilder;

mod box_message;
pub use self::box_message::BoxMessage;

pub type Message = RpcMessage;

pub type Builder = MessageBuilder;
