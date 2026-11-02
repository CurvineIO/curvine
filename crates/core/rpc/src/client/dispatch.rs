// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//! Internal request and callback used to hand work to the Raft actor.
use crate::message::{BoxMessage, Message, RefMessage};
use curvine_io::IOResult;
use tokio::sync::oneshot;

pub type Callback = oneshot::Sender<IOResult<Message>>;

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
        self.cb.send(msg).map_err(|_| {
            curvine_io::IOError::new(std::io::Error::other(format!(
                "Send fail for {}",
                self.msg.req_id()
            )))
        })
    }

    pub fn send_with_log(self, msg: IOResult<Message>) {
        if let Err(e) = self.cb.send(msg) {
            log::warn!("Send fail for {}: {:?}", self.msg.req_id(), e.err());
        }
    }
}
