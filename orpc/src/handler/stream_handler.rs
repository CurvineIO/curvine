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
use crate::handler::{Frame, MessageHandler};
use crate::io::IOResult;
use crate::message::Message;
use crate::runtime::{RpcRuntime, Runtime};
use crate::server::{ServerConf, ServerState, ServerStateListener};
use crate::sync::StateCtl;
use crate::sys::RawPtr;
use log::debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

// Network channel message processor. It associates network connection and message processing logic.
pub struct StreamHandler<F, M> {
    rt: Arc<Runtime>,
    frame: F,
    handler: RawPtr<M>,
    close_idle: bool,
    timeout: Duration,
    shutdown_ctl: Option<StateCtl>,
    shutdown_listener: Option<ServerStateListener>,
}

impl<F: Frame, M: MessageHandler> StreamHandler<F, M> {
    pub fn new(rt: Arc<Runtime>, frame: F, handler: M, conf: &ServerConf) -> Self {
        StreamHandler {
            rt,
            frame,
            handler: RawPtr::from_owned(handler),
            close_idle: conf.close_idle,
            timeout: Duration::from_millis(conf.timeout_ms),
            shutdown_ctl: None,
            shutdown_listener: None,
        }
    }

    pub fn new_with_shutdown(
        rt: Arc<Runtime>,
        frame: F,
        handler: M,
        conf: &ServerConf,
        shutdown_ctl: StateCtl,
        shutdown_listener: ServerStateListener,
    ) -> Self {
        StreamHandler {
            rt,
            frame,
            handler: RawPtr::from_owned(handler),
            close_idle: conf.close_idle,
            timeout: Duration::from_millis(conf.timeout_ms),
            shutdown_ctl: Some(shutdown_ctl),
            shutdown_listener: Some(shutdown_listener),
        }
    }

    pub async fn run(&mut self) -> IOResult<()> {
        loop {
            let res = if let (Some(shutdown_ctl), Some(shutdown_listener)) =
                (&self.shutdown_ctl, &mut self.shutdown_listener)
            {
                if shutdown_ctl.state::<ServerState>() >= ServerState::Shutdown {
                    return Ok(());
                }
                tokio::select! {
                    _ = shutdown_listener.wait_shutdown() => {
                        return Ok(());
                    }
                    res = timeout(self.timeout, self.frame.receive()) => res,
                }
            } else {
                timeout(self.timeout, self.frame.receive()).await
            };
            let res = match res {
                Ok(v) => v,

                Err(_) if self.close_idle => {
                    // Close the timeout connection
                    return Ok(());
                }

                _ => continue,
            };

            match res {
                Ok(request) => {
                    if request.is_empty() {
                        return Ok(());
                    }

                    self.call(request).await?;
                }

                Err(e) => return Err(e),
            };
        }
    }

    pub async fn call(&mut self, request: Message) -> IOResult<()> {
        let response = if self.handler.is_sync(&request) {
            let handler = self.handler.clone();
            self.rt
                .spawn_blocking(move || match handler.as_mut().handle(&request) {
                    Err(e) => {
                        debug!("handler request {} error: {}", request.req_id(), e);
                        request.error_ext(&e)
                    }

                    Ok(v) => v,
                })
                .await?
        } else {
            match self.handler.as_mut().async_handle(request).await {
                Ok(v) => v,
                Err(e) => return err_box!("{}", e),
            }
        };

        if response.not_empty() {
            self.frame.send(response).await
        } else {
            Ok(())
        }
    }

    pub fn frame_mut(&mut self) -> &mut F {
        &mut self.frame
    }
}
