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

use crate::err_ucs;
use crate::io::{IOError, IOResult};
use crate::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender};
use crate::sync::ErrorMonitor;
use crate::sys::RawPtr;
use crate::ucp::bindings::*;
use crate::ucp::UcpExecutor;
use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;
use crate::ucp::core::SockAddr;
use crate::ucp::request::ConnRequest;

struct ConnContext {
    sender: AsyncSender<ConnRequest>,
    error_monitor: Arc<ErrorMonitor<IOError>>,
}

impl ConnContext {
    pub fn new(sender: AsyncSender<ConnRequest>) -> Self {
        Self {
            sender,
            error_monitor: Arc::new(ErrorMonitor::new()),
        }
    }
}

pub struct Listener {
    inner: RawPtr<ucp_listener>,
    executor: UcpExecutor,
    conn_context: Rc<ConnContext>,
    receiver: AsyncReceiver<ConnRequest>,
}

impl Listener {
    pub fn bind(executor: UcpExecutor, addr: &SockAddr) -> IOResult<Self> {
        let (sender, receiver) = AsyncChannel::new(0).split();
        let conn_context = Rc::new(ConnContext::new(sender));

        let params = ucp_listener_params_t {
            field_mask: (ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_SOCK_ADDR
                | ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_CONN_HANDLER)
                .0 as u64,
            sockaddr: addr.as_ucs_sock_addr(),

            accept_handler: ucp_listener_accept_handler_t {
                cb: None,
                arg: ptr::null_mut(),
            },

            conn_handler: ucp_listener_conn_handler_t {
                cb: Some(Self::conn_handler),
                arg: &*conn_context as *const ConnContext as _,
            },
        };

        let mut handle = MaybeUninit::<*mut ucp_listener>::uninit();
        let status = unsafe {
            ucp_listener_create(executor.worker().as_mut_ptr(), &params, handle.as_mut_ptr())
        };
        err_ucs!(status)?;

        Ok(Listener {
            inner: RawPtr::from_uninit(handle),
            executor,
            receiver,
            conn_context,
        })
    }

    unsafe extern "C" fn conn_handler(conn_request: ucp_conn_request_h, arg: *mut c_void) {
        let context = &*(arg as *const ConnContext);
        if let Err(e) = context.sender.send_sync(ConnRequest::new(conn_request)) {
            context.error_monitor.set_error(e)
        }
    }

    pub fn as_ptr(&self) -> *const ucp_listener {
        self.inner.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut ucp_listener {
        self.inner.as_mut_ptr()
    }

    /// 获取监听地址
    pub fn local_addr(&self) -> IOResult<SockAddr> {
        let mut attr = ucp_listener_attr_t {
            field_mask: ucp_listener_attr_field::UCP_LISTENER_ATTR_FIELD_SOCKADDR.0 as u64,
            sockaddr: unsafe { MaybeUninit::zeroed().assume_init() },
        };

        let status = unsafe { ucp_listener_query(self.as_mut_ptr(), &mut attr) };
        err_ucs!(status)?;

        let sockaddr = unsafe { socket2::SockAddr::new(std::mem::transmute(attr.sockaddr), 8) };

        Ok(SockAddr::new(sockaddr))
    }

    pub fn check_error(&self) -> IOResult<()> {
        self.conn_context.error_monitor.check_error()
    }

    pub async fn accept(&mut self) -> IOResult<ConnRequest> {
        self.check_error()?;
        let conn = self.receiver.recv_check().await?;
        Ok(conn)
    }

    pub fn reject(&self, conn: ConnRequest) -> IOResult<()> {
        let status = unsafe { ucp_listener_reject(self.as_mut_ptr(), conn.as_mut_ptr()) };
        err_ucs!(status)
    }

    pub fn executor(&self) -> &UcpExecutor {
        &self.executor
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        unsafe { ucp_listener_destroy(self.as_mut_ptr()) }
    }
}
