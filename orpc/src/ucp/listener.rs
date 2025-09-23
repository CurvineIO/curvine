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

use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;
use futures::channel::mpsc;
use log::{error, info};
use crate::err_ucs;
use crate::io::IOResult;
use crate::sync::channel::{BlockingChannel, BlockingReceiver, BlockingSender};
use crate::sys::RawPtr;
use crate::ucp::bindings::*;
use crate::ucp::{ConnRequest, SockAddr, Worker};

pub struct Listener {
    inner: RawPtr<ucp_listener>,
    #[allow(unused)]
    sender: BlockingSender<ConnRequest>,
    receiver: BlockingReceiver<ConnRequest>,
}

impl Listener {
    pub fn new(worker: Arc<Worker>, addr: &SockAddr) -> IOResult<Self> {
        let (sender, receiver) = BlockingChannel::new(0).split();

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
                cb: Some(Self::connect_handler),
                arg: &sender as *const BlockingSender<ConnRequest> as _,
            },
        };

        let mut handle = MaybeUninit::<*mut ucp_listener>::uninit();
        let status = unsafe {
            ucp_listener_create(worker.as_mut_ptr(), &params, handle.as_mut_ptr())
        };
        err_ucs!(status)?;

        Ok(Listener {
            inner: RawPtr::from_uninit(handle),
            sender,
            receiver,
        })
    }

    unsafe extern "C" fn connect_handler(conn_request: ucp_conn_request_h, arg: *mut c_void) {
        let sender = &*(arg as *const BlockingSender<ConnRequest>);
        let conn = ConnRequest::new(conn_request);
        if let Err(e) = sender.send(conn) {
            error!("send conn request failed: {:?}", e);
            // @todo 处理错误
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

        let status = unsafe {
            ucp_listener_query(self.as_mut_ptr(), &mut attr)
        };
        err_ucs!(status)?;

        let sockaddr = unsafe {
            socket2::SockAddr::new(std::mem::transmute(attr.sockaddr), 8)
        };

        Ok(SockAddr::new(sockaddr))
    }

    pub async fn next(&mut self) -> IOResult<ConnRequest> {
        let conn = self.receiver.recv_check()?;
        Ok(conn)
    }

    pub fn reject(&self, conn: ConnRequest) -> IOResult<()> {
        let status = unsafe {
            ucp_listener_reject(self.as_mut_ptr(), conn.as_mut_ptr())
        };
        err_ucs!(status)
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        unsafe {
            ucp_listener_destroy(self.as_mut_ptr())
        }
    }
}

