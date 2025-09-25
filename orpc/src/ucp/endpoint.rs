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

use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::task::Poll;
use bytes::BytesMut;
use futures::future::err;
use log::{info, warn};
use crate::{err_box, err_ucs};
use crate::io::{IOError, IOResult};
use crate::sync::{ErrorMonitor, StateCtl};
use crate::sys::{DataSlice, RawPtr, RawVec};
use crate::ucp::bindings::*;
use crate::ucp::{ConnRequest, Request, RequestFuture, SockAddr, stderr, UcpUtils, Worker};

pub struct Endpoint {
    inner: RawPtr<ucp_ep>,
    err_monitor: Arc<ErrorMonitor<IOError>>,
}

impl Endpoint {
    fn new(worker: &Worker, mut params: ucp_ep_params) -> IOResult<Self> {
        let err_monitor = Arc::new(ErrorMonitor::new());

        params.field_mask |= (ucp_ep_params_field::UCP_EP_PARAM_FIELD_USER_DATA
            | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLER)
            .0 as u64;
        params.user_data = &*err_monitor as *const _ as *mut c_void;
        params.err_handler = ucp_err_handler {
            cb: Some(Self::err_handler),
            arg: ptr::null_mut()
        };

        let mut inner = MaybeUninit::<*mut ucp_ep>::uninit();
        let status = unsafe {
            ucp_ep_create(worker.as_mut_ptr(), &params, inner.as_mut_ptr())
        };
        err_ucs!(status)?;

        Ok(Self {
            inner: RawPtr::from_uninit(inner),
            err_monitor,
        })
    }

    pub unsafe extern "C" fn err_handler(arg: *mut c_void, _: ucp_ep_h, status: ucs_status_t) {
        let err_monitor = &*(arg as *mut ErrorMonitor<IOError>);
        let err = format!("endpoint handler error: {:?}", status).into();
        err_monitor.set_error(err);
    }

    pub fn as_ptr(&self) -> *const ucp_ep {
        self.inner.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut ucp_ep {
        self.inner.as_mut_ptr()
    }

    fn check_error(&self) -> IOResult<()> {
        self.err_monitor.check_error()
    }

    pub fn connect(worker: &Worker, addr: &SockAddr) -> IOResult<Self> {
        let params = ucp_ep_params {
            field_mask: (ucp_ep_params_field::UCP_EP_PARAM_FIELD_FLAGS
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE)
                .0 as u64,
            flags: ucp_ep_params_flags_field::UCP_EP_PARAMS_FLAGS_CLIENT_SERVER.0,
            sockaddr: addr.as_ucs_sock_addr(),
            err_mode: ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER,
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };

        let endpoint = Self::new(worker, params)?;

        // Workaround for UCX bug: https://github.com/openucx/ucx/issues/6872
        // let buf = [0, 1, 2, 3];
        // endpoint.stream_send(&buf).await?;

        Ok(endpoint)
    }

    pub fn accept(worker: &Worker, conn: ConnRequest) -> IOResult<Self> {
        let params = ucp_ep_params {
            field_mask: ucp_ep_params_field::UCP_EP_PARAM_FIELD_CONN_REQUEST.0 as u64,
            conn_request: conn.as_mut_ptr(),
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };
        let endpoint = Endpoint::new(worker, params)?;
        Ok(endpoint)
    }

    unsafe extern "C" fn flush_handler(request: *mut c_void, _status: ucs_status_t) {
        let request = &mut *(request as *mut Request);
        request.waker.wake();
    }

    pub async fn flush(&self) -> IOResult<()> {
        self.check_error()?;

        let status = unsafe {
            ucp_ep_flush_nb(self.as_mut_ptr(), 0, Some(Self::flush_handler))
        };

        if status.is_null() {
            Ok(())
        } else if UcpUtils::ucs_ptr_is_ptr(status) {
            let f = RequestFuture::new(status, poll_request);
            f.await?;
            Ok(())
        } else {
            err_ucs!(UcpUtils::ucs_ptr_raw_status(status))
        }
    }

    unsafe extern "C" fn send_handler(request: *mut c_void, _status: ucs_status_t) {
        let request = &mut *(request as *mut Request);
        request.waker.wake();
    }

    pub async fn stream_send(&self, buf: DataSlice) -> IOResult<usize> {
        self.check_error()?;

        let status = unsafe {
            ucp_stream_send_nb(
                self.as_mut_ptr(),
                buf.as_ptr() as _,
                buf.len() as _,
                UcpUtils::ucp_dt_make_contig(1),
                Some(Self::send_handler),
                0
            )
        };

        if status.is_null() {
            Ok(buf.len())
        } else if UcpUtils::ucs_ptr_is_ptr(status) {
            let f = RequestFuture::new(status, poll_request);
            f.await?;
            Ok(buf.len())
        } else {
            err_ucs!(UcpUtils::ucs_ptr_raw_status(status))?;
            err_box!("未预期的状态: {:?}", status)
        }

    }

    unsafe extern "C" fn recv_handler(
        request: *mut c_void,
        _status: ucs_status_t,
        _length: usize
    ) {
        let request = &mut *(request as *mut Request);
        request.waker.wake();
    }

    pub async fn stream_recv(&self, mut buf: BytesMut) ->  IOResult<BytesMut> {
        self.check_error()?;

        let mut len = buf.len();
        let status = unsafe {
            ucp_stream_recv_nb(
                self.as_mut_ptr(),
                buf.as_mut_ptr() as _,
                buf.len() as _,
                UcpUtils::ucp_dt_make_contig(1),
                Some(Self::recv_handler),
                &mut len,
                0
            )
        };

        if status.is_null() {
            Ok(BytesMut::new())
        } else if UcpUtils::ucs_ptr_is_ptr(status) {
            let f = RequestFuture::new(status, poll_strem_recv);
            let len = f.await?;
            Ok(buf.split_to(len))
        } else {
            err_ucs!(UcpUtils::ucs_ptr_raw_status(status))?;
            err_box!("未预期的状态: {:?}", status)
        }
    }

    pub fn print(&self) {
        unsafe { ucp_ep_print_info(self.as_mut_ptr(), stderr) };
    }

}

impl Drop for Endpoint {
    fn drop(&mut self) {
        let status = unsafe {
            ucp_ep_close_nb(
                self.as_mut_ptr(),
                ucp_ep_close_mode::UCP_EP_CLOSE_MODE_FORCE as u32
            )
        };
        if let Err(e) = err_ucs!(UcpUtils::ucs_ptr_raw_status(status)) {
            warn!("Close endpoint failed, {}", e);
        }
    }
}

unsafe fn poll_request(ptr: ucs_status_ptr_t) -> Poll<IOResult<()>> {
    let status = ucp_request_check_status(ptr as _);
    if status == ucs_status_t::UCS_INPROGRESS {
        Poll::Pending
    } else {
        Poll::Ready(err_ucs!(status))
    }
}

unsafe fn poll_strem_recv(ptr: ucs_status_ptr_t) -> Poll<IOResult<usize>> {
    let mut len = 0;
    let status = ucp_stream_recv_request_test(ptr as _, &mut len);
    if status == ucs_status_t::UCS_INPROGRESS {
        Poll::Pending
    } else {
        err_ucs!(status)?;
        Poll::Ready(Ok(len))
    }
}