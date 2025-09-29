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

use crate::io::{IOError, IOResult};
use crate::sync::{AtomicBool, ErrorMonitor};
use crate::sys::{DataSlice, RawPtr};
use crate::ucp::bindings::*;
use crate::ucp::{stderr, ConnRequest, RequestWaker, RequestFuture, SockAddr, UcpUtils, WorkerExecutor, RequestParam, RKey};
use crate::{err_box, err_ucs, poll_status};
use bytes::BytesMut;
use log::{info, warn};
use std::mem::MaybeUninit;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::task::Poll;

pub struct Endpoint {
    inner: RawPtr<ucp_ep>,
    executor: WorkerExecutor,
    err_monitor: Arc<ErrorMonitor<IOError>>,
}

impl Endpoint {
    fn new(executor: WorkerExecutor, mut params: ucp_ep_params) -> IOResult<Self> {
        let err_monitor = Arc::new(ErrorMonitor::new());

        params.field_mask |= (ucp_ep_params_field::UCP_EP_PARAM_FIELD_USER_DATA
            | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLER)
            .0 as u64;
        params.user_data = &*err_monitor as *const _ as *mut c_void;
        params.err_handler = ucp_err_handler {
            cb: Some(Self::err_handler),
            arg: ptr::null_mut(),
        };

        let mut inner = MaybeUninit::<*mut ucp_ep>::uninit();
        let status =
            unsafe { ucp_ep_create(executor.worker().as_mut_ptr(), &params, inner.as_mut_ptr()) };
        err_ucs!(status)?;

        Ok(Self {
            inner: RawPtr::from_uninit(inner),
            executor,
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

    pub fn connect(executor: WorkerExecutor, addr: &SockAddr) -> IOResult<Self> {
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

        let endpoint = Self::new(executor, params)?;
        Ok(endpoint)
    }

    pub fn accept(executor: WorkerExecutor, conn: ConnRequest) -> IOResult<Self> {
        let params = ucp_ep_params {
            field_mask: ucp_ep_params_field::UCP_EP_PARAM_FIELD_CONN_REQUEST.0 as u64,
            conn_request: conn.as_mut_ptr(),
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };
        
        let endpoint = Endpoint::new(executor, params)?;
        Ok(endpoint)
    }

    unsafe extern "C" fn stream_handler(
        request: *mut c_void,
        _status: ucs_status_t,
        _user_data: *mut c_void
    ) {
        let request = &mut *(request as *mut RequestWaker);
        request.wake();
    }

    pub async fn stream_send(&self, buf: DataSlice) -> IOResult<usize> {
        self.check_error()?;

        let params = RequestParam::new()
            .send_cb(Some(Self::stream_handler));
        let status = unsafe {
            ucp_stream_send_nbx(
                self.as_mut_ptr(),
                buf.as_ptr() as _,
                buf.len() as _,
                params.as_ptr()
            )
        };
        poll_status!(status, poll_normal)?;
        Ok(buf.len())
    }

    unsafe extern "C" fn recv_handler(
        request: *mut c_void,
        _status: ucs_status_t,
        _length: usize,
        _user_data: *mut c_void,
    ) {
        let request = &mut *(request as *mut RequestWaker);
        request.wake();
    }

    pub async fn stream_recv(&self, mut buf: BytesMut) -> IOResult<Option<BytesMut>> {
        self.check_error()?;

        let mut len = MaybeUninit::<usize>::uninit();
        let param = RequestParam::new()
            .recv_stream_cb(Some(Self::recv_handler));
        let status = unsafe {
            ucp_stream_recv_nbx(
                self.as_mut_ptr(),
                buf.as_mut_ptr() as _,
                buf.len() as _,
                len.as_mut_ptr(),
                param.as_ptr(),
            )
        };

        match poll_status!(status, len, poll_stream)? {
            None => Ok(None),
            Some(v) => Ok(Some(buf.split_to(v)))
        }
    }

    unsafe extern "C" fn flush_handler(request: *mut c_void, _status: ucs_status_t) {
        let request = &mut *(request as *mut RequestWaker);
        request.wake();
    }

    pub async fn flush(&self) -> IOResult<()> {
        self.check_error()?;

        let status = unsafe {
            ucp_ep_flush_nb(self.as_mut_ptr(), 0, Some(Self::flush_handler))
        };
        poll_status!(status, poll_normal)
    }

    unsafe extern "C" fn rma_handler(
        request: *mut c_void,
        _status: ucs_status_t,
        _user_data: *mut c_void,
    ) {
        let request = &mut *(request as *mut RequestWaker);
        request.wake();
    }

    /// 将数据写入远程内存中
    pub async fn put(&self, buf: DataSlice, remote_addr: u64, rkey: &RKey) -> IOResult<()> {
        let param = RequestParam::new()
            .send_cb(Some(Self::rma_handler));

        let status = unsafe {
            ucp_put_nbx(
                self.as_mut_ptr(),
                buf.as_ptr() as _,
                buf.len() as _,
                remote_addr,
                rkey.as_mut_ptr(),
                param.as_ptr(),
            )
        };
        poll_status!(status, poll_normal)
    }

    // 从远程内存读取数据。
    pub async fn get(&self, buf: &mut [u8], remote_addr: u64, rkey: &RKey) -> IOResult<()> {
        let param = RequestParam::new()
            .send_cb(Some(Self::rma_handler));
        let status = unsafe {
            ucp_get_nbx(
                self.as_mut_ptr(),
                buf.as_mut_ptr() as _,
                buf.len() as _,
                remote_addr,
                rkey.as_mut_ptr(),
                param.as_ptr()
            )
        };
        poll_status!(status, poll_normal)
    }

    pub fn print(&self) {
        unsafe { ucp_ep_print_info(self.as_mut_ptr(), stderr) };
    }

    pub fn executor(&self) -> &WorkerExecutor {
        &self.executor
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        let status = unsafe {
            ucp_ep_close_nb(
                self.as_mut_ptr(),
                ucp_ep_close_mode::UCP_EP_CLOSE_MODE_FORCE as u32,
            )
        };
        if let Err(e) = err_ucs!(UcpUtils::ucs_ptr_raw_status(status)) {
            warn!("Close endpoint failed, {}", e);
        }
    }
}

fn poll_stream(ptr: ucs_status_ptr_t) -> Poll<IOResult<Option<usize>>> {
    let mut len = MaybeUninit::<usize>::uninit();

    let status = unsafe {
        ucp_stream_recv_request_test(ptr as _, len.as_mut_ptr() as _)
    };
    if status == ucs_status_t::UCS_INPROGRESS {
        Poll::Pending
    } else if status == ucs_status_t::UCS_ERR_CONNECTION_RESET  {
        Poll::Ready(Ok(None))
    } else {
        err_ucs!(status)?;
        Poll::Ready(Ok(Some(unsafe { len.assume_init() })))
    }
}

fn poll_normal(ptr: ucs_status_ptr_t) -> Poll<IOResult<()>> {
    let status = unsafe { ucp_request_check_status(ptr as _) };
    if status == ucs_status_t::UCS_INPROGRESS {
        Poll::Pending
    } else {
        err_ucs!(status)?;
        Poll::Ready(Ok(()))
    }
}
