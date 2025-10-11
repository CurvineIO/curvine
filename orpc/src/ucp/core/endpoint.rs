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
use crate::ucp::{stderr, UcpUtils, UcpExecutor};
use crate::{err_box, err_ucs, poll_status};
use bytes::{Buf, BufMut, BytesMut};
use log::{info, warn};
use std::mem::MaybeUninit;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::task::Poll;
use serde::__private::de::Content::U64;
use crate::ucp::core::SockAddr;
use crate::ucp::request::{ConnRequest, RequestParam, RequestStatus, RequestWaker};
use crate::ucp::rma::{RemoteMem, RKey};

pub struct Endpoint {
    inner: RawPtr<ucp_ep>,
    executor: UcpExecutor,
    err_monitor: Arc<ErrorMonitor<IOError>>,
}

impl Endpoint {
    fn new(executor: UcpExecutor, mut params: ucp_ep_params) -> IOResult<Self> {
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

    pub fn connect(executor: UcpExecutor, addr: &SockAddr) -> IOResult<Self> {
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

    pub fn accept(executor: UcpExecutor, conn: ConnRequest) -> IOResult<Self> {
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

    pub async fn stream_send(&self, buf: &[u8]) -> IOResult<()> {
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
        poll_status!(status, poll_normal)
    }

    unsafe extern "C" fn stream_recv_callback(
        request: *mut c_void,
        _status: ucs_status_t,
        _length: usize,
        _user_data: *mut c_void,
    ) {
        let request = &mut *(request as *mut RequestWaker);
        request.wake();
    }

    pub async fn stream_recv(&self, buf: &mut [u8]) -> IOResult<usize> {
        self.check_error()?;

        let mut len = MaybeUninit::<usize>::uninit();
        let param = RequestParam::new()
            .recv_stream_cb(Some(Self::stream_recv_callback));
        let status = unsafe {
            ucp_stream_recv_nbx(
                self.as_mut_ptr(),
                buf.as_mut_ptr() as _,
                buf.len() as _,
                len.as_mut_ptr(),
                param.as_ptr(),
            )
        };
        poll_status!(status, len, poll_stream_recv)
    }

    pub async fn stream_recv_full(&self, buf: &mut [u8]) -> IOResult<()> {
        let mut offset = 0;
        let len = buf.len();

        while offset < len {
            let remaining_buf = &mut buf[offset..];
            match self.stream_recv(remaining_buf).await {
                Ok(recv_len) => {
                    if recv_len == 0 {
                        return err_box!("Connection closed while reading data, expected {} bytes but got only {}", len, offset);
                    }
                    offset += recv_len;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    unsafe extern "C" fn flush_callback(request: *mut c_void, _status: ucs_status_t) {
        let request = &mut *(request as *mut RequestWaker);
        request.wake();
    }

    pub async fn flush(&self) -> IOResult<()> {
        self.check_error()?;

        let status = unsafe {
            ucp_ep_flush_nb(self.as_mut_ptr(), 0, Some(Self::flush_callback))
        };
        poll_status!(status, poll_normal)
    }

    unsafe extern "C" fn custom_callback(
        request: *mut c_void,
        _status: ucs_status_t,
        _user_data: *mut c_void,
    ) {
        let request = &mut *(request as *mut RequestWaker);
        request.wake();
    }

    unsafe extern "C" fn tag_recv_callback(
        request: *mut c_void,
        _status: ucs_status_t,
        _info: *const ucp_tag_recv_info,
        _user_data: *mut c_void,
    ) {
        let request = &mut *(request as *mut RequestWaker);
        request.wake();
    }

    /// 将数据写入远程内存中
    pub async fn put(&self, buf: &[u8], remote_mem: &RemoteMem) -> IOResult<()> {
        self.check_error()?;

        let param = RequestParam::new()
            .send_cb(Some(Self::custom_callback));

        let status = unsafe {
            ucp_put_nbx(
                self.as_mut_ptr(),
                buf.as_ptr() as _,
                buf.len() as _,
                remote_mem.addr(),
                remote_mem.rkey_mut_ptr(),
                param.as_ptr(),
            )
        };
        poll_status!(status, poll_normal)
    }

    // 从远程内存读取数据。
    pub async fn get(&self, buf: &mut [u8], remote_mem: &RemoteMem) -> IOResult<()> {
        self.check_error()?;

        let param = RequestParam::new()
            .send_cb(Some(Self::custom_callback));
        let status = unsafe {
            ucp_get_nbx(
                self.as_mut_ptr(),
                buf.as_mut_ptr() as _,
                buf.len() as _,
                remote_mem.addr(),
                remote_mem.rkey_mut_ptr(),
                param.as_ptr()
            )
        };
        poll_status!(status, poll_normal)
    }

    pub async fn tag_send(&self, tag: u64, buf: &[u8]) -> IOResult<()> {
        self.check_error()?;

        let param = RequestParam::new()
            .send_cb(Some(Self::custom_callback));
        let status = unsafe {
            ucp_tag_send_nbx(
                self.as_mut_ptr(),
                buf.as_ptr() as _,
                buf.len() as _,
                tag,
                param.as_ptr(),
            )
        };
        poll_status!(status, poll_normal)
    }

    pub async fn tag_recv(&self, tag: u64, buf: &mut [u8]) -> IOResult<usize> {
        self.check_error()?;
        let param = RequestParam::new()
            .recv_tag_cb(Some(Self::tag_recv_callback));
        let status = unsafe {
            ucp_tag_recv_nbx(
                self.executor.worker().as_mut_ptr(),
                buf.as_mut_ptr() as _,
                buf.len() as _,
                tag,
                u64::MAX,
                param.as_ptr(),
            )
        };

        poll_status!(status, poll_tag).map(|x| x.1)
    }

    pub fn print(&self) {
        unsafe { ucp_ep_print_info(self.as_mut_ptr(), stderr) };
    }

    pub fn executor(&self) -> &UcpExecutor {
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

fn poll_stream_recv(ptr: ucs_status_ptr_t) -> Poll<IOResult<usize>> {
    let mut len = MaybeUninit::<usize>::uninit();

    let status = unsafe {
        ucp_stream_recv_request_test(ptr as _, len.as_mut_ptr() as _)
    };

    if status == ucs_status_t::UCS_INPROGRESS {
        return Poll::Pending
    }

    match err_ucs!(status) {
        Ok(_) => Poll::Ready(Ok(unsafe { len.assume_init() })),
        Err(e) => Poll::Ready(Err(e))
    }
}

fn poll_normal(ptr: ucs_status_ptr_t) -> Poll<IOResult<()>> {
    let status = unsafe { ucp_request_check_status(ptr as _) };

    if status == ucs_status_t::UCS_INPROGRESS {
        return Poll::Pending
    }

    match err_ucs!(status) {
        Ok(_) => Poll::Ready(Ok(())),
        Err(e) => Poll::Ready(Err(e))
    }
}

fn poll_tag(ptr: ucs_status_ptr_t) -> Poll<IOResult<(u64, usize)>> {
    let mut info = MaybeUninit::<ucp_tag_recv_info>::uninit();
    let status = unsafe {
        ucp_tag_recv_request_test(ptr as _, info.as_mut_ptr() as _)
    };

    if status == ucs_status_t::UCS_INPROGRESS {
        return Poll::Pending
    }

    match err_ucs!(status) {
        Ok(_) => {
            let info = unsafe { info.assume_init() };
            Poll::Ready(Ok((info.sender_tag, info.length)))
        }
        Err(e) => Poll::Ready(Err(e))
    }
}
