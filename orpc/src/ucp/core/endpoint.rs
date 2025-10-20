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
use crate::sync::ErrorMonitor;
use crate::sys::RawPtr;
use crate::ucp::bindings::*;
use crate::ucp::core::{SockAddr, Worker};
use crate::ucp::request::{ConnRequest, RequestParam, RequestWaker};
use crate::ucp::rma::RemoteMem;
use crate::ucp::{stderr, UcpUtils};
use crate::{err_box, err_ucs, poll_status};
use log::{info, warn};
use std::ffi::CStr;
use std::mem::MaybeUninit;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::task::Poll;
use crate::io::net::InetAddr;

pub struct Endpoint {
    inner: RawPtr<ucp_ep>,
    worker: Arc<Worker>,
    err_monitor: Arc<ErrorMonitor<IOError>>,
}

impl Endpoint {
    fn new(worker: Arc<Worker>, mut params: ucp_ep_params) -> IOResult<Self> {
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
        let status = unsafe { ucp_ep_create(worker.as_mut_ptr(), &params, inner.as_mut_ptr()) };
        err_ucs!(status)?;

        let endpoint = Self {
            inner: RawPtr::from_uninit(inner),
            worker,
            err_monitor,
        };

        Ok(endpoint)
    }

    pub fn connect(worker: Arc<Worker>, addr: &SockAddr) -> IOResult<Self> {
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
        Ok(endpoint)
    }

    pub fn accept(worker: Arc<Worker>, conn: ConnRequest) -> IOResult<Self> {
        let params = ucp_ep_params {
            field_mask: ucp_ep_params_field::UCP_EP_PARAM_FIELD_CONN_REQUEST.0 as u64,
            conn_request: conn.as_mut_ptr(),
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };

        let endpoint = Endpoint::new(worker, params)?;
        Ok(endpoint)
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

    unsafe extern "C" fn stream_handler(
        request: *mut c_void,
        _status: ucs_status_t,
        _user_data: *mut c_void,
    ) {
        let request = &mut *(request as *mut RequestWaker);
        request.wake();
    }

    pub async fn stream_send(&self, buf: &[u8]) -> IOResult<()> {
        self.check_error()?;

        let params = RequestParam::new().send_cb(Some(Self::stream_handler));
        let status = unsafe {
            ucp_stream_send_nbx(
                self.as_mut_ptr(),
                buf.as_ptr() as _,
                buf.len() as _,
                params.as_ptr(),
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

        let mut len = 0;
        let param = RequestParam::new().recv_stream_cb(Some(Self::stream_recv_callback));
        let status = unsafe {
            ucp_stream_recv_nbx(
                self.as_mut_ptr(),
                buf.as_mut_ptr() as _,
                buf.len() as _,
                &mut len as *mut _,
                param.as_ptr(),
            )
        };
        poll_status!(status, len, poll_stream_recv)
    }

    pub async fn stream_recv_full(&self, buf: &mut [u8]) -> IOResult<()> {
        let mut off = 0;
        let len = buf.len();

        while off < len {
            let remaining_buf = &mut buf[off..];
            match self.stream_recv(remaining_buf).await {
                Ok(recv_len) => {
                    if recv_len == 0 {
                        return err_box!("Connection closed while reading data, expected {} bytes but got only {}", len, off);
                    }
                    off += recv_len;
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

        let status = unsafe { ucp_ep_flush_nb(self.as_mut_ptr(), 0, Some(Self::flush_callback)) };
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

    /// 将数据写入远程内存中
    pub async fn put(&self, buf: &[u8], remote_mem: &RemoteMem) -> IOResult<()> {
        self.check_error()?;

        let param = RequestParam::new().send_cb(Some(Self::custom_callback));

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

        let param = RequestParam::new().send_cb(Some(Self::custom_callback));
        let status = unsafe {
            ucp_get_nbx(
                self.as_mut_ptr(),
                buf.as_mut_ptr() as _,
                buf.len() as _,
                remote_mem.addr(),
                remote_mem.rkey_mut_ptr(),
                param.as_ptr(),
            )
        };
        poll_status!(status, poll_normal)
    }

    pub async fn tag_send(&self, tag: u64, buf: &[u8]) -> IOResult<()> {
        self.check_error()?;

        let param = RequestParam::new().send_cb(Some(Self::custom_callback));
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

    unsafe extern "C" fn tag_recv_callback(
        request: *mut c_void,
        _status: ucs_status_t,
        _info: *const ucp_tag_recv_info,
        _user_data: *mut c_void,
    ) {
        let request = &mut *(request as *mut RequestWaker);
        request.wake();
    }

    pub async fn tag_recv(&self, tag: u64, buf: &mut [u8]) -> IOResult<usize> {
        self.check_error()?;

        let param = RequestParam::new().recv_tag_cb(Some(Self::tag_recv_callback));

        let status = unsafe {
            ucp_tag_recv_nbx(
                self.worker.as_mut_ptr(),
                buf.as_mut_ptr() as _,
                buf.len() as _,
                tag,
                u64::MAX,
                param.as_ptr(),
            )
        };

        // 如果立即完成（极少情况），使用 (tag, buf.len()) 作为默认值
        // 返回的长度为buf.len，应用层需要自行处理实际数据长度。
        let (_, len) = poll_status!(status, (tag, buf.len()), poll_tag)?;
        Ok(len)
    }

    /// 获取连接的 socket 地址
    pub fn conn_sockaddr(&self) -> IOResult<(SockAddr, SockAddr)> {
        let mut attr = ucp_ep_attr {
            field_mask: (ucp_ep_attr_field::UCP_EP_ATTR_FIELD_LOCAL_SOCKADDR.0
                | ucp_ep_attr_field::UCP_EP_ATTR_FIELD_REMOTE_SOCKADDR.0) as u64,
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };

        let status = unsafe { ucp_ep_query(self.as_mut_ptr(), &mut attr as *mut _) };
        err_ucs!(status)?;

        let local = SockAddr::from(attr.local_sockaddr);
        let remote = SockAddr::from(attr.remote_sockaddr);
        Ok((local, remote))
    }


    /// 查询 endpoint 使用的传输层（硬件类型）
    ///
    /// 返回格式化字符串，格式：`"transport1:device1, transport2:device2"`
    ///
    /// # Examples
    /// - `"rc_mlx5=mlx5_0:1, shm=memory"` - InfiniBand + 共享内存
    /// - `"tcp=eth0"` - TCP
    /// - `"dc_mlx5=mlx5_0:1"` - Dynamically Connected Transport
    pub fn query_transports(&self) -> IOResult<String> {
        const MAX_TRANSPORTS: usize = 10;
        let mut entries: Vec<ucp_transport_entry_t> = vec![
            ucp_transport_entry_t {
                transport_name: ptr::null(),
                device_name: ptr::null(),
            };
            MAX_TRANSPORTS
        ];

        let mut attr = ucp_ep_attr {
            field_mask: ucp_ep_attr_field::UCP_EP_ATTR_FIELD_TRANSPORTS.0 as u64,
            transports: ucp_transports_t {
                entries: entries.as_mut_ptr(),
                num_entries: MAX_TRANSPORTS as u32,
                entry_size: size_of::<ucp_transport_entry_t>(),
            },
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };

        let status = unsafe { ucp_ep_query(self.as_mut_ptr(), &mut attr as *mut _) };
        err_ucs!(status)?;

        let num = attr.transports.num_entries as usize;
        if num == 0 {
            return Ok(String::from("none"));
        }

        let mut parts = Vec::with_capacity(num);
        for i in 0..num {
            unsafe {
                let entry = &*attr.transports.entries.add(i);
                if !entry.transport_name.is_null() && !entry.device_name.is_null() {
                    let transport = CStr::from_ptr(entry.transport_name).to_string_lossy();
                    let device = CStr::from_ptr(entry.device_name).to_string_lossy();
                    parts.push(format!("{}={}", transport, device));
                }
            }
        }

        Ok(parts.join(", "))
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

    let status = unsafe { ucp_stream_recv_request_test(ptr as _, len.as_mut_ptr() as _) };

    if status == ucs_status_t::UCS_INPROGRESS {
        return Poll::Pending;
    }

    match err_ucs!(status) {
        Ok(_) => Poll::Ready(Ok(unsafe { len.assume_init() })),
        Err(e) => Poll::Ready(Err(e)),
    }
}

fn poll_normal(ptr: ucs_status_ptr_t) -> Poll<IOResult<()>> {
    let status = unsafe { ucp_request_check_status(ptr as _) };

    if status == ucs_status_t::UCS_INPROGRESS {
        return Poll::Pending;
    }

    match err_ucs!(status) {
        Ok(_) => Poll::Ready(Ok(())),
        Err(e) => Poll::Ready(Err(e)),
    }
}

fn poll_tag(ptr: ucs_status_ptr_t) -> Poll<IOResult<(u64, usize)>> {
    let mut info = ucp_tag_recv_info {
        sender_tag: 0,
        length: 0,
    };

    let status = unsafe { ucp_tag_recv_request_test(ptr as _, &mut info as *mut _ as _) };

    if status == ucs_status_t::UCS_INPROGRESS {
        return Poll::Pending;
    }

    match err_ucs!(status) {
        Ok(_) => Poll::Ready(Ok((info.sender_tag, info.length))),
        Err(e) => Poll::Ready(Err(e)),
    }
}
