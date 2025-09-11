use std::{mem, ptr};
use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use log::{info, warn};
use crate::err_box;
use crate::io::IOResult;
use crate::sys::DataSlice;
use crate::ucp::bindings::{ucp_datatype_t, ucp_dt_type, ucp_ep, ucp_ep_close_flags_t, ucp_ep_close_mode, ucp_ep_close_nb, ucp_ep_close_nbx, ucp_ep_create, ucp_ep_h, ucp_ep_params, ucp_ep_params_field, ucp_ep_params_flags_field, ucp_ep_params_t, ucp_err_handling_mode_t, ucp_params, ucp_request_param_t, ucp_stream_send_nb, ucs_status_t};
use crate::ucp::bindings::ucs_status_t::UCS_OK;
use crate::ucp::ucs_sock_addr::UcsSockAddr;
use crate::ucp::UcpWorker;

pub struct UcpEndpoint {
    handle: ucp_ep_h
}

impl UcpEndpoint {
    pub fn connect(worker: &UcpWorker, addr: &UcsSockAddr) -> IOResult<Self> {
        let mut ep_params: ucp_ep_params_t = unsafe {
            mem::zeroed()
        };

        ep_params.field_mask =  (ucp_ep_params_field::UCP_EP_PARAM_FIELD_FLAGS
            | ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR
            | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE)
        .0 as u64;
        ep_params.flags = ucp_ep_params_flags_field::UCP_EP_PARAMS_FLAGS_CLIENT_SERVER.0;
        ep_params.address = addr.handle();
        ep_params.err_mode = ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER;

        let mut handle = MaybeUninit::<*mut ucp_ep>::uninit();
        let status = unsafe {
            ucp_ep_create(worker.handle(), &ep_params, handle.as_mut_ptr())
        };

        if status != UCS_OK {
            return err_box!("context {:?}", status)
        }

        Ok(Self { handle: unsafe { handle.assume_init() } })
    }

    pub const fn ucp_dt_make_cong(elem_size: usize) -> ucp_datatype_t {
        ((elem_size as ucp_datatype_t) << (ucp_dt_type::UCP_DATATYPE_SHIFT as ucp_datatype_t))
            | ucp_dt_type::UCP_DATATYPE_CONTIG as ucp_datatype_t
    }

    pub fn stream_send(&self, chunk: DataSlice) {
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
            warn!(
                "stream_send: complete. req={:?}, status={:?}",
                request,
                status
            );
            // pass
        }
        let status = unsafe {
            ucp_stream_send_nb(
                self.handle,
                chunk.as_slice().as_ptr() as _,
                chunk.len(),
                Self::ucp_dt_make_cong(1),
                Some(callback),
                0
            )
        };
        if status.is_null() {
            info!("stream_send: complete")
        }
    }
}

impl Drop for UcpEndpoint {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                // 使用非阻塞方式关闭端点
                let status = ucp_ep_close_nb(
                    self.handle,
                    ucp_ep_close_mode::UCP_EP_CLOSE_MODE_FORCE as u32
                );
            }
        }
    }
}