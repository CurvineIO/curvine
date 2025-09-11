use std::{mem, ptr};
use std::sync::Arc;
use crate::err_box;
use crate::io::IOResult;
use crate::ucp::bindings::*;
use crate::ucp::bindings::ucs_status_t::UCS_OK;
use crate::ucp::UcpContext;

pub struct UcpWorker {
    handle: ucp_worker_h,
    context: Arc<UcpContext>
}

impl UcpWorker {
    pub fn new(context: Arc<UcpContext>) -> IOResult<Self> {
        unsafe {
            let mut params: ucp_worker_params_t = mem::zeroed();

            params.field_mask = ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE.0 as _;
            params.thread_mode = ucs_thread_mode_t::UCS_THREAD_MODE_SINGLE;

            let mut handle = ptr::null_mut();
            let status = ucp_worker_create(
                context.handle(),
                &params,
                &mut handle
            );
            if status != UCS_OK {
                return err_box!("context {:?}", status)
            }

            Ok(Self {
                context,
                handle,
            })
        }
    }

    pub fn progress(&self) -> u32 {
        unsafe {
            ucp_worker_progress(self.handle)
        }
    }

    pub fn handle(&self) -> ucp_worker_h {
        self.handle
    }
}

impl Drop for UcpWorker {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { ucp_worker_destroy(self.handle); }
        }
    }
}