use std::{mem, ptr};
use std::mem::MaybeUninit;
use crate::err_box;
use crate::io::IOResult;
use crate::ucp::bindings::*;
use crate::ucp::UcpConf;

#[derive(Debug)]
pub struct Context {
    handle: ucp_context_h,
}

impl Context {
    pub fn new() -> IOResult<Self> {
        Self::with_config(&UcpConf::default())
    }

    pub fn with_config(conf: &UcpConf) -> IOResult<Self> {
        let features = ucp_feature::UCP_FEATURE_RMA
            | ucp_feature::UCP_FEATURE_TAG
            | ucp_feature::UCP_FEATURE_STREAM
            | ucp_feature::UCP_FEATURE_WAKEUP;

        let params = ucp_params_t {
            field_mask: (ucp_params_field::UCP_PARAM_FIELD_FEATURES
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_SIZE
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_INIT
                | ucp_params_field::UCP_PARAM_FIELD_REQUEST_CLEANUP
                | ucp_params_field::UCP_PARAM_FIELD_MT_WORKERS_SHARED)
                .0 as u64,
            features: features.0 as u64,
            ..unsafe { std::mem::zeroed() }
        };

        let mut handle: ucp_context_h = unsafe {
            mem::zeroed()
        };

        let mut handle = MaybeUninit::<*mut ucp_context>::uninit();

        let status = unsafe {
            ucp_init_version(
                UCP_API_MAJOR,
                UCP_API_MINOR,
                &params,
                conf.handle(),
                handle.as_mut_ptr(),
            )
        };
        if status != ucs_status_t::UCS_OK {
            return err_box!("ucp_init_version 失败")
        }
        Ok( Self { handle: unsafe { handle.assume_init()} })
    }

    pub fn handle(&self) -> ucp_context_h {
        self.handle
    }
}


impl Drop for Context {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                ucp_cleanup(self.handle);
            }
        }
    }
}

unsafe impl Send for Context {}

unsafe impl Sync for Context {}
