use std::mem::MaybeUninit;
use std::ptr;
use crate::err_ucs;
use crate::sys::{CString, RawPtr};
use crate::ucp::bindings::*;
use crate::ucp::stderr;

#[derive(Debug)]
pub struct Config {
    inner: RawPtr<ucp_config_t> ,
}

impl Config {
    pub fn as_ptr(&self) -> *const ucp_config_t {
        self.inner.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut ucp_config_t {
        self.inner.as_mut_ptr()
    }

    pub fn print(&self) {
        let flags = ucs_config_print_flags_t::UCS_CONFIG_PRINT_CONFIG
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_DOC
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_HEADER
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_HIDDEN;
        let title = CString::new("UCP conf").expect("Not a valid CStr");
        unsafe { ucp_config_print(self.as_ptr(), stderr, title.as_ptr(), flags) };
    }
}

impl Default for Config {
    fn default() -> Self {
        let mut inner = MaybeUninit::<*mut ucp_config>::uninit();
        let status = unsafe {
            ucp_config_read(ptr::null(), ptr::null(), inner.as_mut_ptr())
        };
        err_ucs!(status).unwrap();

        Self {
            inner: RawPtr::from_uninit(inner),
        }
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        unsafe {
            ucp_config_release(self.as_mut_ptr())
        }
    }
}