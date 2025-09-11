use std::ptr;
use crate::err_box;
use crate::io::IOResult;
use crate::ucp::bindings::*;
use std::ffi::CString;

pub struct UcpConf {
    handle: *mut ucp_config_t,
}

impl UcpConf {
    pub fn new() -> IOResult<Self> {
        let mut handle: *mut ucp_config_t = ptr::null_mut();
        let status = unsafe {
            ucp_config_read(
                ptr::null(),
                ptr::null(),
                &mut handle as *mut _
            )
        };
        
        if status != ucs_status_t::UCS_OK {
            return err_box!("UCX 配置读取失败: {:?}", status);
        }
        
        if handle.is_null() {
            return err_box!("UCX 配置句柄为空");
        }
        
        Ok(UcpConf { handle })
    }
    
    pub fn handle(&self) -> *mut ucp_config_t {
        self.handle
    }
    
    pub fn modify(&mut self, name: &str, value: &str) -> IOResult<()> {
        let name_c = CString::new(name)?;
        let value_c = CString::new(value)?;
        
        let status = unsafe {
            ucp_config_modify(
                self.handle,
                name_c.as_ptr(),
                value_c.as_ptr()
            )
        };
        
        if status != ucs_status_t::UCS_OK {
            return err_box!("配置修改失败: {:?}", status)
        }
        
        Ok(())
    }
}

impl Default for UcpConf {
    fn default() -> Self {
        Self::new().unwrap_or_else(|_| UcpConf {
            handle: ptr::null_mut(),
        })
    }
}

impl Drop for UcpConf {
    fn drop(&mut self) {
        unsafe { ucp_config_release(self.handle) };
    }
}