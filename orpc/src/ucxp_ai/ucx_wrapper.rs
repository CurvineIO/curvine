use std::ffi::{CStr, CString};
use std::net::SocketAddr;
use std::ptr;
use std::os::raw::{c_char, c_void, c_int, c_uint};
use anyhow::{Result, anyhow};
use libc;

use super::bindings::*;

/// UCX 错误处理
pub fn check_ucx_status(status: ucs_status_t, operation: &str) -> Result<()> {
    if status != ucs_status_t_UCS_OK {
        let error_name = unsafe {
            let name_ptr = ucs_status_string(status);
            if !name_ptr.is_null() {
                CStr::from_ptr(name_ptr).to_string_lossy().into_owned()
            } else {
                format!("Unknown error: {}", status)
            }
        };
        return Err(anyhow!("{} 失败: {}", operation, error_name));
    }
    Ok(())
}

/// UCX 上下文封装
pub struct UcxContext {
    context: ucp_context_h,
}

impl UcxContext {
    /// 创建新的 UCX 上下文
    pub fn new() -> Result<Self> {
        unsafe {
            // 设置 UCP 参数
            let mut params: ucp_params_t = std::mem::zeroed();
            params.field_mask = ucp_params_field_UCP_PARAM_FIELD_FEATURES;
            params.features = ucp_feature_UCP_FEATURE_TAG | ucp_feature_UCP_FEATURE_STREAM;

            let mut context: ucp_context_h = ptr::null_mut();
            let status = ucp_init(&params, ptr::null(), &mut context);
            
            check_ucx_status(status, "创建 UCP 上下文")?;

            Ok(UcxContext { context })
        }
    }

    /// 获取上下文句柄
    pub fn handle(&self) -> ucp_context_h {
        self.context
    }
}

impl Drop for UcxContext {
    fn drop(&mut self) {
        if !self.context.is_null() {
            unsafe {
                ucp_cleanup(self.context);
            }
        }
    }
}

/// UCX 工作器封装
pub struct UcxWorker {
    worker: ucp_worker_h,
    _context: UcxContext,
}

impl UcxWorker {
    /// 创建新的工作器
    pub fn new(context: UcxContext) -> Result<Self> {
        unsafe {
            let mut params: ucp_worker_params_t = std::mem::zeroed();
            params.field_mask = ucp_worker_params_field_UCP_WORKER_PARAM_FIELD_THREAD_MODE;
            params.thread_mode = ucs_thread_mode_t_UCS_THREAD_MODE_SINGLE;

            let mut worker: ucp_worker_h = ptr::null_mut();
            let status = ucp_worker_create(context.handle(), &params, &mut worker);
            
            check_ucx_status(status, "创建工作器")?;

            Ok(UcxWorker {
                worker,
                _context: context,
            })
        }
    }

    /// 推进工作器进度
    pub fn progress(&self) -> u32 {
        unsafe { ucp_worker_progress(self.worker) }
    }

    /// 获取工作器句柄
    pub fn handle(&self) -> ucp_worker_h {
        self.worker
    }

    /// 连接到远程端点
    pub fn connect(&self, address: SocketAddr) -> Result<UcxEndpoint> {
        UcxEndpoint::connect(self, address)
    }
}

impl Drop for UcxWorker {
    fn drop(&mut self) {
        if !self.worker.is_null() {
            unsafe {
                ucp_worker_destroy(self.worker);
            }
        }
    }
}

/// UCX 端点封装
pub struct UcxEndpoint {
    endpoint: ucp_ep_h,
}

impl UcxEndpoint {
    /// 连接到指定地址
    pub fn connect(worker: &UcxWorker, address: SocketAddr) -> Result<Self> {
        unsafe {
            // 创建端点参数
            let mut ep_params: ucp_ep_params_t = std::mem::zeroed();
            
            // 设置 socket address
            let addr_str = CString::new(address.to_string()).unwrap();
            let mut sockaddr: libc::sockaddr_in = std::mem::zeroed();
            sockaddr.sin_family = libc::AF_INET as u16;
            sockaddr.sin_port = address.port().to_be();
            sockaddr.sin_addr.s_addr = match address.ip() {
                std::net::IpAddr::V4(ipv4) => u32::from_be_bytes(ipv4.octets()).to_be(),
                std::net::IpAddr::V6(_) => return Err(anyhow!("暂不支持 IPv6")),
            };

            ep_params.field_mask = ucp_ep_params_field_UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
            ep_params.address = &sockaddr as *const _ as *const ucs_sock_addr_t;

            let mut endpoint: ucp_ep_h = ptr::null_mut();
            let status = ucp_ep_create(worker.handle(), &ep_params, &mut endpoint);
            
            check_ucx_status(status, "创建端点")?;

            Ok(UcxEndpoint { endpoint })
        }
    }

    /// 发送带标签的消息
    pub fn send_tag(&self, tag: u64, data: &[u8]) -> Result<()> {
        unsafe {
            let mut params: ucp_request_param_t = std::mem::zeroed();
            params.op_attr_mask = ucp_op_attr_t_UCP_OP_ATTR_FIELD_DATATYPE;
            params.datatype = ucp_dt_make_contig(1); // 连续字节数据

            let request = ucp_tag_send_nbx(
                self.endpoint,
                data.as_ptr() as *const c_void,
                data.len(),
                tag,
                &params,
            );

            // 检查请求状态
            if UCS_PTR_IS_ERR(request) {
                let status = UCS_PTR_STATUS(request);
                check_ucx_status(status, "发送标签消息")?;
            } else if !request.is_null() {
                // 等待请求完成
                let mut status = ucs_status_t_UCS_INPROGRESS;
                while status == ucs_status_t_UCS_INPROGRESS {
                    status = ucp_request_check_status(request);
                }
                ucp_request_free(request);
                check_ucx_status(status, "等待发送完成")?;
            }

            Ok(())
        }
    }

    /// 接收带标签的消息
    pub fn receive_tag(&self, tag: u64, buffer: &mut [u8]) -> Result<usize> {
        unsafe {
            let mut params: ucp_request_param_t = std::mem::zeroed();
            params.op_attr_mask = ucp_op_attr_t_UCP_OP_ATTR_FIELD_DATATYPE;
            params.datatype = ucp_dt_make_contig(1);

            let request = ucp_tag_recv_nbx(
                self.endpoint,
                buffer.as_mut_ptr() as *mut c_void,
                buffer.len(),
                tag,
                0, // tag_mask，0表示精确匹配
                &params,
            );

            if UCS_PTR_IS_ERR(request) {
                let status = UCS_PTR_STATUS(request);
                check_ucx_status(status, "接收标签消息")?;
                return Ok(0);
            }

            // 等待接收完成
            let mut status = ucs_status_t_UCS_INPROGRESS;
            while status == ucs_status_t_UCS_INPROGRESS {
                status = ucp_request_check_status(request);
            }

            if status == ucs_status_t_UCS_OK {
                // 获取实际接收的字节数
                let mut info: ucp_tag_recv_info_t = std::mem::zeroed();
                let info_status = ucp_tag_recv_request_test(request, &mut info);
                ucp_request_free(request);
                
                if info_status == ucs_status_t_UCS_OK {
                    Ok(info.length)
                } else {
                    check_ucx_status(info_status, "获取接收信息")?;
                    Ok(0)
                }
            } else {
                ucp_request_free(request);
                check_ucx_status(status, "等待接收完成")?;
                Ok(0)
            }
        }
    }
}

impl Drop for UcxEndpoint {
    fn drop(&mut self) {
        if !self.endpoint.is_null() {
            unsafe {
                // 使用非阻塞方式关闭端点
                let mut params: ucp_ep_close_params_t = std::mem::zeroed();
                params.mode = ucp_ep_close_mode_UCP_EP_CLOSE_MODE_FORCE;
                ucp_ep_close_nbx(self.endpoint, &params);
            }
        }
    }
}

/// 辅助函数
unsafe fn UCS_PTR_IS_ERR(ptr: *mut c_void) -> bool {
    (ptr as uintptr_t) >= (-(ucs_status_t_UCS_ERR_LAST as isize) as uintptr_t)
}

unsafe fn UCS_PTR_STATUS(ptr: *mut c_void) -> ucs_status_t {
    ptr as ucs_status_t
}

type uintptr_t = usize;
