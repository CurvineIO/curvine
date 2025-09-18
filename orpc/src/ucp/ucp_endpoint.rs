use std::{mem, ptr};
use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use log::{info, warn};
use crate::err_box;
use crate::io::IOResult;
use crate::sys::DataSlice;
use crate::ucp::bindings::{ucp_datatype_t, ucp_dt_type, ucp_ep, ucp_ep_close_flags_t, ucp_ep_close_mode, ucp_ep_close_nb, ucp_ep_close_nbx, ucp_ep_create, ucp_ep_h, ucp_ep_params, ucp_ep_params_field, ucp_ep_params_flags_field, ucp_ep_params_t, ucp_err_handling_mode_t, ucp_params, ucp_request_param_t, ucp_stream_send_nb, ucs_status_t, ucp_ep_flush_nb, ucp_request_check_status, ucp_request_free};
use crate::ucp::bindings::ucs_status_t::{UCS_OK, UCS_INPROGRESS};
use crate::ucp::ucs_sock_addr::UcsSockAddr;
use crate::ucp::UcpWorker;

pub struct UcpEndpoint {
    handle: ucp_ep_h
}

impl UcpEndpoint {
    pub fn connect(worker: &UcpWorker, addr: &UcsSockAddr) -> IOResult<Self> {
        // 验证地址族
        let family = addr.debug_address_family();
        if family == 0 {
            return err_box!("Invalid address family: {}", family);
        }
        
        let mut ep_params: ucp_ep_params_t = unsafe {
            mem::zeroed()
        };

        ep_params.field_mask = (ucp_ep_params_field::UCP_EP_PARAM_FIELD_FLAGS
            | ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR
            | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE)
        .0 as u64;
        ep_params.flags = ucp_ep_params_flags_field::UCP_EP_PARAMS_FLAGS_CLIENT_SERVER.0;
        
        // 关键修复：使用sockaddr字段而不是address字段
        ep_params.sockaddr = addr.handle();
        
        ep_params.err_mode = ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER;

        let mut handle = MaybeUninit::<*mut ucp_ep>::uninit();
        let status = unsafe {
            ucp_ep_create(worker.handle(), &ep_params, handle.as_mut_ptr())
        };

        if status != UCS_OK {
            return err_box!("endpoint creation failed: {:?}", status);
        }

        Ok(Self { handle: unsafe { handle.assume_init() } })
    }

    pub const fn ucp_dt_make_cong(elem_size: usize) -> ucp_datatype_t {
        ((elem_size as ucp_datatype_t) << (ucp_dt_type::UCP_DATATYPE_SHIFT as ucp_datatype_t))
            | ucp_dt_type::UCP_DATATYPE_CONTIG as ucp_datatype_t
    }

    /// 发送数据并等待完成（同步方式，避免pending请求问题）
    pub fn stream_send_and_wait(&self, chunk: &[u8], worker: &UcpWorker) -> IOResult<()> {
        println!("📤 stream_send_and_wait: 发送 {} 字节", chunk.len());
        println!("   数据(hex): {:02x?}", &chunk[..chunk.len().min(32)]);
        println!("   数据(text): {:?}", String::from_utf8_lossy(chunk));
        
        // 使用简单的回调，不需要复杂的状态管理
        unsafe extern "C" fn simple_callback(request: *mut c_void, status: ucs_status_t) {
            if status == UCS_OK {
                println!("✅ 发送成功: req={:?}", request);
            } else {
                println!("❌ 发送失败: req={:?}, status={:?}", request, status);
            }
        }
        
        let request = unsafe {
            ucp_stream_send_nb(
                self.handle,
                chunk.as_ptr() as *const ::std::os::raw::c_void,
                chunk.len(),
                Self::ucp_dt_make_cong(1),
                Some(simple_callback),
                0
            )
        };
        
        if request.is_null() {
            println!("✅ stream_send_and_wait: 同步完成");
            return Ok(());
        }
        
        println!("🔄 stream_send_and_wait: 等待异步请求完成, req={:?}", request);
        
        // 等待请求完成
        let mut attempts = 0;
        let max_attempts = 500;
        
        while attempts < max_attempts {
            // 推进worker
            let progress_count = worker.progress();
            
            // 检查请求状态
            let status = unsafe { ucp_request_check_status(request) };
            
            if status != UCS_INPROGRESS {
                println!("✅ 请求完成: status={:?}, attempts={}", status, attempts);
                
                // 释放请求资源
                unsafe { ucp_request_free(request) };
                
                if status == UCS_OK {
                    return Ok(());
                } else {
                    return err_box!("发送失败: {:?}", status);
                }
            }
            
            if progress_count > 0 {
                println!("   Progress {}: {} operations, request still in progress", attempts, progress_count);
            }
            
            attempts += 1;
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        
        println!("⚠️  请求超时，强制释放资源");
        unsafe { ucp_request_free(request) };
        
        err_box!("发送超时")
    }

    pub fn stream_send(&self, chunk: &[u8]) {
        // 调试：显示要发送的数据
        let data_slice = chunk;
        println!("stream_send: 准备发送 {} 字节数据", data_slice.len());
        println!("stream_send: 数据内容(hex): {:02x?}", &data_slice[..data_slice.len().min(32)]);
        println!("stream_send: 数据内容(text): {:?}", String::from_utf8_lossy(data_slice));
        
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
            if status == ucs_status_t::UCS_OK {
                println!("✅ stream_send: SUCCESS. req={:?}", request);
            } else {
                println!("❌ stream_send: FAILED. req={:?}, status={:?}", request, status);
            }
        }
        
        let status = unsafe {
            ucp_stream_send_nb(
                self.handle,
                data_slice.as_ptr() as _,
                data_slice.len() as _,
                Self::ucp_dt_make_cong(1), // 直接使用CONTIG类型
                Some(callback),
                0
            )
        };
        
        if status.is_null() {
            println!("🔄 stream_send: completed immediately (synchronous)");
        } else {
            println!("🔄 stream_send: request submitted (asynchronous): {:?}", status);
        }
    }

    /// 发送数据并通过worker progress等待
    pub fn stream_send_with_progress(&self, chunk: &[u8], worker: &UcpWorker) {
        // 发送数据
        self.stream_send(chunk);

        // 推进worker多次确保异步操作完成
        for i in 0..100 {  // 最多尝试100次
            let progress_count = worker.progress();
            if progress_count > 0 {
                info!("Worker progress: {} operations completed (iteration {})", progress_count, i + 1);
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        info!("stream_send_with_progress completed");
    }
    
    /// 获取UCX endpoint句柄（测试用）
    pub fn raw_handle(&self) -> ucp_ep_h {
        self.handle
    }
    
    /// 发送字符串数据（调试用）
    pub fn send_string(&self, text: &str) {
        info!("send_string: 发送文本 '{}'", text);
        let bytes = text.as_bytes();
        
        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
            if status == ucs_status_t::UCS_OK {
                info!("send_string: 发送成功. req={:?}", request);
            } else {
                warn!("send_string: 发送失败. req={:?}, status={:?}", request, status);
            }
        }
        
        let status = unsafe {
            ucp_stream_send_nb(
                self.handle,
                bytes.as_ptr() as *const ::std::os::raw::c_void,
                bytes.len(),
                ucp_dt_type::UCP_DATATYPE_CONTIG as ucp_datatype_t,
                Some(callback),
                0
            )
        };
        
        if status.is_null() {
            info!("send_string: 同步完成");
        } else {
            info!("send_string: 异步提交: {:?}", status);
        }
    }
}

impl Drop for UcpEndpoint {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            println!("🔄 UcpEndpoint::drop - 开始安全关闭连接...");
            
            unsafe {
                // 使用 ucp_ep_flush_nb 确保所有请求完成
                println!("   📤 Flushing pending requests...");
                let flush_req = ucp_ep_flush_nb(self.handle, 0, None);
                
                if flush_req.is_null() {
                    println!("   ✅ Flush completed immediately");
                } else {
                    println!("   🔄 Waiting for flush to complete...");
                    // 等待flush完成
                    for i in 0..100 {
                        let status = ucp_request_check_status(flush_req);
                        if status != UCS_INPROGRESS {
                            println!("   ✅ Flush completed after {} iterations, status: {:?}", i, status);
                            ucp_request_free(flush_req);
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                }
                
                // 现在安全地关闭endpoint
                println!("   🔒 Closing endpoint...");
                let close_req = ucp_ep_close_nb(
                    self.handle,
                    ucp_ep_close_mode::UCP_EP_CLOSE_MODE_FLUSH as u32
                );
                
                if close_req.is_null() {
                    println!("✅ UcpEndpoint::drop - 连接已安全关闭");
                } else {
                    println!("🔄 UcpEndpoint::drop - 等待异步关闭完成...");
                    // 等待关闭完成
                    for i in 0..50 {
                        let status = ucp_request_check_status(close_req);
                        if status != UCS_INPROGRESS {
                            println!("✅ Close completed after {} iterations, status: {:?}", i, status);
                            ucp_request_free(close_req);
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(20));
                    }
                }
            }
            
            self.handle = std::ptr::null_mut();
        }
    }
}