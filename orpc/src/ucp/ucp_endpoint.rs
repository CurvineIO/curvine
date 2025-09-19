use std::{mem, ptr};
use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use log::{info, warn};
use crate::err_box;
use crate::io::IOResult;
use crate::sys::DataSlice;
use crate::ucp::bindings::{ucp_datatype_t, ucp_dt_type, ucp_ep, ucp_ep_close_flags_t, ucp_ep_close_mode, ucp_ep_close_nb, ucp_ep_close_nbx, ucp_ep_create, ucp_ep_h, ucp_ep_params, ucp_ep_params_field, ucp_ep_params_flags_field, ucp_ep_params_t, ucp_err_handling_mode_t, ucp_params, ucp_request_param_t, ucp_stream_send_nb, ucp_stream_recv_nb, ucp_stream_recv_data_nb, ucs_status_t, ucp_ep_flush_nb, ucp_request_check_status, ucp_request_free};
use crate::ucp::bindings::ucs_status_t::{UCS_OK, UCS_INPROGRESS};
use crate::ucp::ucs_sock_addr::UcsSockAddr;
use crate::ucp::UcpWorker;

pub struct UcpEndpoint {
    handle: ucp_ep_h
}

impl UcpEndpoint {
    pub fn connect(worker: &UcpWorker, addr: &UcsSockAddr) -> IOResult<Self> {
        // 增强的验证
        let worker_handle = worker.handle();
        if worker_handle.is_null() {
            return err_box!("Worker handle is null");
        }
        
        let family = addr.debug_address_family();
        if family == 0 {
            return err_box!("Invalid address family: {}", family);
        }
        
        println!("🔧 创建endpoint - worker: {:?}, family: {}", worker_handle, family);
        
        // 使用最小必要参数集合，避免过度简化
        let mut ep_params: ucp_ep_params_t = unsafe {
            mem::zeroed()
        };

        // 客户端连接的最小必要参数
        ep_params.field_mask = (ucp_ep_params_field::UCP_EP_PARAM_FIELD_FLAGS
            | ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR).0 as u64;
        ep_params.flags = ucp_ep_params_flags_field::UCP_EP_PARAMS_FLAGS_CLIENT_SERVER.0;
        ep_params.sockaddr = addr.handle();
        
        println!("🎯 Endpoint参数 - field_mask: 0x{:x}", ep_params.field_mask);

        let mut handle = MaybeUninit::<*mut ucp_ep>::uninit();
        let status = unsafe {
            ucp_ep_create(worker_handle, &ep_params, handle.as_mut_ptr())
        };

        if status != UCS_OK {
            println!("❌ ucp_ep_create失败: {:?}", status);
            return err_box!("endpoint creation failed: {:?}", status);
        }

        let endpoint_handle = unsafe { handle.assume_init() };
        if endpoint_handle.is_null() {
            return err_box!("Created endpoint handle is null");
        }
        
        println!("✅ Endpoint创建成功: {:?}", endpoint_handle);
        Ok(Self { handle: endpoint_handle })
    }


    /// 发送数据并等待完成（同步方式，避免pending请求问题）
    pub fn stream_send_and_wait(&self, chunk: &[u8], worker: &UcpWorker) -> IOResult<()> {
        // 严格的预验证
        if self.handle.is_null() {
            return err_box!("Endpoint handle is null");
        }
        
        if chunk.is_empty() {
            return err_box!("发送数据不能为空");
        }
        
        if chunk.len() > 64 * 1024 * 1024 { // 64MB限制
            return err_box!("发送数据过大: {} 字节", chunk.len());
        }
        
        if worker.handle().is_null() {
            return err_box!("Worker handle is null");
        }
        
        println!("📤 stream_send_and_wait: 发送 {} 字节", chunk.len());
        println!("   endpoint: {:?}, worker: {:?}", self.handle, worker.handle());
        println!("   数据(hex): {:02x?}", &chunk[..chunk.len().min(8)]);

        unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t, length: usize) {
            info!(
                "stream_recv: complete. req={:?}, status={:?}, len={}",
                request,
                status,
                length
            );
        }

        // 使用最简单的同步发送，不使用回调
        let request = unsafe {
            ucp_stream_send_nb(
                self.handle,
                chunk.as_ptr() as _,
                chunk.len(),
                Self::ucp_dt_make_contig(1),
                None,
                0
            )
        };
        
        if request.is_null() {
            println!("✅ stream_send_and_wait: 同步完成");
            return Ok(());
        }
        
        println!("🔄 stream_send_and_wait: 等待异步请求完成, req={:?}", request);
        
        // 无限等待逻辑 - 去掉超时限制
        let mut attempt = 0;
        loop {
            // 安全的worker progress
            let progress_count = worker.progress();
            
            if progress_count > 0 {
                println!("   Progress attempt {}: {} operations", attempt, progress_count);
            }
            
            // 安全的状态检查
            let status = unsafe { 
                if request.is_null() {
                    UCS_OK
                } else {
                    ucp_request_check_status(request)
                }
            };
            
            if status != UCS_INPROGRESS {
                println!("✅ 请求完成: status={:?}, attempt={}", status, attempt);
                
                // 安全的资源释放
                if !request.is_null() {
                    unsafe { ucp_request_free(request) };
                }
                
                if status == UCS_OK {
                    return Ok(());
                } else {
                    return err_box!("发送失败: {:?}", status);
                }
            }
            
            attempt += 1;
            std::thread::sleep(std::time::Duration::from_millis(20)); // 增加间隔
        }
    }

    pub fn stream_send(&self, chunk: &[u8]) -> IOResult<()> {
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
                Self::ucp_dt_make_contig(1), // 统一使用标准CONTIG类型
                Some(callback),
                0
            )
        };
        
        if status.is_null() {
            println!("🔄 stream_send: completed immediately (synchronous)");
            Ok(())
        } else {
            println!("🔄 stream_send: request submitted (asynchronous): {:?}", status);
            Ok(())
        }
    }

    /// 从原始handle创建UcpEndpoint（用于服务器端）
    pub fn from_handle(handle: ucp_ep_h) -> Self {
        Self { handle }
    }

    /// 获取原始handle（用于底层UCX调用）
    pub fn raw_handle(&self) -> ucp_ep_h {
        self.handle
    }

    /// 安全关闭endpoint（在有worker访问权限时调用）
    pub fn close_safely(&mut self, worker: &UcpWorker) -> IOResult<()> {
        if self.handle.is_null() {
            println!("⚠️  Endpoint已经关闭");
            return Ok(());
        }
        
        println!("🔒 开始安全关闭endpoint...");
        
        unsafe {
            // 第一步：使用worker progress推进所有pending操作
            println!("🔄 Step 1: 推进所有pending操作...");
            // 运行一些progress周期来推进pending操作
            for i in 0..20 {  // 这里保留有限的预处理，因为是准备工作
                let progress_count = worker.progress();
                if progress_count > 0 {
                    println!("   progress {}: {} operations", i, progress_count);
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            
            // 第二步：flush所有pending requests
            println!("📤 Step 2: Flushing所有pending requests...");
            let flush_req = ucp_ep_flush_nb(self.handle, 0, None);
            
            if flush_req.is_null() {
                println!("✅ Flush立即完成");
            } else {
                println!("⏳ 等待flush完成，使用worker progress...");
                // 无限等待flush完成，这是关键步骤
                let mut i = 0;
                loop {
                    // 关键：使用worker progress推进flush操作
                    worker.progress();
                    
                    let status = ucp_request_check_status(flush_req);
                    if status != UCS_INPROGRESS {
                        println!("✅ Flush完成 after {} iterations, status: {:?}", i, status);
                        ucp_request_free(flush_req);
                        break;
                    }
                    i += 1;
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
            
            // 第三步：关闭endpoint
            println!("🔒 Step 3: 关闭endpoint...");
            let close_req = ucp_ep_close_nb(
                self.handle,
                ucp_ep_close_mode::UCP_EP_CLOSE_MODE_FLUSH as u32
            );
            
            if close_req.is_null() {
                println!("✅ Endpoint同步关闭完成");
            } else {
                println!("⏳ 等待异步关闭完成...");
                // 无限等待关闭完成
                let mut i = 0;
                loop {
                    // 关键：使用worker progress推进关闭操作
                    worker.progress();
                    
                    let status = ucp_request_check_status(close_req);
                    if status != UCS_INPROGRESS {
                        println!("✅ 关闭完成 after {} iterations, status: {:?}", i, status);
                        ucp_request_free(close_req);
                        break;
                    }
                    i += 1;
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
            
            self.handle = std::ptr::null_mut();
            println!("✅ Endpoint安全关闭完成");
        }
        
        Ok(())
    }

    /// 接收流数据
    pub fn stream_recv(&self, buffer: &mut [u8]) -> IOResult<usize> {
        if self.handle.is_null() {
            return err_box!("Endpoint handle is null");
        }
        
        if buffer.is_empty() {
            return err_box!("接收缓冲区为空");
        }
        
        println!("📥 stream_recv: 尝试接收最多 {} 字节", buffer.len());
        
        // 用于接收实际数据长度
        let mut actual_length: usize = 0;
        
        let request = unsafe {
            ucp_stream_recv_nb(
                self.handle,
                buffer.as_mut_ptr() as *mut ::std::os::raw::c_void,
                buffer.len(),
                ucp_dt_type::UCP_DATATYPE_CONTIG as ucp_datatype_t,
                None, // 不使用回调
                &mut actual_length as *mut usize,
                0 // flags
            )
        };
        
        if request.is_null() {
            // 同步完成，数据已接收
            println!("✅ stream_recv: 同步接收完成，实际长度: {} 字节", actual_length);
            return Ok(actual_length);
        }
        
        // 异步请求，返回pending
        println!("🔄 stream_recv: 异步请求提交: {:?}", request);
        
        // 这里需要进一步的状态检查和处理
        // 暂时返回0表示需要进一步处理
        Ok(0)
    }

    pub fn ucp_dt_make_contig(elem_size: usize) -> ucp_datatype_t {
        // UCX标准的contig datatype构造
        // SHIFT=3, CONTIG=0, 所以对于1字节元素：(1 << 3) | 0 = 8
        let result = ((elem_size as ucp_datatype_t) << (ucp_dt_type::UCP_DATATYPE_SHIFT as ucp_datatype_t))
            | (ucp_dt_type::UCP_DATATYPE_CONTIG as ucp_datatype_t);
        println!("🔧 ucp_dt_make_contig({}) = 0x{:x}", elem_size, result);
        result
    }

    /// 接收数据并等待完成（简化安全版本）
    pub fn stream_recv_and_wait(&self, buffer: &mut [u8], worker: &UcpWorker) -> IOResult<usize> {
        if self.handle.is_null() {
            return err_box!("Endpoint handle is null");
        }
        
        if buffer.is_empty() {
            return err_box!("接收缓冲区为空");
        }
        
        if worker.handle().is_null() {
            return err_box!("Worker handle is null");
        }
        
        println!("📥 stream_recv_and_wait: 开始接收流程");
        println!("   缓冲区大小: {} 字节", buffer.len());
        println!("   endpoint: {:?}, worker: {:?}", self.handle, worker.handle());
        
        // 简化的接收流程，减少复杂性
        // 第一步：运行worker progress确保数据到达
        println!("🔄 预处理：推进worker progress...");
        for i in 0..20 {
            let progress_count = worker.progress();
            if progress_count > 0 {
                println!("   进度 {}: {} operations", i, progress_count);
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        
        // 第二步：直接尝试接收数据
        let mut actual_length: usize = 0;
        
        println!("📥 尝试接收数据...");
        let request = unsafe {
            ucp_stream_recv_nb(
                self.handle,
                buffer.as_mut_ptr() as _,
                buffer.len() as _,
                Self::ucp_dt_make_contig(1),
                None, // 不使用回调
                &mut actual_length as *mut usize,
                0 // flags
            )
        };
        
        if request.is_null() {
            // 同步完成
            println!("✅ 同步接收完成，实际长度: {} 字节", actual_length);
            if actual_length > 0 {
                println!("   数据(hex): {:02x?}", &buffer[..actual_length.min(16)]);
                println!("   数据(text): {:?}", String::from_utf8_lossy(&buffer[..actual_length.min(64)]));
            } else {
                println!("⚠️  同步完成但没有数据");
            }
            return Ok(actual_length);
        }
        
        println!("🔄 异步接收请求: {:?}", request);
        
        // 无限等待逻辑 - 去掉超时限制  
        let mut attempt = 0;
        loop {
            let progress_count = 0;

            println!("   异步进度 {}: {} operations", attempt, progress_count);

            let status = unsafe {
                ucp_request_check_status(request)
            };
            
            if status != UCS_INPROGRESS {
                println!("✅ 异步接收完成: status={:?}, attempt={}", status, attempt);
                
                // 安全释放资源
                unsafe { ucp_request_free(request) };
                
                if status == UCS_OK {
                    println!("   最终长度: {} 字节", actual_length);
                    if actual_length > 0 {
                        println!("   数据(hex): {:02x?}", &buffer[..actual_length.min(16)]);
                        println!("   数据(text): {:?}", String::from_utf8_lossy(&buffer[..actual_length]));
                    }
                    return Ok(actual_length);
                } else {
                    return err_box!("接收失败: {:?}", status);
                }
            }
            
            attempt += 1;
            std::thread::sleep(std::time::Duration::from_millis(1000));
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
                Self::ucp_dt_make_contig(1),
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