use std::mem::{self, MaybeUninit};
use std::sync::Arc;
use std::collections::HashMap;
use std::sync::Mutex;
use log::{info, debug, warn, error};
use crate::err_box;
use crate::io::IOResult;
use crate::ucp::bindings::*;
use crate::ucp::ucs_sock_addr::UcsSockAddr;
use crate::ucp::{UcpContext, UcpWorker, UcpEndpoint};

pub struct UcpListener {
    handle: ucp_listener_h,
}

impl UcpListener {
    pub fn new(worker: &UcpWorker, addr: &UcsSockAddr) -> IOResult<Self> {
        info!("🎧 创建UCX Listener，地址: {:?}", addr);
        
        // 创建连接处理器的用户数据
        let server_context = Box::new(ServerContext {
            worker: worker.handle(),
            connections: Arc::new(Mutex::new(HashMap::new())),
        });
        
        let server_context_ptr = Box::into_raw(server_context);
        
        let listener_params = ucp_listener_params_t {
            field_mask: (ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_SOCK_ADDR
                | ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_CONN_HANDLER)
                .0 as u64,
            sockaddr: addr.handle(),
            conn_handler: ucp_listener_conn_handler_t {
                cb: Some(Self::conn_handler_callback),
                arg: server_context_ptr as *mut std::os::raw::c_void,
            },
            accept_handler: unsafe { mem::zeroed() }, // 我们在conn_handler中处理
        };
        
        let mut handle = MaybeUninit::<ucp_listener_h>::uninit();
        
        let status = unsafe {
            ucp_listener_create(
                worker.handle(),
                &listener_params,
                handle.as_mut_ptr()
            )
        };
        
        if status != ucs_status_t::UCS_OK {
            unsafe { Box::from_raw(server_context_ptr) }; // 清理
            return err_box!("UCX Listener 创建失败: {:?}", status);
        }
        
        info!("✅ UCX Listener 创建成功");
        Ok(Self {
            handle: unsafe { handle.assume_init() }
        })
    }
    
    pub fn handle(&self) -> ucp_listener_h {
        self.handle
    }
    
    // 连接请求处理回调
    unsafe extern "C" fn conn_handler_callback(
        conn_request: ucp_conn_request_h,
        arg: *mut std::os::raw::c_void,
    ) {
        info!("🔗 收到新的连接请求: {:?}", conn_request);
        
        let server_context = &mut *(arg as *mut ServerContext);
        
        // 从连接请求创建endpoint
        match Self::create_endpoint_from_request(server_context.worker, conn_request) {
            Ok(endpoint) => {
                let conn_id = conn_request as usize;
                info!("✅ 连接 {} 建立成功", conn_id);
                
                // 保存连接并启动数据处理
                if let Ok(mut connections) = server_context.connections.lock() {
                    connections.insert(conn_id, endpoint);
                }
                
                info!("🎯 连接 {} 已建立，启动数据接收处理", conn_id);
                
                // 直接在当前线程启动数据处理（UCX指针不支持跨线程）
                // 添加小延迟让连接稳定，但保持在同一线程
                info!("🚀 连接 {} 即将启动数据处理", conn_id);
                Self::handle_client_data(server_context.worker, conn_id, &server_context.connections);
            }
            Err(e) => {
                error!("❌ 创建endpoint失败: {:?}", e);
            }
        }
    }
    
    unsafe fn create_endpoint_from_request(
        worker: ucp_worker_h,
        conn_request: ucp_conn_request_h,
    ) -> IOResult<UcpEndpoint> {
        info!("🔧 从连接请求创建endpoint: {:?}", conn_request);
        
        // 初始化endpoint参数结构体，确保所有字段都为零
        let mut ep_params: ucp_ep_params_t = mem::zeroed();
        
        // 设置必需的字段掩码
        ep_params.field_mask = ucp_ep_params_field::UCP_EP_PARAM_FIELD_CONN_REQUEST.0 as u64;
        
        // 设置连接请求
        ep_params.conn_request = conn_request;
        
        // 可选：设置错误处理模式
        // ep_params.field_mask |= ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE.0 as u64;
        // ep_params.err_mode = ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER;
        
        info!("🎯 Endpoint参数 - field_mask: 0x{:x}, conn_request: {:?}", 
              ep_params.field_mask, ep_params.conn_request);
        
        let mut handle = MaybeUninit::<*mut ucp_ep>::uninit();
        let status = ucp_ep_create(worker, &ep_params, handle.as_mut_ptr());
        
        if status != ucs_status_t::UCS_OK {
            error!("❌ ucp_ep_create失败: status={:?}, worker={:?}, params.field_mask=0x{:x}", 
                   status, worker, ep_params.field_mask);
            return err_box!("从连接请求创建endpoint失败: {:?}", status);
        }
        
        let endpoint_handle = handle.assume_init();
        info!("✅ Endpoint创建成功: {:?}", endpoint_handle);
        
        Ok(UcpEndpoint::from_handle(endpoint_handle))
    }
    
    /// 获取活跃连接数量（调试用）
    pub fn get_connection_count(&self) -> usize {
        // 这里暂时返回0，因为我们需要重新设计访问ServerContext的方式
        // 在实际实现中，需要更好的架构来访问连接信息
        info!("📊 连接数量查询（功能开发中）");
        0
    }

    // 处理客户端数据的逻辑（在连接建立后立即调用）
    fn handle_client_data(
        worker: ucp_worker_h,
        conn_id: usize,
        connections: &Arc<Mutex<std::collections::HashMap<usize, UcpEndpoint>>>,
    ) {
        info!("🔄 开始处理连接 {} 的数据", conn_id);
        
        // 创建临时worker包装器用于数据处理
        let temp_context = match UcpContext::new() {
            Ok(ctx) => Arc::new(ctx),
            Err(e) => {
                error!("❌ 创建临时context失败: {:?}", e);
                return;
            }
        };
        
        let temp_worker = match UcpWorker::new(temp_context) {
            Ok(w) => w,
            Err(e) => {
                error!("❌ 创建临时worker失败: {:?}", e);
                return;
            }
        };
        
        // 给连接更多时间稳定并等待客户端发送数据
        info!("⏳ 连接 {} 稳定期，等待客户端发送数据...", conn_id);
        std::thread::sleep(std::time::Duration::from_secs(1));
        
        // 尝试接收数据，让改进的接收函数来处理重试
        info!("📥 连接 {} 开始数据接收", conn_id);
        
        if let Ok(connections_guard) = connections.lock() {
            if let Some(endpoint) = connections_guard.get(&conn_id) {
                // 创建接收缓冲区
                let mut buffer = vec![0u8; 1024];
                
                // 使用改进的接收函数（内置重试机制）
                match endpoint.stream_recv_and_wait(&mut buffer, &temp_worker) {
                    Ok(received_len) => {
                        if received_len > 0 {
                            // 解析接收到的数据
                            let received_data = &buffer[..received_len];
                            let received_text = String::from_utf8_lossy(received_data);
                            
                            info!("✅ 连接 {} 接收到数据 ({} 字节): '{}'", 
                                 conn_id, received_len, received_text.trim());
                            
                            // 准备响应：res: 原始消息
                            let response = format!("res: {}", received_text.trim());
                            info!("📤 连接 {} 准备响应: '{}'", conn_id, response);
                            
                            // 发送响应
                           /* match endpoint.stream_send_and_wait(response.as_bytes(), &temp_worker) {
                                Ok(_) => {
                                    info!("✅ 连接 {} 响应发送成功", conn_id);
                                }
                                Err(e) => {
                                    error!("❌ 连接 {} 响应发送失败: {:?}", conn_id, e);
                                }
                            }*/
                        } else {
                            info!("🔍 连接 {} 最终没有接收到数据", conn_id);
                        }
                    }
                    Err(e) => {
                        error!("❌ 连接 {} 接收数据失败: {:?}", conn_id, e);
                    }
                }
            } else {
                error!("❌ 找不到连接 {}", conn_id);
            }
        } else {
            error!("❌ 无法获取连接锁");
        }
        
        info!("🏁 连接 {} 数据处理完成", conn_id);
    }
}

impl Drop for UcpListener {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            info!("🔒 销毁UCX Listener");
            unsafe {
                ucp_listener_destroy(self.handle);
            }
        }
    }
}

// 服务器上下文，用于管理连接
struct ServerContext {
    worker: ucp_worker_h,
    connections: Arc<Mutex<HashMap<usize, UcpEndpoint>>>,
}

unsafe impl Send for ServerContext {}
unsafe impl Sync for ServerContext {}
