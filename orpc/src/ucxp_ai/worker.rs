use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::net::{SocketAddr, IpAddr};
use std::ptr;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, Instant};
use std::os::raw::{c_void, c_char, c_uint};

use anyhow::{Result, anyhow};
use libc;

use super::bindings::*;

/// UCX Worker 配置
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// 线程模式
    pub thread_mode: ThreadMode,
    /// 缓冲区大小
    pub buffer_size: usize,
    /// 是否启用标签功能
    pub enable_tag: bool,
    /// 是否启用流功能
    pub enable_stream: bool,
    /// 是否启用RMA功能
    pub enable_rma: bool,
    /// 进度轮询间隔 (微秒)
    pub progress_interval_us: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            thread_mode: ThreadMode::Single,
            buffer_size: 64 * 1024, // 64KB 默认缓冲区
            enable_tag: true,
            enable_stream: true,
            enable_rma: false,
            progress_interval_us: 100,
        }
    }
}

/// 线程模式
#[derive(Debug, Clone, Copy)]
pub enum ThreadMode {
    Single,
    Multi,
}

/// UCX Worker 统计信息
#[derive(Debug)]
pub struct WorkerStats {
    pub messages_sent: AtomicU64,
    pub messages_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub connections: AtomicU64,
    pub errors: AtomicU64,
    pub start_time: Instant,
}

impl Default for WorkerStats {
    fn default() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            connections: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }
}

impl WorkerStats {
    /// 记录发送消息
    pub fn record_send(&self, bytes: usize) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    
    /// 记录接收消息
    pub fn record_receive(&self, bytes: usize) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    
    /// 记录连接
    pub fn record_connection(&self) {
        self.connections.fetch_add(1, Ordering::Relaxed);
    }
    
    /// 记录错误
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    
    /// 计算吞吐量 (MB/s)
    pub fn throughput_mbps(&self) -> f64 {
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();
        if elapsed_secs > 0.0 {
            (self.bytes_sent.load(Ordering::Relaxed) + self.bytes_received.load(Ordering::Relaxed)) as f64
                / (1024.0 * 1024.0 * elapsed_secs)
        } else {
            0.0
        }
    }
    
    /// 计算消息速率 (msg/s)
    pub fn message_rate(&self) -> f64 {
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();
        if elapsed_secs > 0.0 {
            (self.messages_sent.load(Ordering::Relaxed) + self.messages_received.load(Ordering::Relaxed)) as f64
                / elapsed_secs
        } else {
            0.0
        }
    }
    
    /// 打印统计信息
    pub fn print(&self) {
        println!("=== UCX Worker 统计信息 ===");
        println!("运行时间: {:?}", self.start_time.elapsed());
        println!("发送消息: {}", self.messages_sent.load(Ordering::Relaxed));
        println!("接收消息: {}", self.messages_received.load(Ordering::Relaxed));
        println!("发送字节: {}", self.bytes_sent.load(Ordering::Relaxed));
        println!("接收字节: {}", self.bytes_received.load(Ordering::Relaxed));
        println!("连接数: {}", self.connections.load(Ordering::Relaxed));
        println!("错误数: {}", self.errors.load(Ordering::Relaxed));
        println!("吞吐量: {:.2} MB/s", self.throughput_mbps());
        println!("消息速率: {:.2} msg/s", self.message_rate());
    }
}

/// UCX Context 封装
pub struct UcxContext {
    context: ucp_context_h,
}

impl UcxContext {
    /// 创建新的 UCX 上下文
    pub fn new(config: &WorkerConfig) -> Result<Self> {
        unsafe {
            let mut params: ucp_params_t = std::mem::zeroed();
            params.field_mask = ucp_params_field_UCP_PARAM_FIELD_FEATURES;
            
            // 设置功能
            let mut features = 0u64;
            if config.enable_tag {
                features |= ucp_feature_UCP_FEATURE_TAG;
            }
            if config.enable_stream {
                features |= ucp_feature_UCP_FEATURE_STREAM;
            }
            if config.enable_rma {
                features |= ucp_feature_UCP_FEATURE_RMA;
            }
            
            params.features = features;
            
            let mut context: ucp_context_h = ptr::null_mut();
            let status = ucp_init(&params, ptr::null(), &mut context);
            
            check_ucx_status(status, "初始化 UCX 上下文")?;
            
            Ok(UcxContext { context })
        }
    }
    
    /// 获取原始句柄
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

/// UCX Worker 主要封装
pub struct UcxWorker {
    worker: ucp_worker_h,
    _context: Arc<UcxContext>,
    config: WorkerConfig,
    stats: WorkerStats,
    endpoints: Arc<Mutex<HashMap<String, UcxEndpoint>>>,
    listener: Option<UcxListener>,
}

impl UcxWorker {
    /// 创建新的 UCX Worker
    pub fn new(config: WorkerConfig) -> Result<Self> {
        let context = Arc::new(UcxContext::new(&config)?);
        
        unsafe {
            let mut params: ucp_worker_params_t = std::mem::zeroed();
            params.field_mask = ucp_worker_params_field_UCP_WORKER_PARAM_FIELD_THREAD_MODE;
            params.thread_mode = match config.thread_mode {
                ThreadMode::Single => ucs_thread_mode_t_UCS_THREAD_MODE_SINGLE,
                ThreadMode::Multi => ucs_thread_mode_t_UCS_THREAD_MODE_MULTI,
            };
            
            let mut worker: ucp_worker_h = ptr::null_mut();
            let status = ucp_worker_create(context.handle(), &params, &mut worker);
            
            check_ucx_status(status, "创建 UCX Worker")?;
            
            Ok(UcxWorker {
                worker,
                _context: context,
                config,
                stats: WorkerStats::default(),
                endpoints: Arc::new(Mutex::new(HashMap::new())),
                listener: None,
            })
        }
    }
    
    /// 推进 Worker 进度
    pub fn progress(&self) -> u32 {
        unsafe {
            ucp_worker_progress(self.worker)
        }
    }
    
    /// 获取 Worker 句柄
    pub fn handle(&self) -> ucp_worker_h {
        self.worker
    }
    
    /// 获取配置
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }
    
    /// 获取统计信息
    pub fn stats(&self) -> &WorkerStats {
        &self.stats
    }
    
    /// 连接到远程地址
    pub fn connect(&self, addr: SocketAddr) -> Result<String> {
        let endpoint = UcxEndpoint::connect(self, addr)?;
        let endpoint_id = format!("{}:{}", addr.ip(), addr.port());
        
        {
            let mut endpoints = self.endpoints.lock().unwrap();
            endpoints.insert(endpoint_id.clone(), endpoint);
        }
        
        self.stats.record_connection();
        Ok(endpoint_id)
    }
    
    /// 断开连接
    pub fn disconnect(&self, endpoint_id: &str) -> Result<()> {
        let mut endpoints = self.endpoints.lock().unwrap();
        if endpoints.remove(endpoint_id).is_some() {
            Ok(())
        } else {
            Err(anyhow!("端点 {} 不存在", endpoint_id))
        }
    }
    
    /// 创建监听器
    pub fn create_listener(&mut self, bind_addr: SocketAddr) -> Result<()> {
        let listener = UcxListener::new(self, bind_addr)?;
        self.listener = Some(listener);
        Ok(())
    }
    
    /// 发送标签消息
    pub fn send_tag(&self, endpoint_id: &str, tag: u64, data: &[u8]) -> Result<()> {
        let endpoints = self.endpoints.lock().unwrap();
        if let Some(endpoint) = endpoints.get(endpoint_id) {
            endpoint.send_tag(tag, data)?;
            self.stats.record_send(data.len());
            Ok(())
        } else {
            Err(anyhow!("端点 {} 不存在", endpoint_id))
        }
    }
    
    /// 接收标签消息
    pub fn receive_tag(&self, endpoint_id: &str, tag: u64, buffer: &mut [u8]) -> Result<usize> {
        let endpoints = self.endpoints.lock().unwrap();
        if let Some(endpoint) = endpoints.get(endpoint_id) {
            let bytes_received = endpoint.receive_tag(tag, buffer)?;
            if bytes_received > 0 {
                self.stats.record_receive(bytes_received);
            }
            Ok(bytes_received)
        } else {
            Err(anyhow!("端点 {} 不存在", endpoint_id))
        }
    }
    
    /// 尝试接收标签消息（非阻塞）
    pub fn try_receive_tag(&self, endpoint_id: &str, tag: u64, buffer: &mut [u8]) -> Result<Option<usize>> {
        let endpoints = self.endpoints.lock().unwrap();
        if let Some(endpoint) = endpoints.get(endpoint_id) {
            match endpoint.try_receive_tag(tag, buffer) {
                Ok(bytes_received) if bytes_received > 0 => {
                    self.stats.record_receive(bytes_received);
                    Ok(Some(bytes_received))
                }
                Ok(_) => Ok(None),
                Err(e) => {
                    self.stats.record_error();
                    Err(e)
                }
            }
        } else {
            Err(anyhow!("端点 {} 不存在", endpoint_id))
        }
    }
    
    /// 广播消息到所有连接
    pub fn broadcast_tag(&self, tag: u64, data: &[u8]) -> Result<()> {
        let endpoints = self.endpoints.lock().unwrap();
        let mut errors = 0;
        
        for (endpoint_id, endpoint) in endpoints.iter() {
            if let Err(e) = endpoint.send_tag(tag, data) {
                eprintln!("向端点 {} 发送消息失败: {}", endpoint_id, e);
                errors += 1;
                self.stats.record_error();
            } else {
                self.stats.record_send(data.len());
            }
        }
        
        if errors > 0 {
            Err(anyhow!("广播失败，{} 个端点发送错误", errors))
        } else {
            Ok(())
        }
    }
    
    /// 获取所有连接的端点ID
    pub fn get_endpoint_ids(&self) -> Vec<String> {
        let endpoints = self.endpoints.lock().unwrap();
        endpoints.keys().cloned().collect()
    }
    
    /// 检查端点是否连接
    pub fn is_connected(&self, endpoint_id: &str) -> bool {
        let endpoints = self.endpoints.lock().unwrap();
        endpoints.contains_key(endpoint_id)
    }
    
    /// 清理所有连接
    pub fn cleanup_all(&self) {
        let mut endpoints = self.endpoints.lock().unwrap();
        endpoints.clear();
    }
    
    /// 运行进度循环
    pub fn run_progress_loop(&self, duration: Option<Duration>) -> u64 {
        let start = Instant::now();
        let mut total_progress = 0u64;
        
        loop {
            total_progress += self.progress() as u64;
            
            if let Some(max_duration) = duration {
                if start.elapsed() >= max_duration {
                    break;
                }
            }
            
            std::thread::sleep(Duration::from_micros(self.config.progress_interval_us));
        }
        
        total_progress
    }
}

impl Drop for UcxWorker {
    fn drop(&mut self) {
        // 清理所有连接
        self.cleanup_all();
        
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
    remote_addr: SocketAddr,
}

impl UcxEndpoint {
    /// 连接到远程地址
    pub fn connect(worker: &UcxWorker, addr: SocketAddr) -> Result<Self> {
        unsafe {
            let mut ep_params: ucp_ep_params_t = std::mem::zeroed();
            
            // 设置远程地址
            let mut sockaddr: libc::sockaddr_in = std::mem::zeroed();
            sockaddr.sin_family = libc::AF_INET as u16;
            sockaddr.sin_port = addr.port().to_be();
            
            match addr.ip() {
                IpAddr::V4(ipv4) => {
                    sockaddr.sin_addr.s_addr = u32::from_be_bytes(ipv4.octets()).to_be();
                }
                IpAddr::V6(_) => {
                    return Err(anyhow!("暂不支持 IPv6"));
                }
            }
            
            ep_params.field_mask = ucp_ep_params_field_UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
            ep_params.address = &sockaddr as *const _ as *const ucs_sock_addr_t;
            
            let mut endpoint: ucp_ep_h = ptr::null_mut();
            let status = ucp_ep_create(worker.handle(), &ep_params, &mut endpoint);
            
            check_ucx_status(status, "创建端点")?;
            
            Ok(UcxEndpoint {
                endpoint,
                remote_addr: addr,
            })
        }
    }
    
    /// 获取远程地址
    pub fn remote_address(&self) -> SocketAddr {
        self.remote_addr
    }
    
    /// 发送标签消息
    pub fn send_tag(&self, tag: u64, data: &[u8]) -> Result<()> {
        unsafe {
            let mut params: ucp_request_param_t = std::mem::zeroed();
            params.op_attr_mask = ucp_op_attr_t_UCP_OP_ATTR_FIELD_DATATYPE;
            params.datatype = ucp_dt_make_contig(1);
            
            let request = ucp_tag_send_nbx(
                self.endpoint,
                data.as_ptr() as *const c_void,
                data.len(),
                tag,
                &params,
            );
            
            self.handle_request(request, "发送标签消息")?;
            Ok(())
        }
    }
    
    /// 接收标签消息
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
                0, // tag_mask
                &params,
            );
            
            self.handle_receive_request(request)
        }
    }
    
    /// 非阻塞接收标签消息
    pub fn try_receive_tag(&self, tag: u64, buffer: &mut [u8]) -> Result<usize> {
        // 这里简化实现，实际应该检查消息是否可用
        match self.receive_tag(tag, buffer) {
            Ok(bytes) => Ok(bytes),
            Err(_) => Ok(0), // 没有消息可用
        }
    }
    
    /// 处理发送/接收请求
    unsafe fn handle_request(&self, request: *mut c_void, operation: &str) -> Result<()> {
        if UCS_PTR_IS_ERR(request) {
            let status = UCS_PTR_STATUS(request);
            check_ucx_status(status, operation)?;
        } else if !request.is_null() {
            // 等待请求完成
            let mut status = ucs_status_t_UCS_INPROGRESS;
            while status == ucs_status_t_UCS_INPROGRESS {
                status = ucp_request_check_status(request);
            }
            ucp_request_free(request);
            check_ucx_status(status, operation)?;
        }
        Ok(())
    }
    
    /// 处理接收请求
    unsafe fn handle_receive_request(&self, request: *mut c_void) -> Result<usize> {
        if UCS_PTR_IS_ERR(request) {
            let status = UCS_PTR_STATUS(request);
            check_ucx_status(status, "接收标签消息")?;
            return Ok(0);
        }
        
        if request.is_null() {
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

impl Drop for UcxEndpoint {
    fn drop(&mut self) {
        if !self.endpoint.is_null() {
            unsafe {
                let mut params: ucp_ep_close_params_t = std::mem::zeroed();
                params.mode = ucp_ep_close_mode_UCP_EP_CLOSE_MODE_FORCE;
                ucp_ep_close_nbx(self.endpoint, &params);
            }
        }
    }
}

/// UCX 监听器封装
pub struct UcxListener {
    listener: ucp_listener_h,
    bind_addr: SocketAddr,
}

impl UcxListener {
    /// 创建新的监听器
    pub fn new(worker: &UcxWorker, bind_addr: SocketAddr) -> Result<Self> {
        unsafe {
            let mut params: ucp_listener_params_t = std::mem::zeroed();
            
            // 设置监听地址
            let mut sockaddr: libc::sockaddr_in = std::mem::zeroed();
            sockaddr.sin_family = libc::AF_INET as u16;
            sockaddr.sin_port = bind_addr.port().to_be();
            
            match bind_addr.ip() {
                IpAddr::V4(ipv4) => {
                    sockaddr.sin_addr.s_addr = u32::from_be_bytes(ipv4.octets()).to_be();
                }
                IpAddr::V6(_) => {
                    return Err(anyhow!("暂不支持 IPv6"));
                }
            }
            
            params.field_mask = ucp_listener_params_field_UCP_LISTENER_PARAM_FIELD_SOCK_ADDR;
            params.sockaddr.addr = &sockaddr as *const _ as *const ucs_sock_addr_t;
            params.sockaddr.addrlen = std::mem::size_of::<libc::sockaddr_in>();
            
            let mut listener: ucp_listener_h = ptr::null_mut();
            let status = ucp_listener_create(worker.handle(), &params, &mut listener);
            
            check_ucx_status(status, "创建监听器")?;
            
            Ok(UcxListener {
                listener,
                bind_addr,
            })
        }
    }
    
    /// 获取绑定地址
    pub fn bind_address(&self) -> SocketAddr {
        self.bind_addr
    }
}

impl Drop for UcxListener {
    fn drop(&mut self) {
        if !self.listener.is_null() {
            unsafe {
                ucp_listener_destroy(self.listener);
            }
        }
    }
}

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

/// 辅助函数
unsafe fn UCS_PTR_IS_ERR(ptr: *mut c_void) -> bool {
    (ptr as uintptr_t) >= (-(ucs_status_t_UCS_ERR_LAST as isize) as uintptr_t)
}

unsafe fn UCS_PTR_STATUS(ptr: *mut c_void) -> ucs_status_t {
    ptr as ucs_status_t
}

type uintptr_t = usize;
