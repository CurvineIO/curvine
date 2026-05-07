use std::sync::Arc;

pub struct SpdkPoller {
    _p: std::marker::PhantomData<*const ()>,
}
impl SpdkPoller {
    pub fn start() -> Self {
        Self {
            _p: std::marker::PhantomData,
        }
    }
    pub fn sender(&self) -> crossbeam::channel::Sender<IoRequest> {
        crossbeam::channel::unbounded().0
    }
    pub fn stop(&mut self) {}
}
impl Drop for SpdkPoller {
    fn drop(&mut self) {
        self.stop()
    }
}

#[derive(Clone)]
pub struct IoRequest {
    pub op: IoOp,
    pub completion: Arc<IoCompletion>,
    pub bdev_inflight: Arc<std::sync::atomic::AtomicUsize>,
}
#[derive(Clone)]
pub enum IoOp {
    Read {
        ns: *mut std::ffi::c_void,
        qpair: *mut std::ffi::c_void,
        buf: *mut std::ffi::c_void,
        offset: u64,
        num_bytes: u64,
    },
    Write {
        ns: *mut std::ffi::c_void,
        qpair: *mut std::ffi::c_void,
        buf: *mut std::ffi::c_void,
        offset: u64,
        num_bytes: u64,
    },
    Flush {
        ns: *mut std::ffi::c_void,
        qpair: *mut std::ffi::c_void,
    },
}
#[allow(dead_code)]
pub struct IoCompletion(std::sync::Mutex<()>);
impl IoCompletion {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(std::sync::Mutex::new(())))
    }
    pub fn complete(&self, _: i32) {}
    pub fn wait(&self, _: u64) -> i32 {
        0
    }
}
