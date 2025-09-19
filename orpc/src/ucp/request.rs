use std::os::raw::c_void;
use futures::task::AtomicWaker;

#[derive(Default)]
pub struct Request {
    waker: AtomicWaker
}

impl Request {
    pub unsafe extern "C" fn init<'a>(request: *mut c_void) {
        (request as *mut Self).write(Request::default());
    }

    pub unsafe extern "C" fn cleanup(request: *mut c_void) {
        std::ptr::drop_in_place(request as *mut Self)
    }
}