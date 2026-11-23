// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::StagedPayload;
use bytes::Bytes;
use compio_buf::{BufResult, IntoInner, IoBuf};
use compio_driver::{
    op::{OpCodeFlag, ReadAt, WriteAt},
    DriverType, Proactor,
};
use compio_runtime::Runtime;
use futures::{stream::FuturesUnordered, StreamExt};
use std::fs::File;
use std::future::Future;
use std::io::{self, ErrorKind};
use std::sync::{mpsc, Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{oneshot, Notify};

// Each permit pays for up to CHUNK_BYTES, including buffers in the queue and
// in the kernel. Acquire before allocating; split larger logical operations.
const MAX_OPERATIONS: usize = 64;
pub(crate) const CHUNK_BYTES: usize = 1024 * 1024;

enum Transfer {
    Read(Vec<u8>),
    Write(Bytes),
}

#[cfg(feature = "test-util")]
pub(crate) static SHARED_WRITE_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

struct Admission {
    available: Mutex<usize>,
    ready: Condvar,
    async_ready: Notify,
}

impl Admission {
    fn new() -> Self {
        Self {
            available: Mutex::new(MAX_OPERATIONS),
            ready: Condvar::new(),
            async_ready: Notify::new(),
        }
    }

    fn acquire(self: &Arc<Self>) -> Permit {
        let mut available = self
            .ready
            .wait_while(self.available.lock().unwrap(), |n| *n == 0)
            .unwrap();
        *available -= 1;
        Permit(self.clone())
    }

    fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        let mut available = self.available.lock().unwrap();
        *available = available.checked_sub(1)?;
        Some(Permit(self.clone()))
    }

    async fn acquire_async(self: &Arc<Self>) -> Permit {
        if let Some(permit) = self.try_acquire() {
            return permit;
        }
        loop {
            // Register before rechecking capacity so releases cannot be lost
            // between a full queue and going to sleep. A dropped notification
            // waiter passes an assigned wakeup to the next waiter.
            let ready = self.async_ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if let Some(permit) = self.try_acquire() {
                return permit;
            }
            ready.await;
        }
    }
}

struct Permit(Arc<Admission>);

impl Drop for Permit {
    fn drop(&mut self) {
        *self.0.available.lock().unwrap() += 1;
        self.0.ready.notify_one();
        self.0.async_ready.notify_one();
    }
}

type Completed = io::Result<(Bytes, Permit)>;

enum Reply {
    Blocking(mpsc::SyncSender<Completed>),
    Async(oneshot::Sender<Completed>),
}

struct Request {
    file: Arc<File>,
    offset: u64,
    op: Transfer,
    reply: Reply,
    permit: Permit,
}

type CompletionObserver = Box<dyn Fn(usize, usize) + Send + Sync>;

struct Executor {
    requests: Option<Sender<Request>>,
    thread: Option<JoinHandle<()>>,
    admission: Arc<Admission>,
    completed: Arc<OnceLock<CompletionObserver>>,
}

impl Drop for Executor {
    fn drop(&mut self) {
        // Closing the only sender makes serve drain all accepted requests.
        drop(self.requests.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Shared Linux executor for buffered, positional worker file I/O.
#[derive(Clone)]
pub struct UringExecutor(Arc<Executor>);

impl UringExecutor {
    pub fn start() -> io::Result<Self> {
        Self::start_with(|| {
            let mut driver = Proactor::builder();
            driver
                .capacity(MAX_OPERATIONS as u32)
                .driver_type(DriverType::IoUring)
                .detect_opcode_support(OpCodeFlag::Read | OpCodeFlag::Write | OpCodeFlag::PollAdd);
            let runtime = Runtime::builder()
                .with_proactor(driver)
                .build()
                .map_err(explain_probe_error)?;
            if runtime.driver_type() != DriverType::IoUring {
                return Err(io::Error::new(ErrorKind::Unsupported, "expected io_uring"));
            }
            // Verify execution as well as advertised opcodes; a sandbox can
            // reject submission even when ring creation and the probe succeed.
            let file = Arc::new(tempfile::tempfile()?);
            runtime.block_on(async {
                let probe = Bytes::from_static(&[0x79, 0x91]);
                transfer(file.clone(), 3, Transfer::Write(probe.clone())).await?;
                if transfer(file, 3, Transfer::Read(vec![0; 2])).await? != probe {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "io_uring probe readback mismatch",
                    ));
                }
                Ok::<(), io::Error>(())
            })?;
            Ok(runtime)
        })
    }

    fn start_with(init: impl FnOnce() -> io::Result<Runtime> + Send + 'static) -> io::Result<Self> {
        let (requests, receiver) = channel(MAX_OPERATIONS);
        let (ready, started) = mpsc::sync_channel(1);
        let completed = Arc::new(OnceLock::<CompletionObserver>::new());
        let observe = completed.clone();
        let thread = thread::Builder::new()
            .name("worker-io-uring".into())
            .spawn(move || {
                let runtime = match init() {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let _ = ready.send(Err(err));
                        return;
                    }
                };
                if ready.send(Ok(())).is_ok() {
                    runtime.block_on(serve(receiver, |file, offset, op| {
                        let observe = &observe;
                        async move {
                            let counts = match &op {
                                Transfer::Read(buf) => (buf.len(), 0),
                                Transfer::Write(buf) => (0, buf.len()),
                            };
                            let data = transfer(file, offset, op).await?;
                            if let Some(observe) = observe.get() {
                                observe(counts.0, counts.1);
                            }
                            Ok(data)
                        }
                    }));
                }
            })?;
        let result = started
            .recv()
            .map_err(|_| stopped())
            .and_then(|result| result);
        if let Err(err) = result {
            let _ = thread.join();
            return Err(err);
        }
        Ok(Self(Arc::new(Executor {
            requests: Some(requests),
            thread: Some(thread),
            admission: Arc::new(Admission::new()),
            completed,
        })))
    }

    pub fn observe_completed_bytes(&self, observer: impl Fn(usize, usize) + Send + Sync + 'static) {
        let _ = self.0.completed.set(Box::new(observer));
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn read_exact_at(
        &self,
        file: &Arc<File>,
        offset: u64,
        dst: &mut [u8],
    ) -> io::Result<()> {
        check_range(offset, dst.len())?;
        for (index, chunk) in dst.chunks_mut(CHUNK_BYTES).enumerate() {
            let offset = offset + (index * CHUNK_BYTES) as u64;
            let (buffer, _permit) = self
                .submit(file, offset, || Transfer::Read(vec![0; chunk.len()]))?
                .recv()
                .map_err(|_| stopped())??;
            chunk.copy_from_slice(&buffer);
        }
        Ok(())
    }

    /// Admit and read one chunk without occupying a blocking-pool thread.
    /// Dropping the future only drops the reply receiver, not accepted I/O.
    pub(crate) async fn read_owned_at(
        &self,
        file: Arc<File>,
        offset: u64,
        len: usize,
    ) -> io::Result<Bytes> {
        check_range(offset, len)?;
        if !(1..=CHUNK_BYTES).contains(&len) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "owned io_uring read must fit one non-empty chunk",
            ));
        }
        let permit = self.0.admission.acquire_async().await;
        let (reply, completed) = oneshot::channel();
        self.enqueue(Request {
            file,
            offset,
            op: Transfer::Read(vec![0; len]),
            reply: Reply::Async(reply),
            permit,
        })?;
        let (buffer, _permit) = completed.await.map_err(|_| stopped())??;
        Ok(buffer)
    }

    pub(crate) fn write_all_at(
        &self,
        file: &Arc<File>,
        offset: u64,
        src: StagedPayload<'_>,
    ) -> io::Result<()> {
        let data = src.as_ref();
        check_range(offset, data.len())?;
        if let StagedPayload::Shared(bytes) = src {
            if (1..=CHUNK_BYTES).contains(&bytes.len()) {
                self.submit(file, offset, || Transfer::Write(bytes.clone()))?
                    .recv()
                    .map_err(|_| stopped())??;
                #[cfg(feature = "test-util")]
                SHARED_WRITE_BYTES
                    .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            }
        }
        // Do not pin an oversized frame in the executor on a single chunk's
        // permit. Copy only the admitted chunk, as for borrowed inputs.
        for (index, chunk) in data.chunks(CHUNK_BYTES).enumerate() {
            let offset = offset + (index * CHUNK_BYTES) as u64;
            self.submit(file, offset, || {
                Transfer::Write(Bytes::copy_from_slice(chunk))
            })?
            .recv()
            .map_err(|_| stopped())??;
        }
        Ok(())
    }

    fn submit(
        &self,
        file: &Arc<File>,
        offset: u64,
        op: impl FnOnce() -> Transfer,
    ) -> io::Result<mpsc::Receiver<Completed>> {
        let permit = self.0.admission.acquire();
        let op = op();
        let (reply, completed) = mpsc::sync_channel(1);
        self.enqueue(Request {
            file: file.clone(),
            offset,
            op,
            reply: Reply::Blocking(reply),
            permit,
        })?;
        Ok(completed)
    }

    fn enqueue(&self, request: Request) -> io::Result<()> {
        // The shared admission cap is also the queue capacity, so an admitted
        // request has space even when blocking and async callers are mixed.
        self.0
            .requests
            .as_ref()
            .unwrap()
            .try_send(request)
            .map_err(|_| stopped())
    }
}

fn explain_probe_error(error: io::Error) -> io::Error {
    if error.kind() != ErrorKind::Unsupported {
        return error;
    }
    // Compio reports an unsupported opcode even if the ring used to probe
    // capabilities could not be set up or queried. Recover the OS error
    // without adding another probe to the successful startup path.
    let probe = io_uring::IoUring::new(2)
        .and_then(|ring| ring.submitter().register_probe(&mut io_uring::Probe::new()));
    probe.err().unwrap_or(error)
}

fn stopped() -> io::Error {
    io::Error::new(ErrorKind::BrokenPipe, "worker io_uring executor stopped")
}

fn check_range(offset: u64, len: usize) -> io::Result<()> {
    if offset
        .checked_add(len as u64)
        .is_none_or(|end| end > i64::MAX as u64)
    {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "io_uring file range overflow",
        ));
    }
    Ok(())
}

async fn serve<F: Future<Output = io::Result<Bytes>>>(
    mut receiver: Receiver<Request>,
    run: impl Fn(Arc<File>, u64, Transfer) -> F,
) {
    let mut active = FuturesUnordered::new();
    loop {
        tokio::select! {
            req = receiver.recv() => {
                let Some(req) = req else { break };
                let Request { file, offset, op, reply, permit } = req;
                let io = run(file, offset, op);
                active.push(async move {
                    // Keep driving the I/O even if the caller dropped its reply receiver.
                    let result = io.await;
                    let result = result.map(|buffer| (buffer, permit));
                    match reply {
                        Reply::Blocking(reply) => { let _ = reply.send(result); }
                        Reply::Async(reply) => { let _ = reply.send(result); }
                    }
                });
            }
            _ = active.next(), if !active.is_empty() => {}
        }
    }
    while active.next().await.is_some() {}
}

async fn transfer(file: Arc<File>, offset: u64, op: Transfer) -> io::Result<Bytes> {
    match op {
        Transfer::Read(buf) => {
            complete_exact(offset, ErrorKind::UnexpectedEof, buf, |off, done, buf| {
                let file = file.clone();
                async move {
                    let len = buf.len();
                    let BufResult(result, op) =
                        compio_runtime::submit(ReadAt::new(file, off, buf.slice(done..len))).await;
                    (result, op.into_inner().into_inner())
                }
            })
            .await
            .map(Bytes::from)
        }
        Transfer::Write(buf) => {
            complete_exact(offset, ErrorKind::WriteZero, buf, |off, done, buf| {
                let file = file.clone();
                async move {
                    let slice = IoBuf::slice(buf, done..);
                    let BufResult(result, op) =
                        compio_runtime::submit(WriteAt::new(file, off, slice)).await;
                    (result, op.into_inner().into_inner())
                }
            })
            .await
        }
    }
}

async fn complete_exact<B: AsRef<[u8]>, F: Future<Output = (io::Result<usize>, B)>>(
    offset: u64,
    zero_progress: ErrorKind,
    mut buffer: B,
    mut submit: impl FnMut(u64, usize, B) -> F,
) -> io::Result<B> {
    let len = buffer.as_ref().len();
    let mut done = 0;
    while done < len {
        let (res, returned) = submit(offset + done as u64, done, buffer).await;
        buffer = returned;
        match res {
            Ok(0) => return Err(zero_progress.into()),
            Ok(n) if n <= len - done => done += n,
            Ok(_) => {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "invalid io_uring completion length",
                ))
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn resumes_only_confirmed_short_write() {
        let mut seen = Vec::new();
        let err = block_on(complete_exact(
            11,
            ErrorKind::WriteZero,
            Bytes::from_static(&[7; 8]),
            |offset, done, buf| {
                seen.push((offset, done));
                std::future::ready((
                    if done == 0 {
                        Ok(3)
                    } else {
                        Err(io::Error::other("injected write error"))
                    },
                    buf,
                ))
            },
        ))
        .unwrap_err();
        assert_eq!(seen, [(11, 0), (14, 3)]);
        assert_eq!(err.to_string(), "injected write error");
    }

    #[test]
    fn zero_completion_is_not_success() {
        for expected in [ErrorKind::UnexpectedEof, ErrorKind::WriteZero] {
            let err = block_on(complete_exact(0, expected, vec![0; 8], |_, _, buf| {
                std::future::ready((Ok(0), buf))
            }))
            .unwrap_err();
            assert_eq!(err.kind(), expected);
        }
    }

    #[test]
    fn shared_writes_reuse_one_frame_but_do_not_pin_oversized_frames() {
        for size in [CHUNK_BYTES, CHUNK_BYTES + 3] {
            let (tx, mut rx) = channel(MAX_OPERATIONS);
            let executor = UringExecutor(Arc::new(Executor {
                requests: Some(tx),
                thread: None,
                admission: Arc::new(Admission::new()),
                completed: Arc::default(),
            }));
            let payload = Bytes::from(vec![7; size]);
            let file = Arc::new(tempfile::tempfile().unwrap());
            thread::scope(|scope| {
                let writing = scope
                    .spawn(|| executor.write_all_at(&file, 17, StagedPayload::Shared(&payload)));
                for (i, expected) in payload.chunks(CHUNK_BYTES).enumerate() {
                    let req = rx.blocking_recv().unwrap();
                    let Transfer::Write(bytes) = req.op else {
                        panic!("expected write")
                    };
                    assert_eq!(req.offset, 17 + (i * CHUNK_BYTES) as u64);
                    assert_eq!(bytes.as_ref(), expected);
                    assert_eq!(bytes.as_ptr() == expected.as_ptr(), size <= CHUNK_BYTES);
                    let Reply::Blocking(reply) = req.reply else {
                        panic!("expected blocking writer")
                    };
                    assert!(reply.send(Ok((bytes, req.permit))).is_ok());
                }
                writing.join().unwrap().unwrap();
            });
        }
    }

    #[test]
    fn admission_waits_for_a_free_slot() {
        let admission = Arc::new(Admission::new());
        let mut permits: Vec<_> = (0..MAX_OPERATIONS).map(|_| admission.acquire()).collect();
        let (tx, rx) = mpsc::channel();
        let thread = thread::spawn({
            let admission = admission.clone();
            move || tx.send(admission.acquire()).unwrap()
        });
        assert!(matches!(
            rx.recv_timeout(std::time::Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(permits.pop());
        let permit = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        assert_eq!(*admission.available.lock().unwrap(), 0);
        thread.join().unwrap();
        drop(permit);
        drop(permits);
        assert_eq!(*admission.available.lock().unwrap(), MAX_OPERATIONS);
    }

    #[test]
    fn async_admission_recovers_a_cancelled_wakeup() {
        block_on(async {
            let admission = Arc::new(Admission::new());
            let mut permits: Vec<_> = (0..MAX_OPERATIONS).map(|_| admission.acquire()).collect();
            let mut cancelled = Box::pin(admission.acquire_async());
            let mut next = Box::pin(admission.acquire_async());
            assert!(futures::poll!(&mut cancelled).is_pending());
            assert!(futures::poll!(&mut next).is_pending());

            drop(permits.pop());
            drop(cancelled);
            let std::task::Poll::Ready(permit) = futures::poll!(&mut next) else {
                panic!("cancelled waiter swallowed the wakeup");
            };
            assert_eq!(*admission.available.lock().unwrap(), 0);
            drop(permit);
            drop(permits);
            assert_eq!(*admission.available.lock().unwrap(), MAX_OPERATIONS);
        });
    }

    #[test]
    fn dropped_owned_read_keeps_fd_and_admission_until_terminal() {
        block_on(async {
            let (requests, rx) = channel(MAX_OPERATIONS);
            let admission = Arc::new(Admission::new());
            let executor = UringExecutor(Arc::new(Executor {
                requests: Some(requests),
                thread: None,
                admission: admission.clone(),
                completed: Arc::default(),
            }));
            let file = Arc::new(tempfile::tempfile().unwrap());
            let weak_file = Arc::downgrade(&file);
            let mut read = Box::pin(executor.read_owned_at(file, 17, 2));
            assert!(futures::poll!(&mut read).is_pending());

            let (finish, finished) = oneshot::channel();
            let finished = std::cell::Cell::new(Some(finished));
            let mut drain = Box::pin(serve(rx, |file, offset, op| {
                let Transfer::Read(buffer) = op else {
                    panic!("expected read")
                };
                assert_eq!(offset, 17);
                let finished = finished.take().unwrap();
                async move {
                    finished.await.unwrap();
                    assert_eq!(buffer.len(), 2);
                    drop(file);
                    Ok(Bytes::from(buffer))
                }
            }));
            assert!(futures::poll!(&mut drain).is_pending());
            assert!(
                finished.take().is_none(),
                "request was not accepted by serve"
            );
            drop(read);
            drop(executor);
            assert!(weak_file.upgrade().is_some());
            assert_eq!(*admission.available.lock().unwrap(), MAX_OPERATIONS - 1);

            finish.send(()).unwrap();
            drain.await;
            assert!(weak_file.upgrade().is_none());
            assert_eq!(*admission.available.lock().unwrap(), MAX_OPERATIONS);
        });
    }

    #[test]
    fn shutdown_drains_a_disconnected_request() {
        let (tx, rx) = channel(MAX_OPERATIONS);
        let (accepted, started) = mpsc::sync_channel(1);
        let (finish, finished) = futures::channel::oneshot::channel();
        let finished = Mutex::new(Some(finished));
        let (reply, caller) = mpsc::sync_channel(1);
        let admission = Arc::new(Admission::new());
        let file = Arc::new(tempfile::tempfile().unwrap());
        let weak_file = Arc::downgrade(&file);
        assert!(tx
            .try_send(Request {
                file,
                offset: 0,
                op: Transfer::Read(vec![0; 2]),
                reply: Reply::Blocking(reply),
                permit: admission.acquire()
            })
            .is_ok());
        let thread = thread::spawn(move || {
            block_on(serve(rx, |file, _, op| {
                let Transfer::Read(buffer) = op else {
                    panic!("expected read")
                };
                let finished = finished.lock().unwrap().take().unwrap();
                let accepted = accepted.clone();
                async move {
                    accepted.send(()).unwrap();
                    finished.await.unwrap();
                    drop(file);
                    Ok(Bytes::from(buffer))
                }
            }))
        });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        drop(caller);
        drop(tx);
        assert!(weak_file.upgrade().is_some());
        assert_eq!(*admission.available.lock().unwrap(), MAX_OPERATIONS - 1);
        finish.send(()).unwrap();
        thread.join().unwrap();
        assert!(weak_file.upgrade().is_none());
        assert_eq!(*admission.available.lock().unwrap(), MAX_OPERATIONS);
    }

    #[test]
    #[ignore = "requires a Linux host that can execute io_uring; fallback must not pass"]
    fn real_io_uring_roundtrip_concurrent_files() -> io::Result<()> {
        let executor = UringExecutor::start()?;
        thread::scope(|scope| {
            let joins: Vec<_> = (0..8)
                .map(|seed| {
                    scope.spawn({
                        let executor = &executor;
                        move || -> io::Result<()> {
                            let data: Vec<_> = (0..CHUNK_BYTES + 31)
                                .map(|i| (i.wrapping_mul(13) ^ seed) as u8)
                                .collect();
                            let file = Arc::new(tempfile::tempfile()?);
                            let data = Bytes::from(data);
                            executor.write_all_at(&file, 17, StagedPayload::Shared(&data))?;
                            let mut out = vec![0; data.len()];
                            executor.read_exact_at(&file, 17, &mut out)?;
                            assert_eq!(out, data);
                            let owned = block_on(executor.read_owned_at(file, 20, CHUNK_BYTES))?;
                            assert_eq!(&owned[..], &data[3..3 + CHUNK_BYTES]);
                            Ok(())
                        }
                    })
                })
                .collect();
            for join in joins {
                join.join().unwrap()?;
            }
            Ok(())
        })
    }
}
