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

use crate::staged_write::WriteReceipt;
#[cfg(feature = "write-bench-profile")]
use crate::write_bench_profile::{self, QueuedWrite, RingReply};
use crate::StagedPayload;
use bytes::Bytes;
use compio_buf::{BufResult, IntoInner, IoBuf, SetLen};
use compio_driver::{
    op::{OpCodeFlag, ReadAt, WriteAt},
    DriverType, Proactor,
};
use compio_runtime::Runtime;
use futures::{stream::FuturesUnordered, StreamExt};
use std::fs::File;
use std::future::Future;
use std::io::{self, ErrorKind};
use std::sync::{mpsc, Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::{self, JoinHandle};
#[cfg(feature = "write-bench-profile")]
use std::time::Instant;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{oneshot, Notify};

// Each admission unit pays for CHUNK_BYTES in the queue or kernel.
// Larger shared frames reserve multiple units before submission.
const MAX_OPERATIONS: usize = 64;
const MAX_RINGS: usize = 2;
// Retain at most 16 MiB after responses have been released.
const IDLE_READ_BUFFERS: usize = 16;
pub(crate) const CHUNK_BYTES: usize = 1024 * 1024;

// Share at most four MiB of an incoming frame in one positional write.
const MAX_SHARED_WRITE_BYTES: usize = 4 * CHUNK_BYTES;

type ReadBuffers = Mutex<Vec<Vec<u8>>>;

// Bytes holds this owner until every response slice has been released.
struct RecycledRead {
    buffer: Vec<u8>,
    available: Weak<ReadBuffers>,
}

impl AsRef<[u8]> for RecycledRead {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

impl Drop for RecycledRead {
    fn drop(&mut self) {
        if let Some(available) = self.available.upgrade() {
            let mut cached = available.lock().unwrap();
            if cached.len() < IDLE_READ_BUFFERS {
                self.buffer.clear();
                cached.push(std::mem::take(&mut self.buffer));
            }
        }
    }
}

enum Transfer {
    Read {
        buffer: Vec<u8>,
        len: usize,
        recycle: Option<Weak<ReadBuffers>>,
    },
    Write(Bytes),
}

impl Transfer {
    fn read(len: usize) -> Self {
        Self::Read {
            buffer: Vec::with_capacity(len),
            len,
            recycle: None,
        }
    }
}

#[cfg(feature = "test-util")]
pub(crate) static SHARED_WRITE_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

struct Admission {
    in_flight: Mutex<[usize; MAX_RINGS]>,
    ring_count: usize,
    ready: Condvar,
    async_ready: Notify,
}

impl Admission {
    fn new(ring_count: usize) -> Self {
        Self {
            in_flight: Mutex::new([0; MAX_RINGS]),
            ring_count,
            ready: Condvar::new(),
            async_ready: Notify::new(),
        }
    }

    fn acquire(self: &Arc<Self>) -> Permit {
        let mut in_flight = self
            .ready
            .wait_while(self.in_flight.lock().unwrap(), |counts| {
                !Self::has_capacity(counts)
            })
            .unwrap();
        self.reserve(&mut in_flight, 1)
    }

    fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        self.try_acquire_units(1)
    }

    fn try_acquire_units(self: &Arc<Self>, units: usize) -> Option<Permit> {
        let mut in_flight = self.in_flight.lock().unwrap();
        (units <= MAX_OPERATIONS - in_flight.iter().sum::<usize>())
            .then(|| self.reserve(&mut in_flight, units))
    }

    fn has_capacity(in_flight: &[usize; MAX_RINGS]) -> bool {
        in_flight[0] + in_flight[1] < MAX_OPERATIONS
    }

    fn reserve(self: &Arc<Self>, in_flight: &mut [usize; MAX_RINGS], units: usize) -> Permit {
        // Prefer the first ring on a tie so serial requests reuse its I/O worker.
        let ring = usize::from(self.ring_count == MAX_RINGS && in_flight[0] > in_flight[1]);
        in_flight[ring] += units;
        Permit {
            admission: self.clone(),
            ring,
            units,
        }
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

struct Permit {
    units: usize,
    admission: Arc<Admission>,
    ring: usize,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.admission.in_flight.lock().unwrap()[self.ring] -= self.units;
        if self.units > 1 {
            self.admission.ready.notify_all();
            self.admission.async_ready.notify_waiters();
            return;
        }
        self.admission.ready.notify_one();
        self.admission.async_ready.notify_one();
    }
}

struct Completion {
    buffer: Bytes,
    _permit: Permit,
    #[cfg(feature = "write-bench-profile")]
    timing: Option<RingReply>,
}

impl Completion {
    fn finish_shared_write(self) -> WriteReceipt {
        // Timestamp receipt before dropping the returned buffer and its permit.
        WriteReceipt {
            #[cfg(feature = "write-bench-profile")]
            ring: self.timing.map(|reply| (reply, Instant::now())),
        }
    }
}

type Completed = io::Result<Completion>;

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
    #[cfg(feature = "write-bench-profile")]
    timing: Option<QueuedWrite>,
}

type CompletionObserver = Box<dyn Fn(usize, usize) + Send + Sync>;

struct Ring {
    requests: Option<Sender<Request>>,
    thread: Option<JoinHandle<()>>,
}

impl Ring {
    fn start(completed: Arc<OnceLock<CompletionObserver>>) -> io::Result<Self> {
        let (requests, receiver) = channel(MAX_OPERATIONS);
        let (ready, started) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("worker-io-uring".into())
            .spawn(move || match start_runtime() {
                Ok(runtime) => {
                    if ready.send(Ok(())).is_ok() {
                        runtime.block_on(serve(receiver, completed, transfer));
                    }
                }
                Err(err) => {
                    let _ = ready.send(Err(err));
                }
            })?;
        let ring = Self {
            requests: Some(requests),
            thread: Some(thread),
        };
        started.recv().map_err(|_| stopped())??;
        Ok(ring)
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        // Closing the sender lets serve drain all accepted requests.
        drop(self.requests.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct Executor {
    rings: Vec<Ring>,
    admission: Arc<Admission>,
    completed: Arc<OnceLock<CompletionObserver>>,
    read_buffers: Arc<ReadBuffers>,
}

/// Shared Linux executor for buffered, positional worker file I/O.
#[derive(Clone)]
pub struct UringExecutor(Arc<Executor>);

impl UringExecutor {
    pub fn start() -> io::Result<Self> {
        let completed = Arc::default();
        let mut rings = vec![Ring::start(Arc::clone(&completed))?];
        // Buffered writes on ext4 can share a single runnable io-wq worker
        // per issuer. A second issuer lets independent writes make progress
        // in parallel while keeping one admission limit for the worker.
        if thread::available_parallelism().is_ok_and(|cpus| cpus.get() > 1) {
            match Ring::start(Arc::clone(&completed)) {
                Ok(ring) => rings.push(ring),
                Err(error) => log::warn!("could not start second worker io_uring ring: {error}"),
            }
        }
        Ok(Self(Arc::new(Executor {
            admission: Arc::new(Admission::new(rings.len())),
            rings,
            completed,
            read_buffers: Arc::default(),
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
            let completed = self
                .submit(
                    file,
                    offset,
                    None,
                    #[cfg(feature = "write-bench-profile")]
                    None,
                    || self.read_transfer(chunk.len()),
                )?
                .recv()
                .map_err(|_| stopped())??;
            chunk.copy_from_slice(&completed.buffer);
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
            op: self.read_transfer(len),
            reply: Reply::Async(reply),
            permit,
            #[cfg(feature = "write-bench-profile")]
            timing: None,
        })?;
        let completed = completed.await.map_err(|_| stopped())??;
        Ok(completed.buffer)
    }

    fn read_transfer(&self, len: usize) -> Transfer {
        if len != CHUNK_BYTES {
            return Transfer::read(len);
        }
        let cached = self.0.read_buffers.lock().unwrap().pop();
        Transfer::Read {
            buffer: cached.unwrap_or_else(|| Vec::with_capacity(len)),
            len,
            recycle: Some(Arc::downgrade(&self.0.read_buffers)),
        }
    }

    pub(crate) fn write_all_at(
        &self,
        file: &Arc<File>,
        offset: u64,
        src: StagedPayload<'_>,
    ) -> io::Result<WriteReceipt> {
        let data = src.as_ref();
        check_range(offset, data.len())?;
        if let StagedPayload::Shared(bytes) = src {
            // Larger requests reserve their entire byte charge atomically. Under
            // pressure the existing one-chunk path can still make progress.
            let prepaid = (bytes.len() > CHUNK_BYTES && bytes.len() <= MAX_SHARED_WRITE_BYTES)
                .then(|| {
                    self.0
                        .admission
                        .try_acquire_units(bytes.len().div_ceil(CHUNK_BYTES))
                })
                .flatten();
            if (1..=CHUNK_BYTES).contains(&bytes.len()) || prepaid.is_some() {
                let receipt = self
                    .submit(
                        file,
                        offset,
                        prepaid,
                        #[cfg(feature = "write-bench-profile")]
                        Some(bytes.len()),
                        || Transfer::Write(bytes.clone()),
                    )?
                    .recv()
                    .map_err(|_| stopped())??
                    .finish_shared_write();
                #[cfg(feature = "test-util")]
                SHARED_WRITE_BYTES
                    .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
                return Ok(receipt);
            }
        }
        // Do not pin an oversized frame in the executor on a single chunk's
        // permit. Copy only the admitted chunk, as for borrowed inputs.
        for (index, chunk) in data.chunks(CHUNK_BYTES).enumerate() {
            let offset = offset + (index * CHUNK_BYTES) as u64;
            self.submit(
                file,
                offset,
                None,
                #[cfg(feature = "write-bench-profile")]
                None,
                || Transfer::Write(Bytes::copy_from_slice(chunk)),
            )?
            .recv()
            .map_err(|_| stopped())??;
        }
        Ok(WriteReceipt::default())
    }

    fn submit(
        &self,
        file: &Arc<File>,
        offset: u64,
        prepaid: Option<Permit>,
        #[cfg(feature = "write-bench-profile")] shared_write_len: Option<usize>,
        op: impl FnOnce() -> Transfer,
    ) -> io::Result<mpsc::Receiver<Completed>> {
        #[cfg(feature = "write-bench-profile")]
        let admission_start = shared_write_len.and_then(write_bench_profile::start_write);
        let permit = prepaid.unwrap_or_else(|| self.0.admission.acquire());
        #[cfg(feature = "write-bench-profile")]
        let admission = admission_start.map(|start| start.elapsed());
        let op = op();
        let (reply, completed) = mpsc::sync_channel(1);
        self.enqueue(Request {
            file: file.clone(),
            offset,
            op,
            reply: Reply::Blocking(reply),
            permit,
            #[cfg(feature = "write-bench-profile")]
            timing: admission.map(|admission| QueuedWrite {
                admission,
                enqueued: Instant::now(),
            }),
        })?;
        Ok(completed)
    }

    fn enqueue(&self, request: Request) -> io::Result<()> {
        // The shared admission cap is also the queue capacity, so an admitted
        // request has space even when blocking and async callers are mixed.
        self.0.rings[request.permit.ring]
            .requests
            .as_ref()
            .unwrap()
            .try_send(request)
            .map_err(|_| stopped())
    }
}

fn build_runtime(cooperative: bool) -> io::Result<Runtime> {
    let mut driver = Proactor::builder();
    driver
        .capacity(MAX_OPERATIONS as u32)
        .driver_type(DriverType::IoUring)
        .detect_opcode_support(OpCodeFlag::Read | OpCodeFlag::Write | OpCodeFlag::PollAdd);
    if cooperative {
        // Each ring is driven by one thread. Let it run completion task work
        // as it returns to userspace, and expose pending work to Compio.
        driver
            .single_issuer(true)
            .coop_taskrun(true)
            .taskrun_flag(true);
    }
    Runtime::builder().with_proactor(driver).build()
}

fn start_runtime() -> io::Result<Runtime> {
    // Older kernels support READ/WRITE without supporting these setup flags.
    let runtime = build_runtime(true)
        .or_else(|error| match error.kind() {
            ErrorKind::InvalidInput | ErrorKind::Unsupported => build_runtime(false),
            _ => Err(error),
        })
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
        if transfer(file, 3, Transfer::read(2)).await? != probe {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "io_uring probe readback mismatch",
            ));
        }
        Ok::<(), io::Error>(())
    })?;
    Ok(runtime)
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
    observer: Arc<OnceLock<CompletionObserver>>,
    run: impl Fn(Arc<File>, u64, Transfer) -> F,
) {
    let mut active = FuturesUnordered::new();
    loop {
        tokio::select! {
            req = receiver.recv() => {
                let Some(req) = req else { break };
                let counts = match &req.op {
                    Transfer::Read { len, .. } => (*len, 0),
                    Transfer::Write(buf) => (0, buf.len()),
                };
                let io = run(req.file, req.offset, req.op);
                let observer = &observer;
                active.push(async move {
                    // FuturesUnordered can defer the first poll after dequeue.
                    #[cfg(feature = "write-bench-profile")]
                    let timing = req.timing.map(|queued| (queued, Instant::now()));
                    // Keep driving the I/O even if the caller dropped its reply receiver.
                    let result = io.await;
                    #[cfg(feature = "write-bench-profile")]
                    let timing = timing.map(|(queued, polled)| (queued, polled, Instant::now()));
                    let result = result.map(|buffer| {
                        // The byte observer is neither transfer time nor reply transit.
                        if let Some(observe) = observer.get() {
                            observe(counts.0, counts.1);
                        }
                        Completion {
                            buffer,
                            _permit: req.permit,
                            #[cfg(feature = "write-bench-profile")]
                            timing: timing.map(|(queued, polled, transferred)| RingReply {
                                queued,
                                polled,
                                transferred,
                                ready: Instant::now(),
                            }),
                        }
                    });
                    match req.reply {
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
        Transfer::Read {
            buffer,
            len,
            recycle,
        } => {
            complete_exact(
                offset,
                len,
                ErrorKind::UnexpectedEof,
                buffer,
                |off, done, buf| {
                    let file = file.clone();
                    async move {
                        let BufResult(result, op) =
                            compio_runtime::submit(ReadAt::new(file, off, buf.slice(done..len)))
                                .await;
                        let mut slice = op.into_inner();
                        match result {
                            Ok(count) if count <= len - done => {
                                // SAFETY: the CQE confirms initialization of this prefix.
                                unsafe { slice.advance_to(count) };
                            }
                            _ => {}
                        }
                        (result, slice.into_inner())
                    }
                },
            )
            .await
            .map(|buffer| match recycle {
                Some(available) => Bytes::from_owner(RecycledRead { buffer, available }),
                None => Bytes::from(buffer),
            })
        }
        Transfer::Write(buf) => {
            complete_exact(
                offset,
                buf.len(),
                ErrorKind::WriteZero,
                buf,
                |off, done, buf| {
                    let file = file.clone();
                    async move {
                        let slice = IoBuf::slice(buf, done..);
                        let BufResult(result, op) =
                            compio_runtime::submit(WriteAt::new(file, off, slice)).await;
                        (result, op.into_inner().into_inner())
                    }
                },
            )
            .await
        }
    }
}

async fn complete_exact<B, F: Future<Output = (io::Result<usize>, B)>>(
    offset: u64,
    len: usize,
    zero_progress: ErrorKind,
    mut buffer: B,
    mut submit: impl FnMut(u64, usize, B) -> F,
) -> io::Result<B> {
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
    fn pooled_buffer_is_recycled_after_all_slices_are_dropped() {
        let available = Arc::<ReadBuffers>::default();
        let buffer = vec![7; CHUNK_BYTES];
        let base = buffer.as_ptr();
        let capacity = buffer.capacity();
        let bytes = Bytes::from_owner(RecycledRead {
            buffer,
            available: Arc::downgrade(&available),
        });
        let slice = Bytes::slice(&bytes, ..64);
        drop(bytes);
        assert!(available.lock().unwrap().is_empty());
        assert_eq!(&slice[..], &[7; 64]);
        drop(slice);
        let buffer = available.lock().unwrap().pop().unwrap();
        assert_eq!(buffer.as_ptr(), base);
        assert_eq!(buffer.capacity(), capacity);
        assert!(buffer.is_empty());
    }

    #[test]
    fn resumes_only_confirmed_short_write() {
        let mut seen = Vec::new();
        let err = block_on(complete_exact(
            11,
            8,
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
            let err = block_on(complete_exact(0, 8, expected, vec![0; 8], |_, _, buf| {
                std::future::ready((Ok(0), buf))
            }))
            .unwrap_err();
            assert_eq!(err.kind(), expected);
        }
    }

    #[test]
    fn shared_writes_reuse_one_frame_but_do_not_pin_oversized_frames() {
        for size in [
            CHUNK_BYTES,
            CHUNK_BYTES + 3,
            MAX_SHARED_WRITE_BYTES,
            MAX_SHARED_WRITE_BYTES + 1,
        ] {
            let (tx, mut rx) = channel(MAX_OPERATIONS);
            let executor = UringExecutor(Arc::new(Executor {
                rings: vec![Ring {
                    requests: Some(tx),
                    thread: None,
                }],
                admission: Arc::new(Admission::new(1)),
                completed: Arc::default(),
                read_buffers: Arc::default(),
            }));
            let payload = Bytes::from(vec![7; size]);
            let file = Arc::new(tempfile::tempfile().unwrap());
            thread::scope(|scope| {
                let writing = scope
                    .spawn(|| executor.write_all_at(&file, 17, StagedPayload::Shared(&payload)));
                let step = if size <= MAX_SHARED_WRITE_BYTES {
                    size
                } else {
                    CHUNK_BYTES
                };
                for (i, expected) in payload.chunks(step).enumerate() {
                    let req = rx.blocking_recv().unwrap();
                    let Transfer::Write(bytes) = req.op else {
                        panic!("expected write")
                    };
                    assert_eq!(req.offset, 17 + (i * CHUNK_BYTES) as u64);
                    assert_eq!(bytes.as_ref(), expected);
                    assert_eq!(
                        bytes.as_ptr() == expected.as_ptr(),
                        size <= MAX_SHARED_WRITE_BYTES
                    );
                    assert_eq!(req.permit.units, expected.len().div_ceil(CHUNK_BYTES));
                    let Reply::Blocking(reply) = req.reply else {
                        panic!("expected blocking writer")
                    };
                    assert!(reply
                        .send(Ok(Completion {
                            buffer: bytes,
                            _permit: req.permit,
                            #[cfg(feature = "write-bench-profile")]
                            timing: None,
                        }))
                        .is_ok());
                }
                writing.join().unwrap().unwrap();
            });
        }
    }

    #[test]
    fn multi_chunk_admission_requires_all_units_at_once() {
        let admission = Arc::new(Admission::new(2));
        let mut held: Vec<_> = (0..MAX_OPERATIONS - 3)
            .map(|_| admission.acquire())
            .collect();
        assert!(admission.try_acquire_units(4).is_none());
        drop(held.pop());
        let large = admission.try_acquire_units(4).unwrap();
        assert!(admission.try_acquire().is_none());
        drop(large);
        assert!(admission.try_acquire_units(4).is_some());
    }

    #[test]
    fn admission_waits_for_a_free_slot() {
        let admission = Arc::new(Admission::new(1));
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
        assert_eq!(admission.in_flight.lock().unwrap()[0], MAX_OPERATIONS);
        thread.join().unwrap();
        drop(permit);
        drop(permits);
        assert_eq!(admission.in_flight.lock().unwrap()[0], 0);
    }

    #[test]
    fn two_rings_share_the_admission_limit_and_favor_the_idle_ring() {
        let admission = Arc::new(Admission::new(2));
        let first = admission.acquire();
        assert_eq!(first.ring, 0);
        let second = admission.try_acquire().unwrap();
        assert_eq!(second.ring, 1);
        drop(first);
        assert_eq!(admission.acquire().ring, 0);
        drop(second);

        let permits: Vec<_> = (0..MAX_OPERATIONS).map(|_| admission.acquire()).collect();
        assert_eq!(*admission.in_flight.lock().unwrap(), [32, 32]);
        assert!(admission.try_acquire().is_none());
        drop(permits);
        assert_eq!(*admission.in_flight.lock().unwrap(), [0, 0]);
    }

    #[test]
    fn async_admission_recovers_a_cancelled_wakeup() {
        block_on(async {
            let admission = Arc::new(Admission::new(1));
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
            assert_eq!(admission.in_flight.lock().unwrap()[0], MAX_OPERATIONS);
            drop(permit);
            drop(permits);
            assert_eq!(admission.in_flight.lock().unwrap()[0], 0);
        });
    }

    #[test]
    fn dropped_owned_read_keeps_fd_and_admission_until_terminal() {
        block_on(async {
            let (requests, rx) = channel(MAX_OPERATIONS);
            let admission = Arc::new(Admission::new(1));
            let executor = UringExecutor(Arc::new(Executor {
                rings: vec![Ring {
                    requests: Some(requests),
                    thread: None,
                }],
                admission: admission.clone(),
                completed: Arc::default(),
                read_buffers: Arc::default(),
            }));
            let file = Arc::new(tempfile::tempfile().unwrap());
            let weak_file = Arc::downgrade(&file);
            let mut read = Box::pin(executor.read_owned_at(file, 17, 2));
            assert!(futures::poll!(&mut read).is_pending());

            let (finish, finished) = oneshot::channel();
            let finished = std::cell::Cell::new(Some(finished));
            let mut drain = Box::pin(serve(rx, Arc::default(), |file, offset, op| {
                let Transfer::Read { len, .. } = op else {
                    panic!("expected read")
                };
                assert_eq!(offset, 17);
                let finished = finished.take().unwrap();
                async move {
                    finished.await.unwrap();
                    assert_eq!(len, 2);
                    drop(file);
                    Ok(Bytes::from_static(&[0; 2]))
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
            assert_eq!(admission.in_flight.lock().unwrap()[0], 1);

            finish.send(()).unwrap();
            drain.await;
            assert!(weak_file.upgrade().is_none());
            assert_eq!(admission.in_flight.lock().unwrap()[0], 0);
        });
    }

    #[test]
    fn shutdown_drains_a_disconnected_request() {
        let (tx, rx) = channel(MAX_OPERATIONS);
        let (accepted, started) = mpsc::sync_channel(1);
        let (finish, finished) = futures::channel::oneshot::channel();
        let finished = Mutex::new(Some(finished));
        let (reply, caller) = mpsc::sync_channel(1);
        let admission = Arc::new(Admission::new(1));
        let file = Arc::new(tempfile::tempfile().unwrap());
        let weak_file = Arc::downgrade(&file);
        assert!(tx
            .try_send(Request {
                file,
                offset: 0,
                op: Transfer::read(2),
                reply: Reply::Blocking(reply),
                permit: admission.acquire(),
                #[cfg(feature = "write-bench-profile")]
                timing: None,
            })
            .is_ok());
        let thread = thread::spawn(move || {
            block_on(serve(rx, Arc::default(), |file, _, op| {
                let Transfer::Read { len, .. } = op else {
                    panic!("expected read")
                };
                let finished = finished.lock().unwrap().take().unwrap();
                let accepted = accepted.clone();
                async move {
                    accepted.send(()).unwrap();
                    finished.await.unwrap();
                    drop(file);
                    Ok(Bytes::from(vec![0; len]))
                }
            }))
        });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        drop(caller);
        drop(tx);
        assert!(weak_file.upgrade().is_some());
        assert_eq!(admission.in_flight.lock().unwrap()[0], 1);
        finish.send(()).unwrap();
        thread.join().unwrap();
        assert!(weak_file.upgrade().is_none());
        assert_eq!(admission.in_flight.lock().unwrap()[0], 0);
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
                            let owned =
                                block_on(executor.read_owned_at(file.clone(), 20, CHUNK_BYTES))?;
                            assert_eq!(&owned[..], &data[3..3 + CHUNK_BYTES]);
                            let tail_offset = (17 + data.len() - 7) as u64;
                            let short =
                                block_on(executor.read_owned_at(file.clone(), tail_offset, 8));
                            assert_eq!(short.unwrap_err().kind(), ErrorKind::UnexpectedEof);
                            let tail = block_on(executor.read_owned_at(file, tail_offset, 7))?;
                            assert_eq!(&tail[..], &data[data.len() - 7..]);
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
