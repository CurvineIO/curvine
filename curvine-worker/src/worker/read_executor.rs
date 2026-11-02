// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//! A bounded pool for synchronous gRPC read operations.

use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};

use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

const READ_THREADS: usize = 4;
const QUEUE_CAPACITY: usize = 64;

struct Job {
    id: u64,
    // A permit represents a queued job, not one already running on a thread.
    permit: OwnedSemaphorePermit,
    run: Box<dyn FnOnce(bool) + Send>,
}

#[derive(Default)]
struct Queue {
    jobs: VecDeque<Job>,
    next_id: u64,
    closed: bool,
}

struct Inner {
    queue: Mutex<Queue>,
    ready: Condvar,
    capacity: Arc<Semaphore>,
}

impl Inner {
    fn close(&self) {
        let mut queue = self.queue.lock().expect("read executor queue poisoned");
        queue.closed = true;
        self.capacity.close();
        self.ready.notify_all();
    }

    fn run(&self) {
        loop {
            let job = {
                let mut queue = self.queue.lock().expect("read executor queue poisoned");
                while queue.jobs.is_empty() && !queue.closed {
                    queue = self
                        .ready
                        .wait(queue)
                        .expect("read executor queue poisoned");
                }
                match queue.jobs.pop_front() {
                    Some(job) => job,
                    None => return,
                }
            };
            let Job { permit, run, .. } = job;
            drop(permit);
            run(true);
        }
    }
}

/// The owner closes and joins the pool once all worker streams have drained.
pub(crate) struct ReadExecutor {
    inner: Arc<Inner>,
    threads: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
pub(crate) struct ReadExecutorHandle(Arc<Inner>);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReadTaskError {
    Closed,
    Cancelled,
    Panicked,
}

pub(crate) struct ReadTask<T> {
    id: u64,
    inner: Weak<Inner>,
    cancelled: Arc<AtomicBool>,
    result: oneshot::Receiver<Result<T, ReadTaskError>>,
    finished: bool,
}

impl<T> ReadTask<T> {
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(inner) = self.inner.upgrade() {
            let job = {
                let mut queue = inner.queue.lock().expect("read executor queue poisoned");
                queue
                    .jobs
                    .iter()
                    .position(|job| job.id == self.id)
                    .and_then(|index| queue.jobs.remove(index))
            };
            if let Some(Job { permit, run, .. }) = job {
                drop(permit);
                // Drop captured resources before waking the stream's cleanup.
                run(false);
            }
        }
    }

    pub(crate) async fn wait(&mut self) -> Result<T, ReadTaskError> {
        let result = (&mut self.result)
            .await
            .unwrap_or(Err(ReadTaskError::Closed));
        self.finished = true;
        result
    }
}

impl<T> Drop for ReadTask<T> {
    fn drop(&mut self) {
        if !self.finished {
            self.cancel();
        }
    }
}

impl ReadExecutor {
    pub(crate) fn new() -> std::io::Result<Self> {
        Self::with_size(READ_THREADS, QUEUE_CAPACITY)
    }

    pub(super) fn with_size(threads: usize, capacity: usize) -> std::io::Result<Self> {
        let inner = Arc::new(Inner {
            queue: Mutex::new(Queue::default()),
            ready: Condvar::new(),
            capacity: Arc::new(Semaphore::new(capacity)),
        });
        let mut executor = Self {
            inner,
            threads: Vec::with_capacity(threads),
        };
        for index in 0..threads {
            let inner = executor.inner.clone();
            executor.threads.push(
                thread::Builder::new()
                    .name(format!("worker-read-{index}"))
                    .spawn(move || inner.run())?,
            );
        }
        Ok(executor)
    }

    pub(crate) fn handle(&self) -> ReadExecutorHandle {
        ReadExecutorHandle(self.inner.clone())
    }

    /// Drain admitted jobs and join all threads without blocking the async runtime.
    pub(crate) async fn shutdown(mut self) {
        self.inner.close();
        let threads = std::mem::take(&mut self.threads);
        if let Err(error) = tokio::task::spawn_blocking(move || join_threads(threads)).await {
            log::error!("failed to join worker read executor: {}", error);
        }
    }
}

fn join_threads(threads: Vec<JoinHandle<()>>) {
    for thread in threads {
        if thread.join().is_err() {
            log::error!("worker read executor thread panicked");
        }
    }
}

impl Drop for ReadExecutor {
    fn drop(&mut self) {
        self.inner.close();
        let threads = std::mem::take(&mut self.threads);
        if !threads.is_empty() {
            // Also handle failed startup or an aborted server task, without joining
            // synchronous I/O threads on an async runtime thread.
            if let Err(error) = thread::Builder::new()
                .name("worker-read-join".to_string())
                .spawn(move || join_threads(threads))
            {
                log::error!("failed to start worker read executor join: {}", error);
            }
        }
    }
}

impl ReadExecutorHandle {
    #[cfg(test)]
    pub(super) fn queued_len(&self) -> usize {
        self.0.queue.lock().unwrap().jobs.len()
    }

    pub(crate) async fn submit<F, T>(&self, work: F) -> Result<ReadTask<T>, ReadTaskError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let permit = self
            .0
            .capacity
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ReadTaskError::Closed)?;
        let (sender, result) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let job_cancelled = cancelled.clone();
        let run = Box::new(move |may_run: bool| {
            let result = if !may_run || job_cancelled.load(Ordering::Acquire) || sender.is_closed()
            {
                drop(work);
                Err(ReadTaskError::Cancelled)
            } else {
                catch_unwind(AssertUnwindSafe(work)).map_err(|_| ReadTaskError::Panicked)
            };
            let _ = sender.send(result);
        });
        let mut queue = self.0.queue.lock().expect("read executor queue poisoned");
        if queue.closed {
            return Err(ReadTaskError::Closed);
        }
        let id = queue.next_id;
        queue.next_id = queue.next_id.wrapping_add(1);
        queue.jobs.push_back(Job { id, permit, run });
        drop(queue);
        self.0.ready.notify_one();
        Ok(ReadTask {
            id,
            inner: Arc::downgrade(&self.0),
            cancelled,
            result,
            finished: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[derive(Default)]
    struct Gate {
        open: Mutex<bool>,
        changed: Condvar,
    }

    impl Gate {
        fn wait(&self) {
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.changed.wait(open).unwrap();
            }
        }

        fn release(&self) {
            *self.open.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn four_threads_and_sixty_four_queued_jobs_bound_admission() {
        let executor = ReadExecutor::new().unwrap();
        let handle = executor.handle();
        let gate = Arc::new(Gate::default());
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut running = Vec::new();
        for _ in 0..4 {
            let gate = gate.clone();
            let started = started_tx.clone();
            running.push(
                handle
                    .submit(move || {
                        started.send(thread::current().id()).unwrap();
                        gate.wait();
                    })
                    .await
                    .unwrap(),
            );
        }
        let mut threads = HashSet::new();
        for _ in 0..4 {
            threads.insert(
                tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(threads.len(), 4);
        let mut queued = Vec::new();
        for index in 0..64 {
            queued.push(handle.submit(move || index).await.unwrap());
        }
        assert_eq!(handle.queued_len(), 64);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), handle.submit(|| ()))
                .await
                .is_err()
        );
        queued[1].cancel();
        assert_eq!(queued[1].wait().await, Err(ReadTaskError::Cancelled));
        let mut replacement = tokio::time::timeout(Duration::from_secs(2), handle.submit(|| 64))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(handle.queued_len(), 64);
        gate.release();
        for mut task in running {
            task.wait().await.unwrap();
        }
        for (index, mut task) in queued.into_iter().enumerate() {
            if index != 1 {
                assert_eq!(task.wait().await.unwrap(), index);
            }
        }
        assert_eq!(replacement.wait().await.unwrap(), 64);
        executor.shutdown().await;
    }

    #[tokio::test]
    async fn queued_jobs_are_fifo_and_cancelled_work_is_dropped_before_completion() {
        let executor = ReadExecutor::with_size(1, 4).unwrap();
        let handle = executor.handle();
        let gate = Arc::new(Gate::default());
        let (started_tx, started_rx) = oneshot::channel();
        let running_gate = gate.clone();
        let mut running = handle
            .submit(move || {
                let _ = started_tx.send(());
                running_gate.wait();
            })
            .await
            .unwrap();
        started_rx.await.unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let drop_flags: Vec<_> = (0..4).map(|_| Arc::new(AtomicBool::new(false))).collect();
        let mut tasks = Vec::new();
        for (index, drop_flag) in drop_flags.iter().enumerate() {
            let order = order.clone();
            let probe = DropFlag(drop_flag.clone());
            tasks.push(
                handle
                    .submit(move || {
                        let _probe = probe;
                        order.lock().unwrap().push(index);
                    })
                    .await
                    .unwrap(),
            );
        }
        tasks[1].cancel();
        gate.release();
        running.wait().await.unwrap();
        for (index, mut task) in tasks.into_iter().enumerate() {
            if index == 1 {
                assert_eq!(task.wait().await, Err(ReadTaskError::Cancelled));
                assert!(drop_flags[index].load(Ordering::Acquire));
            } else {
                task.wait().await.unwrap();
            }
        }
        assert_eq!(*order.lock().unwrap(), vec![0, 2, 3]);
        executor.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_closes_admission_drains_and_joins_off_runtime() {
        let executor = ReadExecutor::with_size(1, 2).unwrap();
        let handle = executor.handle();
        let gate = Arc::new(Gate::default());
        let (started_tx, started_rx) = oneshot::channel();
        let running_gate = gate.clone();
        let mut running = handle
            .submit(move || {
                let _ = started_tx.send(());
                running_gate.wait();
            })
            .await
            .unwrap();
        started_rx.await.unwrap();
        let completed = Arc::new(AtomicUsize::new(0));
        let queued_completed = completed.clone();
        let mut queued = handle
            .submit(move || queued_completed.fetch_add(1, Ordering::SeqCst))
            .await
            .unwrap();
        let mut shutdown = tokio::spawn(executor.shutdown());
        tokio::time::timeout(Duration::from_secs(2), async {
            while !handle.0.capacity.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            handle.submit(|| ()).await,
            Err(ReadTaskError::Closed)
        ));
        // The timer runs on this single-threaded runtime while the pool is joined.
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
                .await
                .is_err()
        );
        gate.release();
        running.wait().await.unwrap();
        queued.wait().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_panicking_job_does_not_kill_a_read_thread() {
        let executor = ReadExecutor::with_size(1, 2).unwrap();
        let handle = executor.handle();
        let mut failed = handle
            .submit(|| -> u8 { panic!("test panic") })
            .await
            .unwrap();
        assert_eq!(failed.wait().await, Err(ReadTaskError::Panicked));
        let mut next = handle.submit(|| 7).await.unwrap();
        assert_eq!(next.wait().await, Ok(7));
        executor.shutdown().await;
    }
}
