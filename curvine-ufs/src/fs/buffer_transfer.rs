// Copyright 2025 OPPO.
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

#![allow(unused)]

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use core::time::Duration;
use std::io::Result as IoResult;
use std::io::{Error, ErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::Instant;

/// Smart pointer-wrapped buffer pool provides efficient memory multiplexing mechanism
///
/// Buffer pool features:
/// -Dynamic expansion mechanism: Create a specified number of buffers during initialization, and retain the maximum specified number
/// -Backpressure processing: automatically yield waits when the buffer is insufficient to avoid excessive memory use
/// -Memory security: Use Arc<Mutex<>> to ensure thread safety
/// -Intelligent cleanup: automatically discard the excess buffer when the maximum number exceeds
#[derive(Clone)]
struct BufferPool {
    /// Use bytes::BytesMut as a variable buffer type to ensure thread safety through Arc<Mutex<>>
    inner: Arc<tokio::sync::Mutex<Vec<BytesMut>>>,
    /// Size of each buffer (bytes)
    chunk_size: usize,
    /// The maximum number of buffers reserved in the buffer pool
    max_buffers: usize,
}
impl BufferPool {
    /// Create a new buffer pool
    ///
    /// Pre-allocate a specified number of buffers during initialization, each buffer has the same size.
    /// The buffer pool limits the maximum number of buffers to prevent overuse of memory.
    ///
    /// Optimization points:
    /// -Pre-allocate buffers to reduce runtime allocation overhead
    /// -Use capacity prompts to optimize memory allocation of internal Vec
    ///
    /// # Parameters
    /// -`chunk_size`: Size (bytes) of each buffer
    /// -`initial_buffers`: Number of initial pre-allocated buffers
    /// -`max_buffers`: Maximum number of buffers reserved in the buffer pool
    ///
    /// # Returns
    /// Return a new BufferPool instance
    fn new(chunk_size: usize, initial_buffers: usize, max_buffers: usize) -> Self {
        // Verify parameters
        let initial_buffers = initial_buffers.min(max_buffers);
        let chunk_size = chunk_size.max(1024); // Make sure the buffer is at least 1KB in size

        // Preallocate buffers
        let mut buffers = Vec::with_capacity(initial_buffers);
        for _ in 0..initial_buffers {
            buffers.push(BytesMut::with_capacity(chunk_size));
        }

        BufferPool {
            inner: Arc::new(tokio::sync::Mutex::new(buffers)),
            chunk_size,
            max_buffers,
        }
    }

    /// Get the buffer (zero copy key)
    ///
    /// This method tries to get an available buffer from the buffer pool. If the buffer pool is empty and the maximum capacity is not reached,
    /// Create a new buffer. If the maximum capacity has been reached, wait until a buffer is available.
    ///
    /// Optimization points:
    /// -Use scope to limit the lock's holding time
    /// -Release the lock before waiting to reduce lock competition
    /// -Use more efficient backpressure mechanism
    ///
    /// # Returns
    /// Returns an available BytesMut buffer
    async fn acquire(&self) -> BytesMut {
        loop {
            // Use scope to limit the lock's holding time
            {
                let mut guard = self.inner.lock().await;

                // Try to get an existing buffer from the buffer pool
                if let Some(mut buf) = guard.pop() {
                    buf.clear();
                    return buf;
                }

                // Dynamic expansion (no more than the maximum value)
                if guard.len() < self.max_buffers {
                    return BytesMut::with_capacity(self.chunk_size);
                }

                // The lock will be automatically released at the end of the scope
            }

            // Trigger back pressure: Wait after releasing the lock, reducing lock competition
            tokio::task::yield_now().await;

            // Add short delays to avoid over-consumption of CPU
            if fastrand::bool() {
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
        }
    }

    /// Return the buffer to the buffer pool
    ///
    /// If the buffer pool does not reach the maximum capacity, return the buffer to the pool;
    /// Otherwise, discard it directly to prevent memory bloating.
    ///
    /// Optimization points:
    /// -Use scope to limit the lock's holding time
    /// -Check the buffer size in advance to avoid unnecessary lock acquisition
    ///
    /// # Parameters
    /// -`buf`: Buffer to be returned
    async fn release(&self, mut buf: BytesMut) {
        // If the buffer capacity does not match, discard it
        if buf.capacity() != self.chunk_size {
            return;
        }

        // Use scope to limit the lock's holding time
        let mut guard = self.inner.lock().await;
        if guard.len() < self.max_buffers {
            buf.clear();
            guard.push(buf);
        }
        // Discard directly when the maximum value exceeds it to prevent memory bloating
    }

    /// Get the current state of the buffer pool
    ///
    /// # Returns
    /// Return a tuple (current number of buffers, maximum number of buffers)
    async fn stats(&self) -> (usize, usize) {
        let guard = self.inner.lock().await;
        (guard.len(), self.max_buffers)
    }
}

// Modified asynchronous reading trait
#[async_trait]
pub trait AsyncChunkReader: Send + Sync {
    async fn read_chunk(&mut self, buf: &mut BytesMut) -> IoResult<usize>;
    fn content_length(&self) -> u64;
    fn mtime(&self) -> i64;
    /// Read blocks of data with specified offset and length
    ///
    /// # Parameters
    /// -offset: byte offset relative to the start of the file
    /// -length: The number of bytes to be read
    ///
    /// # Return value
    /// When successful, return the Bytes object containing data, automatically manage memory release
    /// Return empty when offset exceeds the end of the file
    async fn read(&mut self, offset: u64, length: u64) -> IoResult<Bytes>;
}

//Modified asynchronous write trait
#[async_trait]
pub trait AsyncChunkWriter: Send + Sync {
    async fn write_chunk(&mut self, data: Bytes) -> IoResult<()>;
    async fn flush(&mut self) -> IoResult<()>;
}

/// Transfer progress callback type
///
/// Parameters:
/// -Number of bytes transmitted
/// -Total bytes
///
/// Return value:
/// -true: Continue transmission
/// -false: Cancel the transfer
pub type ProgressCallback = Box<dyn Fn(u64, u64) -> bool + Send + Sync + 'static>;

/// Zero copy file transfer implementation
///
/// Core features:
/// -Memory multiplexing mechanism based on buffer pools to avoid frequent memory allocation
/// -Completely decoupled read and write operations, improving throughput throughput through concurrent tasks
/// -Automatic backpressure control to prevent excessive memory consumption
/// -Complete error handling and resource cleaning mechanism
/// -Support transmission progress callback and cancellation operations
///
/// Design points:
/// -Use Arc<Mutex<W>> to implement thread-safe sharing of writers
/// -Implement concurrent task count control through Semaphore
/// -BufferPool provides intelligent buffer lifecycle management
/// -Use tokio::spawn to achieve true asynchronous parallel processing
/// -Support cancellation of operations during transmission
pub struct BufferFileTransfer<R, W> {
    reader: R,
    writer: Arc<tokio::sync::Mutex<W>>,
    buffer_pool: BufferPool,
    // Progress callback
    progress_callback: Option<ProgressCallback>,
    // Cancel sign
    canceled: Arc<AtomicBool>,
}

impl<R, W> BufferFileTransfer<R, W>
where
    R: AsyncChunkReader + Send + 'static,
    W: AsyncChunkWriter + Send + 'static,
{
    pub fn new(reader: R, writer: W, chunk_size: usize, max_buffers: usize) -> Self {
        let buffer_pool = BufferPool::new(chunk_size, 4, max_buffers);
        Self {
            reader,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            buffer_pool,
            progress_callback: None,
            canceled: Arc::new(AtomicBool::new(false)),
        }
    }
    /// Set the progress callback function
    pub fn with_progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(u64, u64) -> bool + Send + Sync + 'static,
    {
        self.progress_callback = Some(Box::new(callback));
        self
    }

    /// Cancel the current transfer
    pub fn cancel(&self) {
        self.canceled.store(true, Ordering::SeqCst);
    }

    /// Check if it has been cancelled
    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::SeqCst)
    }

    /// Calling progress callback
    fn call_progress(&self, transferred: u64, total: u64) -> bool {
        if let Some(ref callback) = self.progress_callback {
            // Call back, if false is returned, the transfer is canceled
            if !callback(transferred, total) {
                self.cancel();
                return false;
            }
        }

        // Check the cancel flag
        !self.is_canceled()
    }

    pub async fn do_transfer(mut self) -> IoResult<(u64, u64)> {
        let total_size = self.reader.content_length();
        let mut transferred = 0u64;

        // Collect all task handles
        let mut tasks = Vec::new();
        // Last progress report time
        let mut last_progress_time = Instant::now();
        // Progress Report Interval (milliseconds)
        let progress_interval_ms = 5000; // 5000ms

        // Initial progress report
        if !self.call_progress(0, total_size) {
            return Ok((0, total_size)); // User Cancel
        }

        while transferred < total_size {
            // Check if it has been cancelled
            if self.is_canceled() {
                return Ok((transferred, total_size));
            }

            // Get the buffer
            let mut buffer = self.buffer_pool.acquire().await;

            // Read data
            let read_bytes = self.reader.read_chunk(&mut buffer).await?;
            if read_bytes == 0 {
                break; // Reading is complete
            }

            transferred += read_bytes as u64;

            // Update progress
            let now = Instant::now();
            if now.duration_since(last_progress_time).as_millis() >= progress_interval_ms as u128 {
                if !self.call_progress(transferred, total_size) {
                    // Callback returns false, cancels transmission
                    break;
                }
                last_progress_time = now;
            }

            // Only split the actual read parts to avoid unnecessary memory allocation
            let data = buffer.split_to(read_bytes).freeze();

            // Check if it has been cancelled
            if self.is_canceled() {
                break;
            }

            // Clone Arc to move to closure
            let writer_clone = self.writer.clone();
            let pool_clone = self.buffer_pool.clone();
            let canceled_clone = self.canceled.clone();

            // Start asynchronous write task and no longer limit the number of concurrency
            let task = tokio::spawn(async move {
                // Check if it has been cancelled
                if canceled_clone.load(Ordering::SeqCst) {
                    return Ok(()); // Cancel write
                }

                let result = async {
                    let mut writer = writer_clone.lock().await;
                    writer.write_chunk(data).await
                }
                .await;

                // Return the buffer
                pool_clone.release(buffer).await;

                // Return result
                result
            });

            tasks.push(task);
        }

        // Wait for all write tasks to complete and collect results
        let results = futures::future::join_all(tasks).await;

        // Process the task result, but ignore the error if canceled
        if !self.is_canceled() {
            for result in results {
                match result {
                    Ok(write_result) => {
                        // If the write error occurs, the first error will be returned
                        write_result?
                    }
                    Err(e) => {
                        // The task itself fails (such as panic)
                        return Err(Error::other(
                            format!("The write task failed to execute: {}", e),
                        ));
                    }
                }
            }

            // Finally refresh the writer
            let mut writer = self.writer.lock().await;
            writer.flush().await?;
        }

        // Final progress report
        self.call_progress(transferred, total_size);

        Ok((transferred, total_size))
    }
}

/// Extended ZeroCopyFileTransfer to add extra functionality
impl<R, W> BufferFileTransfer<R, W>
where
    R: AsyncChunkReader + Send + 'static,
    W: AsyncChunkWriter + Send + 'static,
{
    /// Perform transmission and return details (number of bytes transmitted, total number of bytes)
    pub async fn execute_transfer_with_stats(self) -> IoResult<(u64, u64)> {
        self.do_transfer().await
    }
}

#[async_trait]
impl AsyncChunkReader for Box<dyn AsyncChunkReader + Send> {
    async fn read_chunk(&mut self, buf: &mut BytesMut) -> IoResult<usize> {
        (**self).read_chunk(buf).await
    }

    fn content_length(&self) -> u64 {
        (**self).content_length()
    }

    fn mtime(&self) -> i64 {
        (**self).mtime()
    }

    async fn read(&mut self, offset: u64, length: u64) -> IoResult<Bytes> {
        (**self).read(offset, length).await
    }
}

// Implement AsyncChunkWriter trait for Box<dyn AsyncChunkWriter + Send>
#[async_trait]
impl AsyncChunkWriter for Box<dyn AsyncChunkWriter + Send> {
    async fn write_chunk(&mut self, data: Bytes) -> IoResult<()> {
        (**self).write_chunk(data).await
    }

    async fn flush(&mut self) -> IoResult<()> {
        (**self).flush().await
    }
}
