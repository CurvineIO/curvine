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

use crate::fs::{AsyncChunkReader, AsyncChunkWriter, ProgressCallback};
use bytes::{Bytes, BytesMut};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender};
use std::io::Error;
use std::io::Result as IoResult;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::Instant;

/// Channel-based file transfer implementation
///
/// Core features:
/// - Uses tokio's mpsc channel to directly pass data chunks, reducing memory copying
/// - Completely decoupled read/write operations through producer-consumer pattern to improve throughput
/// - Automatic backpressure control by limiting channel capacity to prevent excessive memory consumption
/// - Comprehensive error handling and resource cleanup mechanisms
/// - Supports transfer progress callbacks and cancellation operations
///
/// Design points:
/// - Uses bounded channel to implement automatic backpressure control
/// - Reader and writer run in independent asynchronous tasks
/// - Supports cancellation during transfer process
/// - No buffer pool needed, directly passes data chunks to reduce memory copying
pub struct ChannelFileTransfer<R, W> {
    reader: R,
    writer: W,
    // Chunk size in bytes
    chunk_size: usize,
    // Channel capacity (number of buffers)
    channel_capacity: usize,
    // Progress callback
    progress_callback: Option<ProgressCallback>,
    // Cancellation flag
    canceled: Arc<AtomicBool>,
}

impl<R, W> ChannelFileTransfer<R, W>
where
    R: AsyncChunkReader + Send + 'static,
    W: AsyncChunkWriter + Send + 'static,
{
    /// Creates a new Channel-based file transfer instance
    ///
    /// # Parameters
    /// - `reader`: Reader implementing AsyncChunkReader
    /// - `writer`: Writer implementing AsyncChunkWriter
    /// - `chunk_size`: Size of data chunks in bytes
    /// - `channel_capacity`: Channel capacity, controls maximum number of concurrent data chunks
    ///
    /// # Returns
    /// Returns a configured ChannelFileTransfer instance
    pub fn new(reader: R, writer: W, chunk_size: usize, channel_capacity: usize) -> Self {
        Self {
            reader,
            writer,
            chunk_size,
            channel_capacity: channel_capacity.max(4),
            progress_callback: None,
            canceled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Creates a transfer instance with default configuration
    pub fn with_defaults(reader: R, writer: W) -> Self {
        Self::new(reader, writer, 64 * 1024, 16) // Default: 64KB chunks, 16 buffers
    }

    /// Sets the chunk size
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size.max(4096); // Minimum 4KB
        self
    }

    /// Sets the channel capacity
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity.max(4); // Minimum 4
        self
    }

    /// Sets the progress callback function
    pub fn with_progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(u64, u64) -> bool + Send + Sync + 'static,
    {
        self.progress_callback = Some(Box::new(callback));
        self
    }

    /// Cancels the current transfer
    pub fn cancel(&self) {
        self.canceled.store(true, Ordering::SeqCst);
    }

    /// Checks if the transfer has been cancelled
    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::SeqCst)
    }

    /// Calls the progress callback
    fn call_progress(&self, transferred: u64, total: u64) -> bool {
        if let Some(ref callback) = self.progress_callback {
            // Call the callback, if it returns false, cancel the transfer
            if !callback(transferred, total) {
                self.cancel();
                return false;
            }
        }

        // Check cancellation flag
        !self.is_canceled()
    }

    /// Executes the transfer and returns detailed information (bytes transferred, total bytes)
    pub async fn execute_transfer_with_stats(self) -> IoResult<(u64, u64)> {
        self.do_transfer().await
    }

    /// Executes the file transfer
    ///
    /// Core implementation:
    /// 1. Creates a bounded channel as a data conduit between reader and writer
    /// 2. Launches independent read and write tasks
    /// 3. Read task reads data from reader and sends to channel
    /// 4. Write task receives data from channel and writes to writer
    /// 5. Automatically implements backpressure control through channel
    /// 6. Supports progress callbacks and cancellation operations
    ///
    /// # Returns
    /// Returns a tuple (bytes transferred, total bytes)
    pub async fn do_transfer(mut self) -> IoResult<(u64, u64)> {
        let total_size = self.reader.content_length();

        // Create bounded channel with specified buffer capacity
        let channel = AsyncChannel::<(Bytes, usize)>::new(self.channel_capacity);
        let (tx, rx) = channel.split();

        // Create progress reporting channel
        let progress_channel = AsyncChannel::<u64>::new(16);
        let (progress_tx, mut progress_rx) = progress_channel.split();

        // Initial progress report
        if !self.call_progress(0, total_size) {
            return Ok((0, total_size)); // User cancelled
        }

        // Clone cancellation flag for reader task
        let reader_canceled = self.canceled.clone();

        // Start reader task
        let reader_handle = tokio::spawn(async move {
            Self::reader_task(
                self.reader,
                tx,
                self.chunk_size,
                reader_canceled,
                progress_tx,
            )
            .await
        });

        // Clone cancellation flag for writer task
        let writer_canceled = self.canceled.clone();

        // Start writer task
        let writer_handle =
            tokio::spawn(async move { Self::writer_task(self.writer, rx, writer_canceled).await });

        // Progress callback flag and function
        let progress_callback = self.progress_callback.take();
        let canceled = self.canceled.clone();

        // Handle progress updates
        let mut last_reported = 0u64;
        while let Some(transferred) = progress_rx.recv().await {
            last_reported = transferred;

            // Call progress callback
            if let Some(ref callback) = progress_callback {
                if !callback(transferred, total_size) {
                    canceled.store(true, Ordering::SeqCst);
                    break;
                }
            }

            // Check if cancelled
            if canceled.load(Ordering::SeqCst) {
                break;
            }
        }

        // Wait for reader task to complete
        let read_result = reader_handle.await.unwrap_or_else(|e| {
            Err(Error::other(format!("Reader task execution failed: {}", e)))
        })?;

        // Wait for writer task to complete
        writer_handle.await.unwrap_or_else(|e| {
            Err(Error::other(format!("Writer task execution failed: {}", e)))
        })?;

        // Final progress report (if different from last report)
        if let Some(ref callback) = progress_callback {
            if read_result.0 > last_reported {
                let _ = callback(read_result.0, total_size);
            }
        }

        // Return transfer result
        Ok((read_result.0, total_size))
    }

    /// Reader task: reads data from reader and sends to channel
    async fn reader_task(
        mut reader: R,
        tx: AsyncSender<(Bytes, usize)>,
        chunk_size: usize,
        canceled: Arc<AtomicBool>,
        progress_tx: AsyncSender<u64>,
    ) -> IoResult<(u64, u64)> {
        let mut transferred = 0u64;
        let total_size = reader.content_length();

        // Last progress report time
        let mut last_progress_time = Instant::now();
        // Progress report interval (milliseconds)
        let progress_interval_ms = 1000 * 5;

        // Create buffer
        let mut buffer = BytesMut::with_capacity(chunk_size);

        loop {
            // Check if cancelled
            if canceled.load(Ordering::SeqCst) {
                break;
            }
            buffer.clear(); // Clear buffer for reuse
                            // Read data
            let read_bytes = reader.read_chunk(&mut buffer).await?;
            if read_bytes == 0 {
                break; // Reading complete
            }

            transferred += read_bytes as u64;

            // Periodically send progress reports
            let now = Instant::now();
            if now.duration_since(last_progress_time).as_millis() >= progress_interval_ms as u128 {
                // Send progress info, ignore errors (if receiver is closed)
                let _ = progress_tx.send(transferred).await;
                last_progress_time = now;
            }

            // Only split the actually read portion to avoid unnecessary memory allocation
            let data = buffer.split_to(read_bytes).freeze();

            // Check if cancelled
            if canceled.load(Ordering::SeqCst) {
                break;
            }

            // Send data to channel
            // If receiver is closed, it means the writer task has ended, we should end too
            if tx.send((data, read_bytes)).await.is_err() {
                break;
            }
        }

        // Send final progress
        let _ = progress_tx.send(transferred).await;

        // Return transfer result
        Ok((transferred, total_size))
    }

    /// Writer task: receives data from channel and writes to writer
    async fn writer_task(
        mut writer: W,
        mut rx: AsyncReceiver<(Bytes, usize)>,
        canceled: Arc<AtomicBool>,
    ) -> IoResult<()> {
        // Receive and process data until channel closes or cancellation
        while let Some((data, _size)) = rx.recv().await {
            // Check if cancelled
            if canceled.load(Ordering::SeqCst) {
                break;
            }

            // Write data
            writer.write_chunk(data).await?;
        }

        // If not cancelled, flush the writer
        if !canceled.load(Ordering::SeqCst) {
            writer.flush().await?
        }

        Ok(())
    }
}
