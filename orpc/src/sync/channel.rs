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

use crate::err_box;
use crate::io::IOResult;
use std::sync::mpsc as block_mpsc;
use std::time::Duration;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio::sync::mpsc::Permit;
use tokio::sync::{mpsc as async_mpsc, oneshot};

pub enum AsyncSender<T> {
    Unbounded(async_mpsc::UnboundedSender<T>),
    Bounded(async_mpsc::Sender<T>),
}

impl<T> AsyncSender<T> {
    pub async fn send(&self, value: T) -> IOResult<()> {
        match self {
            AsyncSender::Unbounded(s) => s.send(value)?,
            AsyncSender::Bounded(s) => s.send(value).await?,
        }

        Ok(())
    }

    pub fn send_sync(&self, value: T) -> IOResult<()> {
        match self {
            AsyncSender::Unbounded(s) => {
                s.send(value)?;
                Ok(())
            }
            AsyncSender::Bounded(_) => err_box!("Not support"),
        }
    }

    pub fn try_reserve(&self) -> IOResult<Option<Permit<'_, T>>> {
        match self {
            AsyncSender::Unbounded(_) => {
                err_box!("Not support")
            }

            AsyncSender::Bounded(s) => match s.try_reserve() {
                Ok(v) => Ok(Some(v)),

                Err(e) => match e {
                    TrySendError::Full(_) => Ok(None),
                    TrySendError::Closed(_) => err_box!("the sender dropped"),
                },
            },
        }
    }

    pub async fn reserve(&self) -> IOResult<Permit<'_, T>> {
        match self {
            AsyncSender::Unbounded(_) => {
                err_box!("Not support")
            }

            AsyncSender::Bounded(s) => {
                let permit = s.reserve().await?;
                Ok(permit)
            }
        }
    }
}

impl<T> Clone for AsyncSender<T> {
    fn clone(&self) -> Self {
        match self {
            AsyncSender::Unbounded(v) => AsyncSender::Unbounded(v.clone()),
            AsyncSender::Bounded(v) => AsyncSender::Bounded(v.clone()),
        }
    }
}

pub enum AsyncReceiver<T> {
    Unbounded(async_mpsc::UnboundedReceiver<T>),
    Bounded(async_mpsc::Receiver<T>),
}

impl<T> AsyncReceiver<T> {
    // If sender is closed, return None, otherwise it will block until the message arrives.
    pub async fn recv(&mut self) -> Option<T> {
        match self {
            AsyncReceiver::Unbounded(s) => s.recv().await,
            AsyncReceiver::Bounded(s) => s.recv().await,
        }
    }

    pub async fn recv_check(&mut self) -> IOResult<T> {
        match self.recv().await {
            Some(v) => Ok(v),
            None => err_box!("the sender dropped"),
        }
    }

    pub async fn timeout_recv(&mut self, timeout: Duration) -> IOResult<Option<T>> {
        let inner = tokio::time::timeout(timeout, self.recv());
        Ok(inner.await?)
    }

    pub fn try_recv(&mut self) -> IOResult<Option<T>> {
        let res = match self {
            AsyncReceiver::Unbounded(s) => s.try_recv(),
            AsyncReceiver::Bounded(s) => s.try_recv(),
        };

        match res {
            Ok(v) => Ok(Some(v)),
            Err(e) => match e {
                TryRecvError::Empty => Ok(None),
                e => err_box!("{}", e),
            },
        }
    }
}

// Unified unbounded and bounded queues
pub struct AsyncChannel<T> {
    sender: AsyncSender<T>,
    receiver: AsyncReceiver<T>,
}

impl<T> AsyncChannel<T> {
    pub fn new(cap: usize) -> Self {
        let (sender, receiver) = if cap == 0 {
            let (sender, receiver) = async_mpsc::unbounded_channel();
            (
                AsyncSender::Unbounded(sender),
                AsyncReceiver::Unbounded(receiver),
            )
        } else {
            let (sender, receiver) = async_mpsc::channel(cap);
            (
                AsyncSender::Bounded(sender),
                AsyncReceiver::Bounded(receiver),
            )
        };

        Self { sender, receiver }
    }

    pub fn split(self) -> (AsyncSender<T>, AsyncReceiver<T>) {
        (self.sender, self.receiver)
    }
}

pub enum BlockingSender<T> {
    Unbounded(block_mpsc::Sender<T>),
    Bounded(block_mpsc::SyncSender<T>),
}

impl<T> BlockingSender<T> {
    pub fn send(&self, value: T) -> IOResult<()> {
        match self {
            BlockingSender::Unbounded(s) => s.send(value)?,
            BlockingSender::Bounded(s) => s.send(value)?,
        }

        Ok(())
    }
}

impl<T> Clone for BlockingSender<T> {
    fn clone(&self) -> Self {
        match self {
            BlockingSender::Unbounded(v) => BlockingSender::Unbounded(v.clone()),
            BlockingSender::Bounded(v) => BlockingSender::Bounded(v.clone()),
        }
    }
}

pub enum BlockingReceiver<T> {
    Unbounded(block_mpsc::Receiver<T>),
    Bounded(block_mpsc::Receiver<T>),
}

impl<T> BlockingReceiver<T> {
    pub fn recv(&mut self) -> Option<T> {
        let res = match self {
            BlockingReceiver::Unbounded(s) => s.recv(),
            BlockingReceiver::Bounded(s) => s.recv(),
        };
        res.ok()
    }

    pub fn recv_check(&mut self) -> IOResult<T> {
        match self.recv() {
            Some(v) => Ok(v),
            None => err_box!("the sender dropped"),
        }
    }
}

pub struct BlockingChannel<T> {
    sender: BlockingSender<T>,
    receiver: BlockingReceiver<T>,
}

impl<T> BlockingChannel<T> {
    pub fn new(cap: usize) -> Self {
        let (sender, receiver) = if cap == 0 {
            let (sender, receiver) = block_mpsc::channel();
            (
                BlockingSender::Unbounded(sender),
                BlockingReceiver::Unbounded(receiver),
            )
        } else {
            let (sender, receiver) = block_mpsc::sync_channel(cap);
            (
                BlockingSender::Bounded(sender),
                BlockingReceiver::Bounded(receiver),
            )
        };

        Self { sender, receiver }
    }

    pub fn split(self) -> (BlockingSender<T>, BlockingReceiver<T>) {
        (self.sender, self.receiver)
    }
}

pub struct CallSender<T>(oneshot::Sender<T>);

impl<T> CallSender<T> {
    pub fn new(sender: oneshot::Sender<T>) -> Self {
        Self(sender)
    }

    pub fn send(self, value: T) -> IOResult<()> {
        match self.0.send(value) {
            Ok(_) => Ok(()),
            Err(_) => err_box!("the receiver dropped"),
        }
    }
}

pub struct CallReceiver<T>(oneshot::Receiver<T>);

impl<T> CallReceiver<T> {
    pub fn new(receiver: oneshot::Receiver<T>) -> Self {
        Self(receiver)
    }

    pub async fn receive(self) -> IOResult<T> {
        match self.0.await {
            Ok(v) => Ok(v),
            Err(_) => err_box!("the sender dropped"),
        }
    }
}

pub struct CallChannel;

impl CallChannel {
    pub fn channel<T>() -> (CallSender<T>, CallReceiver<T>) {
        let (tx, rx) = oneshot::channel();
        (CallSender::new(tx), CallReceiver::new(rx))
    }
}
