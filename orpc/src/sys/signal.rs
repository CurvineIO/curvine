//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#![allow(unused)]

use crate::err_box;
use crate::io::IOResult;
use futures::future::{select_all, BoxFuture};
use futures::FutureExt;
use std::future::Future;

/// Signal kinds that can be received by the process
/// Values match the standard Linux signal numbers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum SignalKind {
    /// SIGHUP (1) - Hangup detected on controlling terminal or death of controlling process
    Hangup = 1,

    /// SIGINT (2) - Interrupt from keyboard (Ctrl+C)
    Interrupt = 2,

    /// SIGQUIT (3) - Quit from keyboard (Ctrl+\)
    Quit = 3,

    /// SIGUSR1 (10) - User-defined signal 1
    User1 = 10,

    /// SIGUSR2 (12) - User-defined signal 2
    User2 = 12,

    /// SIGTERM (15) - Termination signal
    Terminate = 15,
}

impl SignalKind {
    pub fn as_raw_value(self) -> i32 {
        self as i32
    }

    pub fn from_raw(signum: i32) -> Option<Self> {
        match signum {
            1 => Some(Self::Hangup),
            2 => Some(Self::Interrupt),
            3 => Some(Self::Quit),
            10 => Some(Self::User1),
            12 => Some(Self::User2),
            15 => Some(Self::Terminate),
            _ => None,
        }
    }
}

impl std::fmt::Display for SignalKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hangup => write!(f, "SIGHUP"),
            Self::Interrupt => write!(f, "SIGINT"),
            Self::Quit => write!(f, "SIGQUIT"),
            Self::Terminate => write!(f, "SIGTERM"),
            Self::User1 => write!(f, "SIGUSR1"),
            Self::User2 => write!(f, "SIGUSR2"),
        }
    }
}

pub struct SignalWatch;

impl SignalWatch {
    fn signal_future(kind: SignalKind) -> IOResult<BoxFuture<'static, Option<()>>> {
        #[cfg(target_os = "linux")]
        {
            use tokio::signal::unix::{signal, SignalKind as TokioSignalKind};

            let tokio_kind = match kind {
                SignalKind::Hangup => TokioSignalKind::hangup(),
                SignalKind::Interrupt => TokioSignalKind::interrupt(),
                SignalKind::Quit => TokioSignalKind::quit(),
                SignalKind::Terminate => TokioSignalKind::terminate(),
                SignalKind::User1 => TokioSignalKind::user_defined1(),
                SignalKind::User2 => TokioSignalKind::user_defined2(),
            };

            let mut sig = signal(tokio_kind)?;
            Ok(async move { sig.recv().await }.boxed())
        }

        #[cfg(not(target_os = "linux"))]
        {
            match kind {
                SignalKind::Interrupt => {
                    let ctrl_c = tokio::signal::ctrl_c();
                    Ok(async move { ctrl_c.await.ok() }.boxed())
                }
                _ => {
                    err_box!("signal {:?} not supported on non-Linux platforms", kind)
                }
            }
        }
    }

    fn signal_futures(
        kinds: &[SignalKind],
    ) -> IOResult<Vec<(SignalKind, BoxFuture<'static, Option<()>>)>> {
        if kinds.is_empty() {
            return err_box!("no signals to create futures for");
        }

        let mut futures = Vec::with_capacity(kinds.len());
        for &kind in kinds.iter() {
            let fut = Self::signal_future(kind)?;
            futures.push((kind, fut));
        }

        Ok(futures)
    }

    pub async fn wait_one(target: SignalKind) -> IOResult<SignalKind> {
        let fut = Self::signal_future(target)?;
        match fut.await {
            Some(_) => Ok(target),
            None => err_box!("signal stream closed"),
        }
    }

    pub async fn wait_quit() -> IOResult<SignalKind> {
        #[cfg(target_os = "linux")]
        {
            let quit_signals = [
                SignalKind::Terminate,
                SignalKind::Interrupt,
                SignalKind::Quit,
                SignalKind::Hangup,
            ];

            let futures = Self::signal_futures(&quit_signals)?;
            let futures: Vec<_> = futures
                .into_iter()
                .map(|(kind, fut)| fut.map(move |opt| (kind, opt)).boxed())
                .collect();

            let ((kind, opt), _, _) = select_all(futures).await;
            match opt {
                Some(_) => Ok(kind),
                None => err_box!("signal stream closed"),
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            Self::wait_one(SignalKind::Interrupt).await
        }
    }
}
