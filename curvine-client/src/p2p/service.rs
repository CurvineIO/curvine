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

use crate::p2p::{CacheSnapshot, DiscoverySnapshot};
use curvine_common::conf::ClientP2pConf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2pState {
    Disabled = 0,
    Stopped = 1,
    Running = 2,
}

impl P2pState {
    fn from_u8(value: u8) -> Self {
        match value {
            2 => Self::Running,
            1 => Self::Stopped,
            _ => Self::Disabled,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct P2pStatsSnapshot {
    pub discovery: DiscoverySnapshot,
    pub cache: CacheSnapshot,
}

pub struct P2pService {
    conf: ClientP2pConf,
    state: AtomicU8,
    stats: Mutex<P2pStatsSnapshot>,
}

impl P2pService {
    pub fn new(conf: ClientP2pConf) -> Self {
        let state = if conf.enable {
            P2pState::Stopped
        } else {
            P2pState::Disabled
        };
        Self {
            conf,
            state: AtomicU8::new(state as u8),
            stats: Mutex::new(P2pStatsSnapshot::default()),
        }
    }

    pub fn conf(&self) -> &ClientP2pConf {
        &self.conf
    }

    pub fn state(&self) -> P2pState {
        P2pState::from_u8(self.state.load(Ordering::Relaxed))
    }

    pub fn is_running(&self) -> bool {
        self.state() == P2pState::Running
    }

    pub fn start(&self) -> bool {
        if !self.conf.enable {
            return false;
        }
        self.state.store(P2pState::Running as u8, Ordering::Relaxed);
        true
    }

    pub fn stop(&self) {
        if !self.conf.enable {
            return;
        }
        self.state.store(P2pState::Stopped as u8, Ordering::Relaxed);
    }

    pub fn stats_snapshot(&self) -> P2pStatsSnapshot {
        self.stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn update_stats(&self, snapshot: P2pStatsSnapshot) {
        *self
            .stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot;
    }
}

#[cfg(test)]
mod tests {
    use super::{P2pService, P2pState};
    use curvine_common::conf::ClientP2pConf;

    #[test]
    fn disabled_service_does_not_start() {
        let service = P2pService::new(ClientP2pConf::default());
        assert_eq!(service.state(), P2pState::Disabled);
        assert!(!service.start());
        assert_eq!(service.state(), P2pState::Disabled);
    }

    #[test]
    fn enabled_service_transitions_between_stopped_and_running() {
        let service = P2pService::new(ClientP2pConf {
            enable: true,
            ..ClientP2pConf::default()
        });
        assert_eq!(service.state(), P2pState::Stopped);
        assert!(service.start());
        assert_eq!(service.state(), P2pState::Running);
        service.stop();
        assert_eq!(service.state(), P2pState::Stopped);
    }
}
