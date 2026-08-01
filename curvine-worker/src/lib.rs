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

pub mod worker;
pub use worker::*;

pub mod common {
    pub use curvine_master::UfsFactory;
}

pub mod master {
    pub use curvine_master::master::RpcContext;
}

pub mod transfer {
    pub use curvine_common::transfer::transfer_failure_message;
}

#[cfg(feature = "fault-injection")]
pub(crate) use curvine_fault::fault_point;

#[cfg(not(feature = "fault-injection"))]
pub(crate) use curvine_fault::__noop_fault_point as fault_point;
