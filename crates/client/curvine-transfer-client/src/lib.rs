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

//! RPC clients for the job side of Curvine.
//!
//! Load and export are owned by the transfer service, so this crate only
//! exposes [`TransferClient`]. The legacy master job client (job submit, job
//! status and worker task report RPCs against the master) was removed together
//! with the master job engine.

mod transfer_client;
pub use transfer_client::TransferClient;
