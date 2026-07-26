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

//! Global allocator for the FUSE binary.

#[cfg(all(unix, feature = "jemalloc"))]
pub type Allocator = tikv_jemallocator::Jemalloc;

#[cfg(not(all(unix, feature = "jemalloc")))]
pub type Allocator = std::alloc::System;

#[global_allocator]
static ALLOC: Allocator = allocator();

#[cfg(all(unix, feature = "jemalloc"))]
const fn allocator() -> Allocator {
    tikv_jemallocator::Jemalloc
}

#[cfg(not(all(unix, feature = "jemalloc")))]
const fn allocator() -> Allocator {
    std::alloc::System
}
