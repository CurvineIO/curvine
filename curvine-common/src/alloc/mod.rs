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

//! Global allocator for crates that link `curvine-common`. Feature `mimalloc` wins over `jemalloc`.
//! `jemalloc` is Unix-only; other targets fall back to the system allocator.

#[cfg(all(unix, not(fuzzing), feature = "jemalloc"))]
#[path = "jemalloc.rs"]
mod imp;

#[cfg(all(unix, not(fuzzing), feature = "mimalloc"))]
#[path = "mimalloc.rs"]
mod imp;

#[cfg(not(all(unix, not(fuzzing), any(feature = "jemalloc", feature = "mimalloc"))))]
#[path = "system.rs"]
mod imp;

pub use self::imp::*;

#[global_allocator]
static ALLOC: Allocator = allocator();

#[inline]
pub fn allocator_type_name() -> &'static str {
    std::any::type_name::<Allocator>()
}
