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

pub type Allocator = tikv_jemallocator::Jemalloc;

pub const fn allocator() -> Allocator {
    tikv_jemallocator::Jemalloc
}

/// Activates jemalloc heap profiling at process start.
///
/// jemalloc reads this symbol once during initialisation; there is no way to
/// turn sampling on later, which is why the heap profile is a build-time
/// (`memory-profiling`) plus startup-time decision rather than a runtime
/// toggle like the CPU sampler.
///
/// `prof_active:true` starts sampling immediately. `lg_prof_sample:19` means
/// one sample per ~512 KiB allocated, jemalloc's default: fine enough to
/// attribute a multi-gigabyte resident set without recording a stack for every
/// small allocation.
///
/// The symbol is `malloc_conf` (not `_rjem_malloc_conf`) because the workspace
/// enables `unprefixed_malloc_on_supported_platforms`.
#[cfg(feature = "memory-profiling")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";
