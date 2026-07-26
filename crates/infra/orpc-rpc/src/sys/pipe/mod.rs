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

mod pipe_fd;
pub use self::pipe_fd::PipeFd;

mod async_fd;
pub use self::async_fd::AsyncFd;

mod pipe_writer;
pub use self::pipe_writer::PipeWriter;

mod pipe_reader;
pub use self::pipe_reader::PipeReader;

mod pipe2;
pub use self::pipe2::Pipe2;

mod pipe_pool;
pub use self::pipe_pool::PipePool;

mod owned_fd;
pub use self::owned_fd::*;
