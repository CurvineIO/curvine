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

mod s3_filesystem;
pub use self::s3_filesystem::S3FileSystem;

mod s3_writer;
pub use self::s3_writer::S3Writer;

mod s3_reader;
pub use self::s3_reader::S3Reader;

mod object_status;
pub use self::object_status::ObjectStatus;

pub const SCHEME: &str = "s3";
