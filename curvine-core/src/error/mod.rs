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

mod error_impl;
pub use self::error_impl::ErrorImpl;

mod string_error;
pub use self::string_error::StringError;

mod error_ext;
pub use self::error_ext::ErrorExt;

mod result_ext;
pub use self::result_ext::ResultExt;

mod error_encoder;
pub use self::error_encoder::ErrorEncoder;

mod error_decoder;
pub use self::error_decoder::ErrorDecoder;

mod common_error_ext;
pub use self::common_error_ext::CommonErrorExt;
