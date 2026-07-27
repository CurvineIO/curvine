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

mod common_error_ext;
pub use self::common_error_ext::CommonErrorExt;

mod error_decoder;
pub use self::error_decoder::ErrorDecoder;

mod error_encoder;
pub use self::error_encoder::ErrorEncoder;

mod error_ext;
pub use self::error_ext::ErrorExt;

mod error_impl;
pub use self::error_impl::ErrorImpl;

mod result_ext;
pub use self::result_ext::ResultExt;

mod string_error;
pub use self::string_error::StringError;

pub type CommonError = Box<dyn std::error::Error + Send + Sync>;
pub type CommonResult<T> = Result<T, CommonError>;
pub type CommonResultExt<T> = Result<T, CommonErrorExt>;

pub mod error {
    pub use super::{
        CommonErrorExt, ErrorDecoder, ErrorEncoder, ErrorExt, ErrorImpl, ResultExt, StringError,
    };
}
