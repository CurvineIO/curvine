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

use crate::ucp::bindings::*;

pub struct UcpUtils;

impl UcpUtils {
    pub const fn ucp_dt_make_contig(elem_size: usize) -> ucp_datatype_t {
        ((elem_size as ucp_datatype_t) << (ucp_dt_type::UCP_DATATYPE_SHIFT as ucp_datatype_t))
            | ucp_dt_type::UCP_DATATYPE_CONTIG as ucp_datatype_t
    }

    pub fn ucs_ptr_is_err(ptr: ucs_status_ptr_t) -> bool {
        ptr as usize >= ucs_status_t::UCS_ERR_LAST as usize
    }

    pub fn ucs_ptr_raw_status(ptr: ucs_status_ptr_t) -> ucs_status_t {
        unsafe { std::mem::transmute(ptr as i8) }
    }

    pub fn ucs_ptr_status(ptr: ucs_status_ptr_t) -> ucs_status_t {
        if Self::ucs_ptr_is_ptr(ptr) {
            ucs_status_t::UCS_INPROGRESS
        } else {
            Self::ucs_ptr_raw_status(ptr)
        }
    }

    pub fn ucs_ptr_is_ptr(ptr: ucs_status_ptr_t) -> bool {
        ptr as usize - 1 < ucs_status_t::UCS_ERR_LAST as usize - 1
    }
}