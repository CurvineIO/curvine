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