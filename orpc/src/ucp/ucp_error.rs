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

//! Asynchronous Rust bindings to UCX.

use crate::io::IOError;
use crate::ucp::bindings::{ucs_status_ptr_t, ucs_status_t};
use crate::ucp::UcpUtils;

/// UCX error code.
#[allow(missing_docs)]
#[repr(i8)]
#[derive(thiserror::Error, Debug)]
pub enum UcpError {
    #[error("No error")]
    Ok,

    #[error("Operation in progress")]
    Inprogress,

    #[error("No pending message")]
    NoMessage,

    #[error("No resources are available to initiate the operation")]
    NoReource,

    #[error("Input/output error")]
    IoError,

    #[error("Out of memory")]
    NoMemory,

    #[error("Invalid parameter")]
    InvalidParam,

    #[error("Destination is unreachable")]
    Unreachable,

    #[error("Address not valid")]
    InvalidAddr,

    #[error("Function not implemented")]
    NotImplemented,

    #[error("Message truncated")]
    MessageTruncated,

    #[error("No progress")]
    NoProgress,

    #[error("Provided buffer is too small")]
    BufferTooSmall,

    #[error("No such element")]
    NoElem,

    #[error("Failed to connect some of the requested endpoints")]
    SomeConnectsFailed,

    #[error("No such device")]
    NoDevice,

    #[error("Device is busy")]
    Busy,

    #[error("Request canceled")]
    Canceled,

    #[error("Shared memory error")]
    ShmemSegment,

    #[error("Element already exists")]
    AlreadyExists,

    #[error("Index out of range")]
    OutOfRange,

    #[error("Operation timed out")]
    Timeout,

    #[error("User-defined limit was reached")]
    ExceedsLimit,

    #[error("Unsupported operation")]
    Unsupported,

    #[error("Operation rejected by remote peer")]
    Rejected,

    #[error("Endpoint is not connected")]
    NotConnected,

    #[error("Connection reset by remote peer")]
    ConnectionReset,

    #[error("First link failure")]
    FirstLinkFailure,

    #[error("Last link failure")]
    LastLinkFailure,

    #[error("First endpoint failure")]
    FirstEndpointFailure,

    #[error("Last endpoint failure")]
    LastEndpointFailure,

    #[error("Endpoint timeout")]
    EndpointTimeout,

    #[error("Unknown error")]
    Unknown,
}

impl UcpError {
    pub fn new(status: ucs_status_t) -> Self {
        match status {
            ucs_status_t::UCS_OK => Self::Ok,
            ucs_status_t::UCS_INPROGRESS => Self::Inprogress,
            ucs_status_t::UCS_ERR_NO_MESSAGE => Self::NoMessage,
            ucs_status_t::UCS_ERR_NO_RESOURCE => Self::NoReource,
            ucs_status_t::UCS_ERR_IO_ERROR => Self::IoError,
            ucs_status_t::UCS_ERR_NO_MEMORY => Self::NoMemory,
            ucs_status_t::UCS_ERR_INVALID_PARAM => Self::InvalidParam,
            ucs_status_t::UCS_ERR_UNREACHABLE => Self::Unreachable,
            ucs_status_t::UCS_ERR_INVALID_ADDR => Self::InvalidAddr,
            ucs_status_t::UCS_ERR_NOT_IMPLEMENTED => Self::NotImplemented,
            ucs_status_t::UCS_ERR_MESSAGE_TRUNCATED => Self::MessageTruncated,
            ucs_status_t::UCS_ERR_NO_PROGRESS => Self::NoProgress,
            ucs_status_t::UCS_ERR_BUFFER_TOO_SMALL => Self::BufferTooSmall,
            ucs_status_t::UCS_ERR_NO_ELEM => Self::NoElem,
            ucs_status_t::UCS_ERR_SOME_CONNECTS_FAILED => Self::SomeConnectsFailed,
            ucs_status_t::UCS_ERR_NO_DEVICE => Self::NoDevice,
            ucs_status_t::UCS_ERR_BUSY => Self::Busy,
            ucs_status_t::UCS_ERR_CANCELED => Self::Canceled,
            ucs_status_t::UCS_ERR_SHMEM_SEGMENT => Self::ShmemSegment,
            ucs_status_t::UCS_ERR_ALREADY_EXISTS => Self::AlreadyExists,
            ucs_status_t::UCS_ERR_OUT_OF_RANGE => Self::OutOfRange,
            ucs_status_t::UCS_ERR_TIMED_OUT => Self::Timeout,
            ucs_status_t::UCS_ERR_EXCEEDS_LIMIT => Self::ExceedsLimit,
            ucs_status_t::UCS_ERR_UNSUPPORTED => Self::Unsupported,
            ucs_status_t::UCS_ERR_REJECTED => Self::Rejected,
            ucs_status_t::UCS_ERR_NOT_CONNECTED => Self::NotConnected,
            ucs_status_t::UCS_ERR_CONNECTION_RESET => Self::ConnectionReset,
            ucs_status_t::UCS_ERR_FIRST_LINK_FAILURE => Self::FirstLinkFailure,
            ucs_status_t::UCS_ERR_LAST_LINK_FAILURE => Self::LastLinkFailure,
            ucs_status_t::UCS_ERR_FIRST_ENDPOINT_FAILURE => Self::FirstEndpointFailure,
            ucs_status_t::UCS_ERR_ENDPOINT_TIMEOUT => Self::EndpointTimeout,
            ucs_status_t::UCS_ERR_LAST_ENDPOINT_FAILURE => Self::LastEndpointFailure,

            _ => Self::Unknown,
        }
    }

    pub fn from_status(status: ucs_status_t) -> Result<(), Self> {
        if status == ucs_status_t::UCS_OK {
            Ok(())
        } else {
            Err(Self::new(status))
        }
    }

    pub fn from_ptr(ptr: ucs_status_ptr_t) -> Result<(), Self> {
        if UcpUtils::ucs_ptr_is_err(ptr) {
            Err(Self::new(UcpUtils::ucs_ptr_raw_status(ptr)))
        } else {
            Ok(())
        }
    }
}

impl From<UcpError> for IOError {
    fn from(val: UcpError) -> Self {
        use std::io::ErrorKind::*;
        let kind = match val {
            UcpError::Inprogress => WouldBlock,
            UcpError::NoMessage => WouldBlock,
            UcpError::NoReource => WouldBlock,
            UcpError::IoError => Other,
            UcpError::NoMemory => OutOfMemory,
            UcpError::InvalidParam => InvalidInput,
            UcpError::Unreachable => NotConnected,
            UcpError::InvalidAddr => InvalidInput,
            UcpError::NotImplemented => Unsupported,
            UcpError::MessageTruncated => InvalidData,
            UcpError::NoProgress => WouldBlock,
            UcpError::BufferTooSmall => UnexpectedEof,
            UcpError::NoElem => NotFound,
            UcpError::SomeConnectsFailed => ConnectionAborted,
            UcpError::NoDevice => NotFound,
            UcpError::Busy => ResourceBusy,
            UcpError::Canceled => Interrupted,
            UcpError::ShmemSegment => Other,
            UcpError::AlreadyExists => AlreadyExists,
            UcpError::OutOfRange => InvalidInput,
            UcpError::Timeout => TimedOut,
            UcpError::ExceedsLimit => Other,
            UcpError::Unsupported => Unsupported,
            UcpError::Rejected => ConnectionRefused,
            UcpError::NotConnected => NotConnected,
            UcpError::ConnectionReset => ConnectionReset,
            UcpError::FirstLinkFailure => Other,
            UcpError::LastLinkFailure => Other,
            UcpError::FirstEndpointFailure => Other,
            UcpError::LastEndpointFailure => Other,
            UcpError::EndpointTimeout => TimedOut,
            _ => Other,
        };
        IOError::new(std::io::Error::new(kind, val))
    }
}
