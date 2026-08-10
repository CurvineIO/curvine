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

use crate::fs::operator::*;
use crate::raw::fuse_abi::{
    fuse_batch_forget_in, fuse_forget_one, fuse_in_header, fuse_init_in, fuse_init_in_ext_tail,
    fuse_ioctl_in, fuse_write_in,
};
use crate::session::fuse_decoder::FuseDecoder;
use crate::session::FuseOpCode::{self, *};
use crate::FuseResult;
use crate::{FUSE_INIT_EXT, FUSE_IN_HEADER_LEN};
use bytes::Bytes;
use curvine_core_error::{err_box, CommonResult};
use std::fmt::{Display, Formatter};

// fuse request data
pub struct FuseRequest {
    unique: u64,
    opcode: FuseOpCode,
    buf: Bytes,
}

impl FuseRequest {
    pub fn from_bytes(buf: Bytes) -> CommonResult<Self> {
        let mut req = Self {
            buf,
            unique: 0,
            opcode: NOT_SUPPORTED,
        };
        let header = req.parse_header()?;
        let (unique, opcode) = (header.unique, header.opcode);
        req.unique = unique;
        req.opcode = From::from(opcode);

        Ok(req)
    }

    // Get the header; not saved, to avoid lifetime problems.
    pub fn parse_header(&self) -> FuseResult<&fuse_in_header> {
        if self.buf.len() < FUSE_IN_HEADER_LEN {
            return err_box!("Not enough data for arguments (short read).");
        }

        let header: &fuse_in_header = FuseDecoder::parse(&self.buf[..FUSE_IN_HEADER_LEN])?;
        let declared_len = header.len as usize;
        if declared_len < FUSE_IN_HEADER_LEN {
            return err_box!(
                "Invalid FUSE request length {}, expected at least {}",
                declared_len,
                FUSE_IN_HEADER_LEN
            );
        }
        if self.buf.len() != declared_len {
            return err_box!(
                "FUSE request length mismatch, declared {}, actual {}",
                declared_len,
                self.buf.len()
            );
        }

        Ok(header)
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn unique(&self) -> u64 {
        self.unique
    }

    pub fn opcode(&self) -> FuseOpCode {
        self.opcode
    }

    pub fn is_interruptible_wait(&self) -> bool {
        matches!(self.opcode, FUSE_SETLKW)
    }

    pub fn is_stream(&self) -> bool {
        matches!(
            self.opcode,
            FUSE_READ | FUSE_WRITE | FUSE_FLUSH | FUSE_RELEASE | FUSE_FSYNC
        )
    }

    pub fn expects_reply(&self) -> bool {
        !matches!(
            self.opcode,
            FUSE_FORGET | FUSE_BATCH_FORGET | FUSE_INTERRUPT
        )
    }

    pub fn should_audit(&self) -> bool {
        !matches!(self.opcode, FUSE_READ | FUSE_WRITE)
    }

    pub fn get_header(&self) -> FuseResult<&fuse_in_header> {
        let mut decoder = FuseDecoder::new(&self.buf);
        decoder.get_struct()
    }

    pub fn parse_operator(&self) -> FuseResult<FuseOperator<'_>> {
        let mut decoder = FuseDecoder::new(&self.buf);
        let header: &fuse_in_header = decoder.get_struct()?;

        let op = match self.opcode {
            FUSE_INIT => {
                let arg: &fuse_init_in = decoder.get_struct()?;
                match decoder.len() {
                    0 if arg.flags & FUSE_INIT_EXT == 0 => {}
                    0 => return err_box!("FUSE_INIT_EXT set without extended init payload"),
                    len if len == size_of::<fuse_init_in_ext_tail>() => {
                        if arg.flags & FUSE_INIT_EXT == 0 {
                            return err_box!(
                                "Extended FUSE init payload received without FUSE_INIT_EXT"
                            );
                        }
                        let _: &fuse_init_in_ext_tail = decoder.get_struct()?;
                    }
                    len => {
                        return err_box!(
                            "Invalid FUSE init extension length {}, expected 0 or {}",
                            len,
                            size_of::<fuse_init_in_ext_tail>()
                        )
                    }
                }
                FuseOperator::Init(Init { header, arg })
            }

            FUSE_LOOKUP => {
                let name = decoder.get_os_str()?;
                // A rename racing with dentry revalidation can leave bytes from the
                // previous name after the first NUL. LOOKUP has exactly one name, so
                // the first NUL remains its semantic boundary.
                decoder.get_all()?;
                FuseOperator::Lookup(Lookup { header, name })
            }

            FUSE_ACCESS => FuseOperator::Access(Access {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_GETATTR => FuseOperator::GetAttr(GetAttr {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_READLINK => FuseOperator::Readlink(Readlink { header }),

            FUSE_SYMLINK => FuseOperator::Symlink(Symlink {
                header,
                linkname: decoder.get_os_str()?,
                target: decoder.get_os_str()?,
            }),

            FUSE_GETXATTR => FuseOperator::GetXAttr(GetXAttr {
                header,
                arg: decoder.get_struct()?,
                name: decoder.get_os_str()?,
            }),

            FUSE_SETXATTR => {
                let arg = decoder.get_struct()?;
                FuseOperator::SetXAttr(SetXAttr {
                    header,
                    arg,
                    name: decoder.get_os_str()?,
                    value: decoder.get_bytes(arg.size as usize)?,
                })
            }

            FUSE_REMOVEXATTR => FuseOperator::RemoveXAttr(RemoveXAttr {
                header,
                name: decoder.get_os_str()?,
            }),

            FUSE_SETATTR => FuseOperator::SetAttr(SetAttr {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_OPENDIR => FuseOperator::OpenDir(OpenDir {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_STATFS => FuseOperator::StatFs(StatFs { header }),

            FUSE_MKDIR => FuseOperator::Mkdir(MkDir {
                header,
                arg: decoder.get_struct()?,
                name: decoder.get_os_str()?,
            }),

            FUSE_FALLOCATE => FuseOperator::FAllocate(FAllocate {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_RELEASEDIR => FuseOperator::ReleaseDir(ReleaseDir {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_READDIR => FuseOperator::ReadDir(ReadDir {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_READDIRPLUS => FuseOperator::ReadDirPlus(ReadDirPlus {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_FORGET => FuseOperator::Forget(Forget {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_READ => FuseOperator::Read(Read {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_FLUSH => FuseOperator::Flush(Flush {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_OPEN => FuseOperator::Open(Open {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_WRITE => {
                let arg: &fuse_write_in = decoder.get_struct()?;
                let data_start = self.buf.len() - decoder.len();
                let _ = decoder.get_bytes(arg.size as usize)?;
                let data_end = self.buf.len() - decoder.len();
                let data = self.buf.slice(data_start..data_end);
                FuseOperator::Write(Write { header, arg, data })
            }

            FUSE_MKNOD => FuseOperator::MkNod(MkNod {
                header,
                arg: decoder.get_struct()?,
                name: decoder.get_os_str()?,
            }),

            FUSE_CREATE => FuseOperator::Create(Create {
                header,
                arg: decoder.get_struct()?,
                name: decoder.get_os_str()?,
            }),

            FUSE_RELEASE => FuseOperator::Release(Release {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_UNLINK => FuseOperator::Unlink(Unlink {
                header,
                name: decoder.get_os_str()?,
            }),

            FUSE_LINK => FuseOperator::Link(Link {
                header,
                arg: decoder.get_struct()?,
                name: decoder.get_os_str()?,
            }),

            FUSE_RMDIR => FuseOperator::RmDir(RmDir {
                header,
                name: decoder.get_os_str()?,
            }),

            FUSE_BATCH_FORGET => {
                let arg: &fuse_batch_forget_in = decoder.get_struct()?;
                let nodes = decoder.get_struct_vec::<fuse_forget_one>(arg.count as usize)?;
                FuseOperator::BatchForget(BatchForget { header, arg, nodes })
            }

            FUSE_RENAME => FuseOperator::Rename(Rename {
                header,
                arg: decoder.get_struct()?,
                old_name: decoder.get_os_str()?,
                new_name: decoder.get_os_str()?,
            }),

            FUSE_RENAME2 => FuseOperator::Rename2(Rename2 {
                header,
                arg: decoder.get_struct()?,
                old_name: decoder.get_os_str()?,
                new_name: decoder.get_os_str()?,
            }),

            FUSE_INTERRUPT => FuseOperator::Interrupt(Interrupt {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_LISTXATTR => FuseOperator::ListXAttr(ListXAttr {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_FSYNC => FuseOperator::FSync(FSync {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_GETLK => FuseOperator::GetLk(GetLk {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_SETLK => FuseOperator::SetLk(SetLk {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_SETLKW => FuseOperator::SetLkW(SetLkW {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_IOCTL => {
                let arg: &fuse_ioctl_in = decoder.get_struct()?;
                let in_data = decoder.get_bytes(arg.in_size as usize)?;
                FuseOperator::Ioctl(Ioctl {
                    header,
                    arg,
                    in_data,
                })
            }

            FUSE_FSYNCDIR => FuseOperator::FSyncDir(FSyncDir {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_DESTROY => FuseOperator::Destroy(Destroy { header }),

            // Opcodes with no arm fall through to `Notimplemented` -> ENOSYS.
            // Whether that is intentional (BMAP/POLL/LSEEK etc.) or a gap
            // is recorded authoritatively by `FuseOpCode::expected_dispatch`.
            _ => FuseOperator::Notimplemented,
        };

        if !matches!(&op, FuseOperator::Notimplemented) {
            decoder.ensure_empty()?;
        }

        Ok(op)
    }
}

impl Display for FuseRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unique {}, opcode {:?}, data_len {}",
            self.unique,
            self.opcode,
            self.buf.len() - FUSE_IN_HEADER_LEN
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::fuse_abi::{
        fuse_batch_forget_in, fuse_getattr_in, fuse_init_in, fuse_init_in_ext_tail,
        fuse_setxattr_in, fuse_write_in,
    };
    use crate::FuseUtils;
    use bytes::BytesMut;
    use std::ffi::OsStr;

    fn request_bytes(header: &fuse_in_header, body: &[u8]) -> Bytes {
        let mut bytes = BytesMut::with_capacity(FUSE_IN_HEADER_LEN + body.len());
        bytes.extend_from_slice(FuseUtils::struct_as_bytes(header));
        bytes.extend_from_slice(body);
        bytes.freeze()
    }

    fn header(opcode: FuseOpCode, len: usize) -> fuse_in_header {
        fuse_in_header {
            len: len as u32,
            opcode: opcode as u32,
            unique: 42,
            nodeid: 7,
            ..Default::default()
        }
    }

    #[test]
    fn rejects_declared_length_smaller_than_header() {
        let header = header(FUSE_GETATTR, FUSE_IN_HEADER_LEN - 1);
        let err = match FuseRequest::from_bytes(request_bytes(&header, &[])) {
            Ok(_) => panic!("invalid declared length must be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("expected at least"));
    }

    #[test]
    fn rejects_actual_length_different_from_declared_length() {
        let header = header(FUSE_GETATTR, FUSE_IN_HEADER_LEN);
        let err = match FuseRequest::from_bytes(request_bytes(&header, &[0])) {
            Ok(_) => panic!("mismatched request length must be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("length mismatch"));
    }

    #[test]
    fn rejects_trailing_body_for_fixed_size_operator() {
        let header = header(FUSE_STATFS, FUSE_IN_HEADER_LEN + 1);
        let request = FuseRequest::from_bytes(request_bytes(&header, &[0])).unwrap();
        let err = request.parse_operator().unwrap_err();
        assert!(err.to_string().contains("Unexpected trailing request data"));
    }

    #[test]
    fn lookup_uses_first_nul_terminated_name_when_rename_leaves_old_tail() {
        let body = b"race-alt\0et\0";
        let header = header(FUSE_LOOKUP, FUSE_IN_HEADER_LEN + body.len());
        let request = FuseRequest::from_bytes(request_bytes(&header, body)).unwrap();

        match request.parse_operator().unwrap() {
            FuseOperator::Lookup(op) => assert_eq!(op.name, OsStr::new("race-alt")),
            other => panic!("expected Lookup, got {other:?}"),
        }
    }

    #[test]
    fn lookup_still_requires_a_nul_terminated_name() {
        let body = b"race-alt";
        let header = header(FUSE_LOOKUP, FUSE_IN_HEADER_LEN + body.len());
        let request = FuseRequest::from_bytes(request_bytes(&header, body)).unwrap();

        assert!(request.parse_operator().is_err());
    }

    fn init_request(arg: &fuse_init_in, tail: Option<&fuse_init_in_ext_tail>) -> FuseRequest {
        let mut body = BytesMut::new();
        body.extend_from_slice(FuseUtils::struct_as_bytes(arg));
        if let Some(tail) = tail {
            body.extend_from_slice(FuseUtils::struct_as_bytes(tail));
        }
        let header = header(FUSE_INIT, FUSE_IN_HEADER_LEN + body.len());
        FuseRequest::from_bytes(request_bytes(&header, &body)).unwrap()
    }

    #[test]
    fn init_accepts_legacy_16_byte_payload() {
        assert_eq!(size_of::<fuse_init_in>(), 16);

        let request = init_request(
            &fuse_init_in {
                major: 7,
                minor: 31,
                max_readahead: 4096,
                flags: 0,
            },
            None,
        );

        match request.parse_operator().unwrap() {
            FuseOperator::Init(op) => {
                assert_eq!(op.arg.minor, 31);
            }
            other => panic!("expected Init, got {other:?}"),
        }
    }

    #[test]
    fn init_accepts_extended_64_byte_payload() {
        assert_eq!(size_of::<fuse_init_in_ext_tail>(), 48);

        let request = init_request(
            &fuse_init_in {
                major: 7,
                minor: 36,
                max_readahead: 4096,
                flags: FUSE_INIT_EXT,
            },
            Some(&fuse_init_in_ext_tail {
                flags2: 0x12,
                ..Default::default()
            }),
        );

        match request.parse_operator().unwrap() {
            FuseOperator::Init(op) => {
                assert_eq!(op.arg.minor, 36);
            }
            other => panic!("expected Init, got {other:?}"),
        }
    }

    #[test]
    fn init_rejects_extension_flag_without_tail() {
        let request = init_request(
            &fuse_init_in {
                major: 7,
                minor: 36,
                max_readahead: 4096,
                flags: FUSE_INIT_EXT,
            },
            None,
        );

        let err = request.parse_operator().unwrap_err();
        assert!(err.to_string().contains("without extended init payload"));
    }

    #[test]
    fn init_rejects_extended_tail_without_flag() {
        let request = init_request(
            &fuse_init_in {
                major: 7,
                minor: 36,
                max_readahead: 4096,
                flags: 0,
            },
            Some(&fuse_init_in_ext_tail::default()),
        );

        let err = request.parse_operator().unwrap_err();
        assert!(err.to_string().contains("without FUSE_INIT_EXT"));
    }

    #[test]
    fn init_rejects_invalid_extension_length() {
        let arg = fuse_init_in {
            major: 7,
            minor: 36,
            max_readahead: 4096,
            flags: FUSE_INIT_EXT,
        };
        let mut body = BytesMut::new();
        body.extend_from_slice(FuseUtils::struct_as_bytes(&arg));
        body.extend_from_slice(&[0; 4]);
        let header = header(FUSE_INIT, FUSE_IN_HEADER_LEN + body.len());
        let request = FuseRequest::from_bytes(request_bytes(&header, &body)).unwrap();

        let err = request.parse_operator().unwrap_err();
        assert!(err
            .to_string()
            .contains("Invalid FUSE init extension length"));
    }

    #[test]
    fn getattr_consumes_kernel_argument() {
        assert_eq!(size_of::<fuse_getattr_in>(), 16);

        let arg = fuse_getattr_in {
            getattr_flags: 1,
            dummy: 0,
            fh: 99,
        };
        let body = FuseUtils::struct_as_bytes(&arg);
        let header = header(FUSE_GETATTR, FUSE_IN_HEADER_LEN + body.len());
        let request = FuseRequest::from_bytes(request_bytes(&header, body)).unwrap();

        match request.parse_operator().unwrap() {
            FuseOperator::GetAttr(op) => {
                assert_eq!(op.arg.getattr_flags, 1);
                assert_eq!(op.arg.fh, 99);
            }
            other => panic!("expected GetAttr, got {other:?}"),
        }
    }

    #[test]
    fn write_data_follows_decoder_cursor() {
        let data = b"write-data";
        let arg = fuse_write_in {
            size: data.len() as u32,
            ..Default::default()
        };
        let mut body = BytesMut::new();
        body.extend_from_slice(FuseUtils::struct_as_bytes(&arg));
        body.extend_from_slice(data);
        let header = header(FUSE_WRITE, FUSE_IN_HEADER_LEN + body.len());
        let request = FuseRequest::from_bytes(request_bytes(&header, &body)).unwrap();

        match request.parse_operator().unwrap() {
            FuseOperator::Write(op) => assert_eq!(&op.data[..], data),
            other => panic!("expected Write, got {other:?}"),
        }
    }

    #[test]
    fn rejects_batch_forget_count_larger_than_body() {
        let arg = fuse_batch_forget_in {
            count: u32::MAX,
            dummy: 0,
        };
        let body = FuseUtils::struct_as_bytes(&arg);
        let header = header(FUSE_BATCH_FORGET, FUSE_IN_HEADER_LEN + body.len());
        let request = FuseRequest::from_bytes(request_bytes(&header, body)).unwrap();
        let err = request.parse_operator().unwrap_err();
        assert!(err.to_string().contains("requested 4294967295"));
        assert!(err.to_string().contains("available 0"));
    }

    #[test]
    fn audit_classification_excludes_only_read_and_write() {
        for opcode in [FUSE_READ, FUSE_WRITE] {
            let request = FuseRequest {
                unique: 1,
                opcode,
                buf: Bytes::new(),
            };
            assert!(!request.should_audit(), "{opcode:?} should not be audited");
        }

        for opcode in [FUSE_FLUSH, FUSE_RELEASE, FUSE_FSYNC, FUSE_LOOKUP] {
            let request = FuseRequest {
                unique: 1,
                opcode,
                buf: Bytes::new(),
            };
            assert!(request.should_audit(), "{opcode:?} should be audited");
        }
    }

    #[test]
    fn only_setlkw_is_an_interruptible_wait() {
        {
            let opcode = FUSE_SETLKW;
            let request = FuseRequest {
                unique: 1,
                opcode,
                buf: Bytes::new(),
            };
            assert!(request.is_interruptible_wait());
        }

        for opcode in [FUSE_INTERRUPT, FUSE_SETLK, FUSE_READ] {
            let request = FuseRequest {
                unique: 1,
                opcode,
                buf: Bytes::new(),
            };
            assert!(!request.is_interruptible_wait(), "{opcode:?}");
        }
    }

    #[test]
    fn setxattr_uses_compat_header_and_preserves_name_and_value() {
        assert_eq!(size_of::<fuse_setxattr_in>(), 8);

        let name = b"user.curvine.full-name\0";
        let value = b"xattr-value";
        let arg = fuse_setxattr_in {
            size: value.len() as u32,
            flags: libc::XATTR_CREATE as u32,
        };
        let request_len =
            size_of::<fuse_in_header>() + size_of::<fuse_setxattr_in>() + name.len() + value.len();
        let header = fuse_in_header {
            len: request_len as u32,
            opcode: FUSE_SETXATTR as u32,
            unique: 42,
            nodeid: 7,
            ..Default::default()
        };

        let mut bytes = BytesMut::with_capacity(request_len);
        bytes.extend_from_slice(FuseUtils::struct_as_bytes(&header));
        bytes.extend_from_slice(FuseUtils::struct_as_bytes(&arg));
        bytes.extend_from_slice(name);
        bytes.extend_from_slice(value);

        let request = FuseRequest::from_bytes(bytes.freeze()).unwrap();
        match request.parse_operator().unwrap() {
            FuseOperator::SetXAttr(op) => {
                assert_eq!(op.name, OsStr::new("user.curvine.full-name"));
                assert_eq!(op.value, value);
                assert_eq!(op.arg.flags, libc::XATTR_CREATE as u32);
            }
            other => panic!("expected SetXAttr, got {other:?}"),
        }
    }
}
