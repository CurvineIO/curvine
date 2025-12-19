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
use crate::raw::fuse_abi::{fuse_batch_forget_in, fuse_forget_one, fuse_in_header, fuse_write_in};
use crate::session::fuse_decoder::FuseDecoder;
use crate::session::FuseOpCode::{self, *};
use crate::FuseResult;
use crate::FUSE_IN_HEADER_LEN;
use orpc::{err_box, CommonResult};
use std::fmt::{Display, Formatter};
use tokio_util::bytes::Bytes;

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

    // Get the header, to avoid life cycle problems, it will not be saved
    pub fn parse_header(&self) -> FuseResult<&fuse_in_header> {
        if self.buf.len() < FUSE_IN_HEADER_LEN {
            return err_box!("Not enough data for arguments (short read).");
        }

        let header: &fuse_in_header = FuseDecoder::parse(&self.buf[..FUSE_IN_HEADER_LEN])?;
        if self.buf.len() < header.len as usize {
            return err_box!("Not enough data for arguments (short read).");
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

    pub fn is_interrupt(&self) -> bool {
        matches!(self.opcode, FUSE_SETLKW)
    }

    // Determine whether it is a file data read and write operation
    pub fn is_stream(&self) -> bool {
        matches!(
            self.opcode,
            FUSE_READ | FUSE_WRITE | FUSE_FLUSH | FUSE_RELEASE | FUSE_FSYNC
        )
    }

    fn get_write_bytes(&self, size: usize) -> FuseResult<Bytes> {
        let start = FUSE_IN_HEADER_LEN + size_of::<fuse_write_in>();
        let end = start + size;
        if end != self.buf.len() {
            return err_box!(
                "Abnormal data length, expected {}, actual {}",
                end,
                self.buf.len()
            );
        }
        Ok(self.buf.slice(start..end))
    }

    pub fn get_header(&self) -> FuseResult<&fuse_in_header> {
        let mut decoder = FuseDecoder::new(&self.buf);
        decoder.get_struct()
    }

    pub fn parse_operator(&self) -> FuseResult<FuseOperator<'_>> {
        let mut decoder = FuseDecoder::new(&self.buf);
        let header: &fuse_in_header = decoder.get_struct()?;

        let op = match self.opcode {
            FUSE_INIT => FuseOperator::Init(Init {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_LOOKUP => FuseOperator::Lookup(Lookup {
                header,
                name: decoder.get_os_str()?,
            }),

            FUSE_ACCESS => FuseOperator::Access(Access {
                header,
                arg: decoder.get_struct()?,
            }),

            FUSE_GETATTR => FuseOperator::GetAttr(GetAttr { header }),

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
                let arg = decoder.get_struct()?;
                FuseOperator::Write(Write {
                    header,
                    arg,
                    data: self.get_write_bytes(arg.size as usize)?,
                })
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

            _ => FuseOperator::Notimplemented,
        };

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
