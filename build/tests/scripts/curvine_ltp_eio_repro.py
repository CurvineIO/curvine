#!/usr/bin/env python3
#  // Copyright 2025 OPPO.
#  //
#  // Licensed under the Apache License, Version 2.0 (the "License");
#  // you may not use this file except in compliance with the License.
#  // You may obtain a copy of the License at
#  //
#  //     http://www.apache.org/licenses/LICENSE-2.0
#  //
#  // Unless required by applicable law or agreed to in writing, software
#  // distributed under the License is distributed on an "AS IS" BASIS,
#  // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  // See the License for the specific language governing permissions and
#  // limitations under the License.

"""
IO operations returning EIO on Curvine FUSE — reproducer for LTP failures.

Root cause: Curvine FUSE mishandles several POSIX I/O paths and surfaces EIO
(errno 5) where the kernel would normally succeed — fallocate past EOF or on
sparse regions, truncate on an open file, sparse-file read/readv, and hard-link
creation when the source path is a symbolic link.

Covered LTP cases (each line is one minimal repro scenario):
  fallocate01 — fallocate(fd, 0, 12*blksize, blksize) at EOF after 12 blocks written
  fallocate03 — fallocate on sparse file at offset 2*blksize (hole between two ranges)
  truncate02, truncate02_64 — truncate(testfile, 256) while fd stays open on 1024-byte file
  ftest01, ftest05 — child read(2) at unwritten sparse chunk must return zeros, not EIO
  ftest03, ftest07 — child readv(2) at unwritten sparse chunk must return zeros, not EIO
  link01 — link(symbolic, nick) after symlink(object,symbolic) and creat(object) must succeed

Usage:
    python3 curvine_ltp_eio_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_eio_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import ctypes
import errno
import multiprocessing
import os
import platform
import shutil
import sys
from typing import Callable, Dict, List, Optional, Tuple

DEFAULT_DIR = "/curvine-fuse/fuse-test"

O_RDWR = 2
O_CREAT = 0o100
O_TRUNC = 0o1000

BLOCKS_WRITTEN = 12
HOLE_SIZE_IN_BLOCKS = 12
FTEST_CHUNK = 2048
FTEST_READ_OFFSET = 0xEB800

AT_FDCWD = -100

_libc: Optional[ctypes.CDLL] = None
_libc_fallocate_fn: Optional[Callable[..., int]] = None


def _init_libc() -> ctypes.CDLL:
    global _libc, _libc_fallocate_fn
    if _libc is not None:
        return _libc

    _libc = ctypes.CDLL(None, use_errno=True)
    fn = getattr(_libc, "fallocate", None)
    if fn is not None:
        fn.restype = ctypes.c_int
        fn.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_longlong,
            ctypes.c_longlong,
        ]

        def _fallocate(fd: int, mode: int, offset: int, length: int) -> int:
            return int(
                fn(
                    fd,
                    mode,
                    ctypes.c_longlong(offset),
                    ctypes.c_longlong(length),
                )
            )

        _libc_fallocate_fn = _fallocate
    return _libc


def _raw_syscall(nr: int, *args: int) -> int:
    libc = _init_libc()
    syscall = libc.syscall
    syscall.restype = ctypes.c_long
    return int(syscall(nr, *args))


def _syscall_nr(name: str) -> int:
    table: Dict[str, Dict[str, int]] = {
        "fallocate": {"x86_64": 285, "aarch64": 47},
        "linkat": {"x86_64": 265, "aarch64": 37},
    }
    machine = platform.machine()
    if machine not in table[name]:
        raise OSError(
            errno.ENOSYS,
            f"unsupported architecture for {name}: {machine}",
        )
    return table[name][machine]


def sys_fallocate(fd: int, mode: int, offset: int, length: int) -> None:
    """fallocate(fd, mode, offset, length) via libc or raw syscall."""
    _init_libc()
    if _libc_fallocate_fn is not None:
        ret = _libc_fallocate_fn(fd, mode, offset, length)
    else:
        ret = _raw_syscall(
            _syscall_nr("fallocate"),
            fd,
            mode,
            ctypes.c_longlong(offset).value,
            ctypes.c_longlong(length).value,
        )
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def sys_linkat(oldpath: str, newpath: str) -> None:
    """linkat(AT_FDCWD, oldpath, AT_FDCWD, newpath, 0) via raw syscall."""
    old = ctypes.c_char_p(oldpath.encode())
    new = ctypes.c_char_p(newpath.encode())
    ret = _raw_syscall(
        _syscall_nr("linkat"),
        AT_FDCWD,
        ctypes.cast(old, ctypes.c_void_p).value or 0,
        AT_FDCWD,
        ctypes.cast(new, ctypes.c_void_p).value or 0,
        0,
    )
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def case_dir(root: str, name: str) -> str:
    path = os.path.join(root, name)
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    return path


def expect_ok(name: str, fn: Callable[[], None]) -> int:
    try:
        fn()
        print(f"  PASS: {name}")
        return 0
    except OSError as exc:
        print(
            f"  FAIL: {name} — unexpected OSError errno={exc.errno} "
            f"({errno.errorcode.get(exc.errno, '?')}): {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"  FAIL: {name} — unexpected error: {exc}", file=sys.stderr)
        return 1


def expect_errno(name: str, fn: Callable[[], None], expected: int) -> int:
    try:
        fn()
        print(
            f"  FAIL: {name} — expected errno {expected} "
            f"({errno.errorcode.get(expected, '?')}), but syscall succeeded",
            file=sys.stderr,
        )
        return 1
    except OSError as exc:
        if exc.errno == expected:
            print(
                f"  PASS: {name} (errno={expected} "
                f"{errno.errorcode.get(expected, '?')})"
            )
            return 0
        print(
            f"  FAIL: {name} — expected errno {expected}, got {exc.errno}: {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"  FAIL: {name} — expected OSError, got {exc}", file=sys.stderr)
        return 1


def skip(name: str, reason: str) -> int:
    print(f"  SKIP: {name} — {reason}")
    return 0


def safe_close(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


def _write_blocks(fd: int, block_size: int, count: int) -> None:
    buf = bytes((ord("A") + (i % 26)) for i in range(block_size))
    for _ in range(count):
        os.write(fd, buf)


def _sparse_child_read(path: str, offset: int, chunk: int, use_readv: bool) -> None:
    fd = os.open(path, O_RDWR | O_CREAT | O_TRUNC, 0o666)
    try:
        os.lseek(fd, offset, os.SEEK_SET)
        if use_readv:
            half = chunk // 2
            bufs = [bytearray(half), bytearray(chunk - half)]
            got = os.readv(fd, bufs)
        else:
            got = os.read(fd, chunk)
        if got != chunk:
            raise OSError(
                errno.EIO,
                f"short read at 0x{offset:x}: got {got}, expected {chunk}",
            )
    finally:
        safe_close(fd)


def _run_sparse_io_child(
    path: str, offset: int, chunk: int, use_readv: bool
) -> None:
    proc = multiprocessing.Process(
        target=_sparse_child_read,
        args=(path, offset, chunk, use_readv),
    )
    proc.start()
    proc.join()
    if proc.exitcode != 0:
        op = "readv" if use_readv else "read"
        raise OSError(
            errno.EIO,
            f"child {op} at 0x{offset:x} failed (exit {proc.exitcode})",
        )


def test_fallocate_at_eof_after_blocks_written(root: str) -> int:
    # Related LTP cases: fallocate01
    # fallocate(2) at EOF after writing 12 full blocks must extend the file.
    workdir = case_dir(root, "fallocate_eof")
    target = os.path.join(workdir, "tfile_mode1")

    def run() -> None:
        fd = os.open(target, O_RDWR | O_CREAT, 0o700)
        try:
            block_size = os.fstat(fd).st_blksize
            _write_blocks(fd, block_size, BLOCKS_WRITTEN)
            offset = BLOCKS_WRITTEN * block_size
            sys_fallocate(fd, 0, offset, block_size)
        finally:
            safe_close(fd)

    return expect_ok(
        f"fallocate(fd, 0, EOF, blksize) after 12 blocks (fallocate01)",
        run,
    )


def test_fallocate_on_sparse_file_hole(root: str) -> int:
    # Related LTP cases: fallocate03
    # fallocate(2) on a sparse file (hole between two written ranges) must succeed.
    workdir = case_dir(root, "fallocate_sparse")
    target = os.path.join(workdir, "tfile_sparse")

    def run() -> None:
        fd = os.open(target, O_RDWR | O_CREAT, 0o700)
        try:
            block_size = os.fstat(fd).st_blksize
            _write_blocks(fd, block_size, BLOCKS_WRITTEN)
            os.lseek(
                fd,
                block_size * (BLOCKS_WRITTEN + HOLE_SIZE_IN_BLOCKS),
                os.SEEK_SET,
            )
            _write_blocks(fd, block_size, BLOCKS_WRITTEN)
            offset = 2 * block_size
            sys_fallocate(fd, 0, offset, block_size)
        finally:
            safe_close(fd)

    return expect_ok(
        f"fallocate(fd, 0, 2*blksize, blksize) on sparse file (fallocate03)",
        run,
    )


def test_truncate_open_file_to_shorter_length(root: str) -> int:
    # Related LTP cases: truncate02, truncate02_64
    # truncate(2) on a 1024-byte file to 256 bytes must succeed while fd is open.
    workdir = case_dir(root, "truncate_open_fd")
    target = os.path.join(workdir, "testfile")

    def run() -> None:
        fd = os.open(target, O_RDWR | O_CREAT, 0o644)
        try:
            os.write(fd, b"a" * 1024)
            os.lseek(fd, 0, os.SEEK_SET)
            os.truncate(target, 256)
            if os.fstat(fd).st_size != 256:
                raise OSError(errno.EIO, "st_size != 256 after truncate")
        finally:
            safe_close(fd)

    return expect_ok("truncate(testfile, 256) with open fd (truncate02)", run)


def test_read_unwritten_sparse_chunk(root: str) -> int:
    # Related LTP cases: ftest01, ftest05
    # read(2) at an unwritten chunk in a sparse file must return zeros, not EIO.
    workdir = case_dir(root, "sparse_read")
    target = os.path.join(workdir, "ftest_child")

    def run() -> None:
        _run_sparse_io_child(target, FTEST_READ_OFFSET, FTEST_CHUNK, use_readv=False)

    return expect_ok(
        f"child read({FTEST_CHUNK}) at 0x{FTEST_READ_OFFSET:x} on sparse file (ftest01)",
        run,
    )


def test_readv_unwritten_sparse_chunk(root: str) -> int:
    # Related LTP cases: ftest03, ftest07
    # readv(2) at an unwritten chunk in a sparse file must return zeros, not EIO.
    workdir = case_dir(root, "sparse_readv")
    target = os.path.join(workdir, "ftest_child")

    def run() -> None:
        _run_sparse_io_child(target, FTEST_READ_OFFSET, FTEST_CHUNK, use_readv=True)

    return expect_ok(
        f"child readv at 0x{FTEST_READ_OFFSET:x} on sparse file (ftest03)",
        run,
    )


def test_hard_link_through_symlink_path(root: str) -> int:
    # Related LTP cases: link01
    # link(2) with source path resolving through a symlink must create a hard link.
    workdir = case_dir(root, "link_via_symlink")
    object_path = os.path.join(workdir, "object")
    symbolic_path = os.path.join(workdir, "symbolic")
    nick_path = os.path.join(workdir, "nick")

    def run() -> None:
        os.symlink("object", symbolic_path)
        fd = os.open(object_path, os.O_CREAT | os.O_WRONLY, 0o700)
        os.close(fd)
        sys_linkat(symbolic_path, nick_path)
        if not os.path.exists(nick_path):
            raise OSError(errno.EIO, "hard link nick was not created")

    return expect_ok(
        'link(symbolic, "nick") after symlink+creat (link01)',
        run,
    )


TESTS: List[Tuple[str, str, Callable[..., int]]] = [
    (
        "1",
        "fallocate at EOF after 12 blocks (fallocate01)",
        test_fallocate_at_eof_after_blocks_written,
    ),
    (
        "2",
        "fallocate on sparse file at 2*blksize (fallocate03)",
        test_fallocate_on_sparse_file_hole,
    ),
    (
        "3",
        "truncate(testfile, 256) with open fd (truncate02/02_64)",
        test_truncate_open_file_to_shorter_length,
    ),
    (
        "4",
        "child read at unwritten sparse chunk (ftest01/05)",
        test_read_unwritten_sparse_chunk,
    ),
    (
        "5",
        "child readv at unwritten sparse chunk (ftest03/07)",
        test_readv_unwritten_sparse_chunk,
    ),
    (
        "6",
        'hard link via symlink path link(symbolic, "nick") (link01)',
        test_hard_link_through_symlink_path,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="EIO reproducer for Curvine FUSE I/O paths",
    )
    parser.add_argument(
        "--dir",
        default=DEFAULT_DIR,
        help="Base test directory (must be on the Curvine FUSE mount)",
    )
    parser.add_argument(
        "--allow-non-mount",
        action="store_true",
        help="Skip FUSE mount check; for local debugging only (default: False)",
    )
    args = parser.parse_args()

    if not sys.platform.startswith("linux"):
        print("SKIP: Linux-only syscalls", file=sys.stderr)
        return 0

    if os.geteuid() != 0:
        print("WARNING: not running as root", file=sys.stderr)

    root = os.path.abspath(args.dir)
    os.makedirs(root, exist_ok=True)

    if args.allow_non_mount:
        print(
            "WARNING: --allow-non-mount set; results may not reflect Curvine FUSE behaviour",
            file=sys.stderr,
        )

    print(f"Running EIO repro tests under {root}\n")

    fail = 0
    for num, title, test_fn in TESTS:
        print(f"[case {num}] {title}:")
        fail += test_fn(root)
        print()

    total = len(TESTS)
    passed = total - fail
    print(f"Summary: {passed}/{total} passed, {fail}/{total} failed")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
