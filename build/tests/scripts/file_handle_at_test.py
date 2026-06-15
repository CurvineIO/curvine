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
name_to_handle_at(2) / open_by_handle_at(2) core tests for Curvine FUSE mounts.

Covers the essential scenarios from doc/design-fuse-name-to-handle-at.md §7.2:
  1. Basic round-trip: encode handle, reopen, read correct content
  2. drop_caches round-trip: reopen after kernel inode cache eviction
  3. Stale handle (optional): delete + recreate must not open the wrong file; skipped
     by default because ESTALE timing depends on FUSE FORGET of the old nodeid

Requires Linux and root (open_by_handle_at needs CAP_DAC_READ_SEARCH).

Usage:
    python3 file_handle_at_test.py --dir /curvine-fuse/fuse-test
    python3 file_handle_at_test.py --dir /curvine-fuse/fuse-test --skip-drop-caches
"""

from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import errno
import os
import platform
import shutil
import sys
from dataclasses import dataclass
from typing import Callable

AT_FDCWD = -100
DEFAULT_DIR = "/curvine-fuse/fuse-test"


class FileHandleHeader(ctypes.Structure):
    _fields_ = [
        ("handle_bytes", ctypes.c_uint32),
        ("handle_type", ctypes.c_int32),
    ]


@dataclass
class FileHandleBuffer:
    buf: ctypes.Array
    mount_id: int

    @property
    def ptr(self) -> ctypes.c_void_p:
        return ctypes.cast(self.buf, ctypes.c_void_p)


_libc: ctypes.CDLL | None = None
_name_to_handle_at_fn: Callable[..., int] | None = None
_open_by_handle_at_fn: Callable[..., int] | None = None


def _syscall_nr(name: str) -> int:
    table: dict[str, dict[str, int]] = {
        "name_to_handle_at": {
            "x86_64": 303,
            "aarch64": 264,
            "armv7l": 370,
            "ppc64le": 345,
            "s390x": 345,
        },
        "open_by_handle_at": {
            "x86_64": 304,
            "aarch64": 265,
            "armv7l": 371,
            "ppc64le": 346,
            "s390x": 346,
        },
    }
    machine = platform.machine()
    if machine not in table[name]:
        raise OSError(
            errno.ENOSYS,
            f"unsupported architecture for {name}: {machine}",
        )
    return table[name][machine]


def _init_libc() -> ctypes.CDLL:
    global _libc, _name_to_handle_at_fn, _open_by_handle_at_fn
    if _libc is not None:
        return _libc

    libname = ctypes.util.find_library("c")
    if not libname:
        raise OSError(errno.ENOENT, "libc not found")
    _libc = ctypes.CDLL(libname, use_errno=True)

    syscall = _libc.syscall
    syscall.argtypes = [
        ctypes.c_long,
        ctypes.c_long,
        ctypes.c_long,
        ctypes.c_long,
        ctypes.c_long,
        ctypes.c_long,
    ]
    syscall.restype = ctypes.c_long

    fn_name = getattr(_libc, "name_to_handle_at", None)
    fn_open = getattr(_libc, "open_by_handle_at", None)

    if fn_name is not None:
        fn_name.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_int),
            ctypes.c_int,
        ]
        fn_name.restype = ctypes.c_int

        def _name_to_handle_at(
            dfd: int,
            pathname: bytes | None,
            handle: ctypes.c_void_p,
            mount_id: ctypes.POINTER(ctypes.c_int),
            flags: int,
        ) -> int:
            return int(fn_name(dfd, pathname, handle, mount_id, flags))

        _name_to_handle_at_fn = _name_to_handle_at
    else:
        nr = _syscall_nr("name_to_handle_at")

        def _name_to_handle_at(
            dfd: int,
            pathname: bytes | None,
            handle: ctypes.c_void_p,
            mount_id: ctypes.POINTER(ctypes.c_int),
            flags: int,
        ) -> int:
            path_ptr = 0
            if pathname is not None:
                path_buf = ctypes.create_string_buffer(pathname)
                path_ptr = ctypes.cast(path_buf, ctypes.c_void_p).value or 0
            return int(
                syscall(
                    nr,
                    dfd,
                    path_ptr,
                    ctypes.cast(handle, ctypes.c_void_p).value or 0,
                    ctypes.cast(mount_id, ctypes.c_void_p).value or 0,
                    flags,
                )
            )

        _name_to_handle_at_fn = _name_to_handle_at

    if fn_open is not None:
        fn_open.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
        fn_open.restype = ctypes.c_int

        def _open_by_handle_at(
            mount_fd: int,
            handle: ctypes.c_void_p,
            flags: int,
        ) -> int:
            return int(fn_open(mount_fd, handle, flags))

        _open_by_handle_at_fn = _open_by_handle_at
    else:
        nr = _syscall_nr("open_by_handle_at")

        def _open_by_handle_at(
            mount_fd: int,
            handle: ctypes.c_void_p,
            flags: int,
        ) -> int:
            return int(
                syscall(
                    nr,
                    mount_fd,
                    ctypes.cast(handle, ctypes.c_void_p).value or 0,
                    flags,
                )
            )

        _open_by_handle_at_fn = _open_by_handle_at
    return _libc


def _path_bytes(pathname: str | bytes) -> bytes:
    if isinstance(pathname, bytes):
        return pathname
    return pathname.encode()


def name_to_handle_at(
    dfd: int,
    pathname: str | bytes,
    handle: FileHandleBuffer,
    name_flags: int = 0,
) -> int:
    _init_libc()
    assert _name_to_handle_at_fn is not None

    mount_id = ctypes.c_int()
    ret = _name_to_handle_at_fn(
        dfd,
        _path_bytes(pathname),
        handle.ptr,
        ctypes.byref(mount_id),
        name_flags,
    )
    if ret == -1:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))

    handle.mount_id = mount_id.value
    return mount_id.value


def open_by_handle_at(mount_fd: int, handle: FileHandleBuffer, flags: int) -> int:
    _init_libc()
    assert _open_by_handle_at_fn is not None

    ret = _open_by_handle_at_fn(mount_fd, handle.ptr, flags)
    if ret == -1:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
    return ret


def _make_handle_buffer(handle_bytes: int, handle_type: int = 0) -> FileHandleBuffer:
    total = ctypes.sizeof(FileHandleHeader) + handle_bytes
    buf = (ctypes.c_byte * total)()
    hdr = FileHandleHeader.from_buffer(buf)
    hdr.handle_bytes = handle_bytes
    hdr.handle_type = handle_type
    return FileHandleBuffer(buf=buf, mount_id=0)


def allocate_file_handle(
    dfd: int,
    pathname: str | bytes,
    name_flags: int = 0,
) -> FileHandleBuffer:
    mount_id = ctypes.c_int()
    hdr = FileHandleHeader(handle_bytes=0, handle_type=0)
    _init_libc()
    assert _name_to_handle_at_fn is not None

    ret = _name_to_handle_at_fn(
        dfd,
        _path_bytes(pathname),
        ctypes.byref(hdr),
        ctypes.byref(mount_id),
        name_flags,
    )
    err = ctypes.get_errno()
    if ret != -1 or err != errno.EOVERFLOW:
        raise OSError(
            err,
            f"probe name_to_handle_at expected EOVERFLOW, ret={ret}",
        )

    fh = _make_handle_buffer(hdr.handle_bytes, hdr.handle_type)
    name_to_handle_at(dfd, pathname, fh, name_flags)
    return fh


def drop_caches() -> None:
    os.sync()
    with open("/proc/sys/vm/drop_caches", "w", encoding="ascii") as f:
        f.write("3\n")


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


def open_on_mount(mount_dir: str, fh: FileHandleBuffer, flags: int) -> int:
    """open_by_handle_at needs an fd on the target filesystem, not AT_FDCWD."""
    mount_fd = os.open(mount_dir, os.O_RDONLY | os.O_DIRECTORY)
    try:
        return open_by_handle_at(mount_fd, fh, flags)
    finally:
        os.close(mount_fd)


def test_basic_round_trip(root: str) -> int:
    workdir = case_dir(root, "basic_round_trip")
    target = os.path.join(workdir, "payload.txt")
    payload = b"curvine-handle-basic\n"
    with open(target, "wb") as f:
        f.write(payload)

    fh = allocate_file_handle(AT_FDCWD, target)

    def run() -> None:
        fd = open_on_mount(workdir, fh, os.O_RDONLY)
        try:
            data = os.read(fd, len(payload))
            if data != payload:
                raise OSError(f"content mismatch: {data!r}")
        finally:
            os.close(fd)

    return expect_ok("basic round-trip", run)


def test_drop_caches_round_trip(root: str, *, skip_drop_caches: bool) -> int:
    if skip_drop_caches:
        return skip("drop_caches round-trip", "--skip-drop-caches set")
    if os.geteuid() != 0:
        return skip("drop_caches round-trip", "requires root for drop_caches")

    workdir = case_dir(root, "drop_caches")
    target = os.path.join(workdir, "payload.txt")
    payload = b"curvine-handle-roundtrip\n"
    with open(target, "wb") as f:
        f.write(payload)

    fh = allocate_file_handle(AT_FDCWD, target)

    def run() -> None:
        drop_caches()
        fd = open_on_mount(workdir, fh, os.O_RDONLY)
        try:
            data = os.read(fd, len(payload))
            if data != payload:
                raise OSError(f"content mismatch: {data!r}")
        finally:
            os.close(fd)

    return expect_ok("drop_caches round-trip", run)


def test_stale_handle_after_recreate(root: str, *, enabled: bool) -> int:
    """Stale-handle semantics after same-path delete + recreate.

    A handle captured before delete must not silently open the replacement file.
    Curvine only returns ESTALE reliably once the old FUSE nodeid is FORGOTTEN;
    without that, open_by_handle_at may succeed against the new file.  This case
    is therefore skipped by default and enabled with --test-stale-handle.
    """
    if not enabled:
        return skip(
            "stale handle after recreate",
            "same-path recreate needs old nodeid FORGET for deterministic ESTALE; "
            "pass --test-stale-handle to run the content check",
        )

    workdir = case_dir(root, "stale_handle")
    target = os.path.join(workdir, "reuse.txt")

    with open(target, "wb") as f:
        f.write(b"original\n")
    fh = allocate_file_handle(AT_FDCWD, target)

    os.remove(target)
    with open(target, "wb") as f:
        f.write(b"replacement\n")

    if os.geteuid() == 0:
        drop_caches()

    def run() -> None:
        try:
            fd = open_on_mount(workdir, fh, os.O_RDONLY)
        except OSError as exc:
            if exc.errno in (errno.ESTALE, errno.ENOENT, errno.EBADF, errno.EIO):
                return
            raise
        try:
            data = os.read(fd, 32)
        finally:
            os.close(fd)
        if data.startswith(b"replacement"):
            raise OSError(
                errno.ESTALE,
                "stale handle opened recreated file (wrong inode)",
            )
        if data.startswith(b"original"):
            raise OSError(
                errno.ESTALE,
                "stale handle still reads deleted file content",
            )
        raise OSError(f"unexpected read from stale handle: {data!r}")

    return expect_ok("stale handle after recreate", run)


TESTS: list[tuple[str, str, Callable[..., int]]] = [
    ("1", "basic round-trip", test_basic_round_trip),
    ("2", "drop_caches round-trip", test_drop_caches_round_trip),
    ("3", "stale handle after recreate", test_stale_handle_after_recreate),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="name_to_handle_at / open_by_handle_at core tests",
    )
    parser.add_argument("--dir", default=DEFAULT_DIR, help="Base test directory")
    parser.add_argument(
        "--skip-drop-caches",
        action="store_true",
        help="Skip drop_caches round-trip (requires root)",
    )
    parser.add_argument(
        "--test-stale-handle",
        action="store_true",
        help="Run stale-handle case (same-path recreate; may fail until generation fix)",
    )
    args = parser.parse_args()

    if not sys.platform.startswith("linux"):
        print("SKIP: Linux-only syscalls", file=sys.stderr)
        return 0

    if os.geteuid() != 0:
        print("WARNING: tests require root (CAP_DAC_READ_SEARCH)", file=sys.stderr)

    root = os.path.abspath(args.dir)
    os.makedirs(root, exist_ok=True)

    print(f"Running file_handle_at tests under {root}\n")

    fail = 0
    for num, title, test_fn in TESTS:
        print(f"[case {num}] {title}:")
        if test_fn is test_drop_caches_round_trip:
            fail += test_fn(root, skip_drop_caches=args.skip_drop_caches)
        elif test_fn is test_stale_handle_after_recreate:
            fail += test_fn(root, enabled=args.test_stale_handle)
        else:
            fail += test_fn(root)
        print()

    total = len(TESTS)
    passed = total - fail
    print(f"Summary: {passed}/{total} passed, {fail}/{total} failed")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
