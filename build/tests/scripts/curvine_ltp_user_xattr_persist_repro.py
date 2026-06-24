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
user.* extended attributes not persisted on Curvine FUSE — reproducer for LTP failures.

Root cause: setxattr(2)/lsetxattr(2)/fsetxattr(2) on user.* (and other) namespaces
appear to succeed, but subsequent getxattr(2)/lgetxattr(2)/fgetxattr(2) return
ENODATA as if the attribute was never stored.  removexattr(2) on a missing
attribute incorrectly succeeds instead of returning ENODATA.

Covered LTP cases (each line is one minimal repro scenario):
  getxattr01 — setxattr(user.testkey) then getxattr must return the stored value (TFAIL ENODATA)
  getxattr01 — setxattr(user.testkey) then getxattr(buf=1) must return ERANGE (TFAIL ENODATA)
  getxattr03 — setxattr(user.testkey) then getxattr(NULL,0) must return value size (TFAIL ENODATA)
  lgetxattr01 — lsetxattr on symlink then lgetxattr must read symlink xattr (TFAIL ENODATA)
  lgetxattr02 — lsetxattr on symlink then lgetxattr(buf=1) must return ERANGE (TFAIL ENODATA)
  fgetxattr03 — fsetxattr(user.testkey) then fgetxattr(NULL,0) must return value size (TFAIL ENODATA)
  removexattr02 — removexattr(user.test) on file without that xattr must fail ENODATA (TFAIL success)

Usage:
    python3 curvine_ltp_user_xattr_persist_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_user_xattr_persist_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import ctypes
import errno
import os
import platform
import shutil
import sys
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"

XATTR_CREATE = 0x1

# getxattr01 / fgetxattr03
USER_TEST_KEY = "user.testkey"
USER_TEST_VALUE = b"this is a test value"
USER_TEST_VALUE_SIZE = len(USER_TEST_VALUE)

# getxattr03
USER_TEST_KEY_03 = "user.testkey"
USER_TEST_VALUE_03 = b"test value"
USER_TEST_VALUE_SIZE_03 = len(USER_TEST_VALUE_03)

# lgetxattr01
SECURITY_KEY1 = "security.ltptest1"
SECURITY_KEY2 = "security.ltptest2"
VALUE1 = b"test1"
VALUE2 = b"test2"

# lgetxattr02
SECURITY_KEY = "security.ltptest"
SECURITY_VALUE = b"this is a test value"

# removexattr02
REMOVE_KEY = "user.test"


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


def case_dir(root: str, name: str) -> str:
    path = os.path.join(root, name)
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    return path


def _syscall_nr(name: str) -> int:
    table = {
        "setxattr": {"x86_64": 188, "aarch64": 5},
        "lsetxattr": {"x86_64": 189, "aarch64": 6},
        "fsetxattr": {"x86_64": 190, "aarch64": 7},
        "getxattr": {"x86_64": 191, "aarch64": 8},
        "lgetxattr": {"x86_64": 192, "aarch64": 9},
        "fgetxattr": {"x86_64": 193, "aarch64": 10},
        "removexattr": {"x86_64": 197, "aarch64": 14},
    }
    machine = platform.machine()
    if name not in table or machine not in table[name]:
        raise OSError(
            errno.ENOSYS,
            f"unsupported architecture for {name}: {machine}",
        )
    return table[name][machine]


def _raw_syscall(nr: int, *args: int) -> int:
    libc = ctypes.CDLL(None, use_errno=True)
    syscall_fn = libc.syscall
    syscall_fn.restype = ctypes.c_long
    ret = int(syscall_fn(nr, *args))
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
    return ret


def sys_setxattr(path: str, name: str, value: bytes, flags: int = 0) -> None:
    """setxattr(path, name, value, size, flags) via raw syscall."""
    _raw_syscall(
        _syscall_nr("setxattr"),
        ctypes.c_char_p(path.encode()),
        ctypes.c_char_p(name.encode()),
        ctypes.c_char_p(value),
        len(value),
        flags,
    )


def sys_lsetxattr(path: str, name: str, value: bytes, flags: int = 0) -> None:
    """lsetxattr(path, name, value, size, flags) via raw syscall."""
    _raw_syscall(
        _syscall_nr("lsetxattr"),
        ctypes.c_char_p(path.encode()),
        ctypes.c_char_p(name.encode()),
        ctypes.c_char_p(value),
        len(value),
        flags,
    )


def sys_fsetxattr(fd: int, name: str, value: bytes, flags: int = 0) -> None:
    """fsetxattr(fd, name, value, size, flags) via raw syscall."""
    _raw_syscall(
        _syscall_nr("fsetxattr"),
        fd,
        ctypes.c_char_p(name.encode()),
        ctypes.c_char_p(value),
        len(value),
        flags,
    )


def sys_getxattr(path: str, name: str, value: int, size: int) -> int:
    """getxattr(path, name, value, size) via raw syscall."""
    return _raw_syscall(
        _syscall_nr("getxattr"),
        ctypes.c_char_p(path.encode()),
        ctypes.c_char_p(name.encode()),
        value,
        size,
    )


def sys_lgetxattr(path: str, name: str, value: int, size: int) -> int:
    """lgetxattr(path, name, value, size) via raw syscall."""
    return _raw_syscall(
        _syscall_nr("lgetxattr"),
        ctypes.c_char_p(path.encode()),
        ctypes.c_char_p(name.encode()),
        value,
        size,
    )


def sys_fgetxattr(fd: int, name: str, value: int, size: int) -> int:
    """fgetxattr(fd, name, value, size) via raw syscall."""
    return _raw_syscall(
        _syscall_nr("fgetxattr"),
        fd,
        ctypes.c_char_p(name.encode()),
        value,
        size,
    )


def sys_removexattr(path: str, name: str) -> None:
    """removexattr(path, name) via raw syscall."""
    _raw_syscall(
        _syscall_nr("removexattr"),
        ctypes.c_char_p(path.encode()),
        ctypes.c_char_p(name.encode()),
    )


def test_getxattr_reads_user_xattr_value(root: str) -> int:
    # Related LTP cases: getxattr01
    # getxattr(2) after setxattr(2) on user.* must return the stored attribute value.
    if os.geteuid() != 0:
        return skip("getxattr reads user xattr", "requires root")

    workdir = case_dir(root, "getxattr_read_value")
    target = os.path.join(workdir, "testfile")
    fd = os.open(target, os.O_CREAT | os.O_RDWR, 0o644)
    os.close(fd)
    sys_setxattr(target, USER_TEST_KEY, USER_TEST_VALUE, flags=XATTR_CREATE)

    def run() -> None:
        buf = ctypes.create_string_buffer(64)
        ret = sys_getxattr(target, USER_TEST_KEY, ctypes.addressof(buf), 63)
        if ret != USER_TEST_VALUE_SIZE:
            raise OSError(
                errno.EINVAL,
                f"getxattr returned size {ret}, expected {USER_TEST_VALUE_SIZE}",
            )
        if bytes(buf[:ret]) != USER_TEST_VALUE:
            raise OSError(errno.EINVAL, "getxattr returned unexpected value")

    return expect_ok(
        "setxattr(user.testkey) then getxattr returns stored value (getxattr01)",
        run,
    )


def test_getxattr_user_xattr_small_buffer_erange(root: str) -> int:
    # Related LTP cases: getxattr01
    # getxattr(2) with buffer smaller than the attribute must fail with ERANGE.
    if os.geteuid() != 0:
        return skip("getxattr small buffer ERANGE", "requires root")

    workdir = case_dir(root, "getxattr_erange")
    target = os.path.join(workdir, "testfile")
    fd = os.open(target, os.O_CREAT | os.O_RDWR, 0o644)
    os.close(fd)
    sys_setxattr(target, USER_TEST_KEY, USER_TEST_VALUE, flags=XATTR_CREATE)

    def run() -> None:
        buf = ctypes.create_string_buffer(1)
        sys_getxattr(target, USER_TEST_KEY, ctypes.addressof(buf), 1)

    return expect_errno(
        "setxattr(user.testkey) then getxattr(buf=1) returns ERANGE (getxattr01)",
        run,
        errno.ERANGE,
    )


def test_getxattr_null_buffer_returns_attr_size(root: str) -> int:
    # Related LTP cases: getxattr03
    # getxattr(2) with NULL buffer and size 0 must return the attribute value length.
    if os.geteuid() != 0:
        return skip("getxattr null buffer size", "requires root")

    workdir = case_dir(root, "getxattr_null_size")
    target = os.path.join(workdir, "testfile")
    fd = os.open(target, os.O_CREAT | os.O_RDWR, 0o644)
    os.close(fd)
    sys_setxattr(target, USER_TEST_KEY_03, USER_TEST_VALUE_03, flags=XATTR_CREATE)

    def run() -> None:
        ret = sys_getxattr(target, USER_TEST_KEY_03, 0, 0)
        if ret != USER_TEST_VALUE_SIZE_03:
            raise OSError(
                errno.EINVAL,
                f"getxattr(NULL,0) returned {ret}, expected {USER_TEST_VALUE_SIZE_03}",
            )

    return expect_ok(
        "setxattr(user.testkey) then getxattr(NULL,0) returns value size (getxattr03)",
        run,
    )


def test_lgetxattr_reads_symlink_xattr(root: str) -> int:
    # Related LTP cases: lgetxattr01
    # lgetxattr(2) on a symlink must return the xattr set on the link itself.
    if os.geteuid() != 0:
        return skip("lgetxattr reads symlink xattr", "requires root")

    workdir = case_dir(root, "lgetxattr_symlink")
    testfile = os.path.join(workdir, "testfile")
    symlink = os.path.join(workdir, "symlink")
    fd = os.open(testfile, os.O_CREAT | os.O_RDWR, 0o644)
    os.close(fd)
    os.symlink("testfile", symlink)
    sys_lsetxattr(testfile, SECURITY_KEY1, VALUE1, flags=XATTR_CREATE)
    sys_lsetxattr(symlink, SECURITY_KEY2, VALUE2, flags=XATTR_CREATE)

    def run() -> None:
        buf = ctypes.create_string_buffer(64)
        ret = sys_lgetxattr(symlink, SECURITY_KEY2, ctypes.addressof(buf), 64)
        if ret != len(VALUE2):
            raise OSError(
                errno.EINVAL,
                f"lgetxattr returned size {ret}, expected {len(VALUE2)}",
            )
        if bytes(buf[:ret]) != VALUE2:
            raise OSError(errno.EINVAL, "lgetxattr returned unexpected value")

    return expect_ok(
        "lsetxattr on symlink then lgetxattr returns link xattr (lgetxattr01)",
        run,
    )


def test_lgetxattr_symlink_small_buffer_erange(root: str) -> int:
    # Related LTP cases: lgetxattr02
    # lgetxattr(2) with buffer too small for the stored xattr must fail with ERANGE.
    if os.geteuid() != 0:
        return skip("lgetxattr symlink ERANGE", "requires root")

    workdir = case_dir(root, "lgetxattr_erange")
    testfile = os.path.join(workdir, "testfile")
    symlink = os.path.join(workdir, "symlink")
    fd = os.open(testfile, os.O_CREAT | os.O_RDWR, 0o644)
    os.close(fd)
    os.symlink("testfile", symlink)
    sys_lsetxattr(symlink, SECURITY_KEY, SECURITY_VALUE, flags=XATTR_CREATE)

    def run() -> None:
        buf = ctypes.create_string_buffer(1)
        sys_lgetxattr(symlink, SECURITY_KEY, ctypes.addressof(buf), 1)

    return expect_errno(
        "lsetxattr on symlink then lgetxattr(buf=1) returns ERANGE (lgetxattr02)",
        run,
        errno.ERANGE,
    )


def test_fgetxattr_null_buffer_returns_attr_size(root: str) -> int:
    # Related LTP cases: fgetxattr03
    # fgetxattr(2) with NULL buffer and size 0 must return the attribute value length.
    if os.geteuid() != 0:
        return skip("fgetxattr null buffer size", "requires root")

    workdir = case_dir(root, "fgetxattr_null_size")
    target = os.path.join(workdir, "testfile")
    wfd = os.open(target, os.O_CREAT | os.O_RDWR, 0o644)
    sys_fsetxattr(wfd, USER_TEST_KEY, USER_TEST_VALUE, flags=XATTR_CREATE)
    os.close(wfd)
    fd = os.open(target, os.O_RDONLY)

    def run() -> None:
        ret = sys_fgetxattr(fd, USER_TEST_KEY, 0, 0)
        if ret != USER_TEST_VALUE_SIZE:
            raise OSError(
                errno.EINVAL,
                f"fgetxattr(NULL,0) returned {ret}, expected {USER_TEST_VALUE_SIZE}",
            )

    try:
        return expect_ok(
            "fsetxattr(user.testkey) then fgetxattr(NULL,0) returns value size "
            "(fgetxattr03)",
            run,
        )
    finally:
        os.close(fd)


def test_removexattr_missing_user_xattr_enodata(root: str) -> int:
    # Related LTP cases: removexattr02
    # removexattr(2) on a file without the named user.* xattr must fail with ENODATA.
    if os.geteuid() != 0:
        return skip("removexattr missing xattr", "requires root")

    workdir = case_dir(root, "removexattr_enodata")
    target = os.path.join(workdir, "testfile")
    fd = os.open(target, os.O_CREAT | os.O_RDWR, 0o644)
    os.close(fd)

    def run() -> None:
        sys_removexattr(target, REMOVE_KEY)

    return expect_errno(
        "removexattr(user.test) on file without that xattr returns ENODATA "
        "(removexattr02)",
        run,
        errno.ENODATA,
    )


TESTS: list[tuple[str, str, Callable[..., int]]] = [
    (
        "1",
        "getxattr reads user.testkey value after setxattr (getxattr01)",
        test_getxattr_reads_user_xattr_value,
    ),
    (
        "2",
        "getxattr(buf=1) returns ERANGE after setxattr (getxattr01)",
        test_getxattr_user_xattr_small_buffer_erange,
    ),
    (
        "3",
        "getxattr(NULL,0) returns value size after setxattr (getxattr03)",
        test_getxattr_null_buffer_returns_attr_size,
    ),
    (
        "4",
        "lgetxattr reads xattr set on symlink (lgetxattr01)",
        test_lgetxattr_reads_symlink_xattr,
    ),
    (
        "5",
        "lgetxattr(buf=1) on symlink returns ERANGE (lgetxattr02)",
        test_lgetxattr_symlink_small_buffer_erange,
    ),
    (
        "6",
        "fgetxattr(NULL,0) returns value size after fsetxattr (fgetxattr03)",
        test_fgetxattr_null_buffer_returns_attr_size,
    ),
    (
        "7",
        "removexattr on missing user.test returns ENODATA (removexattr02)",
        test_removexattr_missing_user_xattr_enodata,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="user.* xattr not persisted on Curvine FUSE — reproducer",
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

    print(f"Running user-xattr-persist reproducer under {root}\n")

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
