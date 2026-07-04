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
Directory-entry visibility after O_CREAT on Curvine FUSE mounts.

Root cause: Curvine FUSE sometimes accepts open(2) with O_CREAT (and may even
allow writes through the returned fd) but the new directory entry is not
visible to subsequent lookups.  A later open(2) on the same path then returns
ENOENT even though the create appeared to succeed — dentry persistence or
cache-coherency bug.

Covered LTP cases (each line is one minimal repro scenario):
  fcntl23, fcntl23_64, fcntl25, fcntl25_64, ftruncate03, sendfile03,
      sendfile03_64, io_submit01, fallocate02, readlinkat01 — setup
      open(path, O_RDONLY|O_CREAT, mode) with flag 64 must create a visible file
  linkat01, symlinkat01 — setup open(olddir/oldfile, O_CREAT|O_EXCL, 0600) in a
      subdirectory must succeed and leave a visible file
  fcntl16, fcntl16_64 — block-2 open(path, O_RDWR|O_CREAT|O_TRUNC, 02600) must
      create a visible mandatory-lock test file
  pwritev202, pwritev202_64 — after file1 exists, setup open(file2,
      O_RDONLY|O_CREAT, 0644) must create a second visible file
  stream03 — after unlink, fopen/open with a+ (O_RDWR|O_APPEND|O_CREAT) must
      recreate a visible file (block-1 TFAIL)
  writev07 (partial) — tst_fill_file-created testfile must remain openable with
      O_RDWR after a prior writev EFAULT iteration
  fallocate02 (FNAMER path) — same O_RDONLY|O_CREAT setup as other flag-64 cases

Usage:
    python3 curvine_ltp_creat_enoent_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_creat_enoent_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import ctypes
import errno
import os
import platform
import shutil
import sys
from typing import Callable, List, Tuple

DEFAULT_DIR = "/curvine-fuse/fuse-test"

AT_FDCWD = -100
O_RDONLY = 0
O_WRONLY = 1
O_RDWR = 2
O_CREAT = 0o100
O_EXCL = 0o200
O_TRUNC = 0o1000
O_APPEND = 0o2000

# LTP logs show decimal 64 / 192 / 578 for these flag combinations.
LTP_O_RDONLY_O_CREAT = 64
LTP_O_CREAT_O_EXCL = 192
LTP_O_RDWR_O_CREAT_O_TRUNC = 578


def _syscall_nr_openat() -> int:
    table = {
        "x86_64": 257,
        "aarch64": 56,
    }
    machine = platform.machine()
    if machine not in table:
        raise OSError(
            errno.ENOSYS,
            f"unsupported architecture for openat: {machine}",
        )
    return table[machine]


def sys_openat(path: str, flags: int, mode: int = 0) -> int:
    """openat(AT_FDCWD, path, flags, mode) via raw syscall."""
    libc = ctypes.CDLL(None, use_errno=True)
    syscall = libc.syscall
    syscall.argtypes = [
        ctypes.c_long,
        ctypes.c_long,
        ctypes.c_long,
        ctypes.c_long,
        ctypes.c_long,
        ctypes.c_long,
    ]
    syscall.restype = ctypes.c_long

    path_buf = path.encode()
    ret = int(
        syscall(
            _syscall_nr_openat(),
            AT_FDCWD,
            ctypes.c_char_p(path_buf),
            flags,
            mode,
        )
    )
    if ret < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))
    return ret


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


def _reopen_rdonly(path: str) -> None:
    fd = sys_openat(path, O_RDONLY)
    os.close(fd)


def _reopen_rdwr(path: str) -> None:
    fd = sys_openat(path, O_RDWR)
    os.close(fd)


def test_open_ocreat_only_flag_creates_visible_dentry(root: str) -> int:
    # Related LTP cases: fcntl23, fcntl23_64, fcntl25, fcntl25_64, ftruncate03,
    #                    sendfile03, sendfile03_64, io_submit01, fallocate02,
    #                    readlinkat01
    # LTP setup uses open(path, O_RDONLY|O_CREAT, mode) — numeric flag 64 — to
    # create the test file; a follow-up open must find the dentry.
    workdir = case_dir(root, "ocreat_flag64")
    target = os.path.join(workdir, "tfile")

    def run() -> None:
        fd = sys_openat(target, LTP_O_RDONLY_O_CREAT, 0o777)
        os.close(fd)
        _reopen_rdonly(target)

    return expect_ok(
        "open(path, O_RDONLY|O_CREAT=64, 0777) then reopen O_RDONLY",
        run,
    )


def test_open_ocreat_excl_in_subdir_creates_visible_dentry(root: str) -> int:
    # Related LTP cases: linkat01, symlinkat01
    # LTP setup open(olddir/oldfile, O_CREAT|O_EXCL, 0600) must create the file
    # inside a freshly created subdirectory.
    workdir = case_dir(root, "ocreat_excl_subdir")
    subdir = os.path.join(workdir, "olddir")
    os.makedirs(subdir, mode=0o700)
    target = os.path.join(subdir, "oldfile")

    def run() -> None:
        fd = sys_openat(target, LTP_O_CREAT_O_EXCL, 0o600)
        os.close(fd)
        _reopen_rdonly(target)

    return expect_ok(
        "open(subdir/file, O_CREAT|O_EXCL=192, 0600) then reopen",
        run,
    )


def test_open_rdwr_creat_trunc_setgid_mode_creates_visible_dentry(root: str) -> int:
    # Related LTP cases: fcntl16, fcntl16_64
    # fcntl16 block-2 mandatory-lock setup: open(tmp, O_RDWR|O_CREAT|O_TRUNC, 02600).
    workdir = case_dir(root, "fc16_block2_create")
    target = os.path.join(workdir, "fcntl4")

    def run() -> None:
        fd = sys_openat(target, LTP_O_RDWR_O_CREAT_O_TRUNC, 0o2600)
        os.write(fd, b"0123456789")
        os.close(fd)
        _reopen_rdwr(target)

    return expect_ok(
        "open(path, O_RDWR|O_CREAT|O_TRUNC=578, 02600) then reopen O_RDWR",
        run,
    )


def test_ocreat_write_close_then_reopen_rdonly(root: str) -> int:
    # Related LTP cases: fallocate02 (FNAMEW path), sendfile03 (out_file path)
    # Generic pattern: O_CREAT|O_WRONLY create + write, close, then O_RDONLY open.
    workdir = case_dir(root, "creat_write_reopen")
    target = os.path.join(workdir, "payload")

    def run() -> None:
        fd = sys_openat(target, O_WRONLY | O_CREAT, 0o600)
        os.write(fd, b"curvine-creat-enoent\n")
        os.close(fd)
        _reopen_rdonly(target)

    return expect_ok(
        "O_WRONLY|O_CREAT write+close then reopen O_RDONLY",
        run,
    )


def test_second_file_ocreat_only_after_first_file_exists(root: str) -> int:
    # Related LTP cases: pwritev202, pwritev202_64
    # pwritev202 setup creates file1, then open(file2, O_RDONLY|O_CREAT, 0644).
    workdir = case_dir(root, "pwritev202_setup")
    file1 = os.path.join(workdir, "file1")
    file2 = os.path.join(workdir, "file2")

    def run() -> None:
        fd1 = sys_openat(file1, O_RDWR | O_CREAT, 0o644)
        os.ftruncate(fd1, os.sysconf("SC_PAGE_SIZE"))
        os.close(fd1)

        fd2 = sys_openat(file2, LTP_O_RDONLY_O_CREAT, 0o644)
        os.close(fd2)
        _reopen_rdonly(file2)

    return expect_ok(
        "after file1 exists, open(file2, O_RDONLY|O_CREAT=64, 0644) visible",
        run,
    )


def test_unlink_then_open_append_recreates_visible_file(root: str) -> int:
    # Related LTP cases: stream03
    # stream03 block-0 passes with fopen a+, then unlink; block-1 fopen a+ must
    # recreate the file (O_RDWR|O_APPEND|O_CREAT).
    workdir = case_dir(root, "stream03_recreate")
    target = os.path.join(workdir, "stream03")

    def run() -> None:
        fd = sys_openat(target, O_RDWR | O_APPEND | O_CREAT, 0o644)
        os.write(fd, b"abcdefghijklmnopqrstuvwxyz")
        os.close(fd)
        os.unlink(target)

        fd = sys_openat(target, O_RDWR | O_APPEND | O_CREAT, 0o644)
        os.close(fd)
        _reopen_rdonly(target)

    return expect_ok(
        "unlink then open O_RDWR|O_APPEND|O_CREAT (stream03 a+) recreates file",
        run,
    )


def test_prefilled_file_still_openable_after_prior_io(root: str) -> int:
    # Related LTP cases: writev07 (partial — second iteration at offset 65)
    # tst_fill_file creates testfile; after first writev EFAULT iteration the
    # file must still be openable with O_RDWR for the next iteration.
    workdir = case_dir(root, "writev07_persist")
    target = os.path.join(workdir, "testfile")
    chunk = 64
    bufsize = chunk * 4

    def run() -> None:
        fd = sys_openat(target, O_RDWR | O_CREAT | O_TRUNC, 0o644)
        pattern = bytes(i % (chunk - 1) for i in range(bufsize))
        os.write(fd, pattern)
        os.lseek(fd, 0, os.SEEK_SET)
        os.close(fd)

        fd = sys_openat(target, O_RDWR, 0o644)
        os.lseek(fd, 0, os.SEEK_SET)
        os.close(fd)

        fd = sys_openat(target, O_RDWR, 0o644)
        os.lseek(fd, 65, os.SEEK_SET)
        os.close(fd)

    return expect_ok(
        "tst_fill_file-like file still openable O_RDWR at offset 65 (writev07)",
        run,
    )


TESTS: List[Tuple[str, str, Callable[..., int]]] = [
    (
        "1",
        "open(path, O_RDONLY|O_CREAT=64) creates visible dentry",
        test_open_ocreat_only_flag_creates_visible_dentry,
    ),
    (
        "2",
        "open(subdir/file, O_CREAT|O_EXCL=192) creates visible dentry",
        test_open_ocreat_excl_in_subdir_creates_visible_dentry,
    ),
    (
        "3",
        "open(path, O_RDWR|O_CREAT|O_TRUNC=578, 02600) creates visible dentry",
        test_open_rdwr_creat_trunc_setgid_mode_creates_visible_dentry,
    ),
    (
        "4",
        "O_WRONLY|O_CREAT write+close then reopen O_RDONLY",
        test_ocreat_write_close_then_reopen_rdonly,
    ),
    (
        "5",
        "second file O_RDONLY|O_CREAT=64 after file1 exists (pwritev202 setup)",
        test_second_file_ocreat_only_after_first_file_exists,
    ),
    (
        "6",
        "unlink then O_RDWR|O_APPEND|O_CREAT recreates file (stream03)",
        test_unlink_then_open_append_recreates_visible_file,
    ),
    (
        "7",
        "prefilled file still openable after prior I/O (writev07)",
        test_prefilled_file_still_openable_after_prior_io,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="O_CREAT dentry visibility / ENOENT reproducer for Curvine FUSE",
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

    print(f"Running O_CREAT dentry visibility tests under {root}\n")

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
