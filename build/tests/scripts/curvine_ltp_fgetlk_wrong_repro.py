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
F_GETLK returns wrong lock metadata on Curvine FUSE — reproducer for LTP failures.

Root cause: when a child process issues F_GETLK(2) against a file whose parent
holds POSIX byte-range locks, Curvine FUSE reports the wrong l_type, l_start,
l_len, and l_pid (often the write-lock region at offset 10 instead of the
expected read/write region, or F_UNLCK with pid 0 when a lock is present).

Covered LTP cases (each line is one minimal repro scenario):
  fcntl11, fcntl11_64 — write lock at 10:5 plus read lock at 1:5; F_GETLK for a
      whole-file write probe must report the read lock at 1:5 held by parent.
  fcntl19, fcntl19_64 — write lock 10:5 with partial unlock 5:6; F_GETLK must
      report the surviving write-lock fragment at 11:4.
  fcntl20, fcntl20_64 — read lock 10:5 with partial unlock 5:6; F_GETLK must
      report the surviving read-lock fragment at 11:4.
  fcntl21, fcntl21_64 — read lock 10:5 plus write lock 1:5; F_GETLK whole-file
      write probe must report the nearer write lock at 1:5 held by parent.

Usage:
    python3 curvine_ltp_fgetlk_wrong_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_fgetlk_wrong_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import errno
import fcntl
import multiprocessing as mp
import os
import shutil
import struct
import sys
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"

F_RDLCK = fcntl.F_RDLCK
F_WRLCK = fcntl.F_WRLCK
F_UNLCK = fcntl.F_UNLCK
SEEK_SET = 0
F_GETLK = fcntl.F_GETLK
F_SETLK = fcntl.F_SETLK
FLOCK_STRUCT = "hhqqi"
FLOCK_SIZE = 32
STOP = -16  # 0xFFF0 as signed short (LTP fcntl11 stop sentinel)


def pack_flock(lock_type, start, length, pid=0):
    raw = struct.pack(FLOCK_STRUCT, lock_type, SEEK_SET, start, length, pid)
    return raw + b"\0" * (FLOCK_SIZE - len(raw))


def sys_fcntl(fd, cmd, flock_buf):
    # fcntl(2) via libc — same packed flock layout as LTP fcntl11.
    result = fcntl.fcntl(fd, cmd, flock_buf)
    if isinstance(result, int):
        if result < 0:
            raise OSError(errno.EINVAL, "fcntl failed")
        return flock_buf
    return result


def unpack_flock(buf):
    typ, whence, start, length, pid = struct.unpack(
        FLOCK_STRUCT, buf[: struct.calcsize(FLOCK_STRUCT)]
    )
    return typ, whence, start, length, pid


def setlk(fd, lock_type, start, length):
    sys_fcntl(fd, F_SETLK, pack_flock(lock_type, start, length))


def unlock_all(fd: int) -> None:
    setlk(fd, F_UNLCK, 0, 0)


def expect_flock(
    name: str,
    got: bytes,
    exp_type: int,
    exp_start: int,
    exp_len: int,
    exp_pid: int,
) -> None:
    typ, whence, start, length, pid = unpack_flock(got)
    if whence != SEEK_SET:
        raise OSError(
            errno.EIO,
            f"{name}: l_whence expected {SEEK_SET} got {whence}",
        )
    if typ != exp_type:
        raise OSError(
            errno.EIO,
            f"{name}: l_type expected {exp_type} got {typ}",
        )
    if start != exp_start:
        raise OSError(
            errno.EIO,
            f"{name}: l_start expected {exp_start} got {start}",
        )
    if length != exp_len:
        raise OSError(
            errno.EIO,
            f"{name}: l_len expected {exp_len} got {length}",
        )
    if pid != exp_pid:
        raise OSError(
            errno.EIO,
            f"{name}: l_pid expected {exp_pid} got {pid}",
        )


def _fgetlk_child_main(path: str, req_r: int, resp_w: int) -> None:
    fd = os.open(path, os.O_RDWR)
    req = os.fdopen(req_r, "rb", buffering=0)
    resp = os.fdopen(resp_w, "wb", buffering=0)
    try:
        while True:
            query = req.read(FLOCK_SIZE)
            if not query or len(query) < FLOCK_SIZE:
                break
            typ = struct.unpack(FLOCK_STRUCT, query[: struct.calcsize(FLOCK_STRUCT)])[0]
            if typ == STOP:
                break
            result = sys_fcntl(fd, F_GETLK, query)
            resp.write(result)
    finally:
        os.close(fd)
        req.close()
        resp.close()


class FGetlkProxy:
    """Child process that runs F_GETLK on the shared file (LTP fcntl11 pattern)."""

    def __init__(self, path: str) -> None:
        self._path = path
        parent_req_r, child_req_w = os.pipe()
        child_resp_r, parent_resp_w = os.pipe()
        self._req_w = child_req_w
        self._resp_r = child_resp_r
        self._proc = mp.Process(
            target=_fgetlk_child_main,
            args=(path, parent_req_r, parent_resp_w),
            daemon=True,
        )
        self._proc.start()
        os.close(parent_req_r)
        os.close(parent_resp_w)

    def getlk(self, lock_type: int, start: int, length: int) -> bytes:
        os.write(self._req_w, pack_flock(lock_type, start, length))
        data = os.read(self._resp_r, FLOCK_SIZE)
        if len(data) != FLOCK_SIZE:
            raise OSError(errno.EIO, "short F_GETLK response from child")
        return data

    def close(self) -> None:
        os.write(self._req_w, pack_flock(STOP, 0, 0))
        self._proc.join(timeout=5)
        if self._proc.is_alive():
            self._proc.terminate()
            self._proc.join()
        os.close(self._req_w)
        os.close(self._resp_r)


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


def _prepare_lock_file(workdir):
    path = os.path.join(workdir, "lockfile")
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    os.ftruncate(fd, 27)
    return path, fd


def test_fgetlk_read_lock_around_write_lock(root: str) -> int:
    # Related LTP cases: fcntl11, fcntl11_64
    # F_GETLK must report the read lock at 1:5 when write+read locks coexist.
    workdir = case_dir(root, "fgetlk_read_around_write")
    path, fd = _prepare_lock_file(workdir)
    holder_pid = os.getpid()
    proxy = FGetlkProxy(path)

    def run() -> None:
        setlk(fd, F_WRLCK, 10, 5)
        setlk(fd, F_RDLCK, 1, 5)
        got = proxy.getlk(F_WRLCK, 0, 0)
        expect_flock(
            "F_GETLK reports read lock at 1:5",
            got,
            F_RDLCK,
            1,
            5,
            holder_pid,
        )

    try:
        return expect_ok("F_GETLK read lock around write lock (fcntl11 block 1)", run)
    finally:
        proxy.close()
        unlock_all(fd)
        os.close(fd)


def test_fgetlk_trimmed_write_lock_after_partial_unlock(root: str) -> int:
    # Related LTP cases: fcntl19, fcntl19_64
    # Partial unlock before a write lock must leave fragment 11:4 visible to F_GETLK.
    workdir = case_dir(root, "fgetlk_write_partial_unlock")
    path, fd = _prepare_lock_file(workdir)
    holder_pid = os.getpid()
    proxy = FGetlkProxy(path)

    def run() -> None:
        setlk(fd, F_WRLCK, 10, 5)
        setlk(fd, F_UNLCK, 5, 6)
        got = proxy.getlk(F_WRLCK, 0, 0)
        expect_flock(
            "F_GETLK reports surviving write lock at 11:4",
            got,
            F_WRLCK,
            11,
            4,
            holder_pid,
        )

    try:
        return expect_ok(
            "F_GETLK trimmed write lock after partial unlock (fcntl19 block 2)",
            run,
        )
    finally:
        proxy.close()
        unlock_all(fd)
        os.close(fd)


def test_fgetlk_trimmed_read_lock_after_partial_unlock(root: str) -> int:
    # Related LTP cases: fcntl20, fcntl20_64
    # Partial unlock before a read lock must leave fragment 11:4 visible to F_GETLK.
    workdir = case_dir(root, "fgetlk_read_partial_unlock")
    path, fd = _prepare_lock_file(workdir)
    holder_pid = os.getpid()
    proxy = FGetlkProxy(path)

    def run() -> None:
        setlk(fd, F_RDLCK, 10, 5)
        setlk(fd, F_UNLCK, 5, 6)
        got = proxy.getlk(F_WRLCK, 0, 0)
        expect_flock(
            "F_GETLK reports surviving read lock at 11:4",
            got,
            F_RDLCK,
            11,
            4,
            holder_pid,
        )

    try:
        return expect_ok(
            "F_GETLK trimmed read lock after partial unlock (fcntl20 block 2)",
            run,
        )
    finally:
        proxy.close()
        unlock_all(fd)
        os.close(fd)


def test_fgetlk_write_lock_around_read_lock(root: str) -> int:
    # Related LTP cases: fcntl21, fcntl21_64
    # F_GETLK must report the nearer write lock at 1:5 when read+write locks coexist.
    workdir = case_dir(root, "fgetlk_write_around_read")
    path, fd = _prepare_lock_file(workdir)
    holder_pid = os.getpid()
    proxy = FGetlkProxy(path)

    def run() -> None:
        setlk(fd, F_RDLCK, 10, 5)
        setlk(fd, F_WRLCK, 1, 5)
        got = proxy.getlk(F_WRLCK, 0, 0)
        expect_flock(
            "F_GETLK reports write lock at 1:5",
            got,
            F_WRLCK,
            1,
            5,
            holder_pid,
        )

    try:
        return expect_ok(
            "F_GETLK write lock around read lock (fcntl21 block 3)",
            run,
        )
    finally:
        proxy.close()
        unlock_all(fd)
        os.close(fd)


TESTS = [
    (
        "1",
        "F_GETLK reports read lock when read+write locks coexist (fcntl11)",
        test_fgetlk_read_lock_around_write_lock,
    ),
    (
        "2",
        "F_GETLK reports trimmed write lock after partial unlock (fcntl19)",
        test_fgetlk_trimmed_write_lock_after_partial_unlock,
    ),
    (
        "3",
        "F_GETLK reports trimmed read lock after partial unlock (fcntl20)",
        test_fgetlk_trimmed_read_lock_after_partial_unlock,
    ),
    (
        "4",
        "F_GETLK reports write lock when write+read locks coexist (fcntl21)",
        test_fgetlk_write_lock_around_read_lock,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="F_GETLK wrong lock metadata on Curvine FUSE — reproducer",
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
            "WARNING: --allow-non-mount set; results may not reflect Curvine FUSE",
            file=sys.stderr,
        )

    print(f"Running F_GETLK wrong-lock reproducer under {root}\n")

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
