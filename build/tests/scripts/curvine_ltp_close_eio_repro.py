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
close(2) returning EIO on Curvine FUSE — reproducer for LTP write-back failures.

Root cause: FUSE write-back fails during release/flush when closing an fd after
flock(2) or fcntl(F_SETLEASE) operations.  The lock/lease syscalls themselves
succeed (or fail with the expected errno); only close(2) surfaces EIO.

Covered LTP cases (each line is one minimal repro scenario):
  flock02 — close(fd) must succeed after flock(-1) EBADF then flock(LOCK_NB) EINVAL
      on the same file (LTP runs both error cases before close)
  flock04 — close(fd) must succeed on LTP tc0 (parent LOCK_SH, two forked children)
      after flock02+flock03 precondition on the same FUSE mount (6/17 syscalls-jfs order;
      EIO is intermittent on addda67 — three precondition cycles raise hit rate)
  fcntl32, fcntl32_64 — close(fd) must succeed across the full nine-case
      verify_fcntl loop (EIO observed on close after F_SETLEASE returns EAGAIN)

Usage:
    python3 curvine_ltp_close_eio_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_close_eio_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import ctypes
import errno
import fcntl
import os
import shutil
import sys
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"

# 6/17 syscalls-jfs on addda67: flock04 EIO is intermittent; running flock02
# (close EIO) then flock03 on the same mount raises the hit rate.  Three
# precondition rounds mirror heavy prior-suite pressure without replaying 261 cases.
_FLOCK04_PRECONDITION_CYCLES = 3

LOCK_SH = 1
LOCK_EX = 2
LOCK_NB = 4
LOCK_UN = 8

F_SETLEASE = 1024
F_WRLCK = 1

_libc = None


def _init_libc() -> ctypes.CDLL:
    global _libc
    if _libc is None:
        _libc = ctypes.CDLL(None, use_errno=True)
    return _libc


def sys_flock(fd: int, operation: int) -> None:
    """flock(fd, operation) via libc."""
    ret = _init_libc().flock(fd, operation)
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


def _create_empty_file(path: str, mode: int = 0o666) -> None:
    fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_RDWR, mode)
    os.close(fd)


def _safe_close_ignore_eio(fd: int) -> None:
    try:
        os.close(fd)
    except OSError as exc:
        if exc.errno != errno.EIO:
            raise


def _ltp_flock02_precondition(root: str) -> None:
    """LTP flock02 EBADF+EINVAL on a sibling dir; close EIO poisons the mount."""
    workdir = os.path.join(root, "_ltp_pre_flock02")
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir)
    target = os.path.join(workdir, "testfile")
    _create_empty_file(target, 0o666)

    fd = os.open(target, os.O_RDWR)
    try:
        sys_flock(-1, LOCK_NB)
    except OSError as exc:
        if exc.errno != errno.EBADF:
            raise
    else:
        raise OSError(errno.EINVAL, "flock(-1) succeeded unexpectedly")
    _safe_close_ignore_eio(fd)

    fd = os.open(target, os.O_RDWR)
    try:
        sys_flock(fd, LOCK_NB)
    except OSError as exc:
        if exc.errno != errno.EINVAL:
            raise
    else:
        raise OSError(errno.EINVAL, "flock(LOCK_NB) succeeded unexpectedly")
    _safe_close_ignore_eio(fd)


def _flock03_child_after_fork(target: str, parent_fd: int, go_rfd: int) -> None:
    """LTP flock03 child: wait, probe lock, unlock parent fd, re-lock, close."""
    os.read(go_rfd, 1)
    child_fd = os.open(target, os.O_RDWR)
    if _init_libc().flock(child_fd, LOCK_EX | LOCK_NB) == 0:
        os.close(child_fd)
        os._exit(1)
    if _init_libc().flock(parent_fd, LOCK_UN) < 0:
        os.close(child_fd)
        os._exit(1)
    if _init_libc().flock(child_fd, LOCK_EX | LOCK_NB) < 0:
        os.close(child_fd)
        os._exit(1)
    os.close(parent_fd)
    os.close(child_fd)
    os._exit(0)


def _ltp_flock03_precondition(root: str) -> None:
    """LTP flock03 parent EX lock, child unlocks parent fd then EX lock."""
    workdir = os.path.join(root, "_ltp_pre_flock03")
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir)
    target = os.path.join(workdir, "testfile")
    _create_empty_file(target, 0o666)

    go_rfd, go_wfd = os.pipe()
    parent_fd = os.open(target, os.O_RDWR)
    pid = os.fork()
    if pid == 0:
        os.close(go_wfd)
        _flock03_child_after_fork(target, parent_fd, go_rfd)
    sys_flock(parent_fd, LOCK_EX | LOCK_NB)
    os.write(go_wfd, b"\0")
    os.close(go_wfd)
    os.close(go_rfd)
    _reap_fork_child(pid)
    os.close(parent_fd)


def _flock04_precondition_mount(root: str) -> None:
    for _ in range(_FLOCK04_PRECONDITION_CYCLES):
        _ltp_flock02_precondition(root)
        _ltp_flock03_precondition(root)


def _reap_fork_child(pid: int) -> None:
    _, status = os.waitpid(pid, 0)
    if os.WIFEXITED(status) and os.WEXITSTATUS(status) != 0:
        raise OSError(
            errno.EIO,
            f"child exited with status {os.WEXITSTATUS(status)}",
        )
    if os.WIFSIGNALED(status):
        raise OSError(errno.EIO, f"child killed by signal {os.WTERMSIG(status)}")


def _flock04_child_fork(path: str, flock_op: int, should_pass: bool) -> None:
    """LTP flock04 child body (runs in a forked child)."""
    fd = os.open(path, os.O_RDWR)
    ret = _init_libc().flock(fd, flock_op)
    if should_pass:
        if ret < 0:
            os.close(fd)
            os._exit(ctypes.get_errno() or 1)
    else:
        if ret == 0:
            os.close(fd)
            os._exit(1)
    os.close(fd)
    os._exit(0)


def _run_flock04_tcase(target: str, parent_op: int) -> None:
    """One LTP flock04 verify_flock() iteration."""
    parent_fd = os.open(target, os.O_RDWR)
    sys_flock(parent_fd, parent_op)
    child1_should_pass = bool(parent_op & LOCK_SH)

    pid = os.fork()
    if pid == 0:
        _flock04_child_fork(target, LOCK_SH | LOCK_NB, child1_should_pass)
    _reap_fork_child(pid)

    pid = os.fork()
    if pid == 0:
        _flock04_child_fork(target, LOCK_EX | LOCK_NB, False)
    _reap_fork_child(pid)

    os.close(parent_fd)


# LTP fcntl32 verify_fcntl flag pairs (fd1, fd2) in execution order.
_FCNTL32_FLAG_PAIRS = (
    (os.O_RDONLY, os.O_RDONLY),
    (os.O_RDONLY, os.O_WRONLY),
    (os.O_RDONLY, os.O_RDWR),
    (os.O_WRONLY, os.O_RDONLY),
    (os.O_WRONLY, os.O_WRONLY),
    (os.O_WRONLY, os.O_RDWR),
    (os.O_RDWR, os.O_RDONLY),
    (os.O_RDWR, os.O_WRONLY),
    (os.O_RDWR, os.O_RDWR),
)

_FCNTL32_FILE_MODE = 0o7777  # 07777 — matches LTP FILE_MODE with setuid/setgid bits


def _fcntl32_setlease_cycle(target: str, fd1_flag: int, fd2_flag: int) -> None:
    """One LTP fcntl32 verify_fcntl iteration: open, F_SETLEASE, close both fds."""
    fd1 = os.open(target, fd1_flag)
    fd2 = os.open(target, fd2_flag)
    lease_errno = None
    try:
        fcntl.fcntl(fd1, F_SETLEASE, F_WRLCK)
    except OSError as exc:
        lease_errno = exc.errno
    else:
        os.close(fd1)
        os.close(fd2)
        raise OSError(
            errno.EINVAL,
            "fcntl(F_SETLEASE, F_WRLCK) succeeded unexpectedly",
        )
    if lease_errno not in (errno.EAGAIN, errno.EBUSY):
        os.close(fd1)
        os.close(fd2)
        raise OSError(
            lease_errno,
            f"fcntl(F_SETLEASE, F_WRLCK) unexpected errno={lease_errno}",
        )
    os.close(fd1)
    os.close(fd2)


def test_close_after_flock_invalid_operation(root: str) -> int:
    # Related LTP cases: flock02
    # close(2) must succeed after LTP error-case loop (EBADF then EINVAL) on one file.
    workdir = case_dir(root, "flock_invalid_op_close")
    target = os.path.join(workdir, "testfile")
    _create_empty_file(target, 0o666)

    def run_ltp_flock02_error_cases() -> None:
        fd = os.open(target, os.O_RDWR)
        try:
            sys_flock(-1, LOCK_NB)
        except OSError as exc:
            if exc.errno != errno.EBADF:
                raise
        else:
            raise OSError(errno.EINVAL, "flock(-1) succeeded unexpectedly")
        os.close(fd)

        fd = os.open(target, os.O_RDWR)
        try:
            sys_flock(fd, LOCK_NB)
        except OSError as exc:
            if exc.errno != errno.EINVAL:
                raise
        else:
            raise OSError(errno.EINVAL, "flock(LOCK_NB) succeeded unexpectedly")
        os.close(fd)

    return expect_ok(
        "close(fd) after flock02 EBADF+EINVAL sequence (flock02)",
        run_ltp_flock02_error_cases,
    )


def test_close_after_flock_shared_parent_child(root: str) -> int:
    # Related LTP cases: flock04
    # close(2) must succeed on LTP tc0 after flock02+flock03 mount precondition (6/17 order).
    workdir = case_dir(root, "flock04_close")
    target = os.path.join(workdir, "testfile")
    _create_empty_file(target, 0o644)

    def run_ltp_flock04_after_precondition() -> None:
        _flock04_precondition_mount(root)
        _run_flock04_tcase(target, LOCK_SH)

    return expect_ok(
        "close after flock04 tc0 with flock02+flock03 precondition (flock04)",
        run_ltp_flock04_after_precondition,
    )


def test_close_after_fcntl_setlease_eagain(root: str) -> int:
    # Related LTP cases: fcntl32, fcntl32_64
    # close(2) must succeed across the full nine-case verify_fcntl loop on one file.
    workdir = case_dir(root, "fcntl_setlease_close")
    target = os.path.join(workdir, "file")
    _create_empty_file(target, _FCNTL32_FILE_MODE)

    def run_ltp_fcntl32_all_cases() -> None:
        for fd1_flag, fd2_flag in _FCNTL32_FLAG_PAIRS:
            _fcntl32_setlease_cycle(target, fd1_flag, fd2_flag)

    return expect_ok(
        "close during full fcntl32 verify_fcntl loop (fcntl32/32_64)",
        run_ltp_fcntl32_all_cases,
    )


TESTS = [
    (
        "1",
        "close after flock02 EBADF+EINVAL sequence (flock02)",
        test_close_after_flock_invalid_operation,
    ),
    (
        "2",
        "close after flock04 tc0 + flock02/flock03 pre (flock04)",
        test_close_after_flock_shared_parent_child,
    ),
    (
        "3",
        "close during full fcntl32 loop (fcntl32/32_64)",
        test_close_after_fcntl_setlease_eagain,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="close(2) EIO reproducer for Curvine FUSE write-back on release",
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

    print(f"Running close-EIO repro tests under {root}\n")

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
