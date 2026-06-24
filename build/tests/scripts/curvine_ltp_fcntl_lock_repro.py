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
POSIX advisory lock blocking and deadlock detection tests for Curvine FUSE mounts.

Root cause: after LTP fcntl14 testcase-1 (parent WRLCK, child RDLCK F_SETLKW with
parent wakeup), Curvine FUSE leaves stale lock state on the inode.  LTP testcase-2
then fails with "First parent lock failed" (errno 11/EAGAIN), GETLK pid=0, and
children blocked in F_SETLKW until the alarm kills them ("child didnot terminate").

Covered LTP cases (each line is one minimal repro scenario):
  fcntl14, fcntl14_64 — parent F_SETLK WRLCK must succeed on same file after tc1
      SETLKW wakeup sequence (reproduces "First parent lock failed" / errno 11)
  fcntl14, fcntl14_64 — child F_GETLK WRLCK probe must report parent PID and type
      after tc1 sequence (reproduces "GETLK: pid=0, should be parent's id")
  fcntl14, fcntl14_64 — child F_SETLKW WRLCK must finish after parent F_UNLCK in
      LTP tc2 WILLBLOCK flow (reproduces "child didnot terminate on its own accord")
  fcntl17, fcntl17_64 — three-process delayed deadlock must return EDEADLK on
      F_SETLKW (reproduces "Alarm expired, deadlock not detected")

Usage:
    python3 curvine_ltp_fcntl_lock_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_fcntl_lock_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import errno
import fcntl
import multiprocessing as mp
import os
import shutil
import signal
import struct
import sys
import time
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"

F_RDLCK = fcntl.F_RDLCK
F_WRLCK = fcntl.F_WRLCK
F_UNLCK = fcntl.F_UNLCK
F_GETLK = fcntl.F_GETLK
F_SETLK = fcntl.F_SETLK
F_SETLKW = fcntl.F_SETLKW
SEEK_SET = 0

# struct flock on Linux x86_64 / aarch64 (64-bit off_t), native alignment → 32 B
FLOCK_STRUCT = "hhqqi"
FLOCK_SIZE = struct.calcsize(FLOCK_STRUCT)

_LTP_FILEDATA = b"0123456789"
_LTP17_FILEDATA = b"abcdefghijklmnopqrstuvwxyz\n"


def _pack_flock(lock_type: int, start: int, length: int, pid: int = 0) -> bytes:
    raw = struct.pack(FLOCK_STRUCT, lock_type, SEEK_SET, start, length, pid)
    return raw + b"\x00" * (FLOCK_SIZE - len(raw))


def _unpack_flock(buf: bytes) -> tuple[int, int, int, int, int]:
    return struct.unpack(FLOCK_STRUCT, buf[: struct.calcsize(FLOCK_STRUCT)])


def _flock_bytes(result) -> bytes:
    if isinstance(result, (bytes, bytearray)):
        data = bytes(result)
    else:
        data = bytes(result) if result is not None else b""
    return data + b"\x00" * (FLOCK_SIZE - len(data))


def _do_setlk(fd: int, lock_type: int, start: int = 0, length: int = 0) -> None:
    fcntl.fcntl(fd, F_SETLK, _pack_flock(lock_type, start, length))


def _do_setlkw(fd: int, lock_type: int, start: int = 0, length: int = 0) -> None:
    fcntl.fcntl(fd, F_SETLKW, _pack_flock(lock_type, start, length))


def _do_getlk(fd: int, lock_type: int, start: int = 0, length: int = 0) -> bytes:
    result = fcntl.fcntl(fd, F_GETLK, _pack_flock(lock_type, start, length))
    return _flock_bytes(result)


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


def _make_lock_file(workdir: str, payload: bytes = _LTP_FILEDATA) -> str:
    path = os.path.join(workdir, "lockfile")
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        os.write(fd, payload)
    finally:
        os.close(fd)
    return path


def _force_kill(proc: mp.Process) -> None:
    if not proc.is_alive():
        return
    proc.terminate()
    proc.join(timeout=2.0)
    if proc.is_alive():
        proc.kill()
        proc.join(timeout=2.0)


def _signal_parent_after_delay(parent_pid: int) -> None:
    time.sleep(0.1)
    os.kill(parent_pid, signal.SIGUSR1)


def _tc1_child_setlkw_rdlck(fd: int, parent_pid: int) -> None:
    """LTP fcntl14 testcase-1 child: grandchild signals parent, then F_SETLKW RDLCK."""
    signaler = mp.Process(
        target=_signal_parent_after_delay,
        args=(parent_pid,),
        daemon=True,
    )
    signaler.start()
    _do_setlkw(fd, F_RDLCK, 0, 0)
    signaler.join(timeout=2.0)


def _ltp_fcntl14_tc1_precondition(path: str) -> None:
    """Run LTP fcntl14 testcase-1 (parent WRLCK, child RDLCK WILLBLOCK + wakeup)."""
    fd = os.open(path, os.O_RDWR)
    parent_pid = os.getpid()
    ready = False

    def on_sigusr1(_signum: int, _frame) -> None:
        nonlocal ready
        ready = True

    old_handler = signal.signal(signal.SIGUSR1, on_sigusr1)
    try:
        _do_setlk(fd, F_WRLCK, 0, 0)
        child = mp.Process(target=_tc1_child_setlkw_rdlck, args=(fd, parent_pid))
        child.start()
        deadline = time.time() + 5.0
        while not ready and time.time() < deadline:
            time.sleep(0.01)
        _do_setlk(fd, F_UNLCK, 0, 0)
        child.join(timeout=5.0)
        if child.is_alive():
            _force_kill(child)
        elif child.exitcode not in (0, None):
            raise OSError(
                errno.EIO,
                f"tc1 child exited with status {child.exitcode}",
            )
        time.sleep(0.05)
    finally:
        signal.signal(signal.SIGUSR1, old_handler)
        os.close(fd)


def _tc2_getlk_child(fd: int, parent_pid: int, result_q: mp.Queue) -> None:
    """LTP fcntl14 testcase-2 child GETLK probe (WILLBLOCK path, before SETLKW)."""
    buf = _do_getlk(fd, F_WRLCK, 0, 0)
    l_type, _whence, _start, _length, l_pid = _unpack_flock(buf)
    result_q.put((l_type, l_pid))


def _tc2_setlkw_child(fd: int, parent_pid: int, result_q: mp.Queue) -> None:
    """LTP fcntl14 testcase-2 child WILLBLOCK: SETLK then SETLKW WRLCK."""
    _do_getlk(fd, F_WRLCK, 0, 0)
    try:
        _do_setlk(fd, F_WRLCK, 0, 0)
    except OSError:
        pass
    signaler = mp.Process(
        target=_signal_parent_after_delay,
        args=(parent_pid,),
        daemon=True,
    )
    signaler.start()
    _do_setlkw(fd, F_WRLCK, 0, 0)
    result_q.put("ok")
    signaler.join(timeout=2.0)


def _fcntl17_child1(path: str, cmd_q: mp.Queue, res_q: mp.Queue) -> None:
    fd = os.open(path, os.O_RDWR)
    try:
        cmd_q.get()
        _do_setlk(fd, F_WRLCK, 2, 5)
        res_q.put(("c1", 0))
        cmd_q.get()
        _do_setlk(fd, F_UNLCK, 0, 0)
    except OSError as exc:
        res_q.put(("c1", exc.errno))
    finally:
        os.close(fd)


def _fcntl17_child2(path: str, cmd_q: mp.Queue, res_q: mp.Queue) -> None:
    fd = os.open(path, os.O_RDWR)
    try:
        cmd_q.get()
        _do_setlk(fd, F_WRLCK, 9, 5)
        res_q.put(("c2", 0))
        cmd_q.get()
        _do_setlkw(fd, F_WRLCK, 17, 5)
        res_q.put(("c2", 0))
    except OSError as exc:
        res_q.put(("c2", exc.errno))
    finally:
        os.close(fd)


def _fcntl17_child3(path: str, cmd_q: mp.Queue, res_q: mp.Queue) -> None:
    fd = os.open(path, os.O_RDWR)
    try:
        cmd_q.get()
        _do_setlk(fd, F_WRLCK, 17, 5)
        res_q.put(("c3", 0))
        cmd_q.get()
        _do_setlkw(fd, F_WRLCK, 2, 14)
        res_q.put(("c3", 0))
    except OSError as exc:
        res_q.put(("c3", exc.errno))
    finally:
        os.close(fd)


def _run_fcntl17_delayed_deadlock(path: str, timeout: float = 12.0) -> int:
    """Return errno observed from the deadlock F_SETLKW, or ETIMEDOUT / EIO."""
    cmd1: mp.Queue = mp.Queue()
    cmd2: mp.Queue = mp.Queue()
    cmd3: mp.Queue = mp.Queue()
    res_q: mp.Queue = mp.Queue()

    proc1 = mp.Process(target=_fcntl17_child1, args=(path, cmd1, res_q))
    proc2 = mp.Process(target=_fcntl17_child2, args=(path, cmd2, res_q))
    proc3 = mp.Process(target=_fcntl17_child3, args=(path, cmd3, res_q))
    children = (proc1, proc2, proc3)
    cmds = (cmd1, cmd2, cmd3)

    for proc in children:
        proc.start()

    def expect_stage(tag: str, err: int) -> None:
        if err != 0:
            raise OSError(err, f"{tag} setup lock failed")

    try:
        for cmd in cmds:
            cmd.put(0)
        for _ in range(3):
            who, err = res_q.get(timeout=timeout)
            expect_stage(who, err)

        cmd2.put(0)
        cmd3.put(0)
        cmd1.put(0)

        deadline = time.time() + timeout
        edeadlk = None
        while time.time() < deadline:
            remaining = deadline - time.time()
            try:
                who, err = res_q.get(timeout=max(remaining, 0.05))
            except Exception:
                break
            if who == "c3" and err == errno.EDEADLK:
                edeadlk = errno.EDEADLK
                break
            if who == "c2" and err == errno.EDEADLK:
                edeadlk = errno.EDEADLK
                break
            if err not in (0,):
                return err

        if edeadlk == errno.EDEADLK:
            return errno.EDEADLK
        return errno.ETIMEDOUT
    finally:
        for proc in children:
            _force_kill(proc)


def test_parent_write_lock_after_setlkw_read_wakeup_sequence(root: str) -> int:
    # Related LTP cases: fcntl14, fcntl14_64
    # LTP testcase-2 parent F_SETLK WRLCK must succeed on the same file after testcase-1.
    workdir = case_dir(root, "fc14_parent_setlk")
    path = _make_lock_file(workdir)
    _ltp_fcntl14_tc1_precondition(path)

    def run() -> None:
        fd = os.open(path, os.O_RDWR)
        try:
            _do_setlk(fd, F_WRLCK, 0, 0)
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

    return expect_ok(
        "parent F_SETLK WRLCK after tc1 SETLKW wakeup sequence",
        run,
    )


def test_getlk_write_probe_reports_parent_pid_after_wakeup_sequence(root: str) -> int:
    # Related LTP cases: fcntl14, fcntl14_64
    # LTP testcase-2 child F_GETLK WRLCK probe must see parent WRLCK and parent PID.
    workdir = case_dir(root, "fc14_getlk_pid")
    path = _make_lock_file(workdir)
    _ltp_fcntl14_tc1_precondition(path)
    parent_pid = os.getpid()

    def run() -> None:
        fd = os.open(path, os.O_RDWR)
        try:
            try:
                _do_setlk(fd, F_WRLCK, 0, 0)
            except OSError:
                pass

            result_q: mp.Queue = mp.Queue()
            child = mp.Process(
                target=_tc2_getlk_child,
                args=(fd, parent_pid, result_q),
            )
            child.start()
            child.join(timeout=5.0)
            if child.is_alive():
                _force_kill(child)
                raise OSError(
                    errno.ETIMEDOUT,
                    "GETLK child hung",
                )

            l_type, l_pid = result_q.get(timeout=1.0)
            if l_type != F_WRLCK:
                raise OSError(
                    errno.EIO,
                    f"F_GETLK returned l_type={l_type} (F_UNLCK={F_UNLCK}), "
                    f"expected WRLCK={F_WRLCK}",
                )
            if l_pid != parent_pid:
                raise OSError(
                    errno.EIO,
                    f"F_GETLK returned pid={l_pid}, expected parent pid={parent_pid}",
                )
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

    return expect_ok(
        "F_GETLK WRLCK probe reports parent PID after tc1 sequence",
        run,
    )


def test_setlkw_write_completes_after_parent_unlock_post_wakeup(root: str) -> int:
    # Related LTP cases: fcntl14, fcntl14_64
    # LTP testcase-2 WILLBLOCK: child F_SETLKW WRLCK must finish after parent F_UNLCK.
    workdir = case_dir(root, "fc14_setlkw_wakeup")
    path = _make_lock_file(workdir)
    _ltp_fcntl14_tc1_precondition(path)
    parent_pid = os.getpid()

    def run() -> None:
        fd = os.open(path, os.O_RDWR)
        usr1_seen = False

        def on_sigusr1(_signum: int, _frame) -> None:
            nonlocal usr1_seen
            usr1_seen = True

        old_handler = signal.signal(signal.SIGUSR1, on_sigusr1)
        try:
            try:
                _do_setlk(fd, F_WRLCK, 0, 0)
            except OSError:
                pass

            result_q: mp.Queue = mp.Queue()
            child = mp.Process(
                target=_tc2_setlkw_child,
                args=(fd, parent_pid, result_q),
            )
            child.start()

            deadline = time.time() + 5.0
            while not usr1_seen and time.time() < deadline:
                time.sleep(0.01)

            _do_setlk(fd, F_UNLCK, 0, 0)
            child.join(timeout=5.0)
            if child.is_alive():
                _force_kill(child)
                raise OSError(
                    errno.ETIMEDOUT,
                    "child F_SETLKW never completed after parent F_UNLCK — "
                    "matches LTP 'child didnot terminate on its own accord'",
                )

            status = result_q.get(timeout=1.0)
            if status != "ok":
                raise OSError(errno.EIO, f"child SETLKW unexpected status: {status!r}")
        finally:
            signal.signal(signal.SIGUSR1, old_handler)
            try:
                os.close(fd)
            except OSError:
                pass

    return expect_ok(
        "F_SETLKW WRLCK completes after parent unlock (LTP tc2 WILLBLOCK)",
        run,
    )


def test_setlkw_delayed_deadlock_returns_edeadlk(root: str) -> int:
    # Related LTP cases: fcntl17, fcntl17_64
    # Three-process delayed deadlock: F_SETLKW must return EDEADLK, not block until alarm.
    workdir = case_dir(root, "fc17_deadlock")
    path = _make_lock_file(workdir, _LTP17_FILEDATA)

    def run() -> None:
        result = _run_fcntl17_delayed_deadlock(path)
        if result == errno.EDEADLK:
            return
        if result == errno.ETIMEDOUT:
            raise OSError(
                errno.ETIMEDOUT,
                "deadlock not detected — F_SETLKW blocked until timeout; "
                "matches LTP 'Alarm expired, deadlock not detected'",
            )
        raise OSError(
            result,
            f"F_SETLKW returned errno={result} "
            f"({errno.errorcode.get(result, '?')}), expected EDEADLK={errno.EDEADLK}",
        )

    return expect_ok("F_SETLKW delayed deadlock returns EDEADLK", run)


TESTS: list[tuple[str, str, Callable[..., int]]] = [
    (
        "1",
        "parent F_SETLK after tc1 SETLKW wakeup (First parent lock failed)",
        test_parent_write_lock_after_setlkw_read_wakeup_sequence,
    ),
    (
        "2",
        "F_GETLK reports parent PID after tc1 sequence",
        test_getlk_write_probe_reports_parent_pid_after_wakeup_sequence,
    ),
    (
        "3",
        "F_SETLKW completes after parent unlock (child hang)",
        test_setlkw_write_completes_after_parent_unlock_post_wakeup,
    ),
    (
        "4",
        "F_SETLKW delayed deadlock returns EDEADLK",
        test_setlkw_delayed_deadlock_returns_edeadlk,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="POSIX fcntl lock blocking and deadlock detection tests for Curvine FUSE",
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

    print(f"Running fcntl lock blocking / deadlock tests under {root}\n")

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
