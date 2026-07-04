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
chmod/fchmod mode bits not persisted on Curvine FUSE — reproducer for LTP failures.

Root cause: setattr (chmod/fchmod) and create-time umask application do not
persist permission mode 0000.  chmod(2)/fchmod(2) return success, but stat()
still reports the previous mode (e.g. 0755).  Likewise creat(2) under
umask(0777) should yield mode 0000 but the file keeps a non-zero mode.

Covered LTP cases (each line is one minimal repro scenario):
  chmod01  — chmod(path, 0000) succeeds yet stat() mode stays 0755 (TFAIL test 1)
  fchmod01 — fchmod(fd, 0000) succeeds yet fstat() mode stays 0755 (TFAIL)
  umask01  — umask(0777) + creat(0777) yields mode 0755 instead of 0000 (TFAIL)

Usage:
    python3 curvine_ltp_chmod_mode_repro.py --dir /curvine-fuse/fuse-test
    python3 curvine_ltp_chmod_mode_repro.py --dir /curvine-fuse/fuse-test --allow-non-mount
"""

import argparse
import errno
import os
import shutil
import sys
from typing import Callable

DEFAULT_DIR = "/curvine-fuse/fuse-test"

# LTP chmod01 / fchmod01 initial file mode (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH).
_LTP_FILE_MODE = 0o644


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


def _low9_mode(st_mode: int) -> int:
    return st_mode & 0o7777


def _raise_mode_mismatch(actual: int, expected: int, label: str) -> None:
    raise OSError(
        errno.EINVAL,
        f"{label}: mode got {actual:#06o}, expected {expected:#06o}",
    )


def test_chmod_zero_mode_persisted(root: str) -> int:
    # Related LTP cases: chmod01
    # chmod(2) with mode 0000 must clear all permission bits visible via stat(2).
    workdir = case_dir(root, "chmod_zero_mode")
    target = os.path.join(workdir, "testfile")

    def run_chmod_zero() -> None:
        fd = os.open(target, os.O_CREAT | os.O_RDWR, _LTP_FILE_MODE)
        os.close(fd)
        os.chmod(target, 0)
        actual = _low9_mode(os.stat(target).st_mode)
        if actual != 0:
            _raise_mode_mismatch(actual, 0, "stat after chmod(path, 0000)")

    return expect_ok(
        "chmod(path, 0000) then stat() shows mode 0000 (chmod01)",
        run_chmod_zero,
    )


def test_fchmod_zero_mode_persisted(root: str) -> int:
    # Related LTP cases: fchmod01
    # fchmod(2) with mode 0000 must clear all permission bits visible via fstat(2).
    workdir = case_dir(root, "fchmod_zero_mode")
    target = os.path.join(workdir, "testfile")

    def run_fchmod_zero() -> None:
        fd = os.open(target, os.O_CREAT | os.O_RDWR, _LTP_FILE_MODE)
        os.fchmod(fd, 0)
        actual = _low9_mode(os.fstat(fd).st_mode)
        os.close(fd)
        if actual != 0:
            _raise_mode_mismatch(actual, 0, "fstat after fchmod(fd, 0000)")

    return expect_ok(
        "fchmod(fd, 0000) then fstat() shows mode 0000 (fchmod01)",
        run_fchmod_zero,
    )


def test_creat_under_full_umask_has_zero_mode(root: str) -> int:
    # Related LTP cases: umask01
    # creat(2) under umask(0777) must produce a file whose low 9 mode bits are 0000.
    workdir = case_dir(root, "umask_zero_mode")
    target = os.path.join(workdir, "testfile")

    def run_umask_creat_zero() -> None:
        prev = os.umask(0o777)
        try:
            fd = os.open(target, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o777)
            os.close(fd)
            actual = _low9_mode(os.stat(target).st_mode)
            if actual != 0:
                _raise_mode_mismatch(actual, 0, "stat after umask(0777)+creat(0777)")
        finally:
            os.umask(prev)

    return expect_ok(
        "umask(0777)+creat(0777) yields file mode 0000 (umask01)",
        run_umask_creat_zero,
    )


TESTS: list[tuple[str, str, Callable[..., int]]] = [
    (
        "1",
        "chmod(path, 0000) persists via stat (chmod01)",
        test_chmod_zero_mode_persisted,
    ),
    (
        "2",
        "fchmod(fd, 0000) persists via fstat (fchmod01)",
        test_fchmod_zero_mode_persisted,
    ),
    (
        "3",
        "umask(0777)+creat yields mode 0000 (umask01)",
        test_creat_under_full_umask_has_zero_mode,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="chmod/fchmod mode-0000 not persisted on Curvine FUSE — reproducer",
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

    print(f"Running chmod-mode reproducer under {root}\n")

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
