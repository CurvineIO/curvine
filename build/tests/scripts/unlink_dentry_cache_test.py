#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
unlink(2) dentry / readdir cache invalidation test (issue #840).

Ported from repro_issue5.c:
https://github.com/CurvineIO/curvine/issues/840

After unlink() and closing the last fd, readdir(2) on the parent must not
list the removed name and stat(2) on that path must return ENOENT — without
requiring drop_caches.

Usage:
    python3 unlink_dentry_cache_test.py --dir /curvine-fuse/repro/issue5
    python3 unlink_dentry_cache_test.py --dir /curvine-fuse/repro/issue5 -v
    python3 unlink_dentry_cache_test.py --dir /curvine-fuse/repro/issue5 --with-drop-caches
"""

from __future__ import annotations

import argparse
import errno
import os
import shutil
import sys
import time

DEFAULT_DIR = "/curvine-fuse/repro/issue5"
FILENAME = "keepopen"


def list_dir_entries(dirpath: str) -> list[str]:
    return sorted(
        name
        for name in os.listdir(dirpath)
        if name not in (".", "..")
    )


def log_list_dir(dirpath: str, label: str, *, verbose: bool) -> list[str]:
    entries = list_dir_entries(dirpath)
    if verbose:
        print(f"  [{label}] readdir {dirpath}:", file=sys.stderr)
        for name in entries:
            print(f"    visible: {name}", file=sys.stderr)
        print(f"    total: {len(entries)} entries", file=sys.stderr)
    return entries


def log_stat(path: str, label: str, *, verbose: bool) -> tuple[bool, os.stat_result | None]:
    try:
        st = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        if verbose:
            print(
                f"  [{label}] stat({path}) FAIL errno={exc.errno} ({exc.strerror})",
                file=sys.stderr,
            )
        return False, None
    if verbose:
        print(
            f"  [{label}] stat({path}) OK ino={st.st_ino} "
            f"size={st.st_size} nlink={st.st_nlink}",
            file=sys.stderr,
        )
    return True, st


def drop_caches() -> None:
    os.sync()
    with open("/proc/sys/vm/drop_caches", "w", encoding="ascii") as f:
        f.write("3\n")


def run_test(base_dir: str, *, verbose: bool, with_drop_caches: bool) -> int:
    base_dir = os.path.abspath(base_dir)
    file_path = os.path.join(base_dir, FILENAME)
    fail = 0

    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    os.makedirs(base_dir, exist_ok=True)

    if verbose:
        print("=== Phase A: file just created ===", file=sys.stderr)

    fd = os.open(file_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        if os.write(fd, b"data") != 4:
            print("ERROR: short write in phase A", file=sys.stderr)
            return 1

        entries_a = log_list_dir(base_dir, "A", verbose=verbose)
        stat_ok_a, _ = log_stat(file_path, "A", verbose=verbose)
        if entries_a != [FILENAME] or not stat_ok_a:
            print("[phase A] unexpected state", file=sys.stderr)
            fail += 1

        if verbose:
            print("=== Phase B: after unlink (fd still open) ===", file=sys.stderr)

        os.unlink(file_path)
        stat_ok_b, _ = log_stat(file_path, "B", verbose=verbose)
        entries_b = log_list_dir(base_dir, "B", verbose=verbose)
        if stat_ok_b:
            print(
                f"[phase B] FAIL: stat({file_path}) succeeded after unlink; expected ENOENT",
                file=sys.stderr,
            )
            fail += 1
        elif verbose:
            print(
                "[phase B] stat returns ENOENT while fd still open (namespace path gone)",
                file=sys.stderr,
            )
        if FILENAME in entries_b:
            print(
                f"[phase B] WARN: readdir still lists {FILENAME!r} while fd open "
                "(kernel dentry may lag; phase C checks post-close)",
                file=sys.stderr,
            )

        if verbose:
            print("=== Phase C: after close(fd) ===", file=sys.stderr)

    finally:
        os.close(fd)

    time.sleep(1)
    entries_c = log_list_dir(base_dir, "C", verbose=verbose)
    stat_ok_c, _ = log_stat(file_path, "C", verbose=verbose)

    if entries_c:
        print(
            f"[phase C] FAIL: readdir still lists ghost entries: {entries_c}",
            file=sys.stderr,
        )
        fail += 1
    if stat_ok_c:
        print(
            f"[phase C] FAIL: stat({file_path}) succeeded; expected ENOENT",
            file=sys.stderr,
        )
        fail += 1
    if not fail:
        print("[phase C] PASS: parent empty and stat returns ENOENT")

    if with_drop_caches:
        if verbose:
            print(
                "=== Phase D: after drop_caches (bypass kernel cache) ===",
                file=sys.stderr,
            )
        try:
            drop_caches()
        except OSError as exc:
            print(
                f"[phase D] SKIP: drop_caches failed ({exc}); run as root?",
                file=sys.stderr,
            )
        else:
            entries_d = log_list_dir(base_dir, "D", verbose=verbose)
            stat_ok_d, _ = log_stat(file_path, "D", verbose=verbose)
            if entries_d:
                print(
                    f"[phase D] FAIL: readdir not empty after drop_caches: {entries_d}",
                    file=sys.stderr,
                )
                fail += 1
            if stat_ok_d:
                print(
                    f"[phase D] FAIL: stat still succeeds after drop_caches",
                    file=sys.stderr,
                )
                fail += 1
            elif not fail:
                print("[phase D] PASS: daemon view consistent after cache bypass")

    if verbose:
        print("=== Phase E: rmdir(base) ===", file=sys.stderr)

    try:
        os.rmdir(base_dir)
        if verbose:
            print("  rmdir BASE: OK", file=sys.stderr)
    except OSError as exc:
        print(
            f"[phase E] rmdir({base_dir}) FAIL errno={exc.errno} ({exc.strerror})",
            file=sys.stderr,
        )
        fail += 1

    return 1 if fail else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test unlink dentry/readdir cache invalidation (issue #840)",
    )
    parser.add_argument(
        "--dir",
        default=DEFAULT_DIR,
        help="Base directory for the repro (default: %(default)s)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print per-phase readdir/stat details (like repro_issue5.c)",
    )
    parser.add_argument(
        "--with-drop-caches",
        action="store_true",
        help="Also run phase D (requires root: write /proc/sys/vm/drop_caches)",
    )
    args = parser.parse_args()

    return run_test(
        args.dir,
        verbose=args.verbose,
        with_drop_caches=args.with_drop_caches,
    )


if __name__ == "__main__":
    sys.exit(main())
