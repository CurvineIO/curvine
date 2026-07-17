#!/usr/bin/env python3
# O_TRUNC end-to-end probe for curvine-fuse (issue #1122).
# Runs INSIDE the dev container against a NATIVE curvine path (not a UFS mount),
# so it never touches S3 objects.
import os
import sys

# curvine-fuse mountpoint
BASE = "/mnt/curvine/oslo"

tag = sys.argv[1] if len(sys.argv) > 1 else "run"

def fsize(path):
    try:
        return os.stat(path).st_size
    except FileNotFoundError:
        return -1

def make_file(path, n):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.write(fd, b"X" * n)
    os.close(fd)

def case_single_fd_trunc():
    # Baseline: single-fd O_TRUNC on an existing file. Usually works even
    # pre-fix because the writer is freshly created with the O_TRUNC flag.
    p = f"{BASE}/qyy-trunc-single-{tag}.txt"
    make_file(p, 100)
    before = fsize(p)
    fd = os.open(p, os.O_WRONLY | os.O_TRUNC)   # no lingering writer
    os.close(fd)
    after = fsize(p)
    ok = (after == 0)
    print(f"[single-fd O_TRUNC ] before={before:4d} after={after:4d} -> {'PASS' if ok else 'FAIL'}")
    return ok

def case_multi_fd_trunc():
    # Bug scenario (#1122): a writer for the ino already exists (fd1 held open),
    # then fd2 opens the SAME file with O_TRUNC. Pre-fix, the shared writer
    # ignores fd2's O_TRUNC and the file is NOT truncated.
    p = f"{BASE}/qyy-trunc-multi-{tag}.txt"
    make_file(p, 100)
    before = fsize(p)
    fd1 = os.open(p, os.O_WRONLY)               # establish + hold a writer for the ino
    try:
        fd2 = os.open(p, os.O_WRONLY | os.O_TRUNC)  # second open with O_TRUNC
        try:
            size_via_fd2 = os.fstat(fd2).st_size
        finally:
            os.close(fd2)
    finally:
        os.close(fd1)
    after = fsize(p)
    ok = (after == 0)
    print(f"[multi-fd  O_TRUNC ] before={before:4d} after={after:4d} fstat(fd2)={size_via_fd2:4d} -> {'PASS' if ok else 'FAIL'}")
    return ok

if __name__ == "__main__":
    print(f"=== O_TRUNC probe (tag={tag}) base={BASE} ===")
    r1 = case_single_fd_trunc()
    r2 = case_multi_fd_trunc()
    print(f"=== summary: single={'PASS' if r1 else 'FAIL'} multi={'PASS' if r2 else 'FAIL'} ===")
    sys.exit(0 if (r1 and r2) else 1)
