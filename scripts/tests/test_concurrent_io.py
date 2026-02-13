#!/usr/bin/env python3
"""
Concurrent I/O diagnostic script for curvine-fuse.

This script was originally used in issue #646 and is extended with an
issue-660 scenario to guard training-style read correctness.
"""

import argparse
import multiprocessing as mp
import os
import random
import shutil
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


def ts() -> str:
    """Return current timestamp string for log correlation."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def safe_remove(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        return
    except OSError:
        return


def safe_rmtree(path: str, retries: int = 3, sleep_sec: float = 0.2) -> None:
    for _ in range(retries):
        try:
            shutil.rmtree(path)
            return
        except FileNotFoundError:
            return
        except OSError:
            time.sleep(sleep_sec)


def atomic_write_text(path: str, content: str) -> None:
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    tmp_path = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def atomic_write_bytes(path: str, payload: bytes) -> None:
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    tmp_path = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp_path, "wb") as f:
        f.write(payload)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def is_valid_training_yaml(data: str) -> bool:
    return bool(data) and ("train:" in data) and ("val:" in data)


def print_error_items(errors, limit: int = 8) -> None:
    for item in errors[:limit]:
        if isinstance(item, tuple) and len(item) >= 2:
            ts_part = item[0]
            detail = " | ".join(str(x) for x in item[1:])
            print(f"    [{ts_part}] {detail}")
        else:
            print(f"    {item}")


def join_threads_with_timeout(
    threads,
    errors,
    tag: str,
    timeout_sec: float = 20.0,
) -> None:
    deadline = time.time() + timeout_sec
    for t in threads:
        remain = max(0.1, deadline - time.time())
        t.join(timeout=remain)

    alive = [t.name for t in threads if t.is_alive()]
    if alive:
        errors.append((ts(), tag, "thread_hang", ",".join(alive[:8])))


def collect_mp_results(
    out_queue,
    expected: int,
    timeout_sec: float,
):
    results = []
    deadline = time.time() + timeout_sec
    while len(results) < expected and time.time() < deadline:
        remain = max(0.1, deadline - time.time())
        try:
            results.append(out_queue.get(timeout=min(1.0, remain)))
        except Exception:  # noqa: BLE001
            continue
    return results, expected - len(results)


def ensure_binary_files(
    dir_path: str,
    count: int,
    size: int,
    prefix: str,
    suffix: str,
    parallelism: int = 8,
):
    os.makedirs(dir_path, exist_ok=True)
    file_names = [f"{prefix}{i:012d}{suffix}" for i in range(count)]
    file_paths = [os.path.join(dir_path, name) for name in file_names]
    try:
        existing = set(os.listdir(dir_path))
    except FileNotFoundError:
        existing = set()
    missing = [fp for fp, name in zip(file_paths, file_names) if name not in existing]
    if not missing:
        return file_paths

    payload = os.urandom(size)

    def create_one(fp: str) -> None:
        with open(fp, "wb") as f:
            f.write(payload)

    workers = max(1, min(parallelism, len(missing)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed([ex.submit(create_one, fp) for fp in missing]):
            fut.result()
    return file_paths


def test_concurrent_append(base_path: str, num_writers: int = 2, iterations: int = 20) -> bool:
    """Test concurrent append to the same file."""
    file_path = os.path.join(base_path, "concurrent_append.txt")
    errors = []
    success_count = [0]
    lock = threading.Lock()

    print(
        f"\n[{ts()}] === Test: Concurrent append ({num_writers} writers, {iterations} iterations) ==="
    )

    with open(file_path, "w", encoding="utf-8") as f:
        f.write("init\n")

    def writer(thread_id: int) -> None:
        for i in range(iterations):
            try:
                with open(file_path, "a", encoding="utf-8") as f:
                    f.write(f"T{thread_id}-I{i}\n")
                with lock:
                    success_count[0] += 1
            except Exception as e:  # noqa: BLE001
                errors.append((ts(), thread_id, i, type(e).__name__, str(e)))
            time.sleep(0.01)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(num_writers)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start

    total_ops = num_writers * iterations
    print(f"[{ts()}]   Success: {success_count[0]}/{total_ops} ({100*success_count[0]/total_ops:.1f}%)")
    print(f"[{ts()}]   Errors: {len(errors)}")
    print(f"[{ts()}]   Time: {elapsed:.2f}s")

    if errors:
        print(f"[{ts()}]   Error details:")
        for err_ts, tid, idx, etype, msg in errors[:5]:
            print(f"    [{err_ts}] Thread {tid}, iter {idx}: {etype}: {msg}")

    safe_remove(file_path)
    return len(errors) == 0


def test_concurrent_read_same_file(base_path: str, num_readers: int = 5, iterations: int = 20) -> bool:
    """Test concurrent reads from the same file."""
    file_path = os.path.join(base_path, "concurrent_read.txt")
    errors = []
    success_count = [0]
    lock = threading.Lock()

    print(f"\n[{ts()}] === Test: Concurrent read ({num_readers} readers, {iterations} iterations) ===")

    test_content = "test line\n" * 100
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(test_content)

    time.sleep(0.5)

    def reader(thread_id: int) -> None:
        for i in range(iterations):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                if len(content) > 0:
                    with lock:
                        success_count[0] += 1
            except Exception as e:  # noqa: BLE001
                errors.append((ts(), thread_id, i, type(e).__name__, str(e)))
            time.sleep(0.01)

    threads = [threading.Thread(target=reader, args=(i,)) for i in range(num_readers)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start

    total_ops = num_readers * iterations
    print(f"[{ts()}]   Success: {success_count[0]}/{total_ops} ({100*success_count[0]/total_ops:.1f}%)")
    print(f"[{ts()}]   Errors: {len(errors)}")
    print(f"[{ts()}]   Time: {elapsed:.2f}s")

    if errors:
        print(f"[{ts()}]   Error details:")
        for err_ts, tid, idx, etype, msg in errors[:5]:
            print(f"    [{err_ts}] Thread {tid}, iter {idx}: {etype}: {msg}")

    safe_remove(file_path)
    return len(errors) == 0


def test_concurrent_read_different_files(base_path: str, num_files: int = 10, num_readers: int = 5) -> bool:
    """Test concurrent reads across multiple files (dataloader-like)."""
    errors = []
    success_count = [0]
    lock = threading.Lock()

    print(
        f"\n[{ts()}] === Test: Concurrent read different files ({num_files} files, {num_readers} readers) ==="
    )

    test_dir = os.path.join(base_path, "multi_files")
    os.makedirs(test_dir, exist_ok=True)

    files = []
    for i in range(num_files):
        fp = os.path.join(test_dir, f"file_{i:03d}.txt")
        with open(fp, "w", encoding="utf-8") as f:
            f.write(f"content of file {i}\n" * 50)
        files.append(fp)

    time.sleep(0.5)

    def reader(file_list) -> None:
        for fp in file_list:
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    content = f.read()
                if len(content) > 0:
                    with lock:
                        success_count[0] += 1
            except Exception as e:  # noqa: BLE001
                errors.append((ts(), fp, type(e).__name__, str(e)))

    threads = [threading.Thread(target=reader, args=(files,)) for _ in range(num_readers)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start

    total_ops = num_files * num_readers
    print(f"[{ts()}]   Success: {success_count[0]}/{total_ops} ({100*success_count[0]/total_ops:.1f}%)")
    print(f"[{ts()}]   Errors: {len(errors)}")
    print(f"[{ts()}]   Time: {elapsed:.2f}s")

    if errors:
        print(f"[{ts()}]   Error details:")
        for err_ts, fp, etype, msg in errors[:5]:
            print(f"    [{err_ts}] {os.path.basename(fp)}: {etype}: {msg}")

    safe_rmtree(test_dir)
    return len(errors) == 0


def test_read_while_write(base_path: str, duration_sec: float = 3.0) -> bool:
    """Test readers running concurrently with an appending writer."""
    file_path = os.path.join(base_path, "read_while_write.txt")
    errors = defaultdict(list)
    counts = {"read": 0, "write": 0}
    lock = threading.Lock()
    stop_flag = [False]

    print(f"\n[{ts()}] === Test: Read while write ({duration_sec}s) ===")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write("init\n")

    def writer() -> None:
        i = 0
        while not stop_flag[0]:
            try:
                with open(file_path, "a", encoding="utf-8") as f:
                    f.write(f"line-{i}\n")
                with lock:
                    counts["write"] += 1
                i += 1
            except Exception as e:  # noqa: BLE001
                errors["write"].append((ts(), i, type(e).__name__, str(e)))
            time.sleep(0.02)

    def reader() -> None:
        i = 0
        while not stop_flag[0]:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    _ = f.read()
                with lock:
                    counts["read"] += 1
                i += 1
            except Exception as e:  # noqa: BLE001
                errors["read"].append((ts(), i, type(e).__name__, str(e)))
            time.sleep(0.01)

    writer_thread = threading.Thread(target=writer)
    reader_threads = [threading.Thread(target=reader) for _ in range(3)]

    writer_thread.start()
    for t in reader_threads:
        t.start()

    time.sleep(duration_sec)
    stop_flag[0] = True

    writer_thread.join()
    for t in reader_threads:
        t.join()

    print(f"[{ts()}]   Write ops: {counts['write']}, errors: {len(errors['write'])}")
    print(f"[{ts()}]   Read ops: {counts['read']}, errors: {len(errors['read'])}")

    all_errors = errors["write"] + errors["read"]
    if all_errors:
        print(f"[{ts()}]   Error details:")
        for item in all_errors[:5]:
            print(f"    [{item[0]}] iter {item[1]}: {item[2]}: {item[3]}")

    safe_remove(file_path)
    return len(all_errors) == 0


def test_open_close_rapid(base_path: str, iterations: int = 100) -> bool:
    """Test rapid open/close cycles."""
    file_path = os.path.join(base_path, "rapid_open_close.txt")
    errors = []

    print(f"\n[{ts()}] === Test: Rapid open/close ({iterations} iterations) ===")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write("test content\n" * 10)

    time.sleep(0.5)

    start = time.time()
    for i in range(iterations):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                _ = f.read()
        except Exception as e:  # noqa: BLE001
            errors.append((ts(), i, type(e).__name__, str(e)))

    elapsed = time.time() - start
    print(f"[{ts()}]   Success: {iterations - len(errors)}/{iterations}")
    print(f"[{ts()}]   Errors: {len(errors)}")
    print(f"[{ts()}]   Time: {elapsed:.2f}s ({iterations / elapsed:.1f} ops/s)")

    if errors:
        print(f"[{ts()}]   Error details:")
        for err_ts, idx, etype, msg in errors[:5]:
            print(f"    [{err_ts}] iter {idx}: {etype}: {msg}")

    safe_remove(file_path)
    return len(errors) == 0


def test_threadpool_read(base_path: str, num_files: int = 20, max_workers: int = 8) -> bool:
    """Test concurrent reads with ThreadPoolExecutor."""
    errors = []
    success_count = [0]

    print(
        f"\n[{ts()}] === Test: ThreadPoolExecutor read ({num_files} files, {max_workers} workers) ==="
    )

    test_dir = os.path.join(base_path, "threadpool_test")
    os.makedirs(test_dir, exist_ok=True)

    files = []
    for i in range(num_files):
        fp = os.path.join(test_dir, f"data_{i:03d}.bin")
        with open(fp, "wb") as f:
            f.write(os.urandom(10000))
        files.append(fp)

    time.sleep(0.5)

    def read_file(fp: str) -> bytes:
        with open(fp, "rb") as f:
            return f.read()

    start = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(read_file, fp): fp for fp in files}
        for future in as_completed(futures):
            fp = futures[future]
            try:
                data = future.result()
                if len(data) > 0:
                    success_count[0] += 1
            except Exception as e:  # noqa: BLE001
                errors.append((ts(), os.path.basename(fp), type(e).__name__, str(e)))

    elapsed = time.time() - start
    print(f"[{ts()}]   Success: {success_count[0]}/{num_files}")
    print(f"[{ts()}]   Errors: {len(errors)}")
    print(f"[{ts()}]   Time: {elapsed:.2f}s")

    if errors:
        print(f"[{ts()}]   Error details:")
        for err_ts, fp, etype, msg in errors[:5]:
            print(f"    [{err_ts}] {fp}: {etype}: {msg}")

    safe_rmtree(test_dir)
    return len(errors) == 0


def test_issue_660_training_read(
    base_path: str,
    duration_sec: float = 8.0,
    readers: int = 8,
    images: int = 128,
    keep_artifacts: bool = False,
) -> bool:
    """
    Reproduce training-style read workload from issue #660.

    Success criteria:
    - yaml read never returns empty/invalid content
    - image reads never fail and never return empty payload
    - no EIO-like OSError during concurrent reads
    """
    root = os.path.join(base_path, "issue_660")
    yolo_root = os.path.join(root, "yolov5", "code", "yolov5-master")
    yaml_dir = os.path.join(yolo_root, "data")
    yaml_path = os.path.join(yaml_dir, "coco128.yaml")
    dataset_root = os.path.join(root, "yolov5", "code", "datasets", "coco128")
    image_dir = os.path.join(dataset_root, "images", "train2017")

    print(
        f"\n[{ts()}] === Test: Issue-660 training-style read ({readers} readers, {images} images, {duration_sec:.1f}s) ==="
    )

    os.makedirs(yaml_dir, exist_ok=True)
    os.makedirs(image_dir, exist_ok=True)

    yaml_content = """path: ../datasets/coco128
train: images/train2017
val: images/train2017
names:
  0: person
"""
    with open(yaml_path, "w", encoding="utf-8") as f:
        f.write(yaml_content)

    image_files = ensure_binary_files(
        image_dir,
        count=images,
        size=4096,
        prefix="",
        suffix=".jpg",
        parallelism=12,
    )

    # Let writes settle before high-concurrency reads.
    time.sleep(0.5)

    errors = []
    counters = {"yaml_ok": 0, "img_ok": 0}
    lock = threading.Lock()
    stop = [False]

    candidate_yaml = [
        os.path.join(yolo_root, "coco128.yaml"),
        os.path.join(yolo_root, "data", "hyps", "coco128.yaml"),
        os.path.join(yolo_root, "models", "coco128.yaml"),
        yaml_path,
    ]

    def yaml_worker() -> None:
        while not stop[0]:
            for cand in candidate_yaml:
                try:
                    with open(cand, "r", encoding="utf-8") as f:
                        data = f.read()
                    # Only the real file should pass strict checks.
                    if cand == yaml_path:
                        if not is_valid_training_yaml(data):
                            raise AssertionError(
                                f"yaml content invalid for {cand}: len={len(data)}"
                            )
                        with lock:
                            counters["yaml_ok"] += 1
                except FileNotFoundError:
                    # Expected for probe paths that do not exist.
                    if cand == yaml_path:
                        with lock:
                            errors.append((ts(), "yaml", "FileNotFoundError", cand))
                    continue
                except Exception as e:  # noqa: BLE001
                    with lock:
                        errors.append((ts(), "yaml", type(e).__name__, str(e)))
            time.sleep(0.005)

    def image_worker(worker_id: int) -> None:
        rng = random.Random(worker_id + int(time.time()))
        while not stop[0]:
            fp = image_files[rng.randrange(len(image_files))]
            try:
                with open(fp, "rb") as f:
                    payload = f.read()
                if not payload:
                    raise AssertionError(f"empty image payload: {fp}")
                with lock:
                    counters["img_ok"] += 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append((ts(), "image", type(e).__name__, str(e)))
            time.sleep(0.002)

    threads = [threading.Thread(target=yaml_worker, name="yaml-worker")]
    threads.extend(
        threading.Thread(target=image_worker, args=(i,), name=f"image-worker-{i}")
        for i in range(readers)
    )

    for t in threads:
        t.start()

    time.sleep(duration_sec)
    stop[0] = True

    join_threads_with_timeout(threads, errors, "issue_660_training_read")

    print(f"[{ts()}]   YAML reads ok: {counters['yaml_ok']}")
    print(f"[{ts()}]   Image reads ok: {counters['img_ok']}")
    print(f"[{ts()}]   Errors: {len(errors)}")

    if errors:
        print(f"[{ts()}]   Error details:")
        for item in errors[:8]:
            print(f"    [{item[0]}] {item[1]}: {item[2]}: {item[3]}")

    if not keep_artifacts:
        safe_rmtree(root, retries=5, sleep_sec=0.3)
    return len(errors) == 0


def test_issue_660_delayed_yaml_publish(
    base_path: str,
    duration_sec: float = 6.0,
    publish_delay_sec: float = 2.0,
    readers: int = 6,
    keep_artifacts: bool = False,
) -> bool:
    """
    Edge case:
    - Readers start before yaml exists
    - yaml appears later (atomic publish)
    - After publish, readers must only see valid yaml and no EIO-like errors
    """
    root = os.path.join(base_path, "issue_660_delayed_yaml")
    yaml_path = os.path.join(
        root, "yolov5", "code", "yolov5-master", "data", "coco128.yaml"
    )
    yaml_content = """path: ../datasets/coco128
train: images/train2017
val: images/train2017
"""

    print(
        f"\n[{ts()}] === Test: Issue-660 delayed yaml publish ({readers} readers, publish_delay={publish_delay_sec:.1f}s, duration={duration_sec:.1f}s) ==="
    )

    os.makedirs(os.path.dirname(yaml_path), exist_ok=True)
    safe_remove(yaml_path)

    lock = threading.Lock()
    stop = [False]
    published = [False]
    errors = []
    counters = {"valid_reads": 0, "not_found": 0}

    def publisher() -> None:
        time.sleep(publish_delay_sec)
        atomic_write_text(yaml_path, yaml_content)
        with lock:
            published[0] = True

    def reader() -> None:
        while not stop[0]:
            try:
                with open(yaml_path, "r", encoding="utf-8") as f:
                    data = f.read()
                if not is_valid_training_yaml(data):
                    with lock:
                        errors.append((ts(), "invalid_yaml", f"len={len(data)}"))
                else:
                    with lock:
                        counters["valid_reads"] += 1
            except FileNotFoundError:
                with lock:
                    counters["not_found"] += 1
                    if published[0]:
                        errors.append((ts(), "unexpected_not_found", yaml_path))
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append((ts(), type(e).__name__, str(e)))
            time.sleep(0.003)

    threads = [threading.Thread(target=publisher, name="yaml-publisher")]
    threads.extend(threading.Thread(target=reader, name=f"yaml-reader-{i}") for i in range(readers))
    for t in threads:
        t.start()

    time.sleep(duration_sec)
    stop[0] = True
    join_threads_with_timeout(threads, errors, "issue_660_delayed_yaml")

    print(f"[{ts()}]   Valid yaml reads: {counters['valid_reads']}")
    print(f"[{ts()}]   NotFound reads: {counters['not_found']}")
    print(f"[{ts()}]   Errors: {len(errors)}")
    if errors:
        print(f"[{ts()}]   Error details:")
        print_error_items(errors)

    if not keep_artifacts:
        safe_rmtree(root, retries=6, sleep_sec=0.3)
    return len(errors) == 0 and counters["valid_reads"] > 0


def test_read_delete_recreate_race(
    base_path: str,
    duration_sec: float = 6.0,
    readers: int = 6,
    keep_artifacts: bool = False,
) -> bool:
    """
    Edge case:
    - Writer repeatedly replace/delete+recreate the same path
    - Readers continuously open/read same path
    - Allowed: transient FileNotFound during delete window
    - Not allowed: malformed content / EIO-like failures
    """
    root = os.path.join(base_path, "read_delete_recreate_race")
    file_path = os.path.join(root, "race_data.txt")
    os.makedirs(root, exist_ok=True)

    print(
        f"\n[{ts()}] === Test: Read-delete-recreate race ({readers} readers, duration={duration_sec:.1f}s) ==="
    )

    atomic_write_text(file_path, "ver:0\nseed\n")

    lock = threading.Lock()
    stop = [False]
    errors = []
    stats = {"reads_ok": 0, "not_found": 0, "writes": 0}
    versions_seen = set()

    def writer() -> None:
        version = 1
        while not stop[0]:
            content = f"ver:{version}\n" + ("x" * 8192)
            try:
                atomic_write_text(file_path, content)
                with lock:
                    stats["writes"] += 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append((ts(), "writer", type(e).__name__, str(e)))

            # Every few rounds, create a short delete window.
            if version % 5 == 0:
                try:
                    safe_remove(file_path)
                except Exception as e:  # noqa: BLE001
                    with lock:
                        errors.append((ts(), "writer_delete", type(e).__name__, str(e)))
                time.sleep(0.01)
                try:
                    atomic_write_text(file_path, content)
                except Exception as e:  # noqa: BLE001
                    with lock:
                        errors.append((ts(), "writer_recreate", type(e).__name__, str(e)))
            version += 1
            time.sleep(0.005)

    def reader() -> None:
        while not stop[0]:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = f.read()
                if not data.startswith("ver:"):
                    with lock:
                        errors.append((ts(), "reader_malformed", "content", data[:32]))
                else:
                    first_line = data.splitlines()[0]
                    with lock:
                        stats["reads_ok"] += 1
                        versions_seen.add(first_line)
            except FileNotFoundError:
                with lock:
                    stats["not_found"] += 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append((ts(), "reader", type(e).__name__, str(e)))
            time.sleep(0.002)

    threads = [threading.Thread(target=writer, name="race-writer")]
    threads.extend(threading.Thread(target=reader, name=f"race-reader-{i}") for i in range(readers))
    for t in threads:
        t.start()
    time.sleep(duration_sec)
    stop[0] = True
    join_threads_with_timeout(threads, errors, "read_delete_recreate_race")

    print(f"[{ts()}]   Writes: {stats['writes']}")
    print(f"[{ts()}]   Reads ok: {stats['reads_ok']}")
    print(f"[{ts()}]   NotFound reads: {stats['not_found']}")
    print(f"[{ts()}]   Distinct versions observed: {len(versions_seen)}")
    print(f"[{ts()}]   Errors: {len(errors)}")
    if errors:
        print(f"[{ts()}]   Error details:")
        print_error_items(errors)

    if not keep_artifacts:
        safe_rmtree(root, retries=6, sleep_sec=0.3)
    return len(errors) == 0 and stats["reads_ok"] > 0 and stats["writes"] > 0


def test_stat_open_storm(
    base_path: str,
    duration_sec: float = 5.0,
    workers: int = 8,
    files: int = 64,
    keep_artifacts: bool = False,
) -> bool:
    """
    Edge case:
    - High-frequency stat/list/open mix with existing and non-existing paths
    - Catches metadata race paths that can manifest as EIO
    """
    root = os.path.join(base_path, "stat_open_storm")
    os.makedirs(root, exist_ok=True)

    per_dir = max(1, files // 8)
    all_files = []
    for d in range(8):
        sub = os.path.join(root, f"d{d:02d}")
        os.makedirs(sub, exist_ok=True)
        for i in range(per_dir):
            fp = os.path.join(sub, f"f{i:03d}.txt")
            if not os.path.exists(fp):
                with open(fp, "w", encoding="utf-8") as f:
                    f.write(f"storm-file-{d}-{i}\n" + ("z" * 512))
            all_files.append(fp)

    print(
        f"\n[{ts()}] === Test: Stat/open storm ({workers} workers, files={len(all_files)}, duration={duration_sec:.1f}s) ==="
    )

    lock = threading.Lock()
    stop = [False]
    errors = []
    counters = {"stat_ok": 0, "open_ok": 0, "list_ok": 0, "not_found": 0}

    def worker(worker_id: int) -> None:
        rng = random.Random(worker_id + int(time.time()))
        dirs = [os.path.join(root, f"d{d:02d}") for d in range(8)]
        while not stop[0]:
            op = rng.randrange(5)
            try:
                if op == 0:
                    fp = all_files[rng.randrange(len(all_files))]
                    os.stat(fp)
                    with lock:
                        counters["stat_ok"] += 1
                elif op == 1:
                    fp = all_files[rng.randrange(len(all_files))]
                    with open(fp, "r", encoding="utf-8") as f:
                        data = f.read(64)
                    if not data:
                        raise AssertionError(f"empty read from {fp}")
                    with lock:
                        counters["open_ok"] += 1
                elif op == 2:
                    dp = dirs[rng.randrange(len(dirs))]
                    _ = os.listdir(dp)
                    with lock:
                        counters["list_ok"] += 1
                elif op == 3:
                    np = os.path.join(root, "missing", f"no_{rng.randrange(1024):04d}.txt")
                    try:
                        os.stat(np)
                    except FileNotFoundError:
                        with lock:
                            counters["not_found"] += 1
                else:
                    np = os.path.join(root, "missing", f"open_{rng.randrange(1024):04d}.txt")
                    try:
                        with open(np, "r", encoding="utf-8"):
                            pass
                    except FileNotFoundError:
                        with lock:
                            counters["not_found"] += 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append((ts(), type(e).__name__, str(e)))
            time.sleep(0.001)

    threads = [threading.Thread(target=worker, args=(i,), name=f"storm-worker-{i}") for i in range(workers)]
    for t in threads:
        t.start()
    time.sleep(duration_sec)
    stop[0] = True
    join_threads_with_timeout(threads, errors, "stat_open_storm")

    print(f"[{ts()}]   stat_ok: {counters['stat_ok']}")
    print(f"[{ts()}]   open_ok: {counters['open_ok']}")
    print(f"[{ts()}]   list_ok: {counters['list_ok']}")
    print(f"[{ts()}]   not_found_ok: {counters['not_found']}")
    print(f"[{ts()}]   Errors: {len(errors)}")
    if errors:
        print(f"[{ts()}]   Error details:")
        print_error_items(errors)

    if not keep_artifacts:
        safe_rmtree(root, retries=6, sleep_sec=0.3)
    return len(errors) == 0


def _multiprocess_read_worker(
    file_paths,
    duration_sec: float,
    seed: int,
    out_queue,
) -> None:
    rng = random.Random(seed)
    end_at = time.time() + duration_sec
    read_ok = 0
    errors = []
    while time.time() < end_at:
        fp = file_paths[rng.randrange(len(file_paths))]
        try:
            with open(fp, "rb") as f:
                data = f.read(256)
            if not data:
                errors.append((ts(), "empty_read", fp))
            else:
                read_ok += 1
        except Exception as e:  # noqa: BLE001
            errors.append((ts(), type(e).__name__, str(e)))
        time.sleep(0.001)
    out_queue.put((read_ok, errors[:10], len(errors)))


def test_multiprocess_dataloader_read(
    base_path: str,
    duration_sec: float = 6.0,
    processes: int = 4,
    files: int = 128,
    keep_artifacts: bool = False,
) -> bool:
    """
    Edge case:
    - Multi-process read workload, closer to torch dataloader workers
    - Ensures no cross-process EIO/empty-read surprises
    """
    root = os.path.join(base_path, "multiprocess_dataloader_read")
    file_paths = ensure_binary_files(
        root,
        count=files,
        size=8192,
        prefix="sample_",
        suffix=".bin",
        parallelism=12,
    )

    print(
        f"\n[{ts()}] === Test: Multiprocess dataloader read ({processes} procs, files={files}, duration={duration_sec:.1f}s) ==="
    )

    ctx = mp.get_context("spawn")
    out_queue = ctx.Queue()
    procs = []
    for i in range(processes):
        p = ctx.Process(
            target=_multiprocess_read_worker,
            args=(file_paths, duration_sec, int(time.time()) + i, out_queue),
        )
        p.start()
        procs.append(p)

    total_ok = 0
    total_errors = 0
    error_samples = []
    results, missing = collect_mp_results(out_queue, processes, timeout_sec=duration_sec + 10)
    for ok_count, err_samples, err_count in results:
        total_ok += ok_count
        total_errors += err_count
        error_samples.extend(err_samples)
    if missing:
        total_errors += missing
        error_samples.append((ts(), "queue_timeout", f"missing_results={missing}"))

    for p in procs:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()
            p.join()

    print(f"[{ts()}]   read_ok: {total_ok}")
    print(f"[{ts()}]   errors: {total_errors}")
    if error_samples:
        print(f"[{ts()}]   Error details:")
        print_error_items(error_samples)

    if not keep_artifacts:
        safe_rmtree(root, retries=6, sleep_sec=0.3)
    return total_errors == 0 and total_ok > 0


def test_training_mixed_io_soak(
    base_path: str,
    duration_sec: float = 12.0,
    images: int = 256,
    dataloader_processes: int = 4,
    yaml_readers: int = 4,
    ckpt_readers: int = 4,
    ckpt_writers: int = 1,
    keep_artifacts: bool = False,
) -> bool:
    """
    Training-like mixed I/O soak:
    - Multiprocess dataloader-style image reads
    - Concurrent yaml probing
    - Concurrent checkpoint atomic publish/read
    - Concurrent tensorboard-like append writes and checkpoint cleanup

    This is intentionally aggressive and targets real training orchestration behavior.
    """
    root = os.path.join(base_path, "training_mixed_io")
    yolo_root = os.path.join(root, "yolov5", "code", "yolov5-master")
    yaml_dir = os.path.join(yolo_root, "data")
    yaml_path = os.path.join(yaml_dir, "coco128.yaml")
    dataset_root = os.path.join(root, "yolov5", "code", "datasets", "coco128")
    image_dir = os.path.join(dataset_root, "images", "train2017")
    run_dir = os.path.join(root, "runs", "train")
    ckpt_dir = os.path.join(run_dir, "weights")
    tb_event = os.path.join(run_dir, "events.out.tfevents")
    last_ckpt = os.path.join(ckpt_dir, "last.pt")

    print(
        f"\n[{ts()}] === Test: Training mixed I/O soak ({dataloader_processes} procs, "
        f"yaml_readers={yaml_readers}, ckpt_readers={ckpt_readers}, ckpt_writers={ckpt_writers}, "
        f"images={images}, duration={duration_sec:.1f}s) ==="
    )

    os.makedirs(yaml_dir, exist_ok=True)
    os.makedirs(image_dir, exist_ok=True)
    os.makedirs(ckpt_dir, exist_ok=True)

    yaml_content = """path: ../datasets/coco128
train: images/train2017
val: images/train2017
names:
  0: person
"""
    atomic_write_text(yaml_path, yaml_content)

    image_files = ensure_binary_files(
        image_dir,
        count=images,
        size=8192,
        prefix="",
        suffix=".jpg",
        parallelism=16,
    )

    atomic_write_bytes(last_ckpt, b"epoch=0\n" + os.urandom(128 * 1024))
    atomic_write_text(tb_event, "step=0 loss=1.0\n")
    time.sleep(0.5)

    lock = threading.Lock()
    stop = [False]
    errors = []
    counters = {
        "yaml_ok": 0,
        "ckpt_write": 0,
        "ckpt_read": 0,
        "tb_append": 0,
        "tb_miss": 0,
        "walk_ok": 0,
        "proc_read_ok": 0,
    }

    def yaml_reader() -> None:
        while not stop[0]:
            try:
                with open(yaml_path, "r", encoding="utf-8") as f:
                    data = f.read()
                if not is_valid_training_yaml(data):
                    raise AssertionError(f"invalid yaml len={len(data)}")
                with lock:
                    counters["yaml_ok"] += 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append((ts(), "yaml_reader", type(e).__name__, str(e)))
            time.sleep(0.003)

    def checkpoint_writer(writer_id: int) -> None:
        epoch = 1
        while not stop[0]:
            try:
                os.makedirs(ckpt_dir, exist_ok=True)
                payload = (
                    f"epoch={writer_id}-{epoch}\n".encode("utf-8")
                    + os.urandom(96 * 1024 + ((epoch + writer_id) % 8) * 16 * 1024)
                )
                atomic_write_bytes(last_ckpt, payload)
                if epoch % 2 == 0:
                    atomic_write_bytes(
                        os.path.join(ckpt_dir, f"epoch_{writer_id}_{epoch:06d}.pt"),
                        payload,
                    )
                with lock:
                    counters["ckpt_write"] += 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append((ts(), "ckpt_writer", type(e).__name__, str(e)))
            epoch += 1
            time.sleep(0.02)

    def checkpoint_reader() -> None:
        while not stop[0]:
            try:
                os.makedirs(ckpt_dir, exist_ok=True)
                with open(last_ckpt, "rb") as f:
                    head = f.read(16)
                    f.seek(0, os.SEEK_END)
                    size = f.tell()
                if not head or size <= 0:
                    raise AssertionError(f"checkpoint invalid size={size}")
                with lock:
                    counters["ckpt_read"] += 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append((ts(), "ckpt_reader", type(e).__name__, str(e)))
            time.sleep(0.004)

    def tensorboard_appender() -> None:
        step = 0
        while not stop[0]:
            try:
                os.makedirs(run_dir, exist_ok=True)
                with open(tb_event, "a", encoding="utf-8") as f:
                    f.write(f"step={step} loss={random.random():.6f}\n")
                    f.flush()
                    os.fsync(f.fileno())
                with lock:
                    counters["tb_append"] += 1
            except FileNotFoundError:
                # Mounted backends may expose short visibility gaps under heavy churn.
                # Recreate parent and retry without failing the whole scenario.
                os.makedirs(run_dir, exist_ok=True)
                with lock:
                    counters["tb_miss"] += 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append((ts(), "tb_append", type(e).__name__, str(e)))
            step += 1
            time.sleep(0.03)

    def checkpoint_cleaner() -> None:
        while not stop[0]:
            try:
                names = [n for n in os.listdir(ckpt_dir) if n.startswith("epoch_")]
                names.sort()
                # Keep only latest 6 epoch snapshots to simulate training cleanup.
                for old in names[:-6]:
                    safe_remove(os.path.join(ckpt_dir, old))
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append((ts(), "ckpt_cleaner", type(e).__name__, str(e)))
            time.sleep(0.07)

    def dataset_walker() -> None:
        while not stop[0]:
            try:
                found = 0
                for _root, _dirs, files in os.walk(image_dir):
                    found += len(files)
                if found <= 0:
                    raise AssertionError("image set vanished")
                with lock:
                    counters["walk_ok"] += 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append((ts(), "dataset_walker", type(e).__name__, str(e)))
            time.sleep(0.05)

    threads = []
    threads.extend(threading.Thread(target=yaml_reader, name=f"yaml-reader-{i}") for i in range(yaml_readers))
    threads.extend(
        threading.Thread(target=checkpoint_reader, name=f"ckpt-reader-{i}")
        for i in range(ckpt_readers)
    )
    threads.extend(
        threading.Thread(target=checkpoint_writer, args=(i,), name=f"ckpt-writer-{i}")
        for i in range(ckpt_writers)
    )
    threads.append(threading.Thread(target=tensorboard_appender, name="tb-appender"))
    threads.append(threading.Thread(target=checkpoint_cleaner, name="ckpt-cleaner"))
    threads.append(threading.Thread(target=dataset_walker, name="dataset-walker"))
    for t in threads:
        t.start()

    ctx = mp.get_context("spawn")
    out_queue = ctx.Queue()
    procs = []
    for i in range(dataloader_processes):
        p = ctx.Process(
            target=_multiprocess_read_worker,
            args=(image_files, duration_sec, int(time.time()) + 31 * i, out_queue),
        )
        p.start()
        procs.append(p)

    time.sleep(duration_sec)
    stop[0] = True

    join_threads_with_timeout(threads, errors, "training_mixed_io_soak", timeout_sec=25.0)

    proc_error_count = 0
    proc_error_samples = []
    results, missing = collect_mp_results(
        out_queue,
        dataloader_processes,
        timeout_sec=duration_sec + 10,
    )
    for ok_count, err_samples, err_count in results:
        counters["proc_read_ok"] += ok_count
        proc_error_count += err_count
        proc_error_samples.extend(err_samples)
    if missing:
        proc_error_count += missing
        proc_error_samples.append((ts(), "proc_queue_timeout", f"missing_results={missing}"))

    for p in procs:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()
            p.join()
            proc_error_count += 1
            proc_error_samples.append((ts(), "proc_alive", p.pid))

    if proc_error_count:
        with lock:
            errors.extend(
                (item[0], "proc_reader", item[1], item[2])
                if len(item) >= 3
                else (ts(), "proc_reader", "error", str(item))
                for item in proc_error_samples[:20]
            )

    print(f"[{ts()}]   yaml_ok: {counters['yaml_ok']}")
    print(f"[{ts()}]   ckpt_write: {counters['ckpt_write']}")
    print(f"[{ts()}]   ckpt_read: {counters['ckpt_read']}")
    print(f"[{ts()}]   tb_append: {counters['tb_append']}")
    print(f"[{ts()}]   tb_miss: {counters['tb_miss']}")
    print(f"[{ts()}]   walk_ok: {counters['walk_ok']}")
    print(f"[{ts()}]   proc_read_ok: {counters['proc_read_ok']}")
    print(f"[{ts()}]   Errors: {len(errors)}")
    if errors:
        print(f"[{ts()}]   Error details:")
        print_error_items(errors)

    if not keep_artifacts:
        safe_rmtree(root, retries=8, sleep_sec=0.4)
    return (
        len(errors) == 0
        and counters["yaml_ok"] > 0
        and counters["ckpt_write"] > 0
        and counters["ckpt_read"] > 0
        and counters["tb_append"] > 0
        and counters["proc_read_ok"] > 0
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Test concurrent I/O on curvine-fuse")
    parser.add_argument(
        "--path",
        "-p",
        default="/mnt/curvine/lsr-test/fs-test",
        help="Base path to test",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=8,
        help="Number of workers for threadpool and issue-660 readers",
    )
    parser.add_argument(
        "--issue660-duration",
        type=float,
        default=8.0,
        help="Duration (seconds) for issue-660 training read test",
    )
    parser.add_argument(
        "--issue660-images",
        type=int,
        default=128,
        help="Number of image files for issue-660 training read test",
    )
    parser.add_argument(
        "--edge-duration",
        type=float,
        default=6.0,
        help="Duration (seconds) for additional edge-case race tests",
    )
    parser.add_argument(
        "--training-duration",
        type=float,
        default=12.0,
        help="Duration (seconds) for training mixed I/O soak test",
    )
    parser.add_argument(
        "--training-images",
        type=int,
        default=256,
        help="Number of training image files in mixed I/O soak test",
    )
    parser.add_argument(
        "--training-procs",
        type=int,
        default=4,
        help="Number of dataloader-like processes in mixed I/O soak test",
    )
    parser.add_argument(
        "--storm-files",
        type=int,
        default=128,
        help="Number of files used by stat/open storm",
    )
    parser.add_argument(
        "--dataloader-files",
        type=int,
        default=128,
        help="Number of files used by multiprocess dataloader test",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=1,
        help="Run the full suite multiple rounds",
    )
    parser.add_argument(
        "--stress-only",
        action="store_true",
        help="Run only training/issue-660 stress scenarios",
    )
    parser.add_argument(
        "--aggressive",
        action="store_true",
        help="Use stronger training-like pressure profile",
    )
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Keep generated test data between rounds for faster repeated stress",
    )
    args = parser.parse_args()

    if not os.path.exists(args.path):
        print(f"[{ts()}] [ERROR] Path does not exist: {args.path}")
        sys.exit(1)

    if args.rounds < 1:
        print(f"[{ts()}] [ERROR] --rounds must be >= 1")
        sys.exit(1)

    if args.aggressive:
        args.workers = max(args.workers, 20)
        args.issue660_duration = max(args.issue660_duration, 16.0)
        args.edge_duration = max(args.edge_duration, 12.0)
        args.training_duration = max(args.training_duration, 30.0)
        args.issue660_images = max(args.issue660_images, 96)
        args.training_images = max(args.training_images, 160)
        args.training_procs = max(args.training_procs, 8)
        args.storm_files = max(args.storm_files, 256)
        args.dataloader_files = max(args.dataloader_files, 256)
        args.rounds = max(args.rounds, 3)
        args.keep_artifacts = True

    keep_artifacts = args.keep_artifacts or args.rounds > 1
    ckpt_writers = 2 if args.aggressive else 1

    print("=" * 60)
    print(f"[{ts()}] Concurrent I/O Test on: {args.path}")
    print(
        f"[{ts()}] Profile: rounds={args.rounds}, stress_only={args.stress_only}, "
        f"aggressive={args.aggressive}, keep_artifacts={keep_artifacts}"
    )
    print("=" * 60)

    aggregate = defaultdict(lambda: {"pass": 0, "total": 0})
    round_results = []
    for ridx in range(1, args.rounds + 1):
        print(f"\n[{ts()}] ===== Round {ridx}/{args.rounds} =====")
        results = {}
        if not args.stress_only:
            results["concurrent_append"] = test_concurrent_append(args.path)
            results["concurrent_read_same"] = test_concurrent_read_same_file(args.path)
            results["concurrent_read_diff"] = test_concurrent_read_different_files(args.path)
            results["read_while_write"] = test_read_while_write(args.path)
            results["rapid_open_close"] = test_open_close_rapid(args.path)
            results["threadpool_read"] = test_threadpool_read(args.path, max_workers=args.workers)

        results["issue_660_training_read"] = test_issue_660_training_read(
            args.path,
            duration_sec=args.issue660_duration,
            readers=args.workers,
            images=args.issue660_images,
            keep_artifacts=keep_artifacts,
        )
        results["issue_660_delayed_yaml_publish"] = test_issue_660_delayed_yaml_publish(
            args.path,
            duration_sec=args.edge_duration,
            publish_delay_sec=max(1.0, args.edge_duration / 3.0),
            readers=max(4, args.workers // 2),
            keep_artifacts=keep_artifacts,
        )
        results["read_delete_recreate_race"] = test_read_delete_recreate_race(
            args.path,
            duration_sec=args.edge_duration,
            readers=max(4, args.workers // 2),
            keep_artifacts=keep_artifacts,
        )
        results["stat_open_storm"] = test_stat_open_storm(
            args.path,
            duration_sec=max(4.0, args.edge_duration - 1.0),
            workers=args.workers,
            files=max(64, args.storm_files),
            keep_artifacts=keep_artifacts,
        )
        results["multiprocess_dataloader_read"] = test_multiprocess_dataloader_read(
            args.path,
            duration_sec=args.edge_duration,
            processes=max(2, args.workers // 2),
            files=max(64, args.dataloader_files),
            keep_artifacts=keep_artifacts,
        )
        results["training_mixed_io_soak"] = test_training_mixed_io_soak(
            args.path,
            duration_sec=args.training_duration,
            images=max(64, args.training_images),
            dataloader_processes=max(2, args.training_procs),
            yaml_readers=max(2, args.workers // 2),
            ckpt_readers=max(2, args.workers // 2),
            ckpt_writers=ckpt_writers,
            keep_artifacts=keep_artifacts,
        )

        round_results.append(results)
        for name, ok in results.items():
            aggregate[name]["total"] += 1
            if ok:
                aggregate[name]["pass"] += 1

    print("\n" + "=" * 60)
    print(f"[{ts()}] SUMMARY")
    print("=" * 60)
    full_pass = True
    for name, stat in aggregate.items():
        passed = stat["pass"]
        total = stat["total"]
        ok = passed == total
        full_pass = full_pass and ok
        status = "PASS" if ok else "FAIL"
        print(f"  {name}: {status} ({passed}/{total})")

    total_tests = sum(stat["total"] for stat in aggregate.values())
    passed_tests = sum(stat["pass"] for stat in aggregate.values())
    print(f"\n[{ts()}]   Total: {passed_tests}/{total_tests} passed")
    print("=" * 60)
    sys.exit(0 if full_pass else 1)


if __name__ == "__main__":
    main()
