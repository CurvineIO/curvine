#!/usr/bin/env python3
"""
Purpose:
  Stress-test mounted filesystem read/write behavior under training-like mixed I/O.
  This script focuses on the issue-646/660 style scenarios:
    1) training_mixed_io_soak
    2) rapid_overwrite_immediate_reopen
    3) training_ckpt_direct_overwrite
    4) release_getattr_visibility_race

How to run:
  1) Mounted UFS path (recommended for mount regression):
     python3 scripts/tests/test_concurrent_io.py \
       --path /curvine-fuse/mnt/s3/lsr-test/fs-test \
       --scenario training_mixed_io_soak,rapid_overwrite_immediate_reopen,training_ckpt_direct_overwrite,release_getattr_visibility_race \
       --training-duration 8 --training-images 128 --training-procs 4 --workers 8 \
       --rapid-overwrite-iterations 80 --edge-duration 8 --rounds 6 --strict

  2) Pure Curvine path:
     python3 scripts/tests/test_concurrent_io.py \
       --path /curvine-fuse/cv-only-regression/fs-test \
       --scenario training_mixed_io_soak,rapid_overwrite_immediate_reopen,training_ckpt_direct_overwrite,release_getattr_visibility_race \
       --training-duration 8 --training-images 128 --training-procs 4 --workers 8 \
       --rapid-overwrite-iterations 80 --edge-duration 8 --rounds 6 --strict

  3) Single scenario quick smoke:
     python3 scripts/tests/test_concurrent_io.py \
       --path /tmp/cv-io-smoke \
       --scenario rapid_overwrite_immediate_reopen \
       --rapid-overwrite-iterations 20 --rounds 1 --strict
"""

from __future__ import annotations

import argparse
import os
import random
import shutil
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


def safe_unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except IsADirectoryError:
        shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def fsync_file(fh) -> None:
    fh.flush()
    os.fsync(fh.fileno())


def write_atomic(path: Path, data: bytes) -> None:
    ensure_parent(path)
    tmp = path.with_name(
        f"{path.name}.__cv_stage__.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}"
    )
    with tmp.open("wb") as fh:
        fh.write(data)
        fsync_file(fh)
    os.replace(tmp, path)


def write_direct(path: Path, data: bytes) -> None:
    ensure_parent(path)
    with path.open("wb") as fh:
        fh.write(data)
        fsync_file(fh)


def read_non_empty(path: Path) -> int:
    with path.open("rb") as fh:
        data = fh.read()
    return len(data)


def prepare_toy_dataset(base: Path, images: int) -> tuple[Path, Path, list[Path]]:
    dataset = base / "datasets" / "toy-mini"
    img_train = dataset / "images" / "train"
    img_val = dataset / "images" / "val"
    lbl_train = dataset / "labels" / "train"
    lbl_val = dataset / "labels" / "val"

    for d in (img_train, img_val, lbl_train, lbl_val):
        d.mkdir(parents=True, exist_ok=True)

    train_count = max(1, int(images * 0.8))
    val_count = max(1, images - train_count)
    image_paths: list[Path] = []

    for i in range(train_count):
        img = img_train / f"train_{i:04d}.jpg"
        lbl = lbl_train / f"train_{i:04d}.txt"
        if not img.exists():
            img.write_bytes(os.urandom(1024))
        if not lbl.exists():
            lbl.write_text("0 0.5 0.5 0.5 0.5\n")
        image_paths.append(img)

    for i in range(val_count):
        img = img_val / f"val_{i:04d}.jpg"
        lbl = lbl_val / f"val_{i:04d}.txt"
        if not img.exists():
            img.write_bytes(os.urandom(1024))
        if not lbl.exists():
            lbl.write_text("0 0.5 0.5 0.5 0.5\n")
        image_paths.append(img)

    yaml_path = dataset / "toy-mini.yaml"
    yaml_path.write_text(
        "path: " + str(dataset) + "\n"
        "train: images/train\n"
        "val: images/val\n"
        "nc: 1\n"
        'names: ["obj"]\n'
    )

    ckpt = base / "runs" / "train" / "exp" / "weights" / "last.pt"
    if not ckpt.exists():
        write_atomic(ckpt, os.urandom(1024 * 256))

    return dataset, ckpt, image_paths


@dataclass
class ScenarioOutcome:
    ok: bool
    metrics: dict[str, int | float] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


def scenario_rapid_overwrite_immediate_reopen(
    base_path: Path,
    iterations: int,
    strict: bool,
) -> ScenarioOutcome:
    work = base_path / "rapid_overwrite_immediate_reopen"
    work.mkdir(parents=True, exist_ok=True)
    target = work / "weights.bin"
    errors: list[str] = []
    ok_cnt = 0
    start = time.time()

    for i in range(iterations):
        try:
            payload = os.urandom(1024 * (64 + (i % 32)))
            write_atomic(target, payload)
            got = read_non_empty(target)
            if got <= 0:
                errors.append(f"iter={i} empty_after_reopen")
            else:
                ok_cnt += 1
        except Exception as e:
            errors.append(f"iter={i} {type(e).__name__}: {e}")

    elapsed = time.time() - start
    metrics = {
        "iterations": iterations,
        "ok_iterations": ok_cnt,
        "errors": len(errors),
        "elapsed_sec": round(elapsed, 3),
        "ops_per_sec": round(iterations / elapsed, 2) if elapsed > 0 else 0.0,
    }
    passed = len(errors) == 0 if strict else ok_cnt == iterations
    return ScenarioOutcome(ok=passed, metrics=metrics, errors=errors)


def scenario_training_ckpt_direct_overwrite(
    base_path: Path,
    duration_sec: float,
    workers: int,
    strict: bool,
) -> ScenarioOutcome:
    work = base_path / "training_ckpt_direct_overwrite"
    work.mkdir(parents=True, exist_ok=True)
    ckpt = work / "model.pt"
    write_direct(ckpt, os.urandom(1024 * 256))

    stop = threading.Event()
    lock = threading.Lock()
    counters = defaultdict(int)
    errors: list[str] = []

    def writer_loop(writer_id: int) -> None:
        idx = 0
        while not stop.is_set():
            try:
                payload = os.urandom(1024 * (128 + ((idx + writer_id) % 64)))
                write_direct(ckpt, payload)
                with lock:
                    counters["ckpt_write"] += 1
                idx += 1
            except Exception as e:
                with lock:
                    counters["writer_errors"] += 1
                    errors.append(f"writer={writer_id} {type(e).__name__}: {e}")

    def reader_loop(reader_id: int) -> None:
        while not stop.is_set():
            try:
                n = read_non_empty(ckpt)
                with lock:
                    counters["ckpt_read"] += 1
                if n <= 0:
                    with lock:
                        counters["zero_read"] += 1
            except FileNotFoundError as e:
                with lock:
                    counters["reader_enoent"] += 1
                    errors.append(f"reader={reader_id} FileNotFoundError: {e}")
            except OSError as e:
                with lock:
                    counters["reader_oserror"] += 1
                    errors.append(f"reader={reader_id} OSError[{e.errno}]: {e}")
            except Exception as e:
                with lock:
                    counters["reader_errors"] += 1
                    errors.append(f"reader={reader_id} {type(e).__name__}: {e}")

    writer_threads = [
        threading.Thread(target=writer_loop, args=(i,), daemon=True)
        for i in range(1)
    ]
    reader_threads = [
        threading.Thread(target=reader_loop, args=(i,), daemon=True)
        for i in range(max(1, workers - 1))
    ]
    for t in writer_threads + reader_threads:
        t.start()

    time.sleep(max(0.5, duration_sec))
    stop.set()
    for t in writer_threads + reader_threads:
        t.join(timeout=2.0)

    final_size = ckpt.stat().st_size if ckpt.exists() else 0
    zero_ratio = (
        float(counters["zero_read"]) / float(counters["ckpt_read"])
        if counters["ckpt_read"] > 0
        else 1.0
    )
    metrics = {
        "ckpt_write": counters["ckpt_write"],
        "ckpt_read": counters["ckpt_read"],
        "zero_read": counters["zero_read"],
        "zero_ratio": round(zero_ratio, 4),
        "reader_enoent": counters["reader_enoent"],
        "reader_oserror": counters["reader_oserror"],
        "errors": len(errors),
        "final_size": final_size,
    }
    if strict:
        passed = (
            len(errors) == 0
            and counters["ckpt_write"] > 0
            and counters["ckpt_read"] > 0
            and final_size > 0
            and zero_ratio <= 0.05
        )
    else:
        passed = final_size > 0 and counters["ckpt_write"] > 0
    return ScenarioOutcome(ok=passed, metrics=metrics, errors=errors)


def scenario_training_mixed_io_soak(
    base_path: Path,
    duration_sec: float,
    images: int,
    dataloader_processes: int,
    workers: int,
    strict: bool,
) -> ScenarioOutcome:
    work = base_path / "training_mixed_io"
    work.mkdir(parents=True, exist_ok=True)
    yaml_path, ckpt_path, image_paths = None, None, None
    try:
        dataset, ckpt, imgs = prepare_toy_dataset(work, images)
        yaml_path = dataset / "toy-mini.yaml"
        ckpt_path = ckpt
        image_paths = imgs
    except Exception as e:
        return ScenarioOutcome(
            ok=False, metrics={"errors": 1}, errors=[f"prepare_dataset_failed: {e}"]
        )

    stop = threading.Event()
    lock = threading.Lock()
    counters = defaultdict(int)
    errors: list[str] = []

    def dataloader_loop(worker_id: int) -> None:
        rng = random.Random(1000 + worker_id)
        assert image_paths is not None
        while not stop.is_set():
            p = rng.choice(image_paths)
            try:
                if read_non_empty(p) <= 0:
                    with lock:
                        counters["image_zero"] += 1
                        errors.append(f"image_zero {p}")
                with lock:
                    counters["image_read"] += 1
            except Exception as e:
                with lock:
                    counters["image_errors"] += 1
                    errors.append(f"dataloader={worker_id} {type(e).__name__}: {e}")

    def yaml_reader_loop(reader_id: int) -> None:
        assert yaml_path is not None
        while not stop.is_set():
            try:
                text = yaml_path.read_text()
                if not text:
                    with lock:
                        counters["yaml_empty"] += 1
                        errors.append("yaml_empty")
                with lock:
                    counters["yaml_read"] += 1
            except Exception as e:
                with lock:
                    counters["yaml_errors"] += 1
                    errors.append(f"yaml_reader={reader_id} {type(e).__name__}: {e}")

    def ckpt_writer_loop(writer_id: int) -> None:
        assert ckpt_path is not None
        i = 0
        while not stop.is_set():
            try:
                payload = os.urandom(1024 * (192 + ((i + writer_id) % 64)))
                write_atomic(ckpt_path, payload)
                with lock:
                    counters["ckpt_write"] += 1
                i += 1
            except Exception as e:
                with lock:
                    counters["ckpt_writer_errors"] += 1
                    errors.append(f"ckpt_writer={writer_id} {type(e).__name__}: {e}")

    def ckpt_reader_loop(reader_id: int) -> None:
        assert ckpt_path is not None
        while not stop.is_set():
            try:
                n = read_non_empty(ckpt_path)
                with lock:
                    counters["ckpt_read"] += 1
                if n <= 0:
                    with lock:
                        counters["ckpt_zero"] += 1
                        errors.append(f"ckpt_reader={reader_id} zero_size")
            except FileNotFoundError as e:
                with lock:
                    counters["ckpt_enoent"] += 1
                    errors.append(f"ckpt_reader={reader_id} FileNotFoundError: {e}")
            except OSError as e:
                with lock:
                    counters["ckpt_oserror"] += 1
                    errors.append(f"ckpt_reader={reader_id} OSError[{e.errno}]: {e}")
            except Exception as e:
                with lock:
                    counters["ckpt_reader_errors"] += 1
                    errors.append(f"ckpt_reader={reader_id} {type(e).__name__}: {e}")

    threads: list[threading.Thread] = []
    for i in range(max(1, dataloader_processes)):
        threads.append(threading.Thread(target=dataloader_loop, args=(i,), daemon=True))
    yaml_readers = max(1, workers // 4)
    ckpt_readers = max(1, workers // 2)
    for i in range(yaml_readers):
        threads.append(threading.Thread(target=yaml_reader_loop, args=(i,), daemon=True))
    threads.append(threading.Thread(target=ckpt_writer_loop, args=(0,), daemon=True))
    for i in range(ckpt_readers):
        threads.append(threading.Thread(target=ckpt_reader_loop, args=(i,), daemon=True))

    for t in threads:
        t.start()
    time.sleep(max(1.0, duration_sec))
    stop.set()
    for t in threads:
        t.join(timeout=2.0)

    final_size = ckpt_path.stat().st_size if ckpt_path and ckpt_path.exists() else 0
    metrics = {
        "image_read": counters["image_read"],
        "yaml_read": counters["yaml_read"],
        "ckpt_write": counters["ckpt_write"],
        "ckpt_read": counters["ckpt_read"],
        "ckpt_enoent": counters["ckpt_enoent"],
        "ckpt_oserror": counters["ckpt_oserror"],
        "errors": len(errors),
        "final_ckpt_size": final_size,
    }
    if strict:
        passed = (
            len(errors) == 0
            and counters["image_read"] > 0
            and counters["ckpt_write"] > 0
            and counters["ckpt_read"] > 0
            and final_size > 0
        )
    else:
        passed = final_size > 0 and counters["ckpt_write"] > 0
    return ScenarioOutcome(ok=passed, metrics=metrics, errors=errors)


def scenario_release_getattr_visibility_race(
    base_path: Path,
    duration_sec: float,
    strict: bool,
) -> ScenarioOutcome:
    work = base_path / "release_getattr_visibility_race"
    work.mkdir(parents=True, exist_ok=True)
    target = work / "weights.bin"
    write_atomic(target, os.urandom(1024 * 128))

    stop = threading.Event()
    lock = threading.Lock()
    counters = defaultdict(int)
    errors: list[str] = []

    def writer() -> None:
        i = 0
        while not stop.is_set():
            try:
                payload = os.urandom(1024 * (64 + (i % 32)))
                write_atomic(target, payload)
                with lock:
                    counters["writer_ok"] += 1
                i += 1
            except Exception as e:
                with lock:
                    counters["writer_err"] += 1
                    errors.append(f"writer {type(e).__name__}: {e}")

    def reader(reader_id: int) -> None:
        while not stop.is_set():
            try:
                os.stat(target)
                n = read_non_empty(target)
                if n <= 0:
                    with lock:
                        counters["reader_zero"] += 1
                        errors.append(f"reader={reader_id} zero_size")
                with lock:
                    counters["reader_ok"] += 1
            except FileNotFoundError as e:
                with lock:
                    counters["reader_enoent"] += 1
                    errors.append(f"reader={reader_id} FileNotFoundError: {e}")
            except OSError as e:
                with lock:
                    if e.errno == 5:
                        counters["reader_eio"] += 1
                    else:
                        counters["reader_oserror"] += 1
                    errors.append(f"reader={reader_id} OSError[{e.errno}]: {e}")
            except Exception as e:
                with lock:
                    counters["reader_err"] += 1
                    errors.append(f"reader={reader_id} {type(e).__name__}: {e}")

    threads = [threading.Thread(target=writer, daemon=True)] + [
        threading.Thread(target=reader, args=(i,), daemon=True) for i in range(4)
    ]
    for t in threads:
        t.start()
    time.sleep(max(1.0, duration_sec))
    stop.set()
    for t in threads:
        t.join(timeout=2.0)

    metrics = {
        "reader_ok": counters["reader_ok"],
        "reader_enoent": counters["reader_enoent"],
        "reader_eio": counters["reader_eio"],
        "reader_oserror": counters["reader_oserror"],
        "writer_ok": counters["writer_ok"],
        "errors": len(errors),
    }
    if strict:
        passed = (
            counters["reader_eio"] == 0
            and counters["reader_enoent"] == 0
            and counters["writer_err"] == 0
            and counters["reader_ok"] > 0
        )
    else:
        passed = counters["reader_eio"] == 0 and counters["writer_err"] == 0
    return ScenarioOutcome(ok=passed, metrics=metrics, errors=errors)


def run_scenario(
    name: str,
    base_path: Path,
    args: argparse.Namespace,
) -> ScenarioOutcome:
    if name == "training_mixed_io_soak":
        return scenario_training_mixed_io_soak(
            base_path=base_path,
            duration_sec=args.training_duration,
            images=args.training_images,
            dataloader_processes=args.training_procs,
            workers=args.workers,
            strict=args.strict,
        )
    if name == "rapid_overwrite_immediate_reopen":
        return scenario_rapid_overwrite_immediate_reopen(
            base_path=base_path,
            iterations=args.rapid_overwrite_iterations,
            strict=args.strict,
        )
    if name == "training_ckpt_direct_overwrite":
        return scenario_training_ckpt_direct_overwrite(
            base_path=base_path,
            duration_sec=args.training_duration,
            workers=args.workers,
            strict=args.strict,
        )
    if name == "release_getattr_visibility_race":
        return scenario_release_getattr_visibility_race(
            base_path=base_path,
            duration_sec=args.edge_duration,
            strict=args.strict,
        )
    return ScenarioOutcome(ok=False, metrics={"errors": 1}, errors=[f"unknown scenario {name}"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Concurrent I/O stress test for Curvine mount paths")
    parser.add_argument("--path", default="/curvine-fuse/mnt/s3/lsr-test/fs-test", help="Base test path")
    parser.add_argument(
        "--scenario",
        default="training_mixed_io_soak,rapid_overwrite_immediate_reopen,training_ckpt_direct_overwrite,release_getattr_visibility_race",
        help="Comma-separated scenario list",
    )
    parser.add_argument("--rounds", type=int, default=1, help="Rounds per scenario")
    parser.add_argument("--strict", action="store_true", help="Strict assertion mode")
    parser.add_argument("--training-duration", type=float, default=8.0, help="Training scenario duration seconds")
    parser.add_argument("--training-images", type=int, default=128, help="Toy image count")
    parser.add_argument("--training-procs", type=int, default=4, help="Dataloader worker thread count")
    parser.add_argument("--workers", type=int, default=8, help="Generic worker/read thread count")
    parser.add_argument("--rapid-overwrite-iterations", type=int, default=80, help="Rapid overwrite loop count")
    parser.add_argument("--edge-duration", type=float, default=8.0, help="Edge race scenario duration seconds")
    parser.add_argument("--keep-artifacts", action="store_true", help="Keep generated files")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    parser.add_argument("--stress-only", action="store_true")
    parser.add_argument("--real-train-cmd", action="store_true")
    parser.add_argument("--issue660-duration", type=float, default=0.0)
    parser.add_argument("--real-train-duration", type=float, default=0.0)
    parser.add_argument("--real-train-readers", type=int, default=0)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    random.seed(args.seed)

    base_path = Path(args.path)
    base_path.mkdir(parents=True, exist_ok=True)

    scenario_list = [s.strip() for s in args.scenario.split(",") if s.strip()]
    supported = {
        "training_mixed_io_soak",
        "rapid_overwrite_immediate_reopen",
        "training_ckpt_direct_overwrite",
        "release_getattr_visibility_race",
    }
    invalid = [s for s in scenario_list if s not in supported]
    if invalid:
        log(f"[ERROR] Unsupported scenarios: {invalid}")
        log(f"Supported: {sorted(supported)}")
        return 2

    log("=" * 72)
    log(f"Concurrent I/O stress test start, base_path={base_path}")
    log(f"Scenarios={scenario_list}, rounds={args.rounds}, strict={args.strict}")
    log("=" * 72)

    total_pass = 0
    total_cnt = len(scenario_list) * max(1, args.rounds)
    scenario_summary: dict[str, tuple[int, int, dict[str, int | float], list[str]]] = {}

    for name in scenario_list:
        pass_cnt = 0
        last_metrics: dict[str, int | float] = {}
        err_samples: list[str] = []

        for r in range(1, max(1, args.rounds) + 1):
            log(f">>> {name} round {r}/{args.rounds}")
            outcome = run_scenario(name, base_path, args)
            last_metrics = outcome.metrics
            if outcome.ok:
                pass_cnt += 1
                total_pass += 1
            elif len(err_samples) < 5:
                err_samples.extend(outcome.errors[: 5 - len(err_samples)])
            status = "PASS" if outcome.ok else "FAIL"
            log(f"<<< {name} round {r}/{args.rounds} {status} metrics={outcome.metrics}")
            if outcome.errors and len(err_samples) < 5:
                err_samples.extend(outcome.errors[: 5 - len(err_samples)])

        scenario_summary[name] = (pass_cnt, args.rounds, last_metrics, err_samples)

    log("=" * 72)
    log("SUMMARY")
    for name in scenario_list:
        pass_cnt, rounds, metrics, err_samples = scenario_summary[name]
        status = "PASS" if pass_cnt == rounds else "FAIL"
        log(f"{name}: {status} ({pass_cnt}/{rounds}) metrics={metrics}")
        if err_samples:
            for e in err_samples[:3]:
                log(f"{name} sample_error: {e}")
    log(f"Total: {total_pass}/{total_cnt} passed")
    log("=" * 72)

    if not args.keep_artifacts:
        for d in (
            "training_mixed_io",
            "rapid_overwrite_immediate_reopen",
            "training_ckpt_direct_overwrite",
            "release_getattr_visibility_race",
        ):
            shutil.rmtree(base_path / d, ignore_errors=True)

    return 0 if total_pass == total_cnt else 1


if __name__ == "__main__":
    raise SystemExit(main())
