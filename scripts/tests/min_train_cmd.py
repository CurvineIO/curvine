#!/usr/bin/env python3
"""
Minimal real training command emulator for tmp->rename publish path.

This script intentionally uses:
  - last.pt.tmp -> last.pt
  - events.out.tfevents.tmp -> events.out.tfevents
"""

import argparse
import os
import random
import time
from datetime import datetime


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def publish_bytes_by_rename(path: str, payload: bytes) -> None:
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "wb") as f:
        f.write(payload)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def publish_text_by_rename(path: str, text: str) -> None:
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def ensure_training_layout(root: str, images: int) -> None:
    yolo_root = os.path.join(root, "yolov5", "code", "yolov5-master")
    yaml_path = os.path.join(yolo_root, "data", "coco128.yaml")
    image_dir = os.path.join(root, "yolov5", "code", "datasets", "coco128", "images", "train2017")
    run_dir = os.path.join(root, "runs", "train")
    ckpt_dir = os.path.join(run_dir, "weights")

    os.makedirs(os.path.dirname(yaml_path), exist_ok=True)
    os.makedirs(image_dir, exist_ok=True)
    os.makedirs(ckpt_dir, exist_ok=True)

    yaml_content = """path: ../datasets/coco128
train: images/train2017
val: images/train2017
names:
  0: person
"""
    publish_text_by_rename(yaml_path, yaml_content)

    payload = os.urandom(4096)
    for i in range(images):
        fp = os.path.join(image_dir, f"{i:012d}.jpg")
        if not os.path.exists(fp):
            with open(fp, "wb") as f:
                f.write(payload)


def run(root: str, duration_sec: float, step_interval_sec: float, images: int) -> int:
    ensure_training_layout(root, images)

    run_dir = os.path.join(root, "runs", "train")
    ckpt_dir = os.path.join(run_dir, "weights")
    last_ckpt = os.path.join(ckpt_dir, "last.pt")
    tb_event = os.path.join(run_dir, "events.out.tfevents")

    start = time.time()
    steps = 0
    ckpt_renames = 0
    event_renames = 0

    while time.time() - start < duration_sec:
        steps += 1
        blob = (
            f"epoch={steps}\n".encode("utf-8")
            + os.urandom(64 * 1024 + (steps % 4) * 8 * 1024)
        )
        publish_bytes_by_rename(last_ckpt, blob)
        ckpt_renames += 1

        if steps % 5 == 0:
            publish_bytes_by_rename(os.path.join(ckpt_dir, f"epoch_{steps:06d}.pt"), blob)

        event_content = f"step={steps} loss={random.random():.6f} t={time.time():.3f}\n"
        publish_text_by_rename(tb_event, event_content)
        event_renames += 1

        if steps % 20 == 0:
            print(
                f"[{ts()}] TRAIN_CMD_PROGRESS steps={steps} "
                f"ckpt_renames={ckpt_renames} event_renames={event_renames}",
                flush=True,
            )
        time.sleep(step_interval_sec)

    print(
        f"TRAIN_CMD_SUMMARY steps={steps} "
        f"ckpt_renames={ckpt_renames} event_renames={event_renames}",
        flush=True,
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal real training command emulator")
    parser.add_argument("--root", required=True, help="Training root directory")
    parser.add_argument("--duration-sec", type=float, default=75.0, help="Run duration in seconds")
    parser.add_argument("--step-interval-sec", type=float, default=0.2, help="Sleep between publish steps")
    parser.add_argument("--images", type=int, default=96, help="Number of image files to ensure")
    args = parser.parse_args()

    rc = run(
        root=args.root,
        duration_sec=max(1.0, args.duration_sec),
        step_interval_sec=max(0.01, args.step_interval_sec),
        images=max(8, args.images),
    )
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
