"""
test image loading speed in disk (standalone version v2 - optimized)

usage:
python3 test_disk_speed_standalone_v2.py \
    <image_txt_file> \
    --num_workers <num_workers> \
    --num_samples <num_samples>

random select num_samples from image_txt_file and test disk speed

Changes from v1:
- Use multiprocessing.Pool instead of ThreadPoolExecutor (bypass GIL)
- Use imap with chunksize for batch scheduling (reduce overhead)
"""

import os
import json
import random
import time
import argparse
from multiprocessing import Pool
from PIL import Image
from tqdm import tqdm
from typing import List


def load_image(img_path: str) -> None:
    """Load a single image"""
    try:
        img = Image.open(img_path).convert('RGB')
        img.load()
    except Exception as e:
        print(f"Failed to load image {img_path}: {e}")


def parallelise(func, items: List, num_workers: int) -> None:
    """Execute function in parallel - using multiprocessing Pool + imap with batch scheduling"""
    chunksize = min(max(1, len(items) // (num_workers * 4)), 50)
    with Pool(processes=num_workers) as pool:
        list(tqdm(pool.imap(func, items, chunksize), total=len(items), desc="Loading images"))


def test_disk_speed(img_list: List[str], num_samples: int, num_workers: int) -> None:
    """Test disk read speed"""
    if num_samples > len(img_list):
        num_samples = len(img_list)

    random.seed(42)
    if num_samples > 0:
        sampled_imgs = random.sample(img_list, num_samples)
    else:
        sampled_imgs = img_list

    start_time = time.time()
    if num_workers > 1:
        parallelise(load_image, sampled_imgs, num_workers=num_workers)
    else:
        for img_path in tqdm(sampled_imgs, desc="Loading images"):
            load_image(img_path)
    end_time = time.time()

    total_time = end_time - start_time
    avg_time = total_time / num_samples
    print(f"\nTotal time: {total_time:.2f}s")
    print(f"Average time per image: {avg_time*1000:.2f}ms")
    print(f"Images per second: {num_samples / total_time:.2f}")


def main():
    parser = argparse.ArgumentParser(description="Test disk image loading speed (optimized)")
    parser.add_argument("image_file", type=str, help="txt file or coco json file containing image paths")
    parser.add_argument("--num_samples", type=int, help="Number of images to test", default=1000)
    parser.add_argument("--num_workers", type=int, help="Number of workers", default=1)
    parser.add_argument("--img_dir", type=str, help="Image directory", default=None)
    args = parser.parse_args()

    img_list = []
    if args.image_file.endswith(".json"):
        with open(args.image_file, "r") as f:
            coco_data = json.load(f)
        img_list = [img["file_name"] for img in coco_data["images"]]
    elif args.image_file.endswith(".txt"):
        with open(args.image_file, "r") as f:
            img_list = [line.strip() for line in f.readlines()]
    else:
        raise ValueError("Unsupported file format. Please provide a .txt or .json file.")

    if args.img_dir:
        img_list = [os.path.join(args.img_dir, img) for img in img_list]
    test_disk_speed(img_list, args.num_samples, args.num_workers)


if __name__ == "__main__":
    main()
