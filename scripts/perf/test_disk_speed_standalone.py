"""
test image loading speed in disk (standalone version)

usage:
python3 test_disk_speed_standalone.py \
    <image_txt_file> \
    --num_threads <num_threads> \
    --num_samples <num_samples>

random select num_samples from image_txt_file and test disk speed
"""

import os
import json
import random
import time
import argparse
from PIL import Image
from tqdm import tqdm
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed


def load_image(img_path: str) -> None:
    """Load a single image"""
    try:
        img = Image.open(img_path).convert('RGB')
        img.load()
    except Exception as e:
        print(f"Failed to load image {img_path}: {e}")


def parallelise(func, items: List, num_workers: int) -> None:
    """Execute function in parallel"""
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(func, item) for item in items]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="Loading images"):
            pass


def test_disk_speed(img_list: List[str], num_samples: int, num_threads: int) -> None:
    """Test disk read speed"""
    if num_samples > len(img_list):
        num_samples = len(img_list)

    random.seed(42)
    if num_samples > 0:
        sampled_imgs = random.sample(img_list, num_samples)
    else:
        sampled_imgs = img_list

    start_time = time.time()
    if num_threads > 1:
        parallelise(load_image, sampled_imgs, num_workers=num_threads)
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
    parser = argparse.ArgumentParser(description="Test disk image loading speed")
    parser.add_argument("image_file", type=str, help="txt file or coco json file containing image paths")
    parser.add_argument("--num_samples", type=int, help="Number of images to test", default=1000)
    parser.add_argument("--num_threads", type=int, help="Number of threads", default=1)
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
    test_disk_speed(img_list, args.num_samples, args.num_threads)


if __name__ == "__main__":
    main()