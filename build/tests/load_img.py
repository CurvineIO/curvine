# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
test image loading speed in disk

usage:
python3 load_img.py \
    <image_directory> \
    --num_workers <num_workers> \
    --num_samples <num_samples>

recursively get all files from image_directory and test disk speed
"""

import os
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


def get_files_recursive(directory: str) -> List[str]:
    """Recursively get all files in a directory"""
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_list.append(os.path.abspath(file_path))
    return file_list


def main():
    parser = argparse.ArgumentParser(description="Test disk image loading speed (optimized)")
    parser.add_argument("img_dir", type=str, help="Image directory path")
    parser.add_argument("--num_samples", type=int, help="Number of images to test", default=1000)
    parser.add_argument("--num_workers", type=int, help="Number of workers", default=1)
    args = parser.parse_args()

    if not os.path.isdir(args.img_dir):
        raise ValueError(f"Directory does not exist: {args.img_dir}")

    # Recursively get all files in the directory
    img_list = get_files_recursive(args.img_dir)
    print(f"Found {len(img_list)} files in directory: {args.img_dir}")
    
    test_disk_speed(img_list, args.num_samples, args.num_workers)


if __name__ == "__main__":
    main()