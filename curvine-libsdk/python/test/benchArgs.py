import argparse
import re

# Transform size string to bytes
def parse_size(size_str: str) -> int:
    # Transform size string to bytes
    units = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }
    
    # Match number and unit (e.g. '128KB' -> (128, 'KB'))
    match = re.match(r"^(\d+)([KMGTP]?B)$", size_str.upper())
    if not match:
        raise ValueError(f"Invalid size format: {size_str}")
    
    num, unit = match.groups()
    return int(num) * units[unit]

class BenchArgs:
    def __init__(self):
        parser = argparse.ArgumentParser(description="Curvine file system bench test")
        
        parser.add_argument("-a", "--action", default="write", help="bench test type (read/write)")
        parser.add_argument("-c", "--conf", default="D:/Users/S9059520/Curvine/testing/curvine-cluster.toml", help="config file path")
        parser.add_argument("-d", "--dir", default="file:///bench", 
                          help="test directory (support cv:// or file:// protocol)")
        parser.add_argument("--file-num", type=int, default=10, 
                          help="test file number")
        parser.add_argument("--client-threads", type=int, default=4, 
                          help="client thread number")
        parser.add_argument("--buf-size", default="128KB", 
                          help="read/write buffer size")
        parser.add_argument("--file-size", default="100MB", 
                          help="each test file size")
        parser.add_argument("--delete-file", action="store_false", 
                          help="delete test file after read")
        parser.add_argument("--no-checksum", dest="checksum", action="store_true",
                          help="disable checksum (default enabled)")
        
        args = parser.parse_args()
        
        self.action: str = args.action
        self.conf: str = args.conf
        self.dir: str = args.dir
        self.file_num: int = args.file_num
        self.client_threads: int = args.client_threads
        self.buf_size: int = parse_size(args.buf_size)
        self.file_size: int = parse_size(args.file_size)
        self.delete_file: bool = args.delete_file
        self.checksum: bool = args.checksum


if __name__ == "__main__":
    args = BenchArgs()
    
    print("Bench test config:")
    print(f"- Action: {args.action}")
    print(f"- Directory: {args.dir}")
    print(f"- File Count: {args.file_num}")
    print(f"- Threads: {args.client_threads}")
    print(f"- Buffer Size: {args.buf_size} bytes")
    print(f"- File Size: {args.file_size} bytes")
    print(f"- Delete Files: {args.delete_file}")
    print(f"- Checksum: {args.checksum}")