from curvinefs.curvineFileSystem import CurvineFileSystem
from benchArgs import BenchArgs
from benchAction import BenchAction
from concurrent.futures import ThreadPoolExecutor
import time
import zlib
import threading

class SpeedCounter:
    def __init__(self):
        self.start_time = time.time()
    
    def print(self, action: str, total_bytes: int):
        duration = time.time() - self.start_time
        speed = total_bytes / duration / (1024 * 1024) 
        print(f"{action} Throughput: {speed:.2f} MB/s")

class CurvineBench:
    def __init__(self, args: BenchArgs):
        self.args = args
        self.fs = CurvineFileSystem(args.conf, args.buf_size, write_chunk_num=8)
        self.action = BenchAction(args)
        self.executor = ThreadPoolExecutor(max_workers=args.client_threads)
        self.total_len = 0
        self.checksum = 0
        self.lock = threading.Lock()
        self.thread_data = {}
        self.bytes_data = self.action.rand_data()

    def update_metrics(self, data: bytes, thread_id=None):
        if thread_id is None:
            thread_id = threading.get_ident()
        
        if thread_id not in self.thread_data:
            self.thread_data[thread_id] = {"total_len": 0, "checksum": 0}

        self.thread_data[thread_id]["total_len"] += len(data)
        if self.args.checksum:
            self.thread_data[thread_id]["checksum"] = zlib.crc32(
                data, self.thread_data[thread_id]["checksum"]
            )

    def merge_metrics(self):
        with self.lock:  
            for data in self.thread_data.values():
                self.total_len += data["total_len"]
                if self.args.checksum:
                    self.checksum = zlib.crc32(
                        data["checksum"].to_bytes(4, "big"), self.checksum
                    )

    def run_write(self):
        self.fs.mkdir(self.args.dir, True)
        speed = SpeedCounter()
        futures = [self.executor.submit(self.fs_write, self.action.get_path(i)) for i in range(self.args.file_num)]
        for f in futures:
            f.result()
        self.merge_metrics()
        speed.print(self.args.action, self.total_len)
        print(f"Total Data Size: {self.total_len} bytes")
        if self.args.checksum:
            print(f"Checksum: {self.checksum}")
        print(f"args: {self.args}")
    
    def run_read(self):
        speed = SpeedCounter()
        futures = [self.executor.submit(self.fs_read, self.action.get_path(i)) for i in range(self.args.file_num)]
        for f in futures:
            f.result()
        self.merge_metrics()
        speed.print(self.args.action, self.total_len)
        print(f"Total Data Size: {self.total_len} bytes")
        if self.args.checksum:
            print(f"Checksum: {self.checksum}")
    
    def fs_write(self, path: str):
        writer = self.fs.create(path, True)
        for _ in range(self.action.loom_num):            
            writer.write(self.bytes_data)
            self.update_metrics(self.bytes_data)
        writer.close()
        
    def fs_read(self, path: str):
        reader = self.fs.open(path)
        data = reader.read(self.action.buf_size,self.action.file_size)
        while data:
            self.update_metrics(data)
            data = reader.read(self.action.buf_size)
        reader.close()       

# python test/curvineBench.py
if __name__ == "__main__":
    args = BenchArgs()
    bench = CurvineBench(args)
    if args.action == "write":
        bench.run_write()
    elif args.action == "read":
        bench.run_read()
    else:
        raise ValueError("Action must be write or read")

