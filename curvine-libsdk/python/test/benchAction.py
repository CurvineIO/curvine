import os
from benchArgs import BenchArgs
class BenchAction:
    action_type = ""
    args = None
    buf_size = 0
    file_size = 0
    file_num = 0
    loom_num = 0
    total_len = 0
    checks_sum = False

    def __init__(self, args: BenchArgs):
        self.action_type = args.action
        self.args = args
        self.buf_size = args.buf_size
        self.file_size = args.file_size
        self.file_num = args.file_num
        self.loom_num: int = int(self.file_size / self.buf_size)
        self.total_len = self.file_num * self.file_size
        self.checks_sum = args.checksum
    
    def rand_data(self):
        return os.urandom(self.buf_size)
    
    def get_path(self, index: int) -> str:
        path = f"{self.args.dir}/{index}.txt"
        return path.replace("//", "/")
