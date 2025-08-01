from fsspec import AbstractFileSystem
import curvinefs.curvineClient as curvineClient
import os;
from urllib.parse import urlsplit

class CurvineFileSystem(AbstractFileSystem):

    def __init__(self, config_path, write_chunk_size, write_chunk_num, *args, **storage_options):
        super().__init__(*args, **storage_options)
        self.client = curvineClient.CurvineClient(config_path, write_chunk_num, write_chunk_size)
        
    def _open(self, path, mode="rb", block_size=None, autocommit=True, cache_options=None, **kwargs):
        path = self.formatPath(path)
        if "r" in mode and ("w" in mode or "a" in mode):
            return self
        elif "w" in mode or "a" in mode:
            return self.create(path)
        elif "r" in mode:
            return self.open(path)
        else:
            raise ValueError("Mode must be r, w, a, or rw")
   
    def formatPath(self, full_path):
        if full_path is None:
            raise ValueError("Path must be non-empty string")
        parsed = urlsplit(full_path)
        path = parsed.path
        normalized = os.path.normpath(path).replace("\\", "/")
        if len(normalized) > 1 and normalized.endswith("/"):
            normalized = normalized.rstrip("/")

        return normalized
        
    def cat(self, path, **kwargs):
        path = self.formatPath(path)
        if isinstance(path, str):
            if self.isfile(path):
                return self.client.read_range(path, 0, -1)
            else:
                return self.client.ls(path, False)
        elif isinstance(path, list):
            res = {}
            for item in list:
                temp_path = item["name"]
                if self.isfile(temp_path):
                    res[temp_path] = self.client.read_range(temp_path, 0, -1)
                else:
                    res[temp_path] = self.client.ls(path, False)
            return res
        else:
            raise ValueError("Path can be only string or list")
          
    def cat_file(self, path, start=0, end=None, **kwargs):
        path = self.formatPath(path)

        if start is None:
            start = 0

        if end is None:  # Read until the end    
            length = -1
        else: # Read from start to end
            length = end - start

        return self.client.read_range(path, start, length)
   
    def cat_ranges(self, paths, starts, ends, max_gap=None, on_error="return", **kwargs):
        raise NotImplementedError
    
    def checksum(self, path):
        raise NotImplementedError
    
    def clear_instance_cache(self):
        raise NotImplementedError
    
    def copy(self, path1, path2, **kwargs):
        path1 = self.formatPath(path1)
        path2 = self.formatPath(path2)
        self.client.copy(path1, path2)

    def cp(self, path1, path2, **kwargs):
        path1 = self.formatPath(path1)
        path2 = self.formatPath(path2)

        self.client.copy(path1, path2)
    
    def created(self, path):
        raise NotImplementedError
    
    def current(self):
        raise NotImplementedError
    
    def delete(self, path, recursive=False, **kwargs):
        path = self.formatPath(path)
        self.client.rm(path, recursive)
    
    def disk_usage(self, path, total=True, maxdepth=None, **kwargs):
        raise NotImplementedError
    
    def download(self, rpath, lpath, *args, **kwargs):
        rpath = self.formatPath(rpath)
        lpath = self.formatPath(lpath)

        return self.client.download(rpath, lpath)
    
    def du(self, path, total=True, maxdepth=None, withdirs=False, **kwargs):
        raise NotImplementedError
    
    def end_transaction(self):
        raise NotImplementedError
    
    def exists(self, path, **kwargs):
        path = self.formatPath(path)
        try:
            status = self.client.get_file_status(path)
        except Exception as e:
            raise
        
        if status is None:
            return False
        return True
    
    def expand_path(self, path, recursive=False, maxdepth=None, **kwargs):
        raise NotImplementedError
    
    def find(self, path, maxdepth=None, withdirs=False, detail=False, **kwargs):
        raise NotImplementedError
    
    def get(self, rpath, lpath, recursive=False, callback=..., maxdepth=None, **kwargs):
        raise NotImplementedError
    
    def get_file(self, rpath, lpath, callback=..., outfile=None, **kwargs):
        raise NotImplementedError
    
    def get_mapper(self, root="", check=False, create=False, missing_exceptions=None):
        raise NotImplementedError
    
    def glob(self, path, maxdepth=None, **kwargs):
        raise NotImplementedError
    
    def head(self, path, size=1024):
        path = self.formatPath(path)
        return self.client.head(path, size)
    
    def info(self, path, *args, **kwargs):
        path = self.formatPath(path)
        file_status = self.client.get_file_status(path)
        return file_status 
   
    def invalidate_cache(self, path=None):
        raise NotImplementedError
    
    def isdir(self, path):
        path = self.formatPath(path)
        file_status = self.client.get_file_status(path)
        return file_status["is_dir"] 
    
    def isfile(self, path):
        path = self.formatPath(path)
        file_status = self.client.get_file_status(path)
        type = file_status["file_type"]
        if type == 1:
            return True
        return False
    
    def lexists(self, path, **kwargs):
        raise NotImplementedError
    
    def listdir(self, path, detail=True, **kwargs):
        raise NotImplementedError
    
    def ls(self, path, detail=True, **kwargs):
        path = self.formatPath(path)
        file_statuses = self.client.ls(path, detail, **kwargs)
        return file_statuses
 
    def mkdir(self, path, create_parents, **kwargs):
        path = self.formatPath(path)
        self.client.mkdir(path, create_parents, **kwargs)
    
    def mkdirs(self, path, exist_ok=False):
        raise NotImplementedError
    
    def makedir(self, path, create_parents=True, **kwargs):
        path = self.formatPath(path)
        return self.client.mkdir(path, create_parents, **kwargs)
    
    def makedirs(self, path, exist_ok=False):
        raise NotImplementedError
    
    def modified(self, path):
        raise NotImplementedError
    
    def move(self, path1, path2, **kwargs):
        path1 = self.formatPath(path1)
        path2 = self.formatPath(path2)
        self.client.mv(path1, path2)
    
    def mv(self, path1, path2, *args, **kwargs):
        path1 = self.formatPath(path1)
        path2 = self.formatPath(path2)
        self.client.mv(path1, path2)
    
    def open(self, path):
        path = self.formatPath(path)
        return self.client.open(path) 
    
    def pipe(self, path, value=None, **kwargs):
        raise NotImplementedError
    
    def pipe_file(self, path, value, mode="overwrite", **kwargs):
        raise NotImplementedError
    
    def put(self, lpath, rpath, recursive=False, callback=..., maxdepth=None, **kwargs):
        raise NotImplementedError
    
    def put_file(self, lpath, rpath, callback=..., mode="overwrite", **kwargs):
        raise NotImplementedError
    
    def read_block(self, fn, offset, length, delimiter=None):
        raise NotImplementedError
    
    def read_bytes(self, path, start=None, end=None, **kwargs):
        raise NotImplementedError
    
    def read_text(self, path, encoding=None, errors=None, newline=None, **kwargs):
        raise NotImplementedError
    
    def rename(self, path1, path2, **kwargs):
        path1 = self.formatPath(path1)
        path2 = self.formatPath(path2)
        return self.client.rename(path1, path2)
    
    def rm(self, path, recursive=False, **kwargs):
        path = self.formatPath(path)
        self.client.rm(path, recursive)
    
    def rm_file(self, path):
        path = self.formatPath(path)
        self.client.rm(path)
    
    def rmdir(self, path):
        raise NotImplementedError
    
    def sign(self, path, expiration=100, **kwargs):
        raise NotImplementedError
    
    def size(self, path):
        raise NotImplementedError
    
    def sizes(self, paths):
        raise NotImplementedError
    
    def start_transaction(self):
        raise NotImplementedError
    
    def stat(self, path, **kwargs):
        raise NotImplementedError
    
    def tail(self, path, size=1024):
        path = self.formatPath(path)
        return self.client.tail(path, size)
    
    def to_dict(self, *, include_password = True):
        raise NotImplementedError
    
    def to_json(self, *, include_password = True):
        raise NotImplementedError

    def touch(self, path, truncate=True, **kwargs):
        path = self.formatPath(path)
        return self.client.touch(path, truncate)
    
    def tree(self, path = "/", recursion_limit = 2, max_display = 25, display_size = False, prefix = "", is_last = True, first = True, indent_size = 4):
        raise NotImplementedError
    
    def ukey(self, path):
        raise NotImplementedError
    
    def unstrip_protocol(self, name):
        raise NotImplementedError 
    
    def upload(self, lpath, rpath, *args, **kwargs):
        lpath = self.formatPath(lpath)
        rpath = self.formatPath(rpath)
        self.client.upload(lpath, rpath)
    
    def walk(self, path, maxdepth=None, topdown=True, on_error="omit", **kwargs):
        raise NotImplementedError
    
    def write_bytes(self, path, value, **kwargs):
        raise NotImplementedError
    
    def write_text(self, path, value, encoding=None, errors=None, newline=None, **kwargs):
        raise NotImplementedError
    
    def create(self, path, overwrite=True):
        path = self.formatPath(path)
        return self.client.create(path, overwrite)

    def append(self, path):
        path = self.formatPath(path)
        return self.client.append(path)

    def get_file_status(self, path):
        path = self.formatPath(path)
        return self.client.get_file_status(path)
    
    def get_master_info(self):
        return self.client.get_master_info()
    
    def close(self):
        self.client.close()