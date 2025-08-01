import curvine_libsdk
from common_pb2 import FileStatusProto
from master_pb2 import GetFileStatusResponse, GetMasterInfoResponse,ListStatusResponse
from curvinefs.curvineReader import CurvineReader
from curvinefs.curvineWriter import CurvineWriter

class CurvineClient:
    def __init__(self, config_path, write_chunk_num, write_chunk_size):
        # Create curvine file system
        try:
            self.file_system_ptr = curvine_libsdk.python_io_curvine_curvine_native_new_filesystem(config_path)
        except Exception as e:
            raise IOError(f"Native create file system failed: {e}")
        self.write_chunk_num = write_chunk_num
        self.write_chunk_size = write_chunk_size

    def get_file_status(self, path):   
        try:
            status_bytes = curvine_libsdk.python_io_curvine_curvine_native_get_file_status(self.file_system_ptr, path)    
        except Exception as e:
            return None
        status = GetFileStatusResponse()
        status.ParseFromString(status_bytes)  # GetFileStatusResponse
        file_status = status.status # FileStatusProto
        file_staus_dict = {
            "id": file_status.id,
            "path": file_status.path,
            "name": file_status.name,
            "is_dir": file_status.is_dir,
            "mtime": file_status.mtime,
            "atime": file_status.atime,
            "children_num": file_status.children_num,
            "is_complete": file_status.is_complete,
            "len": file_status.len,
            "replicas": file_status.replicas,
            "block_size": file_status.block_size,
            "file_type": file_status.file_type,
        }
        return file_staus_dict

    def get_master_info(self):
        try:
            status_bytes = curvine_libsdk.python_io_curvine_curvine_native_get_master_info(self.file_system_ptr)
        except Exception as e:
            raise IOError(f"Native get master information failed: {e}")
        status = GetMasterInfoResponse()
        status.ParseFromString(status_bytes)  # GetMasterInfoResponse
        status_dict = {
            "active_master": status.active_master,
            "journal_nodes": status.journal_nodes,
            "inode_num": status.inode_num,
            "block_num": status.block_num,
            "capacity": status.capacity,
            "available": status.available,
            "fs_used": status.fs_used,
            "non_fs_used": status.non_fs_used,
            "live_workers": status.live_workers,
            "blacklist_workers": status.blacklist_workers,
            "decommission_workers": status.decommission_workers,
            "lost_workers": status.lost_workers,
        }
        return status_dict

    def mkdir(self, path, create_parents):
        if not isinstance(create_parents, bool):
            raise TypeError("create_parents must be a boolean")
        try:
            is_success = curvine_libsdk.python_io_curvine_curvine_native_mkdir(self.file_system_ptr, path, create_parents)
        except Exception as e:
            raise IOError(f"Native make directory failed: {e}")
        
        if not is_success:
            raise IOError("mkdir failed")

    def rm(self, path, recursive=False): # delete
        try:
            curvine_libsdk.python_io_curvine_curvine_native_delete(self.file_system_ptr, path, recursive)
        except Exception as e:
            raise IOError(f"Native delete file failed: {e}")

    def rename(self, path1, path2):  
        try:
            curvine_libsdk.python_io_curvine_curvine_native_rename(self.file_system_ptr, path1, path2)
        except Exception as e:
            raise IOError(f"Native rename file failed: {e}")

    def list_status(self, path):
        try:
            status_bytes = curvine_libsdk.python_io_curvine_curvine_native_list_status(self.file_system_ptr,path)
        except Exception as e:
            raise IOError(f"Native list status failed: {e}")
        
        if not status_bytes:
            raise IOError("Received empty status data")
        
        status = ListStatusResponse()
        status.ParseFromString(status_bytes)  # ListStatusResponse
        file_statuses = status.statuses # FileStatusProto
        
        return file_statuses

    def ls(self, path, detail=True, **kwargs):
        list_status = self.list_status(path)    

        if not detail:
            return [item.name for item in list_status]
        
        result = []
        for item in list_status:
            type_num = item.file_type
            type = ""
            if type_num == 0:
                type = "directory"
            elif type_num == 1:
                type = "file"
            elif type_num == 2:
                type = "link"
            elif type_num == 3:
                type = "stream"
            elif type_num == 4:
                type = "agg"
            elif type_num == 5:
                type = "object"
            else:
                type = "unknown"

            entry = {
                "name": item.path,
                "size": item.len if not item.is_dir else None,  
                "type":  type,
                "mtime": item.mtime,
                "atime": item.atime,
             }
            result.append(entry)
        
        return result
    
    def open(self, path):
        tmp = [0]
        try:
            readerHandle = curvine_libsdk.python_io_curvine_curvine_native_open(self.file_system_ptr, path, tmp)
        except Exception as e:
            raise IOError(f"Native open reader failed: {e}")
        file_status = self.get_file_status(path)
        reader = CurvineReader(readerHandle, file_status["len"])
        return reader
    
    def read_range(self, path, offset, length): # cat file
        file_status = self.get_file_status(path)

        if file_status is None:
            raise FileNotFoundError("File not found")
            
        if not isinstance(offset, int):
            raise ValueError("Offset must be an integer")
        
        if offset < 0: 
            offset = file_status["len"] + offset
        
        if length is None or length==-1:  # read until the end of the file
            if offset >= file_status["len"]:
                raise ValueError("Offset exceeds file size")
            length = file_status["len"] - offset
        
        if not isinstance(length, int) or length < 0:
            raise ValueError("Length must be a non-negative integer, -1, or None")
        
        if length==0:
            return b""
        reader = self.open(path)
        data = reader.read(offset, length)
        reader.close()

        return data
    
    def head(self, path, size):
        if size < 0 or size is None:
            raise ValueError("size must be non-negative integer")
       
        return self.read_range(path, 0, size)
    
    def tail(self, path, size):
        if size < 0:
            raise ValueError("size must be non-negative")
       
        file_status = self.get_file_status(path)
        file_length = file_status["len"]
        
        if file_length == 0:
            return b""
        
        if size > file_length:
            size = file_length
        
        start = max(0, file_length - size)  
        return self.read_range(path, start, min(size, file_length - start))
    
    def create(self, path, overwrite):
        try:
            writerHandle = curvine_libsdk.python_io_curvine_curvine_native_create(self.file_system_ptr, path, overwrite)
        except Exception as e:
            raise IOError(f"Native create writer failed: {e}")
        writer = CurvineWriter(writerHandle, self.write_chunk_num, self.write_chunk_size)
        return writer
        
    def write_string(self, path, data):
        writer = self.create(path, True)
        byte_data = bytes(data, 'utf-8')
        writer.write(byte_data)
        writer.close()
    
    def append(self, path):
        tmp = [0]
        try:
            writer_handle = curvine_libsdk.python_io_curvine_curvine_native_append(self.file_system_ptr, path, tmp)
        except Exception as e:
            raise IOError(f"Native append failed: {e}")
        writer = CurvineWriter(writer_handle, self.write_chunk_num, self.write_chunk_size)
        return writer

    def mv(self, path1, path2):
        try:
            self.rename(path1, path2)
        except (OSError, NotImplementedError):
            raise OSError("Move file failed")
        
    def touch(self, path, truncate=True):
        file_status = self.get_file_status(path)
        if file_status is None:
            writer =self.create(path, True)
            writer.close()
        elif truncate:
            self.rm(path)
            writer = self.create(path, True)
            writer.close()
        else:
            raise NotImplementedError("Update timestamp operation is not implemented")
        
    def copy(self, path1, path2, **kwargs):

        if isinstance(path1, list) and isinstance(path2, list):
            if len(path1) != len(path2):
                raise ValueError("Source and target lists must have the same length")
            for p1, p2 in zip(path1, path2):
                try:
                    self.copy_file(p1, p2, **kwargs)
                except FileNotFoundError:
                    continue
        elif isinstance(path1, str) and isinstance(path2, str): # file/dir -> dir
            file_status1 = self.get_file_status(path1)
            file_status2 = self.get_file_status(path2)

            src_is_dir = file_status1["is_dir"]
            des_is_dir = file_status2["is_dir"]
            if src_is_dir and des_is_dir: # dir -> dir
                self.copy_dir(path1, path2) 
            elif not src_is_dir and des_is_dir: # file -> dir
                path1_name = file_status1["name"]
                target = path2 + path1_name 
                self.copy_file(path1, target)
            elif not src_is_dir and not des_is_dir: # file -> file
                self.copy_file(path1, path2)
            else: # dir -> file
                raise ValueError("Cannot copy directory to a file")                
        elif isinstance(path1, list) and isinstance(path2, str): # files -> dir
            for p1 in path1:
                path1_name = file_status1["name"]
                target = path2 + path1_name 
                self.copy_file(p1, target)
        else:
            raise ValueError("Invalid path type")

    def copy_dir(self, path1, path2):
        self.mkdir(path2, True)
        list_status = self.list_status(path1)
        for item in list_status:
            target_path = path2 +'/' + item.name
            try:
                if item.is_dir:
                    self.copy_dir(item.path, target_path)
                else:
                    self.copy_file(item.path, target_path)
            except OSError as e:
                raise OSError(f"Copy failed:{item.path} -> {target_path}, error: {e}")
        
    def copy_file(self, path1, path2):
        data = self.read_range(path1,0,-1)
        self.write_to_new_file(path2,data)
        return

    def download(self, rpath, lpath):
        with open(lpath, "wb") as f:
            data = self.read_range(rpath,0,-1)
            return f.write(data)

    def upload(self, lpath, rpath):
        with open(lpath, "rb") as f:
            data = f.read()
            self.write_to_new_file(rpath, data)

    def write_to_new_file(self, path, data):
        try:
            writer =self.create(path, True)
            self.write_string(data)
        finally:
             writer.close_writer()
     
    def close(self):
        try:
            curvine_libsdk.python_io_curvine_curvine_native_close_filesystem(self.file_system_ptr)
        except Exception as e:
            raise IOError(f"Native close file system failed: {e}")
        self.file_system_ptr = None



    
   
    
        
