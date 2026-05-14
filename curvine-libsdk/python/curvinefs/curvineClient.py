import curvine_libsdk
from curvine_libsdk._proto.master_pb2 import (
    GetFileStatusResponse,
    GetMasterInfoResponse,
    ListStatusResponse,
)
from curvinefs.curvineReader import CurvineReader
from curvinefs.curvineWriter import CurvineWriter


class CurvineClient:
    def __init__(self, config_path, write_chunk_num, write_chunk_size):
        try:
            self.file_system_ptr = curvine_libsdk.python_io_curvine_curvine_native_new_filesystem(
                config_path
            )
        except Exception as e:
            raise IOError(f"Native create file system failed: {e}") from e
        self.write_chunk_num = write_chunk_num
        self.write_chunk_size = write_chunk_size

    def get_file_status(self, path):
        status_bytes = curvine_libsdk.python_io_curvine_curvine_native_get_file_status(
            self.file_system_ptr, path
        )
        status = GetFileStatusResponse()
        status.ParseFromString(status_bytes)
        file_status = status.status  # FileStatusProto
        return {
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

    def path_exists(self, path):
        try:
            self.get_file_status(path)
            return True
        except FileNotFoundError:
            return False

    def batch_path_exists(self, paths):
        """Return existence flags in the same order as ``paths``."""
        return [self.path_exists(p) for p in paths]

    def get_master_info(self):
        try:
            status_bytes = curvine_libsdk.python_io_curvine_curvine_native_get_master_info(
                self.file_system_ptr
            )
        except Exception as e:
            raise IOError(f"Native get master information failed: {e}") from e
        status = GetMasterInfoResponse()
        status.ParseFromString(status_bytes)
        return {
            "active_master": status.active_master,
            "journal_nodes": list(status.journal_nodes),
            "inode_dir_num": status.inode_dir_num,
            "inode_file_num": status.inode_file_num,
            "block_num": status.block_num,
            "capacity": status.capacity,
            "available": status.available,
            "fs_used": status.fs_used,
            "non_fs_used": status.non_fs_used,
            "reserved_bytes": status.reserved_bytes,
            "live_workers": list(status.live_workers),
            "blacklist_workers": list(status.blacklist_workers),
            "decommission_workers": list(status.decommission_workers),
            "lost_workers": list(status.lost_workers),
        }

    def mkdir(self, path, create_parents):
        if not isinstance(create_parents, bool):
            raise TypeError("create_parents must be a boolean")
        try:
            is_success = curvine_libsdk.python_io_curvine_curvine_native_mkdir(
                self.file_system_ptr, path, create_parents
            )
        except Exception as e:
            raise IOError(f"Native make directory failed: {e}") from e

        if not is_success:
            raise IOError("mkdir failed")

    def rm(self, path, recursive=False):
        try:
            curvine_libsdk.python_io_curvine_curvine_native_delete(self.file_system_ptr, path, recursive)
        except Exception as e:
            raise IOError(f"Native delete file failed: {e}") from e

    def rename(self, path1, path2):
        try:
            curvine_libsdk.python_io_curvine_curvine_native_rename(self.file_system_ptr, path1, path2)
        except Exception as e:
            raise IOError(f"Native rename file failed: {e}") from e

    def list_status(self, path):
        try:
            status_bytes = curvine_libsdk.python_io_curvine_curvine_native_list_status(
                self.file_system_ptr, path
            )
        except Exception as e:
            raise IOError(f"Native list status failed: {e}") from e

        if not status_bytes:
            raise IOError("Received empty status data")

        status = ListStatusResponse()
        status.ParseFromString(status_bytes)
        return status.statuses

    def ls(self, path, detail=True, **kwargs):
        list_status = self.list_status(path)

        if not detail:
            return [item.name for item in list_status]

        result = []
        for item in list_status:
            type_num = item.file_type
            if type_num == 0:
                ft = "directory"
            elif type_num == 1:
                ft = "file"
            elif type_num == 2:
                ft = "link"
            elif type_num == 3:
                ft = "stream"
            elif type_num == 4:
                ft = "agg"
            elif type_num == 5:
                ft = "object"
            else:
                ft = "unknown"

            entry = {
                "name": item.path,
                "size": item.len if not item.is_dir else None,
                "type": ft,
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
            raise IOError(f"Native open reader failed: {e}") from e
        try:
            file_status = self.get_file_status(path)
        except Exception:
            curvine_libsdk.python_io_curvine_curvine_native_close_reader(readerHandle)
            raise
        reader = CurvineReader(readerHandle, file_status["len"])
        return reader

    def read_range(self, path, offset, length):
        try:
            file_status = self.get_file_status(path)
        except FileNotFoundError as e:
            raise FileNotFoundError(path) from e

        if not isinstance(offset, int):
            raise ValueError("Offset must be an integer")

        if offset < 0:
            offset = file_status["len"] + offset

        if length is None or length == -1:
            if offset >= file_status["len"]:
                raise ValueError("Offset exceeds file size")
            length = file_status["len"] - offset

        if not isinstance(length, int) or length < 0:
            raise ValueError("Length must be a non-negative integer, -1, or None")

        if length == 0:
            return b""
        reader = self.open(path)
        try:
            return reader.read(offset, length)
        finally:
            reader.close()

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
            writerHandle = curvine_libsdk.python_io_curvine_curvine_native_create(
                self.file_system_ptr, path, overwrite
            )
        except Exception as e:
            raise IOError(f"Native create writer failed: {e}") from e
        return CurvineWriter(writerHandle, self.write_chunk_num, self.write_chunk_size)

    def write_string(self, path, data):
        writer = self.create(path, True)
        try:
            if isinstance(data, str):
                byte_data = data.encode("utf-8")
            else:
                byte_data = bytes(data)
            writer.write(byte_data)
        finally:
            writer.close()

    def append(self, path):
        tmp = [0]
        try:
            writer_handle = curvine_libsdk.python_io_curvine_curvine_native_append(self.file_system_ptr, path, tmp)
        except Exception as e:
            raise IOError(f"Native append failed: {e}") from e
        return CurvineWriter(writer_handle, self.write_chunk_num, self.write_chunk_size)

    def mv(self, path1, path2):
        try:
            self.rename(path1, path2)
        except (OSError, NotImplementedError) as e:
            raise OSError("Move file failed") from e

    def touch(self, path, truncate=True):
        try:
            self.get_file_status(path)
        except FileNotFoundError:
            writer = self.create(path, True)
            writer.close()
            return
        if truncate:
            self.rm(path)
            writer = self.create(path, True)
            writer.close()
            return
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
        elif isinstance(path1, str) and isinstance(path2, str):
            file_status1 = self.get_file_status(path1)
            file_status2 = self.get_file_status(path2)

            src_is_dir = file_status1["is_dir"]
            des_is_dir = file_status2["is_dir"]
            if src_is_dir and des_is_dir:
                self.copy_dir(path1, path2)
            elif not src_is_dir and des_is_dir:
                path1_name = file_status1["name"]
                target = path2.rstrip("/") + "/" + path1_name
                self.copy_file(path1, target)
            elif not src_is_dir and not des_is_dir:
                self.copy_file(path1, path2)
            else:
                raise ValueError("Cannot copy directory to a file")
        elif isinstance(path1, list) and isinstance(path2, str):
            dest_prefix = path2.rstrip("/")
            for p1 in path1:
                fs1 = self.get_file_status(p1)
                target = dest_prefix + "/" + fs1["name"]
                self.copy_file(p1, target)
        else:
            raise ValueError("Invalid path type")

    def copy_dir(self, path1, path2):
        self.mkdir(path2, True)
        list_status = self.list_status(path1)
        for item in list_status:
            target_path = path2 + "/" + item.name
            try:
                if item.is_dir:
                    self.copy_dir(item.path, target_path)
                else:
                    self.copy_file(item.path, target_path)
            except OSError as e:
                raise OSError(f"Copy failed:{item.path} -> {target_path}, error: {e}") from e

    def copy_file(self, path1, path2):
        data = self.read_range(path1, 0, -1)
        self.write_to_new_file(path2, data)

    def download(self, rpath, lpath):
        data = self.read_range(rpath, 0, -1)
        with open(lpath, "wb") as f:
            return f.write(data)

    def upload(self, lpath, rpath):
        with open(lpath, "rb") as f:
            data = f.read()
        self.write_to_new_file(rpath, data)

    def write_to_new_file(self, path, data):
        writer = None
        try:
            writer = self.create(path, True)
            if isinstance(data, str):
                payload = data.encode("utf-8")
            else:
                payload = bytes(data)
            writer.write(payload)
        finally:
            if writer is not None:
                writer.close()

    def close(self):
        try:
            curvine_libsdk.python_io_curvine_curvine_native_close_filesystem(self.file_system_ptr)
        except Exception as e:
            raise IOError(f"Native close file system failed: {e}") from e
        self.file_system_ptr = None
