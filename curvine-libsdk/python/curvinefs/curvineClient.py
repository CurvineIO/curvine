"""CurvineClient - Main client for interacting with the Curvine file system.

This module provides the CurvineClient class which serves as the primary
interface for interacting with the Curvine distributed file system.
"""
from typing import Any, Dict, List, Optional, Union

import curvine_libsdk
from curvine_libsdk._proto.common_pb2 import FileStatusProto
from curvine_libsdk._proto.master_pb2 import (
    GetFileStatusResponse,
    GetMasterInfoResponse,
    ListStatusResponse,
)

from curvinefs.curvineReader import CurvineReader
from curvinefs.curvineWriter import CurvineWriter


class CurvineClient:
    """Client for interacting with the Curvine file system.

    Args:
        config_path: Path to the Curvine configuration file.
        write_chunk_num: Number of write chunks to buffer.
        write_chunk_size: Size of each write chunk in bytes.

    Raises:
        IOError: If file system initialization fails.
    """

    def __init__(self, config_path: str, write_chunk_num: int, write_chunk_size: int) -> None:
        """Create a new CurvineClient instance."""
        try:
            self.file_system_ptr = curvine_libsdk.python_io_curvine_curvine_native_new_filesystem(config_path)
        except Exception as e:
            raise IOError(f"Native create file system failed: {e}")
        self.write_chunk_num = write_chunk_num
        self.write_chunk_size = write_chunk_size

    def get_file_status(self, path: str) -> Optional[Dict[str, Any]]:
        """Get the status of a file or directory.

        Args:
            path: The path to the file or directory.

        Returns:
            A dictionary containing file status information, or None if not found.
            The dictionary includes:
                - id: Unique identifier
                - path: Full path
                - name: File/directory name
                - is_dir: Whether it is a directory
                - mtime: Modification time
                - atime: Access time
                - children_num: Number of children (for directories)
                - is_complete: Whether the file is complete
                - len: File size in bytes
                - replicas: Number of replicas
                - block_size: Block size
                - file_type: File type (0=directory, 1=file, 2=link, etc.)
        """
        try:
            status_bytes = curvine_libsdk.python_io_curvine_curvine_native_get_file_status(
                self.file_system_ptr, path
            )
        except Exception:
            return None
        status = GetFileStatusResponse()
        status.ParseFromString(status_bytes)
        file_status = status.status  # FileStatusProto
        file_status_dict = {
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
        return file_status_dict

    def get_master_info(self) -> Dict[str, Any]:
        """Get information about the Curvine cluster.

        Returns:
            A dictionary containing cluster information:
                - active_master: Active master node
                - journal_nodes: List of journal nodes
                - inode_dir_num: Number of directory inodes
                - inode_file_num: Number of file inodes
                - block_num: Number of blocks
                - capacity: Total capacity
                - available: Available capacity
                - fs_used: Used file system space
                - non_fs_used: Used non-file system space
                - reserved_bytes: Reserved bytes
                - live_workers: List of live worker nodes
                - blacklist_workers: List of blacklisted workers
                - decommission_workers: List of decommissioning workers
                - lost_workers: List of lost workers

        Raises:
            IOError: If getting master info fails.
        """
        try:
            status_bytes = curvine_libsdk.python_io_curvine_curvine_native_get_master_info(self.file_system_ptr)
        except Exception as e:
            raise IOError(f"Native get master information failed: {e}")
        status = GetMasterInfoResponse()
        status.ParseFromString(status_bytes)
        status_dict = {
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
        return status_dict

    def mkdir(self, path: str, create_parents: bool) -> None:
        """Create a directory.

        Args:
            path: Directory path to create.
            create_parents: If True, create parent directories as needed.

        Raises:
            TypeError: If create_parents is not a boolean.
            IOError: If directory creation fails.
        """
        if not isinstance(create_parents, bool):
            raise TypeError("create_parents must be a boolean")
        try:
            is_success = curvine_libsdk.python_io_curvine_curvine_native_mkdir(
                self.file_system_ptr, path, create_parents
            )
        except Exception as e:
            raise IOError(f"Native make directory failed: {e}")

        if not is_success:
            raise IOError("mkdir failed")

    def rm(self, path: str, recursive: bool = False) -> None:
        """Delete a file or directory.

        Args:
            path: Path to delete.
            recursive: If True, delete directories recursively.

        Raises:
            IOError: If deletion fails.
        """
        try:
            curvine_libsdk.python_io_curvine_curvine_native_delete(
                self.file_system_ptr, path, recursive
            )
        except Exception as e:
            raise IOError(f"Native delete file failed: {e}")

    def rename(self, src_path: str, dest_path: str) -> None:
        """Rename a file or directory.

        Args:
            src_path: Current path.
            dest_path: New path.

        Raises:
            IOError: If rename fails.
        """
        try:
            curvine_libsdk.python_io_curvine_curvine_native_rename(
                self.file_system_ptr, src_path, dest_path
            )
        except Exception as e:
            raise IOError(f"Native rename file failed: {e}")

    def list_status(self, path: str) -> List[FileStatusProto]:
        """List the status of entries in a directory.

        Args:
            path: Directory path to list.

        Returns:
            A list of FileStatusProto objects.

        Raises:
            IOError: If listing fails.
        """
        try:
            status_bytes = curvine_libsdk.python_io_curvine_curvine_native_list_status(
                self.file_system_ptr, path
            )
        except Exception as e:
            raise IOError(f"Native list status failed: {e}")

        if not status_bytes:
            raise IOError("Received empty status data")

        status = ListStatusResponse()
        status.ParseFromString(status_bytes)
        file_statuses = status.statuses

        return file_statuses

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> Union[List[str], List[Dict[str, Any]]]:
        """List files in a directory.

        Args:
            path: Directory path to list.
            detail: If True, return detailed information. If False, return only names.
            **kwargs: Additional keyword arguments.

        Returns:
            If detail is False: List of file names.
            If detail is True: List of dictionaries with keys:
                - name: Full path
                - size: File size (None for directories)
                - type: File type ("directory", "file", "link", "stream", "agg", "object", "unknown")
                - mtime: Modification time
                - atime: Access time
        """
        list_status = self.list_status(path)

        if not detail:
            return [item.name for item in list_status]

        result = []
        for item in list_status:
            type_num = item.file_type
            file_type = ""
            if type_num == 0:
                file_type = "directory"
            elif type_num == 1:
                file_type = "file"
            elif type_num == 2:
                file_type = "link"
            elif type_num == 3:
                file_type = "stream"
            elif type_num == 4:
                file_type = "agg"
            elif type_num == 5:
                file_type = "object"
            else:
                file_type = "unknown"

            entry = {
                "name": item.path,
                "size": item.len if not item.is_dir else None,
                "type": file_type,
                "mtime": item.mtime,
                "atime": item.atime,
            }
            result.append(entry)

        return result

    def open(self, path: str) -> CurvineReader:
        """Open a file for reading.

        Args:
            path: File path to open.

        Returns:
            A CurvineReader instance.

        Raises:
            IOError: If opening fails.
        """
        tmp = [0]
        try:
            reader_handle = curvine_libsdk.python_io_curvine_curvine_native_open(
                self.file_system_ptr, path, tmp
            )
        except Exception as e:
            raise IOError(f"Native open reader failed: {e}")
        file_status = self.get_file_status(path)
        reader = CurvineReader(reader_handle, file_status["len"])
        return reader

    def read_range(self, path: str, offset: int, length: Optional[int]) -> bytes:
        """Read a range of bytes from a file.

        Args:
            path: File path to read from.
            offset: Starting byte offset. Negative values count from end.
            length: Number of bytes to read. -1 or None reads to end.

        Returns:
            The bytes read from the file.

        Raises:
            FileNotFoundError: If file not found.
            ValueError: If offset or length are invalid.
            IOError: If reading fails.
        """
        file_status = self.get_file_status(path)

        if file_status is None:
            raise FileNotFoundError("File not found")

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
        data = reader.read(offset, length)
        reader.close()

        return data

    def head(self, path: str, size: int) -> bytes:
        """Read the first N bytes of a file.

        Args:
            path: File path to read from.
            size: Number of bytes to read from the beginning.

        Returns:
            The first N bytes of the file.

        Raises:
            ValueError: If size is negative or None.
        """
        if size < 0 or size is None:
            raise ValueError("size must be non-negative integer")

        return self.read_range(path, 0, size)

    def tail(self, path: str, size: int) -> bytes:
        """Read the last N bytes of a file.

        Args:
            path: File path to read from.
            size: Number of bytes to read from the end.

        Returns:
            The last N bytes of the file.

        Raises:
            ValueError: If size is negative.
        """
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

    def create(self, path: str, overwrite: bool) -> CurvineWriter:
        """Create a file for writing.

        Args:
            path: File path to create.
            overwrite: If True, overwrite existing file.

        Returns:
            A CurvineWriter instance.

        Raises:
            IOError: If creation fails.
        """
        try:
            writer_handle = curvine_libsdk.python_io_curvine_curvine_native_create(
                self.file_system_ptr, path, overwrite
            )
        except Exception as e:
            raise IOError(f"Native create writer failed: {e}")
        writer = CurvineWriter(writer_handle, self.write_chunk_num, self.write_chunk_size)
        return writer

    def write_string(self, path: str, data: str) -> None:
        """Write a string to a file.

        Args:
            path: File path to write to.
            data: String data to write.
        """
        writer = self.create(path, True)
        byte_data = bytes(data, "utf-8")
        writer.write(byte_data)
        writer.close()

    def append(self, path: str) -> CurvineWriter:
        """Open a file for appending.

        Args:
            path: File path to append to.

        Returns:
            A CurvineWriter instance.

        Raises:
            IOError: If append fails.
        """
        tmp = [0]
        try:
            writer_handle = curvine_libsdk.python_io_curvine_curvine_native_append(
                self.file_system_ptr, path, tmp
            )
        except Exception as e:
            raise IOError(f"Native append failed: {e}")
        writer = CurvineWriter(writer_handle, self.write_chunk_num, self.write_chunk_size)
        return writer

    def mv(self, src_path: str, dest_path: str) -> None:
        """Move a file or directory.

        Args:
            src_path: Source path.
            dest_path: Destination path.

        Raises:
            OSError: If move fails.
        """
        try:
            self.rename(src_path, dest_path)
        except (OSError, NotImplementedError):
            raise OSError("Move file failed")

    def touch(self, path: str, truncate: bool = True) -> None:
        """Create an empty file or update timestamp.

        Args:
            path: File path to touch.
            truncate: If True, truncate if file exists.

        Raises:
            NotImplementedError: If truncate is False (timestamp update not implemented).
        """
        file_status = self.get_file_status(path)
        if file_status is None:
            writer = self.create(path, True)
            writer.close()
        elif truncate:
            self.rm(path)
            writer = self.create(path, True)
            writer.close()
        else:
            raise NotImplementedError("Update timestamp operation is not implemented")

    def copy(self, src_path: Union[str, List[str]], dest_path: Union[str, List[str]], **kwargs: Any) -> None:
        """Copy a file or directory.

        Args:
            src_path: Source path or list of paths.
            dest_path: Destination path or list of paths.
            **kwargs: Additional keyword arguments.

        Raises:
            ValueError: If path types are invalid or mismatched.
            FileNotFoundError: If source file not found (for list copy, errors are skipped).
        """
        if isinstance(src_path, list) and isinstance(dest_path, list):
            if len(src_path) != len(dest_path):
                raise ValueError("Source and target lists must have the same length")
            for p1, p2 in zip(src_path, dest_path):
                try:
                    self.copy_file(p1, p2, **kwargs)
                except FileNotFoundError:
                    continue
        elif isinstance(src_path, str) and isinstance(dest_path, str):
            src_status = self.get_file_status(src_path)
            dest_status = self.get_file_status(dest_path)

            if src_status is None:
                raise FileNotFoundError(f"Source path not found: {src_path}")
            if dest_status is None:
                raise FileNotFoundError(f"Destination path not found: {dest_path}")

            src_is_dir = src_status["is_dir"]
            dest_is_dir = dest_status["is_dir"]

            if src_is_dir and dest_is_dir:
                self.copy_dir(src_path, dest_path)
            elif not src_is_dir and dest_is_dir:
                path_name = src_status["name"]
                target = dest_path + "/" + path_name
                self.copy_file(src_path, target)
            elif not src_is_dir and not dest_is_dir:
                self.copy_file(src_path, dest_path)
            else:
                raise ValueError("Cannot copy directory to a file")
        elif isinstance(src_path, list) and isinstance(dest_path, str):
            dest_status = self.get_file_status(dest_path)
            if dest_status is None:
                raise FileNotFoundError(f"Destination path not found: {dest_path}")
            if not dest_status["is_dir"]:
                raise ValueError("Destination must be a directory when copying multiple files")
            for p1 in src_path:
                src_status = self.get_file_status(p1)
                if src_status is None:
                    raise FileNotFoundError(f"Source path not found: {p1}")
                path_name = src_status["name"]
                target = dest_path + "/" + path_name
                self.copy_file(p1, target)
        else:
            raise ValueError("Invalid path type")

    def copy_dir(self, src_path: str, dest_path: str) -> None:
        """Recursively copy a directory.

        Args:
            src_path: Source directory path.
            dest_path: Destination directory path.

        Raises:
            OSError: If copy fails.
        """
        self.mkdir(dest_path, True)
        list_status = self.list_status(src_path)
        for item in list_status:
            target_path = dest_path + "/" + item.name
            try:
                if item.is_dir:
                    self.copy_dir(item.path, target_path)
                else:
                    self.copy_file(item.path, target_path)
            except OSError as e:
                raise OSError(f"Copy failed: {item.path} -> {target_path}, error: {e}")

    def copy_file(self, src_path: str, dest_path: str) -> None:
        """Copy a single file.

        Args:
            src_path: Source file path.
            dest_path: Destination file path.
        """
        data = self.read_range(src_path, 0, -1)
        self.write_to_new_file(dest_path, data)

    def download(self, remote_path: str, local_path: str) -> int:
        """Download a remote file to local path.

        Args:
            remote_path: Remote file path.
            local_path: Local destination path.

        Returns:
            Number of bytes written.
        """
        with open(local_path, "wb") as f:
            data = self.read_range(remote_path, 0, -1)
            return f.write(data)

    def upload(self, local_path: str, remote_path: str) -> None:
        """Upload a local file to remote path.

        Args:
            local_path: Local file path.
            remote_path: Remote destination path.
        """
        with open(local_path, "rb") as f:
            data = f.read()
            self.write_to_new_file(remote_path, data)

    def write_to_new_file(self, path: str, data: Union[bytes, str]) -> None:
        """Write data to a new file.

        Args:
            path: File path to write to.
            data: Data to write (bytes or string).
        """
        writer = self.create(path, True)
        if isinstance(data, str):
            byte_data = bytes(data, "utf-8")
        else:
            byte_data = data
        writer.write(byte_data)
        writer.close()

    def close(self) -> None:
        """Close the file system connection.

        Raises:
            IOError: If closing fails.
        """
        try:
            curvine_libsdk.python_io_curvine_curvine_native_close_filesystem(self.file_system_ptr)
        except Exception as e:
            raise IOError(f"Native close file system failed: {e}")
        self.file_system_ptr = None