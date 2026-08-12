"""CurvineFileSystem - fsspec compatible interface for Curvine.

This module provides an fsspec-compatible AbstractFileSystem implementation
for the Curvine distributed file system, allowing integration with tools like
pandas, dask, and other fsspec-based libraries.
"""
from typing import Any, Dict, List, Optional, Union

from fsspec import AbstractFileSystem
from urllib.parse import urlsplit

import curvinefs.curvineClient as curvine_client
import os


class CurvineFileSystem(AbstractFileSystem):
    """fsspec-compatible file system interface for Curvine.

    Args:
        config_path: Path to the Curvine configuration file.
        write_chunk_size: Size of each write chunk in bytes.
        write_chunk_num: Number of write chunks to buffer.
        *args: Additional positional arguments passed to AbstractFileSystem.
        **storage_options: Additional keyword arguments for storage options.
    """

    def __init__(
        self,
        config_path: str,
        write_chunk_size: int,
        write_chunk_num: int,
        *args: Any,
        **storage_options: Any
    ) -> None:
        super().__init__(*args, **storage_options)
        self.client = curvine_client.CurvineClient(
            config_path, write_chunk_num, write_chunk_size
        )

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        autocommit: bool = True,
        cache_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any
    ) -> Any:
        """Open a file for reading or writing.

        Args:
            path: The file path to open.
            mode: The mode to open the file in ('r', 'w', 'a', or 'rw').
            block_size: The block size for reading.
            autocommit: Whether to automatically commit changes.
            cache_options: Options for caching.
            **kwargs: Additional keyword arguments.

        Returns:
            A file-like object (CurvineReader or CurvineWriter).

        Raises:
            ValueError: If mode is not 'r', 'w', 'a', or 'rw'.
        """
        path = self._format_path(path)
        if "r" in mode and ("w" in mode or "a" in mode):
            return self
        elif "w" in mode or "a" in mode:
            return self.create(path)
        elif "r" in mode:
            return self.open(path)
        else:
            raise ValueError("Mode must be r, w, a, or rw")

    def _format_path(self, full_path: str) -> str:
        """Format and normalize a file path.

        Args:
            full_path: The full path to format.

        Returns:
            The normalized path with forward slashes.

        Raises:
            ValueError: If path is None.
        """
        if full_path is None:
            raise ValueError("Path must be non-empty string")
        parsed = urlsplit(full_path)
        path = parsed.path
        normalized = os.path.normpath(path).replace("\\", "/")
        if len(normalized) > 1 and normalized.endswith("/"):
            normalized = normalized.rstrip("/")

        return normalized

    def cat(self, path: Union[str, List[str]], **kwargs: Any) -> Union[bytes, List[str], Dict[str, Any]]:
        """Return the entire contents of a file(s).

        Args:
            path: A single file path or list of paths.
            **kwargs: Additional keyword arguments.

        Returns:
            For a file: bytes of the file contents.
            For a directory: list of file names.
            For a list: dictionary mapping paths to contents.

        Raises:
            ValueError: If path is not a string or list.
        """
        path = self._format_path(path)
        if isinstance(path, str):
            if self.isfile(path):
                return self.client.read_range(path, 0, -1)
            else:
                return self.client.ls(path, False)
        elif isinstance(path, list):
            res = {}
            for item in path:
                temp_path = self._format_path(item)
                if self.isfile(temp_path):
                    res[temp_path] = self.client.read_range(temp_path, 0, -1)
                else:
                    res[temp_path] = self.client.ls(temp_path, False)
            return res
        else:
            raise ValueError("Path can be only string or list")

    def cat_file(self, path: str, start: int = 0, end: Optional[int] = None, **kwargs: Any) -> bytes:
        """Fetch bytes from a file within a byte range.

        Args:
            path: The file path.
            start: Starting byte offset.
            end: Ending byte offset (exclusive). If None, reads to end.
            **kwargs: Additional keyword arguments.

        Returns:
            The bytes read from the file.
        """
        path = self._format_path(path)

        if start is None:
            start = 0

        if end is None:
            length = -1
        else:
            length = end - start

        return self.client.read_range(path, start, length)

    def cat_ranges(
        self,
        paths: List[str],
        starts: List[int],
        ends: List[int],
        max_gap: Optional[int] = None,
        on_error: str = "return",
        **kwargs: Any
    ) -> Any:
        raise NotImplementedError

    def checksum(self, path: str) -> Any:
        raise NotImplementedError

    def clear_instance_cache(self) -> None:
        raise NotImplementedError

    def copy(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Copy a file or directory.

        Args:
            path1: Source path.
            path2: Destination path.
            **kwargs: Additional keyword arguments.
        """
        path1 = self._format_path(path1)
        path2 = self._format_path(path2)
        self.client.copy(path1, path2)

    def cp(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Alias for copy."""
        path1 = self._format_path(path1)
        path2 = self._format_path(path2)
        self.client.copy(path1, path2)

    def created(self, path: str) -> Any:
        raise NotImplementedError

    def current(self) -> Any:
        raise NotImplementedError

    def delete(self, path: str, recursive: bool = False, **kwargs: Any) -> None:
        """Delete a file or directory.

        Args:
            path: Path to delete.
            recursive: If True, delete directories recursively.
            **kwargs: Additional keyword arguments.
        """
        path = self._format_path(path)
        self.client.rm(path, recursive)

    def disk_usage(self, path: str, total: bool = True, maxdepth: Optional[int] = None, **kwargs: Any) -> Any:
        raise NotImplementedError

    def download(self, rpath: str, lpath: str, *args: Any, **kwargs: Any) -> Any:
        """Download a remote file to local path.

        Args:
            rpath: Remote path.
            lpath: Local path.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            Number of bytes written.
        """
        rpath = self._format_path(rpath)
        lpath = self._format_path(lpath)
        return self.client.download(rpath, lpath)

    def du(
        self,
        path: str,
        total: bool = True,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        **kwargs: Any
    ) -> Any:
        raise NotImplementedError

    def end_transaction(self) -> None:
        raise NotImplementedError

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if a file or directory exists.

        Args:
            path: Path to check.
            **kwargs: Additional keyword arguments.

        Returns:
            True if the path exists, False otherwise.
        """
        path = self._format_path(path)
        try:
            status = self.client.get_file_status(path)
        except Exception:
            return False

        return status is not None

    def expand_path(self, path: str, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs: Any) -> Any:
        raise NotImplementedError

    def find(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        withdirs: bool = False,
        detail: bool = False,
        **kwargs: Any
    ) -> Any:
        raise NotImplementedError

    def get(
        self,
        rpath: str,
        lpath: str,
        recursive: bool = False,
        callback: Any = ...,
        maxdepth: Optional[int] = None,
        **kwargs: Any
    ) -> Any:
        raise NotImplementedError

    def get_file(self, rpath: str, lpath: str, callback: Any = ..., outfile: Optional[str] = None, **kwargs: Any) -> Any:
        raise NotImplementedError

    def get_mapper(
        self,
        root: str = "",
        check: bool = False,
        create: bool = False,
        missing_exceptions: Optional[Any] = None
    ) -> Any:
        raise NotImplementedError

    def glob(self, path: str, maxdepth: Optional[int] = None, **kwargs: Any) -> Any:
        raise NotImplementedError

    def head(self, path: str, size: int = 1024) -> bytes:
        """Read the first N bytes of a file.

        Args:
            path: File path.
            size: Number of bytes to read.

        Returns:
            The first N bytes of the file.
        """
        path = self._format_path(path)
        return self.client.head(path, size)

    def info(self, path: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """Get information about a file or directory.

        Args:
            path: Path to get info for.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            Dictionary containing file status information.
        """
        path = self._format_path(path)
        file_status = self.client.get_file_status(path)
        return file_status

    def invalidate_cache(self, path: Optional[str] = None) -> None:
        raise NotImplementedError

    def isdir(self, path: str) -> bool:
        """Check if a path is a directory.

        Args:
            path: Path to check.

        Returns:
            True if the path is a directory, False otherwise.
        """
        path = self._format_path(path)
        file_status = self.client.get_file_status(path)
        return file_status["is_dir"]

    def isfile(self, path: str) -> bool:
        """Check if a path is a file.

        Args:
            path: Path to check.

        Returns:
            True if the path is a file, False otherwise.
        """
        path = self._format_path(path)
        file_status = self.client.get_file_status(path)
        file_type = file_status["file_type"]
        return file_type == 1

    def lexists(self, path: str, **kwargs: Any) -> Any:
        raise NotImplementedError

    def listdir(self, path: str, detail: bool = True, **kwargs: Any) -> Any:
        raise NotImplementedError

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> List[Dict[str, Any]]:
        """List files in a directory.

        Args:
            path: Directory path.
            detail: If True, return detailed information.
            **kwargs: Additional keyword arguments.

        Returns:
            List of file information dictionaries.
        """
        path = self._format_path(path)
        file_statuses = self.client.ls(path, detail, **kwargs)
        return file_statuses

    def mkdir(self, path: str, create_parents: bool, **kwargs: Any) -> None:
        """Create a directory.

        Args:
            path: Directory path to create.
            create_parents: If True, create parent directories as needed.
            **kwargs: Additional keyword arguments.
        """
        path = self._format_path(path)
        self.client.mkdir(path, create_parents, **kwargs)

    def mkdirs(self, path: str, exist_ok: bool = False) -> None:
        raise NotImplementedError

    def makedir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        """Alias for mkdir."""
        path = self._format_path(path)
        return self.client.mkdir(path, create_parents, **kwargs)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        raise NotImplementedError

    def modified(self, path: str) -> Any:
        raise NotImplementedError

    def move(self, path1: str, path2: str, **kwargs: Any) -> None:
        """Move a file or directory.

        Args:
            path1: Source path.
            path2: Destination path.
            **kwargs: Additional keyword arguments.
        """
        path1 = self._format_path(path1)
        path2 = self._format_path(path2)
        self.client.mv(path1, path2)

    def mv(self, path1: str, path2: str, *args: Any, **kwargs: Any) -> None:
        """Alias for move."""
        path1 = self._format_path(path1)
        path2 = self._format_path(path2)
        self.client.mv(path1, path2)

    def open(self, path: str) -> Any:
        """Open a file for reading.

        Args:
            path: File path to open.

        Returns:
            A CurvineReader instance.
        """
        path = self._format_path(path)
        return self.client.open(path)

    def pipe(self, path: str, value: Optional[Any] = None, **kwargs: Any) -> Any:
        raise NotImplementedError

    def pipe_file(self, path: str, value: Any, mode: str = "overwrite", **kwargs: Any) -> Any:
        raise NotImplementedError

    def put(
        self,
        lpath: str,
        rpath: str,
        recursive: bool = False,
        callback: Any = ...,
        maxdepth: Optional[int] = None,
        **kwargs: Any
    ) -> Any:
        raise NotImplementedError

    def put_file(self, lpath: str, rpath: str, callback: Any = ..., mode: str = "overwrite", **kwargs: Any) -> Any:
        raise NotImplementedError

    def read_block(self, fn: str, offset: int, length: int, delimiter: Optional[bytes] = None) -> Any:
        raise NotImplementedError

    def read_bytes(self, path: str, start: Optional[int] = None, end: Optional[int] = None, **kwargs: Any) -> Any:
        raise NotImplementedError

    def read_text(self, path: str, encoding: Optional[str] = None, errors: Optional[str] = None,
                  newline: Optional[str] = None, **kwargs: Any) -> Any:
        raise NotImplementedError

    def rename(self, path1: str, path2: str, **kwargs: Any) -> Any:
        """Rename a file or directory.

        Args:
            path1: Current path.
            path2: New path.
            **kwargs: Additional keyword arguments.

        Returns:
            Result of the rename operation.
        """
        path1 = self._format_path(path1)
        path2 = self._format_path(path2)
        return self.client.rename(path1, path2)

    def rm(self, path: str, recursive: bool = False, **kwargs: Any) -> None:
        """Remove a file or directory.

        Args:
            path: Path to remove.
            recursive: If True, remove directories recursively.
            **kwargs: Additional keyword arguments.
        """
        path = self._format_path(path)
        self.client.rm(path, recursive)

    def rm_file(self, path: str) -> None:
        """Remove a file.

        Args:
            path: File path to remove.
        """
        path = self._format_path(path)
        self.client.rm(path)

    def rmdir(self, path: str) -> None:
        raise NotImplementedError

    def sign(self, path: str, expiration: int = 100, **kwargs: Any) -> Any:
        raise NotImplementedError

    def size(self, path: str) -> Any:
        raise NotImplementedError

    def sizes(self, paths: List[str]) -> Any:
        raise NotImplementedError

    def start_transaction(self) -> None:
        raise NotImplementedError

    def stat(self, path: str, **kwargs: Any) -> Any:
        raise NotImplementedError

    def tail(self, path: str, size: int = 1024) -> bytes:
        """Read the last N bytes of a file.

        Args:
            path: File path.
            size: Number of bytes to read.

        Returns:
            The last N bytes of the file.
        """
        path = self._format_path(path)
        return self.client.tail(path, size)

    def to_dict(self, *, include_password: bool = True) -> Dict[str, Any]:
        raise NotImplementedError

    def to_json(self, *, include_password: bool = True) -> str:
        raise NotImplementedError

    def touch(self, path: str, truncate: bool = True, **kwargs: Any) -> None:
        """Create an empty file or update timestamp.

        Args:
            path: Path to touch.
            truncate: If True, truncate if file exists.
            **kwargs: Additional keyword arguments.
        """
        path = self._format_path(path)
        return self.client.touch(path, truncate)

    def tree(
        self,
        path: str = "/",
        recursion_limit: int = 2,
        max_display: int = 25,
        display_size: bool = False,
        prefix: str = "",
        is_last: bool = True,
        first: bool = True,
        indent_size: int = 4
    ) -> Any:
        raise NotImplementedError

    def ukey(self, path: str) -> Any:
        raise NotImplementedError

    def unstrip_protocol(self, name: str) -> Any:
        raise NotImplementedError

    def upload(self, lpath: str, rpath: str, *args: Any, **kwargs: Any) -> None:
        """Upload a local file to remote path.

        Args:
            lpath: Local file path.
            rpath: Remote destination path.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        lpath = self._format_path(lpath)
        rpath = self._format_path(rpath)
        self.client.upload(lpath, rpath)

    def walk(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        topdown: bool = True,
        on_error: str = "omit",
        **kwargs: Any
    ) -> Any:
        raise NotImplementedError

    def write_bytes(self, path: str, value: bytes, **kwargs: Any) -> Any:
        raise NotImplementedError

    def write_text(
        self,
        path: str,
        value: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        **kwargs: Any
    ) -> Any:
        raise NotImplementedError

    def create(self, path: str, overwrite: bool = True) -> Any:
        """Create a file for writing.

        Args:
            path: File path to create.
            overwrite: If True, overwrite existing file.

        Returns:
            A CurvineWriter instance.
        """
        path = self._format_path(path)
        return self.client.create(path, overwrite)

    def append(self, path: str) -> Any:
        """Open a file for appending.

        Args:
            path: File path to append to.

        Returns:
            A CurvineWriter instance.
        """
        path = self._format_path(path)
        return self.client.append(path)

    def get_file_status(self, path: str) -> Optional[Dict[str, Any]]:
        """Get status information for a file or directory.

        Args:
            path: Path to get status for.

        Returns:
            Dictionary containing file status information, or None if not found.
        """
        path = self._format_path(path)
        return self.client.get_file_status(path)

    def get_master_info(self) -> Dict[str, Any]:
        """Get information about the Curvine cluster.

        Returns:
            Dictionary containing cluster information.
        """
        return self.client.get_master_info()

    def close(self) -> None:
        """Close the file system connection."""
        self.client.close()