"""CurvineReader - File reader for Curvine.

This module provides the CurvineReader class for reading files from the
Curvine distributed file system.
"""
from typing import Any, List, Optional

import ctypes
import curvine_libsdk


class CurvineReader:
    """Reader for reading files from the Curvine file system.

    The native reader is sequential: each ``native_read`` returns the next
    chunk at the native stream position, and ``native_seek`` repositions it.
    This class caches the last returned chunk and serves reads from it, so
    ``offset`` is a relative skip from the current position (0 for plain
    sequential reads). Reading from a fresh reader with ``offset > 0`` starts
    at that absolute position because the stream begins at 0.

    Args:
        native_handle: Native handle for the reader.
        file_size: Total size of the file in bytes.
    """

    def __init__(self, native_handle: Any, file_size: int) -> None:
        """Initialize a new CurvineReader instance."""
        self.reader_handle = native_handle
        self.file_size = file_size
        self.read_buffer = None
        self.read_pos = 0
        self.read_buffer_pos = 0
        self.stream_pos = 0

    def _fill_buffer(self) -> bool:
        """Load the native chunk starting at the current position.

        Returns:
            True if a non-empty chunk was loaded into ``read_buffer``.
        """
        if self.stream_pos != self.read_pos:
            try:
                curvine_libsdk.python_io_curvine_curvine_native_seek(self.reader_handle, self.read_pos)
            except Exception as e:
                raise IOError(f"Native seek failed: {e}")
            self.stream_pos = self.read_pos

        addr_len = [0, 0]
        try:
            curvine_libsdk.python_io_curvine_curvine_native_read(self.reader_handle, addr_len)
        except Exception as e:
            raise IOError(f"Native read file failed: {e}")

        memory_address = addr_len[0]
        memory_length = addr_len[1]
        if memory_length <= 0:
            self.read_buffer = None
            self.read_buffer_pos = self.read_pos
            return False

        buffer_type = ctypes.c_char * memory_length
        self.read_buffer = buffer_type.from_address(memory_address)
        self.read_buffer_pos = self.read_pos
        self.stream_pos += memory_length
        return True

    def read(self, offset: int, length: int) -> bytes:
        """Read data from the file.

        Args:
            offset: Bytes to skip from the current position; it may be negative.
            length: Number of bytes to read.

        Returns:
            The read data as raw bytes; shorter than ``length`` only at EOF.

        Raises:
            IOError: If read fails or position is negative.
            ValueError: If length is negative.
        """
        pos = self.read_pos + offset
        if pos < 0:
            raise IOError("Position is negative")
        if length < 0:
            raise ValueError("Length must be non-negative")
        self.read_pos = pos
        if self.read_pos >= self.file_size or length == 0:
            return b""

        parts = []
        remaining = min(length, self.file_size - self.read_pos)
        while remaining > 0:
            buffer_len = len(self.read_buffer) if self.read_buffer is not None else 0
            if not (self.read_buffer_pos <= self.read_pos < self.read_buffer_pos + buffer_len):
                if not self._fill_buffer():
                    break
                # The refilled chunk may be shorter than the previous one (for
                # example the last chunk of a file), so its length must be
                # re-read instead of reusing the stale one.
                buffer_len = len(self.read_buffer)

            buffer_offset = self.read_pos - self.read_buffer_pos
            read_len = min(remaining, buffer_len - buffer_offset)
            parts.append(
                ctypes.string_at(ctypes.addressof(self.read_buffer) + buffer_offset, read_len)
            )
            self.read_pos += read_len
            remaining -= read_len

        return b"".join(parts)

    def seek(self, pos: int) -> None:
        """Seek to a position in the file.

        Args:
            pos: Position to seek to in bytes.

        Raises:
            ValueError: If position is negative or exceeds file length.
            IOError: If seek fails.
        """
        file_len = self.file_size
        if pos < 0:
            raise ValueError("Seek position cannot be negative")
        if pos > file_len:
            raise ValueError(f"Seek position {pos} exceeds file length {file_len}")

        buffer_len = len(self.read_buffer) if self.read_buffer is not None else 0
        if self.read_buffer is not None and self.read_buffer_pos <= pos < self.read_buffer_pos + buffer_len:
            # Target is inside the cached chunk; no native round-trip needed.
            self.read_pos = pos
        else:
            self.read_buffer = None
            self.read_pos = pos
            if self.stream_pos != pos:
                try:
                    curvine_libsdk.python_io_curvine_curvine_native_seek(self.reader_handle, pos)
                except Exception as e:
                    raise IOError(f"Native seek failed: {e}")
                self.stream_pos = pos

    def close(self) -> None:
        """Close the reader and release resources.

        Raises:
            IOError: If closing fails.
        """
        if self.reader_handle is None:
            return
        try:
            curvine_libsdk.python_io_curvine_curvine_native_close_reader(self.reader_handle)
        except Exception as e:
            raise IOError(f"Native close reader failed: {e}")
        self.reader_handle = None
        self.read_buffer = None
        self.read_buffer_pos = 0
        self.read_pos = 0
        self.stream_pos = 0
        self.file_size = 0
