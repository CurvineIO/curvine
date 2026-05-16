"""CurvineReader - File reader for Curvine.

This module provides the CurvineReader class for reading files from the
Curvine distributed file system.
"""
from typing import Any, List, Optional

import ctypes
import curvine_libsdk


class CurvineReader:
    """Reader for reading files from the Curvine file system.

    This class provides buffered reading capabilities with support for
    seeking within files.

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

    def read(self, offset: int, length: int) -> str:
        """Read data from the file.

        Args:
            offset: Byte offset to read from.
            length: Number of bytes to read.

        Returns:
            The read data as a UTF-8 decoded string.

        Raises:
            IOError: If read fails or position is negative.
            ValueError: If offset and length exceed buffer bounds.
        """
        self.read_pos += offset
        if self.read_pos < 0:
            raise IOError("Position is negative")
        if self.read_pos >= self.file_size:
            return ""

        addr_len = [0, 0]
        if self.read_buffer is None:
            try:
                curvine_libsdk.python_io_curvine_curvine_native_read(self.reader_handle, addr_len)
            except Exception as e:
                raise IOError(f"Native read file failed: {e}")

            memory_address = addr_len[0]
            memory_length = addr_len[1]
            buffer_type = ctypes.c_char * length
            self.read_buffer = buffer_type.from_address(memory_address)
            self.read_pos += length
        else:
            memory_address = ctypes.addressof(self.read_buffer)
            memory_length = self.read_buffer._length_

        if memory_length <= 0:
            return ""
        if offset >= memory_length:
            return ""
        if offset + length > memory_length:
            raise ValueError("Offset and length out of memory length")

        memory_address = memory_address + offset
        data = ctypes.string_at(memory_address, length)

        return data.decode("utf-8", errors="ignore")

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

        to_skip = self.read_pos - pos
        if self.read_buffer and to_skip >= 0 and to_skip <= self.read_buffer._length_:
            self.read_buffer_pos += to_skip
        else:
            self.read_buffer = None
            self.read_pos = 0
            try:
                curvine_libsdk.python_io_curvine_curvine_native_seek(self.reader_handle, pos)
            except Exception as e:
                raise IOError(f"Native seek failed: {e}")
        self.read_pos = pos

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
        self.file_size = 0