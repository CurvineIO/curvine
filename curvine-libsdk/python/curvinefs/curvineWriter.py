"""CurvineWriter - File writer for Curvine.

This module provides the CurvineWriter class for writing files to the
Curvine distributed file system with buffering support.
"""
from typing import Any, List, Optional

import ctypes
import curvine_libsdk


class CurvineWriter:
    """Writer for writing files to the Curvine file system.

    This class provides buffered writing capabilities with multiple chunks
    for improved performance.

    Args:
        native_handle: Native handle for the writer.
        write_chunk_num: Number of write chunks to buffer.
        write_chunk_size: Size of each write chunk in bytes.
    """

    def __init__(self, native_handle: Any, write_chunk_num: int, write_chunk_size: int) -> None:
        """Initialize a new CurvineWriter instance."""
        self.writer_handle = native_handle
        self.write_chunk_num = write_chunk_num
        self.buffer_size = write_chunk_size
        self.write_buffer: List[ctypes.Array[ctypes.c_char]] = [
            (ctypes.c_char * write_chunk_size)() for _ in range(write_chunk_num)
        ]
        self.used_write_buffer: List[int] = [0] * write_chunk_num
        self.write_buffer_index = 0

    def write(self, bytes_data: bytes) -> None:
        """Write bytes to the file.

        Args:
            bytes_data: Bytes to write.

        Raises:
            IOError: If writer handle is None.
        """
        length = len(bytes_data)
        pos = 0
        if length == 0:
            return

        if self.writer_handle is None:
            raise IOError("Writer handle is None")

        while length > 0:
            cur_buffer = self._get_buffer()
            remain_buffer = self.buffer_size - self.used_write_buffer[self.write_buffer_index]
            write_len = min(remain_buffer, length)
            data = bytes_data[pos:pos + write_len]
            ctypes.memmove(ctypes.addressof(cur_buffer), data, write_len)
            self.used_write_buffer[self.write_buffer_index] += write_len
            length -= write_len
            pos += write_len

    def _get_buffer(self) -> ctypes.Array[ctypes.c_char]:
        """Get the current write buffer, flushing if full.

        Returns:
            The current buffer to write to.
        """
        if self.buffer_size - self.used_write_buffer[self.write_buffer_index] == 0:
            self._flush_buffer()

            if self.write_buffer_index == self.write_chunk_num - 1:
                curvine_libsdk.python_io_curvine_curvine_native_flush(self.writer_handle)
                self.write_buffer_index = 0
            else:
                self.write_buffer_index += 1
            buf_address = ctypes.addressof(self.write_buffer[self.write_buffer_index])
            ctypes.memset(buf_address, 0, self.buffer_size)
        return self.write_buffer[self.write_buffer_index]

    def flush(self) -> None:
        """Flush buffered data to the file system.

        Raises:
            IOError: If writer handle is None or flush fails.
        """
        if self.writer_handle is None:
            raise IOError("Writer handle is None")
        try:
            self._flush_buffer()
            curvine_libsdk.python_io_curvine_curvine_native_flush(self.writer_handle)
        except Exception as e:
            raise IOError(f"Native flush failed: {e}")

    def _flush_buffer(self) -> None:
        """Flush the current buffer to the file system.

        Raises:
            IOError: If write or flush fails.
        """
        try:
            buf_address = ctypes.addressof(self.write_buffer[self.write_buffer_index])
            length = self.used_write_buffer[self.write_buffer_index]
            curvine_libsdk.python_io_curvine_curvine_native_write(self.writer_handle, buf_address, length)
            curvine_libsdk.python_io_curvine_curvine_native_flush(self.writer_handle)
        except Exception as e:
            raise IOError(f"Native write failed: {e}")
        ctypes.memset(buf_address, 0, self.buffer_size)
        self.used_write_buffer[self.write_buffer_index] = 0

    def close(self) -> None:
        """Close the writer and release resources.

        Raises:
            IOError: If close fails.
        """
        try:
            self._flush_buffer()
            curvine_libsdk.python_io_curvine_curvine_native_close_writer(self.writer_handle)
        except Exception as e:
            raise IOError(f"Native close writer failed: {e}")
        self.writer_handle = None
        self.write_chunk_num = 0
        self.write_buffer = None
        self.buffer_size = 0
        self.write_buffer_index = 0
        self.used_write_buffer = None