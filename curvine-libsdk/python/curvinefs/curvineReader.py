import ctypes

import curvine_libsdk


class CurvineReader:
    readerHandle = None
    file_size = 0

    def __init__(self, native_handle, file_size):
        self.readerHandle = native_handle
        self.file_size = file_size
        self._pending = bytearray()

    def seek(self, pos):
        if pos < 0:
            raise ValueError("Seek position cannot be negative")
        if pos > self.file_size:
            raise ValueError(f"Seek position {pos} exceeds file length {self.file_size}")
        self._pending.clear()
        try:
            curvine_libsdk.python_io_curvine_curvine_native_seek(self.readerHandle, pos)
        except Exception as e:
            raise IOError(f"Native seek failed: {e}") from e

    def read(self, offset, length):
        """Read `length` bytes starting at absolute file `offset`; returns raw bytes."""
        if length == 0:
            return b""
        self.seek(offset)
        return self._read_bytes(length)

    def _read_bytes(self, length):
        addr_len = [0, 0]
        data = bytearray()
        remaining = length

        if self._pending:
            take = min(len(self._pending), remaining)
            data.extend(memoryview(self._pending)[:take])
            del self._pending[:take]
            remaining -= take
            if remaining == 0:
                return bytes(data)

        while remaining > 0:
            try:
                curvine_libsdk.python_io_curvine_curvine_native_read(self.readerHandle, addr_len)
            except Exception as e:
                raise IOError(f"Native read file failed: {e}") from e

            memory_address = addr_len[0]
            memory_length = addr_len[1]
            if memory_length <= 0:
                break

            chunk = ctypes.string_at(memory_address, memory_length)
            if len(chunk) <= remaining:
                data.extend(chunk)
                remaining -= len(chunk)
            else:
                data.extend(chunk[:remaining])
                self._pending.extend(chunk[remaining:])
                remaining = 0

        return bytes(data)

    def close(self):
        if self.readerHandle is None:
            return
        try:
            curvine_libsdk.python_io_curvine_curvine_native_close_reader(self.readerHandle)
        except Exception as e:
            raise IOError(f"Native close reader failed: {e}") from e
        self.readerHandle = None
        self._pending.clear()
        self.file_size = 0
