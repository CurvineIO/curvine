import ctypes
import curvine_libsdk
from io import BufferedReader

class CurvineReader:
    readerHandle = None
    read_buffer = None
    read_pos = 0  # read position
    read_buffer_pos = 0 # buffer position
    file_size = 0

    def __init__(self, nativeHandle, file_size):
        self.readerHandle = nativeHandle
        self.file_size = file_size
    
    # read
    def read(self, offset, length): 
        self.read_pos += offset
        if self.read_pos < 0:
            raise IOError("Position is negative") 
        if self.read_pos >= self.file_size:
            return b""
        
        addr_len = [0,0]
        if self.read_buffer is None:
            try:
                curvine_libsdk.python_io_curvine_curvine_native_read(self.readerHandle, addr_len)
            except Exception as e:
                raise IOError(f"Native read file failed: {e}")
            
            memory_address = addr_len[0]
            memory_length = addr_len[1]
            buffer_type = (ctypes.c_char * length)
            self.read_buffer =  buffer_type.from_address(memory_address)
            self.read_pos += length
        else:
            memory_address = ctypes.addressof(self.read_buffer)
            memory_length = self.read_buffer._length_
        if memory_length <= 0:
                return b""
        if offset >= memory_length:
            return b""
        if offset + length > memory_length:
            raise ValueError("Offset and length out of memory length")
        
        memory_address = memory_address + offset
        data = ctypes.string_at(memory_address, length)

        return data.decode("utf-8", errors='ignore')
    
    # seek
    def seek(self, pos):        
        file_len = self.file_size
        if pos < 0:
            raise ValueError("Seek position cannot be negative")
        if pos > file_len:
            raise ValueError(f"Seek position {pos} exceeds file length {file_len}")
        
        to_skip = self.read_pos - pos
        if self.read_buffer and to_skip >=0 and to_skip <= self.read_buffer._length_:
            self.read_buffer_pos += to_skip
        else:
            # Discard buffer data.
            self.read_buffer = None
            self.read_pos = 0
            try: 
                curvine_libsdk.python_io_curvine_curvine_native_seek(self.readerHandle, pos)
            except Exception as e:
                raise IOError(f"Native seek failed: {e}")
        self.read_pos = pos

    # close_reader
    def close(self):
        if self.readerHandle == None:
            return
        try:
            curvine_libsdk.python_io_curvine_curvine_native_close_reader(self.readerHandle)
        except Exception as e:
            raise IOError(f"Native close reader failed: {e}")
        self.readerHandle = None
        self.read_buffer = None
        self.read_buffer_pos = 0
        self.read_pos = 0
        self.file_size = 0
