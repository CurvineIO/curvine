import ctypes
import curvine_libsdk

class CurvineWriter:
    writerHandle = None
    write_chunk_num = 0
    write_buffer = None
    buffer_size = 0
    write_buffer_index = 0
    used_write_buffer = None
    
    def __init__(self, nativeHandle, write_chunk_num, write_chunk_size):
        self.writerHandle = nativeHandle
        self.write_chunk_num = write_chunk_num
        self.buffer_size = write_chunk_size
        self.write_buffer = [(ctypes.c_char * write_chunk_size)() for _ in range(write_chunk_num)]
        self.used_write_buffer = [0] * write_chunk_num
        self.write_buffer_index = 0

    # write
    def write(self, bytes_data):
        length = len(bytes_data)
        pos = 0
        if length == 0:
            return
        
        if self.writerHandle is None:
            raise IOError("Writer handle is None")
        
        while length > 0:
            cur_buffer = self.get_buffer()
            remain_buffer = self.buffer_size - self.used_write_buffer[self.write_buffer_index]
            write_len = min(remain_buffer, length)
            data = bytes_data[pos : pos+write_len]
            ctypes.memmove(ctypes.addressof(cur_buffer), data, write_len)
            self.used_write_buffer[self.write_buffer_index] += write_len
            length -= write_len
            pos += write_len 

    def get_buffer(self):
        if self.buffer_size-self.used_write_buffer[self.write_buffer_index] == 0:
            self.flush_buffer()
        
            if self.write_buffer_index == self.write_chunk_num -1:
                curvine_libsdk.python_io_curvine_curvine_native_flush(self.writerHandle)
                self.write_buffer_index = 0
            else:
                self.write_buffer_index += 1
            buf_address = ctypes.addressof(self.write_buffer[self.write_buffer_index])
            ctypes.memset(buf_address, 0, self.buffer_size) 
        return self.write_buffer[self.write_buffer_index]
    
    # flush
    def flush(self):
        if self.writerHandle is None:
            raise IOError("Writer handle is None")
        try:
            self.flush_buffer()
            curvine_libsdk.python_io_curvine_curvine_native_flush(self.writerHandle)
        except Exception as e:
            raise IOError(f"Native flush failed: {e}")
        
    # flush_buffer
    def flush_buffer(self):        
        try:
            buf_address = ctypes.addressof(self.write_buffer[self.write_buffer_index])
            length = self.used_write_buffer[self.write_buffer_index]
            curvine_libsdk.python_io_curvine_curvine_native_write(self.writerHandle, buf_address, length)
            curvine_libsdk.python_io_curvine_curvine_native_flush(self.writerHandle)
        except Exception as e:
            raise IOError(f"Native write failed: {e}")
        ctypes.memset(buf_address, 0, self.buffer_size) 
        self.used_write_buffer[self.write_buffer_index] = 0
    
    # close
    def close(self):
        try:
            self.flush_buffer()
            curvine_libsdk.python_io_curvine_curvine_native_close_writer(self.writerHandle)
        except Exception as e:
            raise IOError(f"Native close writer failed: {e}")
        self.writerHandle = None
        self.write_chunk_num = 0
        self.write_buffer = None
        self.buffer_size = 0
        self.write_buffer_index = 0
        self.used_write_buffer = None
        
       
