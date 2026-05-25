"""Curvine Python SDK.

This SDK provides Python bindings for the Curvine distributed file system.

Example:
    >>> from curvinefs import CurvineClient
    >>> client = CurvineClient(config_path="config.toml", write_chunk_num=4, write_chunk_size=1024*1024)
    >>> writer = client.create("/path/to/file", overwrite=True)
    >>> writer.write(b"Hello, Curvine!")
    >>> writer.close()
    >>> reader = client.open("/path/to/file")
    >>> data = reader.read(0, 100)
    >>> reader.close()
"""
from curvinefs.curvineClient import CurvineClient
from curvinefs.curvineFileSystem import CurvineFileSystem
from curvinefs.curvineReader import CurvineReader
from curvinefs.curvineWriter import CurvineWriter

__all__ = ["CurvineClient", "CurvineFileSystem", "CurvineWriter", "CurvineReader"]