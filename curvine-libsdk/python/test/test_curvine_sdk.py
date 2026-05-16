"""Unit tests for Curvine Python SDK.

This module provides comprehensive unit tests for the Curvine SDK classes
using mocking to avoid requiring a live Curvine cluster.
"""
import os
import sys
import unittest
from unittest.mock import Mock, MagicMock, patch, call
from typing import Optional, Dict, Any

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Import SDK classes
from curvinefs.curvineClient import CurvineClient
from curvinefs.curvineFileSystem import CurvineFileSystem
from curvinefs.curvineReader import CurvineReader
from curvinefs.curvineWriter import CurvineWriter


class TestCurvineClient(unittest.TestCase):
    """Unit tests for CurvineClient class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.config_path = "/test/config.toml"
        self.write_chunk_num = 4
        self.write_chunk_size = 1024

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_init_success(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test successful initialization."""
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = "mock_ptr"
        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        self.assertEqual(client.file_system_ptr, "mock_ptr")
        self.assertEqual(client.write_chunk_num, self.write_chunk_num)
        self.assertEqual(client.write_chunk_size, self.write_chunk_size)
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.assert_called_once_with(self.config_path)

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_init_failure(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test initialization failure."""
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.side_effect = Exception("Connection failed")
        with self.assertRaises(IOError) as context:
            CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        self.assertIn("Native create file system failed", str(context.exception))

    @patch("curvinefs.curvineClient.curvine_libsdk")
    @patch("curvinefs.curvineClient.GetFileStatusResponse")
    def test_get_file_status_success(self, mock_response_class: MagicMock, mock_curvine_libsdk: MagicMock) -> None:
        """Test successful get_file_status."""
        # Setup mocks
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr
        mock_curvine_libsdk.python_io_curvine_curvine_native_get_file_status.return_value = b"mock_data"

        # Setup response mock
        mock_response = MagicMock()
        mock_response.status = MagicMock()
        mock_response.status.id = 123
        mock_response.status.path = "/test/file.txt"
        mock_response.status.name = "file.txt"
        mock_response.status.is_dir = False
        mock_response.status.mtime = 1234567890
        mock_response.status.atime = 1234567891
        mock_response.status.children_num = 0
        mock_response.status.is_complete = True
        mock_response.status.len = 1024
        mock_response.status.replicas = 3
        mock_response.status.block_size = 4096
        mock_response.status.file_type = 1
        mock_response_class.return_value = mock_response

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        result = client.get_file_status("/test/file.txt")

        self.assertIsNotNone(result)
        self.assertEqual(result["id"], 123)
        self.assertEqual(result["path"], "/test/file.txt")
        self.assertEqual(result["name"], "file.txt")
        self.assertEqual(result["is_dir"], False)
        self.assertEqual(result["len"], 1024)

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_get_file_status_not_found(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test get_file_status when file is not found."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr
        mock_curvine_libsdk.python_io_curvine_curvine_native_get_file_status.side_effect = Exception("Not found")

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        result = client.get_file_status("/nonexistent/file.txt")

        self.assertIsNone(result)

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_mkdir_success(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test successful mkdir."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr
        mock_curvine_libsdk.python_io_curvine_curvine_native_mkdir.return_value = True

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        client.mkdir("/test/dir", create_parents=True)

        mock_curvine_libsdk.python_io_curvine_curvine_native_mkdir.assert_called_once_with(mock_fs_ptr, "/test/dir", True)

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_mkdir_invalid_bool(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test mkdir with invalid boolean parameter."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        with self.assertRaises(TypeError) as context:
            client.mkdir("/test/dir", create_parents="yes")
        self.assertIn("create_parents must be a boolean", str(context.exception))

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_mkdir_failure(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test mkdir when native call returns False."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr
        mock_curvine_libsdk.python_io_curvine_curvine_native_mkdir.return_value = False

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        with self.assertRaises(IOError) as context:
            client.mkdir("/test/dir", create_parents=True)
        self.assertEqual("mkdir failed", str(context.exception))

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_rm_success(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test successful rm."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        client.rm("/test/file.txt", recursive=False)

        mock_curvine_libsdk.python_io_curvine_curvine_native_delete.assert_called_once_with(mock_fs_ptr, "/test/file.txt", False)

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_rename_success(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test successful rename."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        client.rename("/test/old.txt", "/test/new.txt")

        mock_curvine_libsdk.python_io_curvine_curvine_native_rename.assert_called_once_with(mock_fs_ptr, "/test/old.txt", "/test/new.txt")

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_mv_success(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test successful mv."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        client.mv("/test/file.txt", "/test/moved.txt")

        mock_curvine_libsdk.python_io_curvine_curvine_native_rename.assert_called_once_with(mock_fs_ptr, "/test/file.txt", "/test/moved.txt")

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_mv_failure(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test mv when rename fails."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr
        mock_curvine_libsdk.python_io_curvine_curvine_native_rename.side_effect = OSError("Rename failed")

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        with self.assertRaises(OSError) as context:
            client.mv("/test/file.txt", "/test/moved.txt")
        self.assertIn("Move file failed", str(context.exception))

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_write_string(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test write_string."""
        mock_fs_ptr = "mock_ptr"
        mock_writer_handle = "mock_writer"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr
        mock_curvine_libsdk.python_io_curvine_curvine_native_create.return_value = mock_writer_handle
        mock_curvine_libsdk.python_io_curvine_curvine_native_close_writer.return_value = None

        with patch.object(CurvineWriter, 'write') as mock_write, \
             patch.object(CurvineWriter, 'close') as mock_close:
            client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
            client.write_string("/test/file.txt", "Hello, Curvine!")

            mock_write.assert_called_once_with(b"Hello, Curvine!")
            mock_close.assert_called_once()

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_touch_new_file(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test touch creating a new file."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        client.get_file_status = MagicMock(return_value=None)

        with patch.object(client, 'create') as mock_create:
            mock_writer = MagicMock()
            mock_create.return_value = mock_writer
            client.touch("/test/newfile.txt")

            mock_create.assert_called_once_with("/test/newfile.txt", True)
            mock_writer.close.assert_called_once()

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_touch_truncate(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test touch with truncate."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        client.get_file_status = MagicMock(return_value={"is_dir": False})
        client.rm = MagicMock()
        client.create = MagicMock()

        mock_writer = MagicMock()
        client.create.return_value = mock_writer
        client.touch("/test/existing.txt", truncate=True)

        client.rm.assert_called_once_with("/test/existing.txt")
        client.create.assert_called_once_with("/test/existing.txt", True)
        mock_writer.close.assert_called_once()

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_touch_no_truncate(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test touch without truncate (should raise NotImplementedError)."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        client.get_file_status = MagicMock(return_value={"is_dir": False})

        with self.assertRaises(NotImplementedError) as context:
            client.touch("/test/existing.txt", truncate=False)
        self.assertIn("Update timestamp operation is not implemented", str(context.exception))

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_write_to_new_file_bytes(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test write_to_new_file with bytes data."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        with patch.object(CurvineWriter, 'write') as mock_write, \
             patch.object(CurvineWriter, 'close') as mock_close:
            client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
            client.write_to_new_file("/test/file.txt", b"binary data")

            mock_write.assert_called_once_with(b"binary data")
            mock_close.assert_called_once()

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_write_to_new_file_string(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test write_to_new_file with string data."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        with patch.object(CurvineWriter, 'write') as mock_write, \
             patch.object(CurvineWriter, 'close') as mock_close:
            client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
            client.write_to_new_file("/test/file.txt", "string data")

            mock_write.assert_called_once_with(b"string data")
            mock_close.assert_called_once()

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_close(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test close."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr
        mock_curvine_libsdk.python_io_curvine_curvine_native_close_filesystem.return_value = None

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        client.close()

        mock_curvine_libsdk.python_io_curvine_curvine_native_close_filesystem.assert_called_once_with(mock_fs_ptr)
        self.assertIsNone(client.file_system_ptr)

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_head(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test head method."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        client.read_range = MagicMock(return_value=b"first 10 bytes")

        result = client.head("/test/file.txt", 10)
        self.assertEqual(result, b"first 10 bytes")
        client.read_range.assert_called_once_with("/test/file.txt", 0, 10)

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_head_invalid_size(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test head with invalid size."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)

        with self.assertRaises(ValueError) as context:
            client.head("/test/file.txt", -1)
        self.assertIn("size must be non-negative integer", str(context.exception))

        with self.assertRaises(ValueError) as context:
            client.head("/test/file.txt", None)
        self.assertIn("size must be non-negative integer", str(context.exception))

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_tail(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test tail method."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        client.get_file_status = MagicMock(return_value={"len": 100})
        client.read_range = MagicMock(return_value=b"last 10 bytes")

        result = client.tail("/test/file.txt", 10)
        self.assertEqual(result, b"last 10 bytes")
        client.read_range.assert_called_once_with("/test/file.txt", 90, 10)

    @patch("curvinefs.curvineClient.curvine_libsdk")
    def test_tail_empty_file(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test tail with empty file."""
        mock_fs_ptr = "mock_ptr"
        mock_curvine_libsdk.python_io_curvine_curvine_native_new_filesystem.return_value = mock_fs_ptr

        client = CurvineClient(self.config_path, self.write_chunk_num, self.write_chunk_size)
        client.get_file_status = MagicMock(return_value={"len": 0})

        result = client.tail("/test/file.txt", 10)
        self.assertEqual(result, b"")


class TestCurvineFileSystem(unittest.TestCase):
    """Unit tests for CurvineFileSystem class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.config_path = "/test/config.toml"
        self.write_chunk_size = 1024
        self.write_chunk_num = 4

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_init(self, mock_curvine_client: MagicMock) -> None:
        """Test initialization."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)

        mock_curvine_client.CurvineClient.assert_called_once_with(
            self.config_path, self.write_chunk_num, self.write_chunk_size
        )
        self.assertEqual(fs.client, mock_client_instance)

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_format_path_normal(self, mock_curvine_client: MagicMock) -> None:
        """Test _format_path with normal path."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs._format_path("/test/path/to/file.txt")
        self.assertEqual(result, "/test/path/to/file.txt")

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_format_path_with_trailing_slash(self, mock_curvine_client: MagicMock) -> None:
        """Test _format_path with trailing slash."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs._format_path("/test/path/")
        self.assertEqual(result, "/test/path")

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_format_path_windows_style(self, mock_curvine_client: MagicMock) -> None:
        """Test _format_path with Windows-style path."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs._format_path("test\\path\\to\\file.txt")
        self.assertEqual(result, "test/path/to/file.txt")

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_format_path_none(self, mock_curvine_client: MagicMock) -> None:
        """Test _format_path with None."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        with self.assertRaises(ValueError) as context:
            fs._format_path(None)
        self.assertIn("Path must be non-empty string", str(context.exception))

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_exists_true(self, mock_curvine_client: MagicMock) -> None:
        """Test exists when file exists."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance
        mock_client_instance.get_file_status.return_value = {"is_dir": False}

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs.exists("/test/file.txt")
        self.assertTrue(result)

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_exists_false(self, mock_curvine_client: MagicMock) -> None:
        """Test exists when file doesn't exist."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance
        mock_client_instance.get_file_status.return_value = None

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs.exists("/test/nonexistent.txt")
        self.assertFalse(result)

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_exists_exception(self, mock_curvine_client: MagicMock) -> None:
        """Test exists when exception occurs."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance
        mock_client_instance.get_file_status.side_effect = Exception("Error")

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs.exists("/test/file.txt")
        self.assertFalse(result)

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_isdir(self, mock_curvine_client: MagicMock) -> None:
        """Test isdir."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance
        mock_client_instance.get_file_status.return_value = {"is_dir": True}

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs.isdir("/test/dir")
        self.assertTrue(result)
        mock_client_instance.get_file_status.assert_called_once_with("/test/dir")

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_isfile(self, mock_curvine_client: MagicMock) -> None:
        """Test isfile."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance
        mock_client_instance.get_file_status.return_value = {"file_type": 1}

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs.isfile("/test/file.txt")
        self.assertTrue(result)

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_isfile_not_a_file(self, mock_curvine_client: MagicMock) -> None:
        """Test isfile when path is not a file."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance
        mock_client_instance.get_file_status.return_value = {"file_type": 0}

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs.isfile("/test/dir")
        self.assertFalse(result)

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_cat_file(self, mock_curvine_client: MagicMock) -> None:
        """Test cat_file."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance
        mock_client_instance.read_range.return_value = b"file content"

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs.cat_file("/test/file.txt", 10, 20)

        self.assertEqual(result, b"file content")
        mock_client_instance.read_range.assert_called_once_with("/test/file.txt", 10, 10)

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_cat_file_to_end(self, mock_curvine_client: MagicMock) -> None:
        """Test cat_file reading to end."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance
        mock_client_instance.read_range.return_value = b"file content"

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs.cat_file("/test/file.txt", 10, end=None)

        self.assertEqual(result, b"file content")
        mock_client_instance.read_range.assert_called_once_with("/test/file.txt", 10, -1)

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_copy(self, mock_curvine_client: MagicMock) -> None:
        """Test copy."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        fs.copy("/test/src.txt", "/test/dest.txt")

        mock_client_instance.copy.assert_called_once_with("/test/src.txt", "/test/dest.txt")

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_delete(self, mock_curvine_client: MagicMock) -> None:
        """Test delete."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        fs.delete("/test/file.txt", recursive=True)

        mock_client_instance.rm.assert_called_once_with("/test/file.txt", True)

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_get_file_status(self, mock_curvine_client: MagicMock) -> None:
        """Test get_file_status."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance
        mock_status = {"id": 123, "path": "/test/file.txt", "is_dir": False}
        mock_client_instance.get_file_status.return_value = mock_status

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs.get_file_status("/test/file.txt")

        self.assertEqual(result, mock_status)
        mock_client_instance.get_file_status.assert_called_once_with("/test/file.txt")

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_get_master_info(self, mock_curvine_client: MagicMock) -> None:
        """Test get_master_info."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance
        mock_info = {"active_master": "node1", "capacity": 1000000}
        mock_client_instance.get_master_info.return_value = mock_info

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        result = fs.get_master_info()

        self.assertEqual(result, mock_info)
        mock_client_instance.get_master_info.assert_called_once()

    @patch("curvinefs.curvineFileSystem.curvine_client")
    def test_close(self, mock_curvine_client: MagicMock) -> None:
        """Test close."""
        mock_client_instance = MagicMock()
        mock_curvine_client.CurvineClient.return_value = mock_client_instance

        fs = CurvineFileSystem(self.config_path, self.write_chunk_size, self.write_chunk_num)
        fs.close()

        mock_client_instance.close.assert_called_once()


class TestCurvineReader(unittest.TestCase):
    """Unit tests for CurvineReader class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_handle = "mock_reader_handle"
        self.file_size = 1024

    def test_init(self) -> None:
        """Test initialization."""
        reader = CurvineReader(self.mock_handle, self.file_size)
        self.assertEqual(reader.reader_handle, self.mock_handle)
        self.assertEqual(reader.file_size, self.file_size)
        self.assertIsNone(reader.read_buffer)
        self.assertEqual(reader.read_pos, 0)

    @patch("curvinefs.curvineReader.curvine_libsdk")
    def test_read_negative_position(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test read with negative position."""
        reader = CurvineReader(self.mock_handle, self.file_size)
        reader.read_pos = -10

        with self.assertRaises(IOError) as context:
            reader.read(0, 10)
        self.assertIn("Position is negative", str(context.exception))

    @patch("curvinefs.curvineReader.curvine_libsdk")
    def test_read_past_end(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test read past end of file."""
        reader = CurvineReader(self.mock_handle, self.file_size)
        reader.read_pos = 2000

        result = reader.read(0, 10)
        self.assertEqual(result, "")

    @patch("curvinefs.curvineReader.curvine_libsdk")
    def test_seek_negative(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test seek with negative position."""
        reader = CurvineReader(self.mock_handle, self.file_size)

        with self.assertRaises(ValueError) as context:
            reader.seek(-10)
        self.assertIn("Seek position cannot be negative", str(context.exception))

    @patch("curvinefs.curvineReader.curvine_libsdk")
    def test_seek_past_end(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test seek past end of file."""
        reader = CurvineReader(self.mock_handle, self.file_size)

        with self.assertRaises(ValueError) as context:
            reader.seek(2000)
        self.assertIn("exceeds file length", str(context.exception))

    @patch("curvinefs.curvineReader.curvine_libsdk")
    def test_close_none_handle(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test close with None handle."""
        reader = CurvineReader(self.mock_handle, self.file_size)
        reader.reader_handle = None

        reader.close()  # Should not raise
        self.assertIsNone(reader.reader_handle)

    @patch("curvinefs.curvineReader.curvine_libsdk")
    def test_close_success(self, mock_curvine_libsdk: MagicMock) -> None:
        """Test successful close."""
        reader = CurvineReader(self.mock_handle, self.file_size)
        mock_curvine_libsdk.python_io_curvine_curvine_native_close_reader.return_value = None

        reader.close()

        mock_curvine_libsdk.python_io_curvine_curvine_native_close_reader.assert_called_once_with(self.mock_handle)
        self.assertIsNone(reader.reader_handle)


class TestCurvineWriter(unittest.TestCase):
    """Unit tests for CurvineWriter class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_handle = "mock_writer_handle"
        self.write_chunk_num = 4
        self.write_chunk_size = 1024

    @patch("curvinefs.curvineWriter.ctypes")
    def test_init(self, mock_ctypes: MagicMock) -> None:
        """Test initialization."""
        mock_buffer = MagicMock()
        mock_ctypes.c_char.__mul__ = MagicMock(return_value=mock_buffer)
        mock_buffer.return_value = MagicMock()

        writer = CurvineWriter(self.mock_handle, self.write_chunk_num, self.write_chunk_size)
        self.assertEqual(writer.writer_handle, self.mock_handle)
        self.assertEqual(writer.write_chunk_num, self.write_chunk_num)
        self.assertEqual(writer.buffer_size, self.write_chunk_size)
        self.assertEqual(writer.write_buffer_index, 0)

    @patch("curvinefs.curvineWriter.ctypes")
    @patch("curvinefs.curvineWriter.curvine_libsdk")
    def test_write_empty_data(self, mock_curvine_libsdk: MagicMock, mock_ctypes: MagicMock) -> None:
        """Test write with empty data."""
        mock_buffer = MagicMock()
        mock_ctypes.c_char.__mul__ = MagicMock(return_value=mock_buffer)
        mock_buffer.return_value = MagicMock()

        writer = CurvineWriter(self.mock_handle, self.write_chunk_num, self.write_chunk_size)
        writer.write(b"")

        # Should not call any write operations
        mock_curvine_libsdk.python_io_curvine_curvine_native_write.assert_not_called()

    @patch("curvinefs.curvineWriter.ctypes")
    def test_write_none_handle(self, mock_ctypes: MagicMock) -> None:
        """Test write with None handle."""
        mock_buffer = MagicMock()
        mock_ctypes.c_char.__mul__ = MagicMock(return_value=mock_buffer)
        mock_buffer.return_value = MagicMock()

        writer = CurvineWriter(self.mock_handle, self.write_chunk_num, self.write_chunk_size)
        writer.writer_handle = None

        with self.assertRaises(IOError) as context:
            writer.write(b"test data")
        self.assertIn("Writer handle is None", str(context.exception))

    @patch("curvinefs.curvineWriter.ctypes")
    @patch("curvinefs.curvineWriter.curvine_libsdk")
    def test_flush_none_handle(self, mock_curvine_libsdk: MagicMock, mock_ctypes: MagicMock) -> None:
        """Test flush with None handle."""
        mock_buffer = MagicMock()
        mock_ctypes.c_char.__mul__ = MagicMock(return_value=mock_buffer)
        mock_buffer.return_value = MagicMock()

        writer = CurvineWriter(self.mock_handle, self.write_chunk_num, self.write_chunk_size)
        writer.writer_handle = None

        with self.assertRaises(IOError) as context:
            writer.flush()
        self.assertIn("Writer handle is None", str(context.exception))

    @patch("curvinefs.curvineWriter.ctypes")
    @patch("curvinefs.curvineWriter.curvine_libsdk")
    def test_close_success(self, mock_curvine_libsdk: MagicMock, mock_ctypes: MagicMock) -> None:
        """Test successful close."""
        mock_buffer = MagicMock()
        mock_ctypes.c_char.__mul__ = MagicMock(return_value=mock_buffer)
        mock_buffer.return_value = MagicMock()
        mock_ctypes.addressof.return_value = "mock_address"

        writer = CurvineWriter(self.mock_handle, self.write_chunk_num, self.write_chunk_size)
        mock_curvine_libsdk.python_io_curvine_curvine_native_close_writer.return_value = None

        writer.close()

        mock_curvine_libsdk.python_io_curvine_curvine_native_close_writer.assert_called_once_with(self.mock_handle)
        self.assertIsNone(writer.writer_handle)


if __name__ == "__main__":
    unittest.main()