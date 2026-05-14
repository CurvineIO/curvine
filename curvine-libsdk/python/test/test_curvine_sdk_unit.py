import unittest
from unittest.mock import patch

from curvinefs.curvineClient import CurvineClient


class TestBatchPathHelpers(unittest.TestCase):
    """Lightweight behavioral tests without a live Curvine cluster."""

    @patch.object(CurvineClient, "get_file_status")
    def test_path_exists_handles_file_not_found(self, mock_gs):
        c = CurvineClient.__new__(CurvineClient)
        c.file_system_ptr = 1
        c.write_chunk_num = 8
        c.write_chunk_size = 128

        mock_gs.side_effect = [FileNotFoundError("gone"), RuntimeError()]
        self.assertFalse(c.path_exists("/a"))
        with self.assertRaises(RuntimeError):
            c.path_exists("/b")

    @patch.object(CurvineClient, "path_exists")
    def test_batch_path_exists_order(self, mock_pe):
        c = CurvineClient.__new__(CurvineClient)
        c.file_system_ptr = 1
        c.write_chunk_num = 8
        c.write_chunk_size = 128
        mock_pe.side_effect = [True, False, True]

        keys = ["x", "y", "z"]
        self.assertEqual(c.batch_path_exists(keys), [True, False, True])
        mock_pe.assert_any_call("x")
        mock_pe.assert_any_call("y")
        mock_pe.assert_any_call("z")


if __name__ == "__main__":
    unittest.main()
