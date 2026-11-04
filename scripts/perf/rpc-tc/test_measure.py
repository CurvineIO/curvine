#!/usr/bin/env python3
"""Checks accounting and comparison without requiring network administration."""
import importlib.util
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("rpc_tc_measure", HERE / "measure.py")
measure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measure)


class MeasurementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = pathlib.Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.client = {
            "bytes": 102400,
            "elapsed_seconds": 1.0,
            "client_cpu_seconds": 0.5,
            "latency_ms": [1.0] * 99 + [2.0],
            "workload": {"file_bytes": 2048, "chunk_bytes": 1024, "reads": 100,
                         "warmup_reads": 10, "concurrency": 1,
                         "source_sha256": "a" * 64,
                         "short_circuit": False, "read_ahead": False},
        }
        self.before = {"ticks_per_second": 100, "processes": {"123": {"start": 42, "ticks": 100}}}
        self.after = {"ticks_per_second": 100, "processes": {"123": {"start": 42, "ticks": 125}}}
        for name, data in (("client", self.client), ("before", self.before), ("after", self.after)):
            (self.path / name).write_text(json.dumps(data), encoding="utf-8")

    def finish(self, name, transport, revision):
        from unittest.mock import patch
        with patch.dict(os.environ, {"RPC_TC_SERVER_SHA256": "b" * 64, "RPC_TC_CLIENT_SHA256": "c" * 64}):
            measure.finish(str(self.path / "client"), str(self.path / "before"), str(self.path / "after"),
                           str(self.path / name), "1gbit", "1ms", transport, revision)
        return json.loads((self.path / name).read_text(encoding="utf-8"))

    def test_cpu_delta_and_comparison(self):
        base = self.finish("base", "orpc", "base-sha")
        self.assertEqual(base["server_cpu_seconds"], 0.25)
        self.finish("candidate", "grpc", "candidate-sha")
        result = subprocess.run([sys.executable, str(HERE / "compare.py"), "--orpc", str(self.path / "base"),
                                 "--grpc", str(self.path / "candidate")], capture_output=True, text=True, check=True)
        report = json.loads(result.stdout)
        self.assertTrue(report["provisional_pass"])
        self.assertEqual(report["grpc_over_orpc"]["p99"], 1.0)

    def test_grpc_checkpoint_comparison(self):
        self.finish("base", "grpc", "first-checkpoint")
        self.finish("candidate", "grpc", "second-checkpoint")
        result = subprocess.run([sys.executable, str(HERE / "compare.py"),
                                 "--baseline", str(self.path / "base"),
                                 "--candidate", str(self.path / "candidate")],
                                capture_output=True, text=True, check=True)
        report = json.loads(result.stdout)
        self.assertEqual(report["candidate_over_baseline"]["cpu_per_gib"], 1.0)
        self.assertEqual(report["revisions"]["baseline"], "first-checkpoint")

    def test_restarted_server_invalidates_result(self):
        self.after["processes"]["123"]["start"] = 43
        (self.path / "after").write_text(json.dumps(self.after), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "restarted"):
            self.finish("result", "orpc", "base-sha")

    def test_changed_workload_is_rejected(self):
        self.finish("base", "orpc", "base-sha")
        candidate = self.finish("candidate", "grpc", "candidate-sha")
        candidate["workload"]["rate"] = "10gbit"
        (self.path / "candidate").write_text(json.dumps(candidate), encoding="utf-8")
        result = subprocess.run([sys.executable, str(HERE / "compare.py"), "--orpc", str(self.path / "base"),
                                 "--grpc", str(self.path / "candidate")], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("workload settings differ", result.stderr)

    def test_missing_or_insufficient_samples_are_rejected(self):
        base = self.finish("base", "orpc", "base-sha")
        base["latency_ms"] = base["latency_ms"][:99]
        (self.path / "base").write_text(json.dumps(base), encoding="utf-8")
        self.finish("candidate", "grpc", "candidate-sha")
        result = subprocess.run([sys.executable, str(HERE / "compare.py"), "--orpc", str(self.path / "base"),
                                 "--grpc", str(self.path / "candidate")], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("at least 100", result.stderr)


if __name__ == "__main__":
    unittest.main()
