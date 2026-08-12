#!/usr/bin/env python3
import importlib.util
import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    tomllib = None


CONFIG_PARSE = Path(__file__).resolve().parents[1] / "config-parse.py"
spec = importlib.util.spec_from_file_location("config_parse", CONFIG_PARSE)
config_parse = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config_parse)


class ConfigParseTest(unittest.TestCase):
    def run_parser(self, config):
        with tempfile.TemporaryDirectory() as tmp_dir:
            curvine_home = Path(tmp_dir) / "curvine"
            config_path = Path(tmp_dir) / "config.json"
            config_path.write_text(json.dumps(config))

            old_home = os.environ.get("CURVINE_HOME")
            os.environ["CURVINE_HOME"] = str(curvine_home)
            try:
                parser = config_parse.ConfigParser(str(config_path))
                with contextlib.redirect_stdout(io.StringIO()):
                    parser.run()
            finally:
                if old_home is None:
                    os.environ.pop("CURVINE_HOME", None)
                else:
                    os.environ["CURVINE_HOME"] = old_home

            rendered_config = (curvine_home / "conf" / "curvine-cluster.toml").read_text()
            rendered_script = (curvine_home / "mount-curvine.sh").read_text()

        return rendered_config, rendered_script

    def test_thinruntime_keeps_all_master_endpoints(self):
        rendered, _ = self.run_parser({
                "mounts": [{
                    "mountPoint": "curvine:///starrocks/datacache",
                    "name": "curvine",
                    "options": {
                        "master-endpoints": "master-0:8995,master-1:8995,master-2:8995",
                        "master-web-port": "9000",
                    },
                }],
                "targetPath": "/runtime-mnt/thin/starrocks/sr-curvine-datacache/thin-fuse",
                "accessModes": ["ReadWriteMany"],
        })

        self.assertEqual(rendered.count("[[client.master_addrs]]"), 3)
        self.assertIn('hostname = "master-0"', rendered)
        self.assertIn('hostname = "master-1"', rendered)
        self.assertIn('hostname = "master-2"', rendered)
        self.assertIn('fs_path = "/starrocks/datacache"', rendered)

    def test_advanced_client_and_fuse_toml_options_are_rendered(self):
        rendered, script = self.run_parser({
            "mounts": [{
                "mountPoint": "curvine:///starrocks/datacache",
                "name": "curvine",
                "options": {
                    "master-endpoints": "master-0:8995",
                    "client.enable_unified_fs": "false",
                    "fuse.enable_write_back": "true",
                    "fuse.entry_timeout_ms": "300000",
                    "fuse.attr_timeout_ms": "300000",
                    "fuse.meta_cache_timeout": "12h",
                    "fuse.node_cache_timeout": "24h",
                    "entry-timeout": "300000",
                },
            }],
            "targetPath": "/runtime-mnt/thin/default/curvine-dataset/thin-fuse",
        })

        self.assertIn("[client]\nenable_unified_fs = false", rendered)
        self.assertIn("write_back_cache = true", rendered)
        self.assertIn("entry_timeout_ms = 300000", rendered)
        self.assertIn("attr_timeout_ms = 300000", rendered)
        self.assertIn('meta_cache_timeout = "12h"', rendered)
        self.assertIn('node_cache_timeout = "24h"', rendered)
        self.assertIn("--entry-timeout-ms 300000", script)
        self.assertNotIn("--entry-timeout 300000", script)

        if tomllib is not None:
            parsed = tomllib.loads(rendered)
            self.assertFalse(parsed["client"]["enable_unified_fs"])
            self.assertTrue(parsed["fuse"]["write_back_cache"])
            self.assertEqual(parsed["fuse"]["entry_timeout_ms"], 300000)
            self.assertEqual(parsed["fuse"]["attr_timeout_ms"], 300000)
            self.assertEqual(parsed["fuse"]["meta_cache_timeout"], "12h")
            self.assertEqual(parsed["fuse"]["node_cache_timeout"], "24h")

    def test_managed_fuse_toml_option_is_rejected(self):
        parser = config_parse.ConfigParser()
        parser.mount_options = {"fuse.mnt_path": "/bad"}
        with self.assertRaisesRegex(ValueError, "managed by Curvine ThinRuntime"):
            parser.collect_advanced_toml_options()

    def test_multiple_dataset_mounts_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config.json"
            config_path.write_text(json.dumps({
                "mounts": [
                    {"mountPoint": "curvine:///a", "options": {"master-endpoints": "master-0:8995"}},
                    {"mountPoint": "curvine:///b", "options": {"master-endpoints": "master-0:8995"}},
                ],
                "targetPath": "/runtime-mnt/thin/default/curvine-dataset/thin-fuse",
            }))

            parser = config_parse.ConfigParser(str(config_path))
            with self.assertRaisesRegex(ValueError, "exactly one Dataset mount"):
                parser.load_config()

    def test_invalid_master_endpoint_is_rejected(self):
        parser = config_parse.ConfigParser()
        parser.mount_options = {"master-endpoints": "master-0:8995,missing-port"}
        with self.assertRaisesRegex(ValueError, "Invalid master endpoint"):
            parser.parse_master_endpoints()


if __name__ == "__main__":
    unittest.main()
