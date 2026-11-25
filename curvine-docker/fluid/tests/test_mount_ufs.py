#!/usr/bin/env python3
"""mountUfs.sh: turning Fluid mount entries into `cv mount` arguments.

The script runs against a stub `cv` that records its arguments, so no Curvine
cluster is needed.
"""
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


MOUNT_UFS = Path(__file__).resolve().parents[1] / "mountUfs.sh"

STUB_CV = """#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$CV_LOG"
"""


class MountUfsTest(unittest.TestCase):
    def run_mount_ufs(self, mount, files):
        """Run mountUfs.sh for one mount; return (stdout, recorded cv calls)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            for name, content in files.items():
                (tmp / name).write_text(content + "\n")
            mount = json.loads(json.dumps(mount).replace("@D@", tmp_dir))

            config_path = tmp / "config.json"
            config_path.write_text(json.dumps({"mounts": [mount]}))
            cv = tmp / "cv"
            cv.write_text(STUB_CV)
            cv.chmod(0o755)
            log = tmp / "cv.log"

            env = dict(os.environ)
            env.update(
                FLUID_RUNTIME_CONFIG_PATH=str(config_path),
                CURVINE_CLI=str(cv),
                CURVINE_CONF_FILE=str(tmp / "curvine-cluster.toml"),
                CV_LOG=str(log),
            )
            result = subprocess.run(
                ["bash", str(MOUNT_UFS)],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            calls = log.read_text().splitlines() if log.exists() else []
        return result.stdout, calls

    def mount_call(self, calls):
        mounts = [c for c in calls if c.startswith("mount s3://")]
        self.assertEqual(len(mounts), 1, calls)
        return mounts[0]

    def test_list_encrypt_options_read_the_secret_files(self):
        # The list form Fluid uses: each entry names a mounted secret file.
        mount = {
            "mountPoint": "s3://bucket/data",
            "path": "/s3data",
            "options": {"endpoint_url": "http://127.0.0.1:9000"},
            "encryptOptions": [
                {"name": "access-key", "valueFrom": {"secretKeyRef": {"path": "@D@/ak"}}},
                {"name": "secret-key", "valueFrom": {"secretKeyRef": {"path": "@D@/sk"}}},
            ],
        }
        stdout, calls = self.run_mount_ufs(mount, {"ak": "ak-from-file", "sk": "sk-from-file"})

        call = self.mount_call(calls)
        self.assertIn("mount s3://bucket/data /s3data --check-path-consist false", call)
        self.assertIn("-c s3.endpoint_url=http://127.0.0.1:9000", call)
        self.assertIn("-c s3.credentials.access=ak-from-file", call)
        self.assertIn("-c s3.credentials.secret=sk-from-file", call)
        self.assertEqual(json.loads(stdout), {"mounted": ["/s3data"]})

    def test_list_entries_without_a_secret_path_are_skipped(self):
        mount = {
            "mountPoint": "s3://bucket/data",
            "path": "/s3data",
            "encryptOptions": [
                {"name": "access-key", "valueFrom": {"secretKeyRef": {"name": "s3-secret"}}},
                {"valueFrom": {"secretKeyRef": {"path": "@D@/sk"}}},
            ],
        }
        _, calls = self.run_mount_ufs(mount, {"sk": "sk-from-file"})

        call = self.mount_call(calls)
        self.assertNotIn("s3.credentials", call)

    def test_dict_encrypt_options_still_read_the_secret_files(self):
        mount = {
            "mountPoint": "s3://bucket/data",
            "path": "/s3data",
            "encryptOptions": {"access-key": "@D@/ak", "secret-key": "@D@/sk"},
        }
        _, calls = self.run_mount_ufs(mount, {"ak": "ak-from-file", "sk": "sk-from-file"})

        call = self.mount_call(calls)
        self.assertIn("-c s3.credentials.access=ak-from-file", call)
        self.assertIn("-c s3.credentials.secret=sk-from-file", call)


if __name__ == "__main__":
    unittest.main()
