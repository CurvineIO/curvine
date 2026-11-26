#!/usr/bin/env python3
"""generate_config.py: pod FQDNs follow POD_CLUSTER_DOMAIN."""
import importlib.util
import os
import unittest
from pathlib import Path
from unittest import mock


GENERATE_CONFIG = Path(__file__).resolve().parents[1] / "generate_config.py"
spec = importlib.util.spec_from_file_location("generate_config", GENERATE_CONFIG)
generate_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generate_config)

FLUID_CONFIG = {"topology": {"master": {"podConfigs": [{"podName": "ds-master-0"}]}}}


class ClusterDomainTest(unittest.TestCase):
    def journal_hostnames(self):
        addrs = generate_config._generate_journal_addrs(FLUID_CONFIG, "svc-ds-master", "ns1")
        return [addr["hostname"] for addr in addrs]

    def test_default_cluster_domain(self):
        env = {k: v for k, v in os.environ.items() if k != "POD_CLUSTER_DOMAIN"}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(self.journal_hostnames(), ["ds-master-0.svc-ds-master.ns1.svc.cluster.local"])

    def test_custom_cluster_domain(self):
        with mock.patch.dict(os.environ, {"POD_CLUSTER_DOMAIN": "k8s.example."}):
            self.assertEqual(self.journal_hostnames(), ["ds-master-0.svc-ds-master.ns1.svc.k8s.example"])


if __name__ == "__main__":
    unittest.main()
