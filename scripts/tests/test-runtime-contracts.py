#!/usr/bin/env python3
"""Exercise the boundary guard with both real same-named HTTP packages."""
import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("runtime_contracts", ROOT / "scripts/check-runtime-contracts.py")
CONTRACTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTRACTS)


class ActualHttpPackages(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        metadata = json.loads(CONTRACTS.cargo("metadata", "--locked", "--no-deps", "--format-version", "1"))
        cls.protocol = next(package for package in metadata["packages"]
                            if Path(package["manifest_path"]) == ROOT / "protocols/http/Cargo.toml")
        cls.identity = CONTRACTS.protocol_identity(cls.protocol)

    def test_registry_http_is_not_a_kernel_protocol(self):
        dependencies = CONTRACTS.dependencies("zero-transport")
        self.assertTrue(any(identity.startswith("http v1.") for identity in dependencies))
        self.assertNotIn(self.identity, dependencies)

    def test_actual_protocol_http_remains_visible_to_the_guard(self):
        tree = CONTRACTS.cargo("tree", "--locked", "-p", "zero-proxy", "--no-default-features",
                               "--features", "http", "--edges", "normal", "--prefix", "none", "--format", "{p}")
        dependencies = {line.removesuffix(" (*)") for line in tree.splitlines()}
        self.assertIn(self.identity, dependencies)
        self.assertTrue(any(identity.startswith("http v1.") for identity in dependencies))


if __name__ == "__main__":
    unittest.main()
