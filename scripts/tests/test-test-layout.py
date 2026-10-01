#!/usr/bin/env python3
"""Regression checks for lost, duplicated and unregistered integration entries."""

import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "layout", Path(__file__).resolve().parents[1] / "check-test-layout.py"
)
layout = importlib.util.module_from_spec(spec)
spec.loader.exec_module(layout)


class LayoutTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "tests/suites").mkdir(parents=True)
        (self.root / "tests/case.rs").write_text("#[test] fn behavior() {}\n")

    def inspect(self, *paths):
        return layout.inspect_package({
            "name": "fixture", "manifest_path": str(self.root / "Cargo.toml"),
            "targets": [{"name": p, "kind": ["test"], "src_path": str(self.root / p)}
                        for p in paths],
        })

    def suite(self, source="../case.rs"):
        (self.root / "tests/suites/contracts.rs").write_text(
            '#[cfg(feature = "optional")]\n#[path = "' + source + '"]\nmod case;\n'
        )

    def test_feature_gated_source_is_covered_once(self):
        self.suite()
        result = self.inspect("tests/suites/contracts.rs")
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["sources"]["case.rs"]["filter"], "case::")

    def test_new_source_not_wired_to_cargo_fails(self):
        self.assertIn("found 0", self.inspect()["errors"][0])

    def test_autodiscovered_and_aggregated_source_fails(self):
        self.suite()
        self.assertIn("found 2", self.inspect("tests/case.rs", "tests/suites/contracts.rs")["errors"][0])

    def test_suite_without_cargo_target_fails(self):
        self.suite()
        self.assertTrue(any("no Cargo test target" in e for e in self.inspect("tests/case.rs")["errors"]))

    def test_missing_module_file_fails(self):
        self.suite("../missing.rs")
        self.assertTrue(any("missing module" in e for e in self.inspect("tests/suites/contracts.rs")["errors"]))


if __name__ == "__main__":
    unittest.main()
