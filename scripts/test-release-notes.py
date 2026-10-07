#!/usr/bin/env python3
"""Release-channel and ancestry regressions using an isolated local Git fixture."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("release_notes", Path(__file__).with_name("release-notes.py"))
notes = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = notes
spec.loader.exec_module(notes)


class ReleaseNotesTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = Path.cwd()
        os.chdir(self.tmp.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Release fixture")
        self.git("config", "user.email", "release@example.invalid")
        self.commit("feat: initial core")
        self.git("tag", "v0.0.1")

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def git(self, *args):
        return subprocess.check_output(["git", *args], text=True).strip()

    def commit(self, subject):
        self.git("commit", "--allow-empty", "-qm", subject)

    def tag(self, tag, subject="fix: carrier"):
        self.commit(subject)
        self.git("tag", "-a", tag, "-m", tag)

    def test_rc_uses_own_previous_rc_ignoring_newer_dev_and_future_tags(self):
        self.tag("v0.0.2-rc.202609290540")
        self.tag("v0.0.3-dev.202610060104", "feat: WireGuard")
        self.tag("v0.0.2-rc.202610070900")
        self.tag("v0.0.2-rc.202610071000")
        self.assertEqual(notes.previous_tag("v0.0.2-rc.202610070900"), "v0.0.2-rc.202609290540")

    def test_unrelated_same_channel_tag_is_excluded(self):
        self.tag("v0.0.2-rc.202609290540")
        parent = self.git("rev-parse", "HEAD")
        self.tag("v0.0.2-rc.202610060900", "fix: unrelated branch")
        self.git("checkout", "-q", "--detach", parent)
        self.tag("v0.0.2-rc.202610070900")
        self.assertEqual(notes.previous_tag("v0.0.2-rc.202610070900"), "v0.0.2-rc.202609290540")

    def test_stable_uses_previous_stable_including_rc_features(self):
        self.tag("v0.0.2-rc.202609290540", "feat: stable scope")
        self.tag("v0.0.2", "release: v0.0.2")
        self.assertEqual(notes.previous_tag("v0.0.2"), "v0.0.1")
        self.assertIn("feat: stable scope", notes.render("v0.0.2"))
        self.assertNotIn("- release: v0.0.2", notes.render("v0.0.2"))

    def test_dev_keeps_same_base_and_channel(self):
        self.tag("v0.0.3-dev.202610060104")
        self.tag("v0.0.2-rc.202610060500")
        self.tag("v0.0.3-dev.202610070900")
        self.assertEqual(notes.previous_tag("v0.0.3-dev.202610070900"), "v0.0.3-dev.202610060104")

    def test_first_rc_uses_earlier_stable(self):
        self.tag("v0.0.2-dev.202609181034")
        self.tag("v0.0.2-rc.202609290540")
        self.assertEqual(notes.previous_tag("v0.0.2-rc.202609290540"), "v0.0.1")

    def test_legacy_sequence_order_is_numeric(self):
        self.tag("v0.0.2-rc.2")
        self.tag("v0.0.2-rc.10")
        self.tag("v0.0.2-rc.11")
        self.assertEqual(notes.previous_tag("v0.0.2-rc.11"), "v0.0.2-rc.10")

    def test_no_baseline_is_explicit_and_unknown_tags_are_ignored(self):
        self.git("tag", "-d", "v0.0.1")
        self.git("tag", "v999-custom")
        self.tag("v0.0.2-rc.202609290540")
        self.assertIsNone(notes.previous_tag("v0.0.2-rc.202609290540"))
        self.assertIn("Initial release", notes.render("v0.0.2-rc.202609290540"))

    def test_invalid_or_missing_current_tag_fails(self):
        with self.assertRaises(ValueError):
            notes.previous_tag("v0.0.2-alpha.1")
        with self.assertRaises(subprocess.CalledProcessError):
            notes.previous_tag("v0.0.2-rc.202610070900")


if __name__ == "__main__":
    unittest.main()
