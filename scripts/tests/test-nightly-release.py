#!/usr/bin/env python3
"""Exercise nightly candidates in real, isolated Git repositories."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
PLANNER = ROOT / "scripts/nightly-release.py"


class NightlyReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="zero-nightly-tests-")
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        (self.repo / "scripts").mkdir()
        (self.repo / "release").mkdir()
        shutil.copy(ROOT / "scripts/release.sh", self.repo / "scripts/release.sh")
        (self.repo / "Cargo.toml").write_text(
            '[workspace]\nmembers = []\n[workspace.package]\nversion = "0.0.1"\n')
        (self.repo / "release/breaking-changes.md").write_text('''# Compatibility ledger

| Version | Area | Migration |
|---|---|---|
| `Unreleased` | - | No pending compatibility changes <!-- version-contract:unreleased-row --> |
| `0.0.1` | - | No pending compatibility changes |

## Unreleased

<!-- Record implemented but unsealed compatibility changes here. -->

## 0.0.1

<!-- No compatibility changes in this release. -->
''')
        self.git("init", "-q")
        self.git("config", "user.name", "Nightly tests")
        self.git("config", "user.email", "nightly@example.invalid")
        self.git("checkout", "-b", "main")
        self.commit("initial")
        self.git("tag", "v0.0.1")
        self.release("0.0.2-dev.202610071000", "--start-development")
        self.commit("release: dev seed")
        self.git("tag", "v0.0.2-dev.202610071000")
        self.release("0.0.2-rc.202610071100", "--seal-only")
        self.commit("release: rc seed")
        self.git("tag", "-a", "v0.0.2-rc.202610071100", "-m", "rc seed")
        self.git("checkout", "-b", "develop")
        self.release("0.0.3-dev.202610071200", "--start-development")
        self.commit("release: next development line")
        self.git("tag", "-a", "v0.0.3-dev.202610071200", "-m", "dev seed")

    def run_cmd(self, *args, check=True):
        env = dict(os.environ, ZERO_RELEASE_TIMESTAMP="202610091917")
        result = subprocess.run(args, cwd=self.repo, env=env, text=True, capture_output=True)
        if check and result.returncode:
            self.fail(f"{args}: {result.stdout}\n{result.stderr}")
        return result

    def git(self, *args):
        return self.run_cmd("git", *args).stdout.strip()

    def release(self, *args, check=True):
        return self.run_cmd("bash", "scripts/release.sh", *args, check=check)

    def commit(self, message):
        self.git("add", ".")
        self.git("commit", "-qm", message)

    def change(self):
        path = self.repo / "feature.txt"
        path.write_text((path.read_text() if path.exists() else "") + "new implementation\n")
        self.commit("feat: implement change")

    def nightly(self, operation, branch, check=True, **kwargs):
        args = [sys.executable, str(PLANNER), operation, "--branch", branch]
        for key, value in kwargs.items():
            args += ["--" + key.replace("_", "-"), value]
        result = self.run_cmd(*args, check=check)
        return json.loads(result.stdout) if check else result

    def test_unchanged_branches_resolve_their_own_annotated_tag(self):
        for branch, expected in [("main", "v0.0.2-rc.202610071100"),
                                 ("develop", "v0.0.3-dev.202610071200")]:
            self.git("checkout", branch)
            result = self.nightly("plan", branch)
            self.assertEqual((result["mode"], result["tag"]), ("existing", expected))

    def test_parallel_branches_prepare_versions_without_cross_line_blocking(self):
        for branch, expected in [("develop", "v0.0.3-dev.202610091917"),
                                 ("main", "v0.0.2-rc.202610091917")]:
            self.git("checkout", branch)
            self.change()
            source = self.git("rev-parse", "HEAD")
            self.assertEqual(self.nightly("plan", branch)["mode"], "prepare")
            result = self.nightly("prepare", branch, expected_sha=source)
            self.assertEqual(result["tag"], expected)
            self.assertTrue(result["pushed_candidate"])
            self.assertEqual(self.git("rev-parse", "HEAD^"), source)
            self.assertEqual(self.git("status", "--porcelain"), "")
            self.assertEqual((self.repo / "release/promotion-source").read_text(), f"{branch}@{source}\n")
            self.assertEqual(self.git("tag", "--list", expected), "")
            self.release("--verify-tag", expected)
            self.git("tag", "-a", expected, "-m", expected)

    def test_ci_failure_reuses_version_commit_instead_of_daily_bumping(self):
        self.change()
        candidate = self.nightly("prepare", "develop", expected_sha=self.git("rev-parse", "HEAD"))
        retry = self.nightly("plan", "develop")
        self.assertEqual(retry["mode"], "retry")
        result = self.nightly("prepare", "develop", expected_sha=retry["sha"])
        self.assertFalse(result["pushed_candidate"])
        self.assertEqual((result["sha"], result["tag"]), (candidate["sha"], candidate["tag"]))

    def test_existing_tag_is_reused_for_incomplete_release(self):
        result = self.nightly("prepare", "develop", expected_sha=self.git("rev-parse", "HEAD"))
        self.assertEqual(result["mode"], "existing")
        self.assertFalse(result["pushed_candidate"])

    def test_non_ancestral_future_tag_does_not_override_branch_series(self):
        self.git("checkout", "-b", "unrelated")
        self.change()
        self.git("tag", "v9.0.0-dev.209901010000")
        self.git("checkout", "main")
        self.change()
        result = self.nightly("prepare", "main", expected_sha=self.git("rev-parse", "HEAD"))
        self.assertEqual(result["tag"], "v0.0.2-rc.202610091917")

    def test_stable_is_never_automatically_bumped_or_published(self):
        self.git("checkout", "main")
        self.release("0.0.2", "--seal-only")
        self.commit("release: stable")
        self.git("tag", "v0.0.2")
        self.change()
        self.assertEqual(self.nightly("plan", "main")["mode"], "skip")

    def test_main_follows_new_rc_line_after_stable_without_workflow_changes(self):
        self.git("checkout", "main")
        self.release("0.0.2", "--seal-only")
        self.commit("release: stable")
        self.git("tag", "v0.0.2")
        self.git("tag", "-d", "v0.0.2-dev.202610071000", "v0.0.2-rc.202610071100")
        self.assertEqual(self.nightly("plan", "main")["mode"], "skip")

        # Model the separately authorized dev -> RC promotion of the next line.
        self.release("0.0.3-dev.202610080800", "--start-development")
        self.commit("release: next line dev source")
        self.git("tag", "v0.0.3-dev.202610080800")
        self.release("0.0.3-rc.202610080900", "--seal-only")
        self.commit("release: next line rc")
        self.git("tag", "-a", "v0.0.3-rc.202610080900", "-m", "new rc line")
        self.assertEqual(self.nightly("plan", "main")["mode"], "existing")

        self.change()
        plan = self.nightly("plan", "main")
        self.assertEqual(plan["previous_tag"], "v0.0.3-rc.202610080900")
        result = self.nightly("prepare", "main", expected_sha=plan["sha"])
        self.assertEqual(result["tag"], "v0.0.3-rc.202610091917")

    def test_develop_follows_patch_minor_and_major_line_changes(self):
        for base in ("0.0.4", "0.1.0", "1.0.0"):
            with self.subTest(base=base):
                self.release(f"{base}-dev.202610080800", "--start-development")
                self.commit("release: change development line")
                self.git("tag", "-a", f"v{base}-dev.202610080800", "-m", "new dev line")
                self.change()
                plan = self.nightly("plan", "develop")
                self.assertEqual(plan["previous_tag"], f"v{base}-dev.202610080800")
                result = self.nightly("prepare", "develop", expected_sha=plan["sha"])
                self.assertEqual(result["tag"], f"v{base}-dev.202610091917")
                self.git("tag", "-a", result["tag"], "-m", result["tag"])

    def test_source_change_or_dirty_checkout_rejects_preparation(self):
        self.change()
        stale = self.git("rev-parse", "HEAD^")
        result = self.nightly("prepare", "develop", check=False, expected_sha=stale)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Source commit changed", result.stderr)
        (self.repo / "feature.txt").write_text("uncommitted")
        result = self.nightly("prepare", "develop", check=False,
                              expected_sha=self.git("rev-parse", "HEAD"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must be clean", result.stderr)

    def test_stage_mismatch_and_unseeded_line_are_rejected(self):
        self.assertNotEqual(self.nightly("plan", "main", check=False).returncode, 0)
        self.git("tag", "-d", "v0.0.3-dev.202610071200")
        result = self.nightly("plan", "develop", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("seed this line manually", result.stderr)

    def test_version_rollback_and_same_minute_reuse_remain_rejected(self):
        self.git("checkout", "main")
        self.assertNotEqual(self.release("0.0.2-rc.202610071059", "--seal-only", check=False).returncode, 0)
        self.change()
        result = self.nightly("prepare", "main", expected_sha=self.git("rev-parse", "HEAD"))
        self.git("tag", result["tag"])
        self.git("update-ref", "refs/remotes/origin/main", result["sha"])
        self.change()
        result = self.nightly("prepare", "main", check=False, expected_sha=self.git("rev-parse", "HEAD"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("wait for the next UTC minute", result.stderr)

    def test_foreign_stable_tag_still_closes_its_entire_version_line(self):
        self.git("checkout", "main")
        self.git("checkout", "-b", "published-stable")
        self.release("0.0.2", "--seal-only")
        self.commit("release: stable")
        self.git("tag", "v0.0.2")
        self.git("checkout", "main")
        self.change()
        result = self.nightly("prepare", "main", check=False, expected_sha=self.git("rev-parse", "HEAD"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("prerelease stages are closed", result.stderr)


if __name__ == "__main__":
    unittest.main()
