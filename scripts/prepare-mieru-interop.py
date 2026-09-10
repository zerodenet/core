#!/usr/bin/env python3
"""Build the Mieru probe against an immutable official source checkout."""
import argparse
from pathlib import Path
import os
import re
import shutil
import subprocess
import tempfile

REVISIONS = {
    "v3.33.0": "48ddb69d5d343d76c9004c5054ee36609579ba13",
    "v3.36.1": "316cc6606c287d45a321b99ac86a1f2e7f2b785a",
}
root = Path(__file__).resolve().parents[1]
manifest = (root / "protocols/mieru/Cargo.toml").read_text()
package = manifest.split("[package]", 1)[1].split("\n[", 1)[0]
package_version = re.search(r'^version\s*=\s*"([^"]+)"', package, re.MULTILINE)
if package_version is None:
    raise SystemExit("Mieru package must declare its official reference version")
baseline = "v" + package_version.group(1)
if baseline not in REVISIONS:
    raise SystemExit("Mieru package version needs an immutable reference pin: " + baseline)
if not (root / "Cargo.lock").is_file():
    raise SystemExit("Generate the workspace lockfile first: cargo generate-lockfile")
locked_versions = []
for entry in (root / "Cargo.lock").read_text().split("[[package]]")[1:]:
    if re.search(r'^name\s*=\s*"mieru"\s*$', entry, re.MULTILINE):
        locked_versions.extend(re.findall(r'^version\s*=\s*"([^"]+)"', entry, re.MULTILINE))
if locked_versions != [package_version.group(1)]:
    raise SystemExit("Mieru Cargo.lock version must match the package reference version")
config_manifest = (root / "protocols/mieru/config/Cargo.toml").read_text()
config_package = config_manifest.split("[package]", 1)[1].split("\n[", 1)[0]
config_version = re.search(r'^version\s*=\s*"([^"]+)"', config_package, re.MULTILINE)
if config_version is None or config_version.group(1) != package_version.group(1):
    raise SystemExit("mieru-config must match the Mieru implementation baseline")
config_locked = []
for entry in (root / "Cargo.lock").read_text().split("[[package]]")[1:]:
    if re.search(r'^name\s*=\s*"mieru-config"\s*$', entry, re.MULTILINE):
        config_locked.extend(re.findall(r'^version\s*=\s*"([^"]+)"', entry, re.MULTILINE))
if config_locked != [package_version.group(1)]:
    raise SystemExit("mieru-config Cargo.lock version must match the implementation baseline")
parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--version", choices=REVISIONS, default=baseline,
                    help="official reference; a non-baseline version is only an extra compatibility probe")
parser.add_argument("--source", type=Path, help="existing clean official checkout matching --version")
parser.add_argument("--output", type=Path)
parser.add_argument("--check-baseline", action="store_true", help="check package, lockfile and immutable pin without building")
args = parser.parse_args()
if args.check_baseline:
    if args.version != baseline:
        parser.error("baseline check must use the package version " + baseline)
    print("Mieru implementation baseline", baseline, REVISIONS[baseline])
    raise SystemExit(0)
if args.output is None:
    parser.error("--output is required when building a reference")
expected_revision = REVISIONS[args.version]
with tempfile.TemporaryDirectory(prefix="zero-mieru-reference-") as work:
    work = Path(work)
    source = args.source.resolve() if args.source else work / "source"
    if not args.source:
        subprocess.run(["git", "clone", "--depth", "1", "--branch", args.version, "https://github.com/enfein/mieru.git", str(source)], check=True)
    revision = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    if revision != expected_revision or subprocess.check_output(["git", "-C", str(source), "status", "--porcelain"], text=True).strip():
        raise SystemExit("reference must be a clean official " + args.version + " checkout: " + expected_revision)
    build = work / "probe"
    build.mkdir()
    for source_file in (root / "crates/proxy/tests/support/mieru_reference").glob("*.go"):
        shutil.copy(source_file, build)
    (build / "go.mod").write_text("module zero-mieru-reference\n\ngo 1.20\n\nrequire github.com/enfein/mieru/v3 " + args.version + "\n")
    go = os.environ.get("GO_BIN", "go")
    subprocess.run([go, "mod", "edit", "-replace", "github.com/enfein/mieru/v3=" + str(source)], cwd=build, check=True)
    subprocess.run([go, "mod", "tidy"], cwd=build, check=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run([go, "build", "-trimpath", "-ldflags", "-X main.referenceVersion=" + args.version + " -X main.referenceRevision=" + expected_revision, "-o", str(args.output.resolve()), "."], cwd=build, check=True)
    print("Built official reference", args.version, expected_revision, "at", args.output.resolve())
