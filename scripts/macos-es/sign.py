#!/usr/bin/env python3
"""Package an explicitly selected, already-built probe with an approved profile.

No account enrollment, certificate creation, entitlement request or probe run.
The signed result remains an unqualified preflight tool, not a tree provider.
"""
import datetime
import os
from pathlib import Path
import plistlib
import re
import shutil
import subprocess
import sys


def required(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Set {name} explicitly; no identity/team is selected automatically")
    return value


def main():
    base = Path(__file__).resolve().parent
    out = Path(sys.argv[1]) if len(sys.argv) == 2 else base / "../../target/macos-es-preflight"
    if len(sys.argv) > 2:
        raise ValueError("usage: sign.py [build-output-directory]")
    identity = required("MACOS_SIGN_IDENTITY")
    if identity.strip() == "-":
        raise ValueError("Select an Apple-issued signing identity; ad-hoc signing cannot grant Endpoint Security")
    team = required("APPLE_TEAM_ID")
    bundle_id = required("MACOS_ES_BUNDLE_ID")
    profile_path = Path(required("MACOS_ES_PROFILE")).expanduser().resolve(strict=True)
    if not re.fullmatch(r"[A-Z0-9]+", team):
        raise ValueError("APPLE_TEAM_ID must match the selected Apple profile")
    if not re.fullmatch(r"[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+", bundle_id):
        raise ValueError("MACOS_ES_BUNDLE_ID must be an explicit bundle identifier")
    binary = (out / "zero-es-preflight").resolve(strict=True)
    if not binary.is_file():
        raise ValueError("Build the unsigned probe before packaging it")
    decoded = subprocess.run(
        ["security", "cms", "-D", "-i", str(profile_path)],
        check=True, capture_output=True,
    ).stdout
    profile = plistlib.loads(decoded)
    grants = profile.get("Entitlements", {})
    if grants.get("com.apple.developer.endpoint-security.client") is not True:
        raise ValueError("Apple's selected profile does not grant Endpoint Security")
    if team not in profile.get("TeamIdentifier", []):
        raise ValueError("Provisioning profile belongs to another Apple team")
    application_id = grants.get("com.apple.application-identifier", grants.get("application-identifier"))
    prefixes = profile.get("ApplicationIdentifierPrefix", [])
    if application_id not in {f"{prefix}.{bundle_id}" for prefix in prefixes}:
        raise ValueError("Provisioning profile does not name this exact team and bundle")
    expiration = profile.get("ExpirationDate")
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    if not isinstance(expiration, datetime.datetime) or expiration.replace(tzinfo=None) <= now:
        raise ValueError("Provisioning profile is expired or lacks its expiration")
    contents = out / "ZeroESPreflight.app/Contents"
    executable = contents / "MacOS/zero-es-preflight"
    executable.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(binary, executable)
    shutil.copy2(profile_path, contents / "embedded.provisionprofile")
    info = {
        "CFBundleExecutable": "zero-es-preflight",
        "CFBundleIdentifier": bundle_id,
        "CFBundleName": "Zero ES Preflight",
        "CFBundlePackageType": "APPL",
        "CFBundleVersion": "1",
        "CFBundleShortVersionString": "0.1.0",
        "LSUIElement": True,
    }
    with (contents / "Info.plist").open("wb") as output:
        plistlib.dump(info, output)
    with (base / "entitlements.plist").open("rb") as source:
        entitlements = plistlib.load(source)
    entitlements["com.apple.application-identifier"] = application_id
    entitlements["com.apple.developer.team-identifier"] = team
    generated = out / "approved-entitlements.plist"
    with generated.open("wb") as output:
        plistlib.dump(entitlements, output)
    bundle = contents.parent
    subprocess.run([
        "codesign", "--force", "--options", "runtime", "--timestamp",
        "--identifier", bundle_id, "--entitlements", str(generated),
        "--sign", identity, str(bundle),
    ], check=True)
    subprocess.run(["codesign", "--verify", "--strict", str(bundle)], check=True)
    actual = subprocess.run(
        ["codesign", "--display", "--verbose=4", str(bundle)],
        check=True, capture_output=True, text=True,
    ).stderr.splitlines()
    if f"TeamIdentifier={team}" not in actual:
        raise ValueError("Signing certificate does not match the approved team")
    print(f"Packaged signed capability probe: {bundle.resolve()}")
    print("Process-tree provider remains unqualified. Probe was not executed.")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError, plistlib.InvalidFileException) as error:
        print(f"Cannot package ES probe: {error}", file=sys.stderr)
        sys.exit(1)
