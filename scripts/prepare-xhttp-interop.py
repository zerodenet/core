#!/usr/bin/env python3
"""Prepare and verify the fixed official Xray reference used by VLESS and VMess."""
import argparse
import hashlib
import os
import pathlib
import platform
import subprocess
import tempfile
import tomllib
import zipfile

VERSION = "26.3.27"
COMMIT = "d2758a023cd7f4174a5a5fa4ff66e487d4342ba0"
# Official release .dgst SHA2-256 values pinned with the reference identity.
SHA256 = {
    "Xray-linux-64.zip": "23cd9af937744d97776ee35ecad4972cf4b2109d1e0fe6be9930467608f7c8ae",
    "Xray-linux-arm64-v8a.zip": "4d30283ae614e3057f730f67cd088a42be6fdf91f8639d82cb69e48cde80413c",
    "Xray-macos-64.zip": "f5b0471d3459eff1b82e48af0aeac186abcc3298210070afbbbd8437a4e8b203",
    "Xray-macos-arm64-v8a.zip": "2e93a67e8aa1936ecefb307e120830fcbd4c643ab9b1c46a2d0838d5f8409eaf",
}
ROOT = pathlib.Path(__file__).resolve().parents[1]


def check_baseline():
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    for protocol in ("vless", "vmess"):
        package = tomllib.loads((ROOT / f"protocols/{protocol}/Cargo.toml").read_text())["package"]
        assert package["version"] == VERSION, f"{protocol} package/reference mismatch"
        assert any(
            item["name"] == protocol and item["version"] == VERSION
            for item in lock["package"]
        ), f"{protocol} lock/reference mismatch"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--check-baseline", action="store_true")
    args = parser.parse_args()
    check_baseline()
    if args.check_baseline:
        print(f"VLESS/VMess reference v{VERSION} / {COMMIT}")
        return
    if args.output is None:
        parser.error("--output is required when preparing the binary")
    system = {"Darwin": "macos", "Linux": "linux"}.get(platform.system())
    machine = {"x86_64": "64", "amd64": "64", "arm64": "arm64-v8a", "aarch64": "arm64-v8a"}.get(platform.machine().lower())
    if not system or not machine:
        parser.error("unsupported reference platform; provide an official XRAY_BIN manually")
    args.output.mkdir(parents=True, exist_ok=True)
    binary = args.output / "xray"
    archive = f"Xray-{system}-{machine}.zip"
    with tempfile.TemporaryDirectory(prefix="zero-xhttp-") as directory:
        download = pathlib.Path(directory) / archive
        subprocess.run(["curl", "--fail", "--location", "--retry", "3", "--output", str(download),
                        f"https://github.com/XTLS/Xray-core/releases/download/v{VERSION}/{archive}"], check=True)
        if hashlib.sha256(download.read_bytes()).hexdigest() != SHA256[archive]:
            raise RuntimeError(f"Official reference archive checksum mismatch: {archive}")
        with zipfile.ZipFile(download) as source:
            binary.write_bytes(source.read("xray"))
    os.chmod(binary, 0o755)
    version = subprocess.check_output([str(binary.resolve()), "version"], text=True)
    if f"Xray {VERSION}" not in version or COMMIT[:7] not in version:
        raise RuntimeError(f"Unexpected official reference identity: {version}")
    print(version.strip())
    print(f"XRAY_BIN={binary.resolve()}")


if __name__ == "__main__":
    main()
