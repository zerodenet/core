#!/usr/bin/env python3
"""Prepare the fixed Shadowsocks-Rust reference and verify its release checksum."""
import argparse
import hashlib
import pathlib
import platform
import subprocess
import tarfile
import tempfile
import tomllib

VERSION = "1.21.2"
COMMIT = "a03006a753486e64717d6e3afa91e0c6d043c557"
ROOT = pathlib.Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    package = tomllib.loads((ROOT / "protocols/shadowsocks/Cargo.toml").read_text())["package"]
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    assert package["version"] == VERSION
    assert any(p["name"] == "shadowsocks" and p["version"] == VERSION for p in lock["package"])
    arch = {"x86_64": "x86_64", "amd64": "x86_64", "arm64": "aarch64", "aarch64": "aarch64"}.get(platform.machine().lower())
    system = {"Darwin": "apple-darwin", "Linux": "unknown-linux-musl"}.get(platform.system())
    if not arch or not system:
        parser.error("unsupported platform; supply official binaries via SS_RUST_BIN_DIR")
    archive = f"shadowsocks-v{VERSION}.{arch}-{system}.tar.xz"
    url = f"https://github.com/shadowsocks/shadowsocks-rust/releases/download/v{VERSION}/{archive}"
    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="zero-ss-reference-") as temp:
        root = pathlib.Path(temp)
        for name, source in [("archive", url), ("sha256", url + ".sha256")]:
            subprocess.run(["curl", "--fail", "--location", "--retry", "3", "--output", str(root / name), source], check=True)
        digest = hashlib.sha256((root / "archive").read_bytes()).hexdigest()
        assert digest in (root / "sha256").read_text().split(), "official archive checksum mismatch"
        with tarfile.open(root / "archive") as source:
            for binary in ["sslocal", "ssserver"]:
                matches = [m for m in source.getmembers() if m.isfile() and pathlib.PurePosixPath(m.name).name == binary]
                assert len(matches) == 1, f"missing or ambiguous {binary}"
                path = args.output / binary
                path.write_bytes(source.extractfile(matches[0]).read())
                path.chmod(0o755)
                version = subprocess.check_output([str(path.resolve()), "--version"], text=True)
                assert f"shadowsocks {VERSION}" in version, version
        print(f"Official v{VERSION} / {COMMIT}; archive SHA256 {digest}")
        print(f"SS_RUST_BIN_DIR={args.output.resolve()}")


if __name__ == "__main__":
    main()
