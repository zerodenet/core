use std::fs;
use std::path::PathBuf;

fn repository_file(path: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

#[test]
fn windows_release_stages_a_pinned_amd64_wintun_distribution() {
    let script = repository_file("scripts/prepare-wintun.ps1");

    assert!(script.contains("$WintunVersion = '0.14.1'"));
    assert!(script.contains(
        "$WintunArchiveSha256 = '07c256185d6ee3652e09fa55c0b673e2624b565e02c4b9091c79ca7d2f24ef51'"
    ));
    assert!(script.contains("https://www.wintun.net/builds/wintun-$WintunVersion.zip"));
    assert!(script.contains("bin/amd64/wintun.dll"));
    assert!(script.contains("checksum mismatch"));
}

#[test]
fn windows_release_requires_and_packages_the_tun_runtime() {
    let workflow = repository_file(".github/workflows/release.yml");

    assert!(workflow.contains("./scripts/prepare-wintun.ps1"));
    assert!(workflow.contains("test -f wintun.dll"));
    assert!(workflow.contains("7z a \"$ARCHIVE\" zero.exe wintun.dll wintun-LICENSE.txt"));
}
