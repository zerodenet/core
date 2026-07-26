use std::io::Read;
use std::process::Command;

use sha2::{Digest, Sha256};

#[test]
fn build_info_exposes_the_source_commit_even_for_tagged_builds() {
    let output = Command::new(env!("CARGO_BIN_EXE_zero"))
        .arg("build-info")
        .output()
        .expect("run zero build-info");

    assert!(
        output.status.success(),
        "build-info failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("build-info output is UTF-8");
    assert!(stdout.contains("build_id: "));
    assert!(stdout.contains("build_time: "));
    assert_eq!(
        value_for(&stdout, "build_profile"),
        Some("debug"),
        "cargo test must identify its dev-profile candidate as debug"
    );
    let expected_sha256 = sha256_file(env!("CARGO_BIN_EXE_zero"));
    assert_eq!(
        value_for(&stdout, "binary_sha256"),
        Some(expected_sha256.as_str()),
        "build-info must hash the exact executable that produced the report"
    );
    let features = value_for(&stdout, "features").expect("build features");
    assert_eq!(
        features
            .split(',')
            .map(str::to_owned)
            .collect::<Vec<String>>(),
        expected_features(),
        "build-info must describe this binary's actual compile-time features"
    );
    assert!(stdout.contains("git: "));
    assert_eq!(
        value_for(&stdout, "git_hash"),
        Some(env!("ZERO_GIT_HASH")),
        "build-info must expose the source commit independently of git describe"
    );
}

fn sha256_file(path: &str) -> String {
    let mut file = std::fs::File::open(path).expect("open zero executable");
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).expect("read zero executable");
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    format!("{:x}", digest.finalize())
}

fn expected_features() -> Vec<String> {
    let mut features = Vec::new();
    if cfg!(feature = "status-api") {
        features.push("status-api".to_owned());
    }
    if cfg!(feature = "event-dispatcher") {
        features.push("event-dispatcher".to_owned());
    }
    if cfg!(feature = "sink-jsonl") {
        features.push("sink-jsonl".to_owned());
    }
    if cfg!(feature = "connector") {
        features.push("connector".to_owned());
    }
    if cfg!(feature = "grpc-api") {
        features.push("grpc-api".to_owned());
    }
    features.extend(zero_proxy::compiled_protocol_features());
    if cfg!(feature = "dns") {
        features.push("dns".to_owned());
    }
    features
}

fn value_for<'a>(output: &'a str, key: &str) -> Option<&'a str> {
    output
        .lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix(": "))
}
