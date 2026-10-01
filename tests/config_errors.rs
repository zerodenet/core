use std::process::Command;

use crate::support;

use support::{remove_temp_file, write_temp_config};

#[test]
fn status_command_reports_missing_config_path_clearly() {
    let output = Command::new(env!("CARGO_BIN_EXE_zero"))
        .args(["status", "does-not-exist.json"])
        .output()
        .expect("run zero status");

    assert!(
        !output.status.success(),
        "status command unexpectedly succeeded"
    );

    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(stderr.contains("error: failed to read config `does-not-exist.json`"));
}

#[test]
fn status_command_reports_parse_errors_clearly() {
    let config_path = write_temp_config("{\"inbounds\": []", "invalid-config");

    let output = Command::new(env!("CARGO_BIN_EXE_zero"))
        .args(["status", config_path.to_str().expect("utf-8 config path")])
        .output()
        .expect("run zero status");

    remove_temp_file(&config_path);

    assert!(
        !output.status.success(),
        "status command unexpectedly succeeded"
    );

    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(stderr.contains("error: failed to parse config:"));
    assert!(stderr.contains("EOF while parsing an object"));
}
