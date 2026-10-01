use std::process::Command;

use crate::support;

use support::config_path;

#[test]
fn status_command_emits_json_export() {
    let output = Command::new(env!("CARGO_BIN_EXE_zero"))
        .args([
            "status",
            "--json",
            config_path().to_str().expect("utf-8 config path"),
        ])
        .output()
        .expect("run zero status");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse status json");

    assert_eq!(json["config"]["mode"]["kind"], "rule");
    assert_eq!(json["config"]["rule_count"], 3);
    assert_eq!(json["config"]["listeners"][0]["tag"], "mixed-in");
    assert_eq!(json["config"]["listeners"][0]["protocol"], "mixed");
    assert_eq!(json["config"]["outbounds"][0]["tag"], "direct");
    assert_eq!(
        json["config"]["outbound_groups"].as_array().unwrap().len(),
        0
    );
    assert_eq!(json["runtime"]["stats"]["active_sessions"], 0);
    assert_eq!(json["runtime"]["udp_upstream_idle_timeout_seconds"], 30);
    assert_eq!(
        json["runtime"]["recent_completed_sessions"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        json["runtime"]["stats"]["udp_upstream"]["active_associations"],
        0
    );
}

#[test]
fn status_command_supports_text_output() {
    let output = Command::new(env!("CARGO_BIN_EXE_zero"))
        .args(["status", config_path().to_str().expect("utf-8 config path")])
        .output()
        .expect("run zero status");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("Engine Status"));
    assert!(stdout.contains("config:"));
    assert!(stdout.contains("mode: rule"));
    assert!(stdout.contains("runtime:"));
    assert!(stdout.contains("udp_upstream:"));
    assert!(stdout.contains("listeners:"));
    assert!(stdout.contains("outbounds:"));
}
