#![cfg(feature = "connector")]

use std::process::Command;

#[test]
fn connector_state_command_reports_event_outbox_and_fails_closed() {
    let directory = tempfile::tempdir().expect("state directory");
    let outbox_path = directory.path().join("event-outbox.jsonl");
    let quota_path = directory.path().join("quota.json");
    let config_path = directory.path().join("config.json");
    std::fs::write(&outbox_path, b"").expect("write compatible event outbox");
    std::fs::write(&quota_path, br#"{"version":1,"balances":[]}"#)
        .expect("write compatible quota state");
    let config = serde_json::json!({
        "inbounds":[],
        "outbounds":[],
        "runtime":{"principal_quota_state_path":quota_path},
        "route":{"rules":[],"final":{"type":"direct"}},
        "api":{"outbox_path":outbox_path}
    });
    std::fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config).expect("serialize config"),
    )
    .expect("write config");

    let compatible = run_state(&config_path);
    assert!(
        compatible.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&compatible.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&compatible.stdout).expect("state report JSON");
    assert_eq!(
        report["schema_id"],
        "zero.connector.upgrade-state-report.v1"
    );
    assert_eq!(report["compatible"], true);
    assert_eq!(report["connector_files"][0]["kind"], "event_outbox");
    assert_eq!(report["connector_files"][0]["status"], "ready");
    assert_eq!(report["principal_quota"]["status"], "ready");

    std::fs::write(&outbox_path, b"{not-json}\n").expect("write incompatible event state");
    let incompatible = run_state(&config_path);
    assert!(!incompatible.status.success());
    let report: serde_json::Value =
        serde_json::from_slice(&incompatible.stdout).expect("incompatible state report JSON");
    assert_eq!(report["compatible"], false);
    assert_eq!(report["connector_files"][0]["status"], "incompatible");
}

fn run_state(config_path: &std::path::Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_zero"))
        .args([
            "connector",
            "state",
            "--json",
            config_path.to_str().expect("UTF-8 config path"),
        ])
        .output()
        .expect("run connector state")
}
