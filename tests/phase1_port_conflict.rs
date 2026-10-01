//! Phase 1: port-conflict detection lives in config validation (DuplicateInboundListen),
//! surfacing at config load time rather than deferred to runtime bind.

use crate::support;

use std::process::Command;
use support::{remove_temp_file, write_temp_config};

#[test]
fn duplicate_inbound_listen_fails_at_config_load() {
    // Two inbound listeners on the same (address, port) must be rejected
    // by config validation (DuplicateInboundListen), not deferred to bind.
    let config = r#"{
        "inbounds": [
            {
                "tag": "a",
                "listen": { "address": "127.0.0.1", "port": 19999 },
                "protocol": { "type": "socks5" }
            },
            {
                "tag": "b",
                "listen": { "address": "127.0.0.1", "port": 19999 },
                "protocol": { "type": "http" }
            }
        ],
        "outbounds": [
            { "tag": "direct", "protocol": { "type": "direct" } }
        ],
        "route": { "rules": [], "final": { "type": "direct" } }
    }"#;

    let config_path = write_temp_config(config, "dup-listen");
    let output = Command::new(env!("CARGO_BIN_EXE_zero"))
        .args(["status", config_path.to_str().expect("utf-8 config path")])
        .output()
        .expect("run zero status");

    remove_temp_file(&config_path);

    assert!(
        !output.status.success(),
        "config with duplicate listen should be rejected"
    );

    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(
        stderr.contains("duplicate inbound listen endpoint"),
        "expected duplicate-listen error, got: {stderr}"
    );
}
