use zero_config::RuntimeConfig;
fn config(ids: &[&str]) -> String {
    serde_json::json!({
        "inbounds": [{"tag": "vless", "listen": {"address": "127.0.0.1", "port": 10001},
            "protocol": {"type": "vless", "users": ids.iter().map(|id| serde_json::json!({"id": id})).collect::<Vec<_>>()}}],
        "route": {"rules": [], "final": {"type": "direct"}}
    }).to_string()
}
#[test]
fn custom_ids_preserve_case_and_detect_uuid_alias_duplicates() {
    RuntimeConfig::parse(&config(&["alice", "Alice"])).unwrap();
    let id = vless::format_uuid(&vless::parse_uuid("alice").unwrap());
    let error = RuntimeConfig::parse(&config(&["alice", &id])).unwrap_err();
    assert!(error.to_string().contains("duplicate user id"));
}
