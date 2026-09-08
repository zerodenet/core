use super::decode_tun_status;

#[test]
fn tun_recover_targets_the_selected_control_socket() {
    let command = crate::cli::parse_args(
        [
            "zero",
            "tun",
            "recover",
            "--socket",
            "/tmp/isolated-zero.sock",
        ]
        .map(str::to_owned),
    )
    .unwrap();
    assert!(
        matches!(command, crate::cli::Command::TunRecover { socket_path }
        if socket_path.as_deref() == Some("/tmp/isolated-zero.sock"))
    );
}

#[test]
fn tun_status_decodes_the_query_response_envelope() {
    let status = decode_tun_status(serde_json::json!({
        "tun_status": {
            "running": true,
            "name": "ZeroTun",
            "healthy": true,
            "dual_stack": false,
            "managed_by_config": true
        }
    }))
    .expect("decode TUN query response");

    assert!(status.running);
    assert!(status.healthy);
    assert!(status.managed_by_config);
    assert_eq!(status.name.as_deref(), Some("ZeroTun"));
    assert!(!status.dual_stack);
}
