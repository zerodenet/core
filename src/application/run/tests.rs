use zero_config::RuntimeConfig;
use zero_engine::EngineError;
use zero_proxy::Proxy;

use super::{proxy_stop_reason, run_supervised_proxy};

#[tokio::test]
async fn proxy_runtime_error_reaches_application_supervisor() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = occupied.local_addr().unwrap().port();
    let config = RuntimeConfig::parse(&format!(
        r#"{{ "inbounds": [{{ "tag":"conflict", "listen":{{"address":"127.0.0.1","port":{port}}}, "protocol":{{"type":"direct"}} }}], "route": {{ "rules": [], "final": {{ "type": "direct" }} }} }}"#
    )).expect("runtime config with externally occupied inbound");
    let proxy = Proxy::new(config).expect("minimal proxy");

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        run_supervised_proxy(&proxy, std::future::pending()),
    )
    .await
    .expect("supervisor must surface bind failure promptly");

    assert!(result.is_err());
}

#[test]
fn proxy_stop_reason_distinguishes_signal_from_runtime_failure() {
    assert_eq!(proxy_stop_reason(&Ok(())), "signal");
    assert_eq!(
        proxy_stop_reason(&Err(EngineError::NoInbounds)),
        "runtime_error"
    );
}

#[cfg(any(feature = "status-api", feature = "grpc-api"))]
#[test]
fn control_api_formats_bare_and_bracketed_ipv6_listeners() {
    for host in ["::1", "[::1]", "0:0:0:0:0:0:0:1"] {
        let config = RuntimeConfig::parse(&serde_json::json!({
            "api":{"control":{"enabled":true,"listen":{"address":host,"port":9090},"api_key":"test"}},
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let engine = zero_engine::Engine::new(config).unwrap();
        let status = super::status_server_spec(&engine, None).unwrap().unwrap();
        #[cfg(feature = "status-api")]
        assert_eq!(status.listen, "[::1]:9090");
        #[cfg(feature = "grpc-api")]
        assert_eq!(status.grpc_listen, "[::1]:9091");
    }
}

#[test]
fn control_commands_and_application_supervision_do_not_embed_the_entire_runtime_state() {
    let command = crate::application::execute(crate::cli::Command::TunStatus { socket_path: None });
    assert!(
        std::mem::size_of_val(&command) < 16 * 1024,
        "even a synchronous TUN status command must fit the Windows main stack"
    );
    let proxy =
        Proxy::new(RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap())
            .unwrap();
    let supervised = run_supervised_proxy(&proxy, std::future::pending());
    assert!(
        std::mem::size_of_val(&supervised) < 16 * 1024,
        "application supervision must retain a pointer to runtime state"
    );
}

#[cfg(feature = "grpc-api")]
#[tokio::test]
async fn initial_tls_failure_retires_original_application_before_same_config_retry() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("zero.json");
    let ipc = directory.path().join("control.sock");
    let config = serde_json::json!({
        "api":{"control":{"enabled":true,"listen":{"address":"127.0.0.1","port":18080},"api_key":"fixture-key",
            "grpc":{"tls":{"cert_path":"absent.pem","key_path":"absent-key.pem"}}}},
        "runtime":{"principal_quota_state_path":"quota.json"},
        "route":{"final":{"type":"direct"}}
    });
    std::fs::write(&config_path, config.to_string()).unwrap();
    for _ in 0..2 {
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            super::run(
                config_path.to_str().unwrap(),
                None,
                Some(ipc.to_str().unwrap()),
                None,
                true,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.to_string().contains("absent.pem"), "{error}");
        assert!(
            !error.to_string().contains("unresolved preparation/cleanup"),
            "{error}"
        );
        #[cfg(unix)]
        assert!(
            !ipc.exists(),
            "TLS preparation fails before creating a listener"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn quota_control_startup_failure_finishes_the_original_writer_before_retry() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("zero.json");
    let quota = directory.path().join("quota.json");
    std::fs::write(
        &config,
        serde_json::json!({
            "runtime": {"principal_quota_state_path": quota},
            "route": {"final": {"type": "direct"}}
        })
        .to_string(),
    )
    .unwrap();
    // The actual IPC startup fails through an existing regular-file parent;
    // this reaches production application cleanup after Host adopted quota.
    let unavailable = config.join("control.sock");
    for _ in 0..2 {
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            super::run_with_logging(
                config.to_str().unwrap(),
                None,
                Some(unavailable.to_str().unwrap()),
                None,
                false,
                None,
            ),
        )
        .await
        .unwrap();
        let error = result.unwrap_err().to_string();
        assert!(
            !error.contains("already owned by another Zero process"),
            "the preceding original quota writer must retire: {error}"
        );
        assert!(
            !error.contains("quota final retirement"),
            "actual final quota cleanup must complete: {error}"
        );
        let frame: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&quota).unwrap()).unwrap();
        assert_eq!(frame["balances"], serde_json::json!([]));
    }
}
