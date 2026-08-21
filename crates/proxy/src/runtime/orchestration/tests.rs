use zero_engine::EngineError;

use super::lifecycle::{
    handle_configured_tun_failure, handle_listener_result, handle_urltest_result,
    has_runtime_inbound,
};

#[test]
fn unexpected_clean_listener_exit_is_fatal() {
    let mut expected_exits = 0;
    let result = handle_listener_result(Some(Ok(Ok(()))), false, &mut expected_exits);

    assert!(matches!(result, Err(EngineError::InboundTaskExited)));
}

#[test]
fn expected_listener_exit_is_consumed_during_reconciliation() {
    let mut expected_exits = 1;
    let result = handle_listener_result(Some(Ok(Ok(()))), false, &mut expected_exits);

    assert!(result.is_ok());
    assert_eq!(expected_exits, 0);
}

#[test]
fn listener_error_is_preserved_during_reconciliation() {
    let mut expected_exits = 1;
    let result = handle_listener_result(
        Some(Ok(Err(EngineError::NoInbounds))),
        false,
        &mut expected_exits,
    );

    assert!(matches!(result, Err(EngineError::NoInbounds)));
    assert_eq!(expected_exits, 1);
}

#[test]
fn clean_listener_exit_is_allowed_during_shutdown() {
    let mut expected_exits = 0;
    let result = handle_listener_result(Some(Ok(Ok(()))), true, &mut expected_exits);

    assert!(result.is_ok());
}

#[test]
fn unexpected_clean_urltest_exit_is_fatal() {
    let result = handle_urltest_result(Some(Ok(Ok(()))), false);

    assert!(matches!(result, Err(EngineError::UrlTestTaskExited)));
}

#[test]
fn clean_urltest_exit_is_allowed_during_shutdown() {
    let result = handle_urltest_result(Some(Ok(Ok(()))), true);

    assert!(result.is_ok());
}

#[test]
fn declarative_tun_counts_as_a_runtime_inbound() {
    let config = zero_config::RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": { "global": { "type": "udp", "host": "1.1.1.1" } },
                    "default_server": "global"
                },
                "tun": { "addr": "10.0.0.1/24" }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("valid TUN-only runtime config");

    assert!(config.inbounds.is_empty());
    assert!(has_runtime_inbound(&config));
}

#[test]
fn configured_tun_failure_is_fatal_to_orchestration() {
    let error = handle_configured_tun_failure(Ok("device read failed".to_owned()))
        .expect_err("configured TUN failure must fail the runtime");

    assert!(error.to_string().contains("configured TUN runtime failed"));
}
