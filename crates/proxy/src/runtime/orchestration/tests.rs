use zero_engine::EngineError;

use super::lifecycle::{handle_listener_result, handle_urltest_result};

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
