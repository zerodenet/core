use std::io;

use zero_api::ApiErrorCode;
use zero_engine::EngineError;

use super::tun::{map_tun_start_error, TUN_PRIVILEGE_MESSAGE};

#[test]
fn tun_start_maps_host_permission_failures_to_a_stable_api_error() {
    let error = map_tun_start_error(EngineError::Io(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "platform-specific permission diagnostic",
    )));

    assert_eq!(error.code, ApiErrorCode::InsufficientOsPrivilege);
    assert_eq!(error.message, TUN_PRIVILEGE_MESSAGE);
    assert_eq!(
        error.cause.as_deref(),
        Some("platform-specific permission diagnostic")
    );
}

#[test]
fn tun_start_keeps_unclassified_runtime_failures_internal() {
    let error = map_tun_start_error(EngineError::Io(io::Error::other("device failed")));

    assert_eq!(error.code, ApiErrorCode::Internal);
    assert_eq!(error.message, "device failed");
}
