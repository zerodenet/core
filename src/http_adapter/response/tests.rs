use zero_api::{ApiError, ApiErrorCode};

use super::api_error_status;

#[test]
fn insufficient_os_privilege_is_forbidden_over_http() {
    let error = ApiError::new(
        ApiErrorCode::InsufficientOsPrivilege,
        "TUN startup requires elevated host operating-system network privileges",
    );

    assert_eq!(api_error_status(&error), "HTTP/1.1 403 Forbidden\r\n");
}
