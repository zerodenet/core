use std::io;

use super::{validate_elevation, WINDOWS_TUN_ELEVATION_MESSAGE};

#[test]
fn non_elevated_process_has_stable_permission_error() {
    let error = validate_elevation(false).expect_err("non-elevated TUN must fail");

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), WINDOWS_TUN_ELEVATION_MESSAGE);
}

#[test]
fn elevated_process_passes_the_privilege_preflight() {
    validate_elevation(true).expect("elevated TUN preflight should pass");
}
