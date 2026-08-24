use std::io;

use windows_sys::Win32::Networking::WinSock::{
    IpDadStateDeprecated, IpDadStateDuplicate, IpDadStateInvalid, IpDadStatePreferred,
    IpDadStateTentative,
};

use super::{dad_state_is_ready, validate_elevation, WINDOWS_TUN_ELEVATION_MESSAGE};

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

#[test]
fn tentative_address_waits_until_windows_marks_it_preferred() {
    let address = "10.67.0.1".parse().unwrap();

    assert!(!dad_state_is_ready(address, IpDadStateTentative).unwrap());
    assert!(dad_state_is_ready(address, IpDadStatePreferred).unwrap());
}

#[test]
fn unusable_address_states_fail_with_stable_io_kinds() {
    let address = "10.67.0.1".parse().unwrap();

    assert_eq!(
        dad_state_is_ready(address, IpDadStateDuplicate)
            .unwrap_err()
            .kind(),
        io::ErrorKind::AddrInUse
    );
    for state in [IpDadStateInvalid, IpDadStateDeprecated] {
        assert_eq!(
            dad_state_is_ready(address, state).unwrap_err().kind(),
            io::ErrorKind::AddrNotAvailable
        );
    }
}
