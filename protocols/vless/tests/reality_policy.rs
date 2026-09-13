#![cfg(any(feature = "runtime", feature = "validation"))]
use vless::reality_policy::{version, PolicyRef, ServerPolicy};

#[test]
fn authenticated_hello_policy_checks_exact_names_versions_and_millisecond_clock_skew() {
    let names = ["one.test".into(), "two.test".into()];
    let policy = ServerPolicy::new(
        PolicyRef {
            server_names: &names,
            min_client_version: Some("26.3.26"),
            max_client_version: Some("26.3.28"),
            max_time_diff_ms: 1500,
        },
        None,
    )
    .unwrap();
    let mut session = [0; 16];
    session[..3].copy_from_slice(&[26, 3, 27]);
    session[4..8].copy_from_slice(&100u32.to_be_bytes());
    for now in [98_500, 100_000, 101_500] {
        for name in &names {
            assert!(policy.accepts(Some(name), &session, now));
        }
    }
    for now in [98_499, 101_501] {
        assert!(!policy.accepts(Some(&names[0]), &session, now));
    }
    assert!(!policy.accepts(None, &session, 100_000));
    assert!(!policy.accepts(Some("ONE.test"), &session, 100_000));
    for patch in [25, 29] {
        session[2] = patch;
        assert!(!policy.accepts(Some(&names[0]), &session, 100_000));
    }
    let no_time_limit = ServerPolicy::new(PolicyRef::default(), Some("one.test")).unwrap();
    assert!(no_time_limit.accepts(Some("one.test"), &session, u128::MAX));
    assert!(!no_time_limit.accepts(Some("elsewhere.test"), &session, 100_000));
}

#[test]
fn invalid_version_bounds_fail_during_preparation() {
    for value in ["", "26..3", "26.3.27.0", "256", "-1", "1.a"] {
        assert!(version(value).is_err(), "{value}");
    }
    assert_eq!(version("26").unwrap(), [26, 0, 0]);
    assert_eq!(version("26.3").unwrap(), [26, 3, 0]);
    assert!(ServerPolicy::new(
        PolicyRef {
            min_client_version: Some("27"),
            max_client_version: Some("26"),
            ..Default::default()
        },
        None
    )
    .is_err());
    assert!(ServerPolicy::new(Default::default(), Some("*.example")).is_err());
}
