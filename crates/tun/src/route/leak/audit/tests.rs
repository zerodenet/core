use super::*;

#[test]
fn unchanged_complete_rules_do_not_write() {
    assert_eq!(
        repair_policy(
            "accept tun; reject protected",
            Some("accept tun; reject protected"),
            false,
            || panic!("intact policy must not be rewritten")
        )
        .unwrap(),
        None
    );
}

#[test]
fn unchanged_configuration_repairs_missing_container_chain_or_rule() {
    let expected = "output hook priority -200; accept tun; reject protected";
    for observed in [
        None,
        Some("table only"),
        Some("output hook priority -200; accept tun"),
        Some("output hook priority 0; accept tun; reject protected"),
        Some("output hook priority -200; accept all; reject protected"),
    ] {
        assert_eq!(
            repair_policy(expected, observed, false, || Ok(expected.to_owned())).unwrap(),
            Some(expected.to_owned())
        );
    }
}

#[test]
fn failed_repair_and_incomplete_readback_are_not_success() {
    let installed = "accept tun; reject protected";
    assert!(
        repair_policy(installed, None, false, || Err(io::Error::other(
            "transaction rejected"
        )))
        .is_err()
    );
    assert!(repair_policy(installed, Some("accept tun"), false, || Ok(
        "accept tun".to_owned()
    ))
    .is_err());
    // The original baseline remains usable for the next retry.
    assert!(repair_policy(installed, Some("accept tun"), false, || Ok(
        installed.to_owned()
    ))
    .unwrap()
    .is_some());
}

#[test]
fn changed_policy_commits_a_new_snapshot_only_after_readback() {
    let updated = "accept tun; accept bootstrap; reject protected";
    assert_eq!(
        repair_policy("old policy", Some("old policy"), true, || Ok(
            updated.to_owned()
        ))
        .unwrap(),
        Some(updated.to_owned())
    );
}

#[test]
fn empty_or_invalid_readbacks_are_rejected() {
    assert!(snapshot(b" \n").is_err());
    assert!(snapshot(&[0xff]).is_err());
    assert_eq!(snapshot(b"  block out\n").unwrap(), "block out");
}

#[test]
fn repair_failure_reports_first_difference_without_weakening_comparison() {
    let expected = "pass out quick all\nblock drop out to 192.0.2.0/24";
    let observed = "pass out quick all\nblock drop out to 198.51.100.0/24";
    let error = repair_policy(expected, None, false, || Ok(observed.to_owned()))
        .unwrap_err()
        .to_string();
    assert!(error.contains("line 2"));
    assert!(error.contains("192.0.2.0/24"));
    assert!(error.contains("198.51.100.0/24"));
    assert!(!error.contains("pass out"));
}

#[test]
fn repair_diagnostics_bound_long_lines_and_distinguish_missing_rules() {
    let expected = format!("{}SENSITIVE_TAIL", "x".repeat(200));
    let error = repair_policy(&expected, None, false, || Ok("y".repeat(500)))
        .unwrap_err()
        .to_string();
    assert!(error.len() < 450);
    assert!(!error.contains("SENSITIVE_TAIL"));
    let error = repair_policy("pass\nblock", None, false, || Ok("pass".to_owned()))
        .unwrap_err()
        .to_string();
    assert!(error.contains("line 2"));
    assert!(error.contains("observed None"));
}
