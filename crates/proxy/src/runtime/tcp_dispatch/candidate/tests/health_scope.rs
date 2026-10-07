use super::*;

#[tokio::test]
async fn traffic_attempts_a_cooling_candidate_and_recovers_on_success() {
    let services = test_services();
    quarantine(&services);

    let result = dispatch_prepared_tcp_candidate(
        services.clone(),
        &test_session(),
        successful_candidate(),
        TcpDispatchIntent::Traffic,
    )
    .await;

    assert!(
        result.is_ok(),
        "fixed traffic must actually execute the candidate"
    );
    services
        .engine()
        .check_outbound_health(HEALTH_TAG)
        .expect("real traffic success restores URLTest eligibility immediately");
}

#[tokio::test]
async fn cooling_candidates_return_real_connection_failures_to_traffic() {
    let services = test_services();
    quarantine(&services);

    let failure = dispatch_prepared_tcp_candidate(
        services,
        &test_session(),
        failing_candidate(),
        TcpDispatchIntent::Traffic,
    )
    .await
    .err()
    .expect("controlled connection failure");

    assert_eq!(failure.stage, "test_connect");
    assert!(failure
        .error
        .to_string()
        .contains("controlled connect failure"));
}

#[tokio::test]
async fn policy_probes_execute_during_candidate_cooldown_without_recording_connect_success() {
    let services = test_services();
    quarantine(&services);

    let result = dispatch_prepared_tcp_candidate(
        services.clone(),
        &test_session(),
        successful_candidate(),
        TcpDispatchIntent::PolicyProbe,
    )
    .await;

    assert!(result.is_ok(), "URLTest must actually probe the candidate");
    services
        .engine()
        .check_outbound_health(HEALTH_TAG)
        .expect_err("connection establishment alone is not a completed policy probe");
}
