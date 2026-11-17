use super::*;
#[tokio::test]
async fn real_traffic_connect_completion_recovers_shared_health() {
    let services = test_services();
    quarantine(&services);
    assert!(complete_wire(&services, TcpDispatchIntent::Traffic)
        .await
        .is_ok());
    services
        .engine()
        .check_outbound_health(HEALTH_TAG)
        .expect("real traffic establishment restores eligibility");
}
#[tokio::test]
async fn cooling_candidates_retain_the_original_typed_failure_completion() {
    let services = test_services();
    quarantine(&services);
    let failure = complete_error(
        &services,
        TcpDispatchIntent::Traffic,
        std::io::Error::other("controlled connect failure").into(),
    );
    assert_eq!(failure.stage, "test_connect");
    assert!(failure
        .error
        .to_string()
        .contains("controlled connect failure"));
}
#[tokio::test]
async fn real_policy_connect_completion_is_not_a_completed_policy_probe() {
    let services = test_services();
    quarantine(&services);
    assert!(complete_wire(&services, TcpDispatchIntent::PolicyProbe)
        .await
        .is_ok());
    services
        .engine()
        .check_outbound_health(HEALTH_TAG)
        .expect_err("probe response, not connection establishment, controls policy recovery");
}
