//! Exercise the production completion boundary without a replacement protocol
//! executor. Success cases complete real loopback TCP connects, not Block probes.
use super::{finish_candidate_attempt, TcpAttemptFacts};
use crate::protocol_registry::TcpExecutionServices;
use crate::runtime::tcp_dispatch::TcpDispatchIntent;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure};
use zero_engine::EngineError;
const HEALTH_TAG: &str = "health-isolation-test";
mod diagnostics;
mod health_scope;
mod local_failures;
fn test_services() -> TcpExecutionServices {
    crate::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
    .tcp_runtime_services()
    .execution()
}
fn attempt_facts() -> TcpAttemptFacts {
    TcpAttemptFacts {
        health_tag: Some(HEALTH_TAG.into()),
        tag: Some(HEALTH_TAG.into()),
        protocol: "test".into(),
        endpoint: None,
    }
}
fn complete_error(
    services: &TcpExecutionServices,
    intent: TcpDispatchIntent,
    error: EngineError,
) -> TcpOutboundFailure {
    finish_candidate_attempt(
        services,
        attempt_facts(),
        intent,
        &Default::default(),
        Err(TcpOutboundFailure {
            stage: "test_connect",
            error,
            upstream_endpoint: None,
            network: None,
        }),
    )
    .err()
    .expect("typed failure is retained")
}
async fn complete_wire(
    services: &TcpExecutionServices,
    intent: TcpDispatchIntent,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let original = tokio::net::TcpStream::connect(address).await.unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    let result = finish_candidate_attempt(
        services,
        attempt_facts(),
        intent,
        &Default::default(),
        Ok(EstablishedTcpOutbound::proxied(
            HEALTH_TAG,
            "127.0.0.1",
            address.port(),
            Box::pin(original),
        )),
    );
    drop(peer);
    result
}
fn quarantine(services: &TcpExecutionServices) {
    for _ in 0..5 {
        services.record_outbound_failure(HEALTH_TAG);
    }
    services
        .engine()
        .check_outbound_health(HEALTH_TAG)
        .expect_err("initial cooldown");
}
#[tokio::test]
async fn non_traffic_failure_completions_leave_shared_health_unchanged() {
    for intent in [
        TcpDispatchIntent::DiagnosticProbe,
        TcpDispatchIntent::PolicyProbe,
        TcpDispatchIntent::DnsDetour,
    ] {
        let services = test_services();
        for _ in 0..6 {
            let failure = complete_error(
                &services,
                intent,
                std::io::Error::other("controlled completion failure").into(),
            );
            assert_eq!(failure.stage, "test_connect");
        }
        services
            .engine()
            .check_outbound_health(HEALTH_TAG)
            .expect("non-traffic failures do not quarantine business traffic");
    }
}
#[tokio::test]
async fn traffic_failure_completions_update_shared_health() {
    let services = test_services();
    for _ in 0..5 {
        complete_error(
            &services,
            TcpDispatchIntent::Traffic,
            std::io::Error::other("controlled completion failure").into(),
        );
    }
    assert_eq!(
        services
            .engine()
            .check_outbound_health(HEALTH_TAG)
            .unwrap_err()
            .code(),
        "unhealthy_outbound"
    );
}
#[tokio::test]
async fn real_diagnostic_and_dns_connect_completions_preserve_existing_cooldown() {
    for intent in [
        TcpDispatchIntent::DiagnosticProbe,
        TcpDispatchIntent::DnsDetour,
    ] {
        let services = test_services();
        quarantine(&services);
        assert!(complete_wire(&services, intent).await.is_ok());
        assert_eq!(
            services
                .engine()
                .check_outbound_health(HEALTH_TAG)
                .unwrap_err()
                .code(),
            "unhealthy_outbound"
        );
    }
}
