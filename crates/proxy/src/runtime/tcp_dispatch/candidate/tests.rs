use std::future::Future;
use std::pin::Pin;

use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::EngineError;

use super::dispatch_prepared_tcp_candidate;
use crate::inventory::{PreparedTcpCandidate, PreparedTcpCandidateExecution};
use crate::protocol_registry::TcpRuntimeServices;
use crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation;
use crate::runtime::tcp_dispatch::TcpDispatchIntent;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure};

const HEALTH_TAG: &str = "health-isolation-test";

struct FailingConnectOperation;

impl PreparedTcpConnectOperation for FailingConnectOperation {
    fn execute<'a>(
        self: Box<Self>,
        _services: TcpRuntimeServices,
        _session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async {
            Err(TcpOutboundFailure {
                stage: "test_connect",
                error: EngineError::Io(std::io::Error::other("controlled connect failure")),
                upstream_endpoint: None,
                network: None,
            })
        })
    }
}

#[tokio::test]
async fn repeated_diagnostic_probe_failures_do_not_quarantine_traffic() {
    let services = test_services();
    let session = test_session();

    for _ in 0..5 {
        let failure = match dispatch_prepared_tcp_candidate(
            services.clone(),
            &session,
            failing_candidate(),
            TcpDispatchIntent::DiagnosticProbe,
        )
        .await
        {
            Ok(_) => panic!("controlled diagnostic probe must fail"),
            Err(failure) => failure,
        };
        assert_eq!(failure.stage, "test_connect");
    }

    services
        .check_outbound_health(HEALTH_TAG)
        .expect("diagnostic failures must not quarantine real traffic");
}

#[tokio::test]
async fn diagnostic_probe_bypasses_quarantine_without_clearing_it() {
    let services = test_services();
    let session = test_session();
    quarantine(&services);

    if let Err(failure) = dispatch_prepared_tcp_candidate(
        services.clone(),
        &session,
        successful_candidate(),
        TcpDispatchIntent::DiagnosticProbe,
    )
    .await
    {
        panic!(
            "diagnostic probe must bypass the shared quarantine: {}",
            failure.error
        );
    }

    let error = services
        .check_outbound_health(HEALTH_TAG)
        .expect_err("diagnostic success must not clear shared traffic health");
    assert_eq!(error.code(), "unhealthy_outbound");
}

#[tokio::test]
async fn traffic_failures_still_update_shared_outbound_health() {
    let services = test_services();
    let session = test_session();

    for _ in 0..5 {
        if dispatch_prepared_tcp_candidate(
            services.clone(),
            &session,
            failing_candidate(),
            TcpDispatchIntent::Traffic,
        )
        .await
        .is_ok()
        {
            panic!("controlled traffic connection must fail");
        }
    }

    let error = services
        .check_outbound_health(HEALTH_TAG)
        .expect_err("five traffic failures must quarantine the outbound");
    assert_eq!(error.code(), "unhealthy_outbound");
}

#[tokio::test]
async fn policy_probe_failures_do_not_mutate_shared_outbound_health() {
    let services = test_services();
    let session = test_session();

    for _ in 0..5 {
        if dispatch_prepared_tcp_candidate(
            services.clone(),
            &session,
            failing_candidate(),
            TcpDispatchIntent::PolicyProbe,
        )
        .await
        .is_ok()
        {
            panic!("controlled policy probe must fail");
        }
    }

    services
        .check_outbound_health(HEALTH_TAG)
        .expect("policy probe results must be applied only through policy state");
}

#[tokio::test]
async fn policy_probe_still_respects_the_shared_traffic_quarantine() {
    let services = test_services();
    let session = test_session();
    quarantine(&services);

    let failure = match dispatch_prepared_tcp_candidate(
        services,
        &session,
        successful_candidate(),
        TcpDispatchIntent::PolicyProbe,
    )
    .await
    {
        Ok(_) => panic!("policy probe must preserve existing quarantine semantics"),
        Err(failure) => failure,
    };
    assert_eq!(failure.stage, "health_check");
    assert_eq!(failure.error.code(), "unhealthy_outbound");
}

fn test_services() -> TcpRuntimeServices {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .expect("parse test config");
    crate::Proxy::new(config)
        .expect("build test proxy")
        .tcp_runtime_services()
}

fn test_session() -> Session {
    Session::new(
        0,
        Address::Domain("probe.example".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    )
}

fn failing_candidate() -> PreparedTcpCandidate<'static> {
    PreparedTcpCandidate {
        health_tag: Some(HEALTH_TAG.to_owned()),
        tag: Some(HEALTH_TAG.to_owned()),
        protocol: "test".to_owned(),
        endpoint: None,
        execution: PreparedTcpCandidateExecution::Connect(Box::new(FailingConnectOperation)),
    }
}

fn successful_candidate() -> PreparedTcpCandidate<'static> {
    PreparedTcpCandidate {
        health_tag: Some(HEALTH_TAG.to_owned()),
        tag: Some(HEALTH_TAG.to_owned()),
        protocol: "test".to_owned(),
        endpoint: None,
        execution: PreparedTcpCandidateExecution::Block {
            tag: HEALTH_TAG.to_owned(),
        },
    }
}

fn quarantine(services: &TcpRuntimeServices) {
    for _ in 0..5 {
        services.record_outbound_failure(HEALTH_TAG);
    }
    services
        .check_outbound_health(HEALTH_TAG)
        .expect_err("test setup must quarantine the outbound");
}
