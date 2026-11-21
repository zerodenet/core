#[path = "relay/fixtures.rs"]
pub(crate) mod fixtures;
use crate::{
    inventory::PreparedTcpOutbound,
    runtime::tcp_dispatch::{dispatch_tcp_outbound, TcpDispatchIntent},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{timeout, Duration},
};
use zero_core::{component::SessionId, Address};
use zero_engine::{RouteDecision, RouteMode};
use zero_platform_tokio::contract::{DeliveryState, Reason};

#[derive(Debug, PartialEq)]
struct Evidence {
    tag: String,
    chain: Vec<(String, String)>,
    session_bytes: (u64, u64),
    outer: Vec<(u64, u64)>,
}

fn admitted_services(
    proxy: &crate::Proxy,
    runtime: &crate::runtime::udp_ingress::UdpIngressRuntime,
    session: &mut zero_core::Session,
) -> crate::protocol_registry::TcpRuntimeServices {
    let caller = runtime.accepted_caller().unwrap().clone();
    let admission = proxy
        .engine()
        .prepare_session_admitted(session, "incoming")
        .unwrap();
    let demand = crate::component::business_demand::AdmittedBusinessDemand::new(
        proxy.engine(),
        SessionId(session.id),
        admission,
    )
    .unwrap();
    proxy
        .tcp_runtime_services()
        .for_inbound("incoming".into())
        .with_inbound_instance(caller.source_instance().unwrap())
        .with_admitted_business_identity(session, demand)
        .unwrap()
        .with_accepted_caller(Some(caller))
        .unwrap()
}
async fn probe<T, F, Fut>(proxy: &crate::Proxy, work: F) -> T
where
    T: Send + 'static,
    F: FnOnce(crate::protocol_registry::TcpRuntimeServices) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T> + Send + 'static,
{
    let actual = proxy.clone();
    let config = proxy.engine().config();
    let host = proxy.protocols.runtime_host();
    let operation = host.run_probe(
        &config,
        None,
        zero_runtime::reserve_probe().await.unwrap(),
        zero_core::request::ServicePurpose::DiagnosticProbe,
        tokio::time::Instant::now() + Duration::from_secs(5),
        move |call| async move {
            let services = actual.tcp_runtime_services().with_service_invocation(
                crate::component::direct_bridge::ServiceInvocation {
                    deadline: call.deadline(),
                    purpose: zero_core::request::ServicePurpose::DiagnosticProbe,
                    caller: Some(call),
                },
            );
            work(services).await
        },
    );
    proxy
        .protocols
        .drive_component_supervision(operation)
        .await
        .unwrap()
}
async fn exercise(count: usize, native: bool) -> Evidence {
    let (proxy, mut session, _, listeners) = fixtures::setup(count).await;
    let first_port = listeners[0].local_addr().unwrap().port();
    let result = if native {
        crate::runtime::udp_ingress::test_source::run(&proxy, move |proxy, runtime| async move {
            let services = admitted_services(&proxy, &runtime, &mut session);
            exercise_io(
                proxy,
                session,
                services,
                listeners,
                count,
                first_port,
                TcpDispatchIntent::Traffic,
            )
            .await
        })
        .await
    } else {
        let actual = proxy.clone();
        probe(&proxy, move |services| async move {
            assert_eq!(
                session.id, 0,
                "Service never admits a fabricated business session"
            );
            exercise_io(
                actual,
                session,
                services,
                listeners,
                count,
                first_port,
                TcpDispatchIntent::DiagnosticProbe,
            )
            .await
        })
        .await
    };
    proxy
        .protocols
        .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    result
}
async fn exercise_io(
    proxy: crate::Proxy,
    session: zero_core::Session,
    services: crate::protocol_registry::TcpRuntimeServices,
    listeners: Vec<tokio::net::TcpListener>,
    count: usize,
    first_port: u16,
    intent: TcpDispatchIntent,
) -> Evidence {
    let mut servers = fixtures::servers(listeners);
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("relay".into()))
        .unwrap();
    assert!(matches!(
        services
            .prepare_tcp_outbound(&resolved, RouteMode::Flow)
            .unwrap_or_else(|failure| panic!("{}", failure.error)),
        PreparedTcpOutbound::NativeRelay(_)
    ));
    let established = dispatch_tcp_outbound(services, &session, resolved, RouteMode::Flow, intent)
        .await
        .unwrap_or_else(|e| panic!("{}: {}", e.stage, e.error));
    let mut route = crate::transport::extract_tcp_stream(established).unwrap();
    assert_eq!(
        route.upstream_endpoint,
        Some(("127.0.0.1".into(), first_port))
    );
    assert!(!route.is_direct);
    let active = proxy.engine().active_sessions();
    assert!(active
        .iter()
        .all(|session| (session.outbound_tx_bytes, session.outbound_rx_bytes) == (0, 0)));
    let mut pre = [0; 3];
    route.upstream.read_exact(&mut pre).await.unwrap();
    assert_eq!(&pre, b"pre");
    route.upstream.write_all(b"request").await.unwrap();
    route.upstream.shutdown().await.unwrap();
    let mut tail = Vec::new();
    route.upstream.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, b"tail");
    while let Some(server) = servers.join_next().await {
        server.unwrap();
    }
    let active = proxy.engine().active_sessions();
    let outer = (0..count)
        .map(|index| {
            let snapshot = proxy
                .engine()
                .traffic_snapshot(&zero_api::TrafficGetQuery {
                    scope: zero_api::TrafficScope::Outbound {
                        tag: format!("node-{index}"),
                    },
                })
                .unwrap();
            let counters = &snapshot.planes[2].counters;
            let remaining = (count - index) as u64;
            let bytes = (counters.tx_bytes.unwrap(), counters.rx_bytes.unwrap());
            assert_eq!(
                bytes,
                (remaining * 13 + (remaining - 1) * 6 + 7, remaining * 12 + 7)
            );
            assert_eq!(counters.tx_packets, None);
            bytes
        })
        .collect();
    let evidence = Evidence {
        tag: route.outbound_tag.clone(),
        chain: route.relay_chain.clone(),
        session_bytes: active
            .iter()
            .find(|active| active.id == session.id)
            .map_or((0, 0), |active| {
                (active.outbound_tx_bytes, active.outbound_rx_bytes)
            }),
        outer,
    };
    drop(route);
    if session.id != 0 {
        proxy
            .engine()
            .track_session(session.id)
            .finish(zero_engine::SessionOutcome::DirectRelayed)
            .unwrap();
    }
    evidence
}

#[tokio::test]
async fn native_two_and_three_hop_streams_preserve_wire_payload_halfclose_metadata_and_accounting()
{
    timeout(Duration::from_secs(5), async {
        for count in [2, 3] {
            let old = exercise(count, false).await;
            let native = exercise(count, true).await;
            assert_eq!(native, old);
            assert_eq!(native.tag, format!("node-{}", count - 1));
            assert_eq!(native.chain.len(), count);
        }
    })
    .await
    .expect("all real relay carriers must complete");
}

#[tokio::test]
async fn revoked_middle_native_role_fails_before_any_carrier_and_retains_business_identity() {
    let (proxy, mut session, services, listeners) = fixtures::setup(3).await;
    let slot = services
        .execution()
        .components
        .unwrap()
        .configured("node-1")
        .unwrap();
    slot.revoke();
    crate::runtime::udp_ingress::test_source::run(&proxy, move |proxy, runtime| async move {
        let services = admitted_services(&proxy, &runtime, &mut session);
        let (resolved, _plan) = proxy
            .engine()
            .resolve_route_decision(RouteDecision::Route("relay".into()))
            .unwrap();
        let failure = match dispatch_tcp_outbound(
            services,
            &session,
            resolved,
            RouteMode::Flow,
            TcpDispatchIntent::Traffic,
        )
        .await
        {
            Ok(_) => panic!("revoked middle role opened a carrier"),
            Err(error) => error,
        };
        assert_eq!(failure.stage, "relay_last");
        let channel = crate::transport::channel_failure(&failure.error).unwrap();
        assert_eq!(channel.component, Some(slot.id));
        assert_eq!(channel.session, Some(SessionId(session.id)));
        assert_eq!(channel.delivery, DeliveryState::NotSubmitted);
        assert_eq!(
            crate::transport::failure_origin(&failure.error),
            Some(crate::transport::TransportFailureOrigin::LocalNetwork)
        );
        for listener in listeners {
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(listener.poll_accept(&mut cx).is_pending());
        }
        proxy
            .engine()
            .track_session(session.id)
            .finish(zero_engine::SessionOutcome::Failed)
            .unwrap();
    })
    .await;
    proxy
        .protocols
        .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}

async fn reject_after_prefix(native: bool) -> (String, String, &'static str) {
    let (proxy, mut session, native_services, mut listeners) = fixtures::setup(2).await;
    let first = listeners.remove(0);
    let last = listeners.remove(0);
    let port = last.local_addr().unwrap().port();
    let middle_id = native_services
        .execution()
        .components
        .unwrap()
        .configured("node-1")
        .unwrap()
        .id;
    let forward = tokio::spawn(async move {
        let (mut io, _) = first.accept().await.unwrap();
        assert_eq!(
            fixtures::request(&mut io).await,
            (Address::Domain("127.0.0.1".into()), port)
        );
        let mut next = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        fixtures::accept(&mut io).await;
        let _ = tokio::io::copy_bidirectional(&mut io, &mut next).await;
    });
    let reject = tokio::spawn(async move {
        let (mut io, _) = last.accept().await.unwrap();
        fixtures::greeting(&mut io).await;
        io.write_all(&[5, 255]).await.unwrap();
        // Reject authentication before receiving any CONNECT or business bytes.
        assert_eq!(
            io.read_u8().await.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    });
    let failure = if native {
        let actual = proxy.clone();
        crate::runtime::udp_ingress::test_source::run(&proxy, move |proxy, runtime| async move {
            let services = admitted_services(&proxy, &runtime, &mut session);
            let (resolved, _) = actual
                .engine()
                .resolve_route_decision(RouteDecision::Route("relay".into()))
                .unwrap();
            let result = dispatch_tcp_outbound(
                services,
                &session,
                resolved,
                RouteMode::Flow,
                TcpDispatchIntent::Traffic,
            )
            .await;
            let error = match result {
                Ok(_) => panic!("peer rejection accepted"),
                Err(error) => error,
            };
            let id = session.id;
            proxy
                .engine()
                .track_session(id)
                .finish(zero_engine::SessionOutcome::Failed)
                .unwrap();
            (error, id)
        })
        .await
    } else {
        let actual = proxy.clone();
        probe(&proxy, move |services| async move {
            let (resolved, _) = actual
                .engine()
                .resolve_route_decision(RouteDecision::Route("relay".into()))
                .unwrap();
            let result = dispatch_tcp_outbound(
                services,
                &session,
                resolved,
                RouteMode::Flow,
                TcpDispatchIntent::DiagnosticProbe,
            )
            .await;
            (
                match result {
                    Ok(_) => panic!("peer rejection accepted"),
                    Err(error) => error,
                },
                0,
            )
        })
        .await
    };
    let (failure, session_id) = failure;
    if native {
        let channel =
            crate::transport::channel_failure(&failure.error).expect("channel facts retained");
        assert_eq!(channel.session, Some(SessionId(session_id)));
        assert_eq!(channel.component, Some(middle_id));
        assert_eq!(channel.reason, Reason::Protocol);
        assert_eq!(channel.delivery, DeliveryState::Unknown);
        assert!(!channel.retry_permitted());
    }
    assert!(failure.upstream_endpoint.is_none());
    reject.await.unwrap();
    forward.await.unwrap();
    proxy
        .protocols
        .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    let message = failure.error.to_string();
    let endpoint = format!("upstream_port={port}");
    assert!(
        message.contains(&endpoint),
        "actual failed endpoint retained: {message}"
    );
    (
        failure.error.code().to_owned(),
        message.replace(&endpoint, "upstream_port=<actual-port>"),
        failure.stage,
    )
}

#[tokio::test]
async fn rejection_after_a_real_prefix_preserves_v1_failure_and_actual_identity_without_replay() {
    timeout(Duration::from_secs(5), async {
        let old = reject_after_prefix(false).await;
        let native = reject_after_prefix(true).await;
        assert_eq!(native, old);
    })
    .await
    .expect("prefix rejection must terminate both actual sockets");
}
