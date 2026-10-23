use crate::{
    runtime::tcp_dispatch::{dispatch_tcp_outbound, TcpDispatchIntent},
    Proxy,
};
use std::sync::Arc;
use tokio::{
    net::TcpListener,
    time::{Duration, Instant},
};
use zero_core::{component::SessionId, Address, Network, ProtocolType, Session, SessionAuth};
use zero_platform_tokio::contract::context::RequestIdentity;

fn proxy() -> Proxy {
    Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn ingress_owner_identity_survives_clones_and_snapshot_refresh() {
    let proxy = proxy();
    let owner = proxy.tcp_runtime_services().for_inbound("incoming".into());
    let id = owner.execution().ingress.unwrap().role;
    let same = owner
        .clone()
        .for_inbound("incoming".into())
        .with_current_snapshot();
    assert_eq!(same.execution().ingress.unwrap().role, id);
    let other = owner.for_inbound("another".into());
    assert_ne!(
        other.execution().ingress.unwrap().role.component,
        id.component
    );
}

#[tokio::test]
async fn business_projection_preserves_assigned_identity_auth_and_available_facts() {
    let proxy = proxy();
    let services = proxy.tcp_runtime_services().for_inbound("incoming".into());
    let mut session = Session::new(
        0,
        Address::Domain("target.example".into()),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    session.source_ip = Some(Address::Ipv4([198, 51, 100, 7]));
    session.source_port = Some(12345);
    session.inbound_local = Some((Address::Ipv4([127, 0, 0, 1]), 1234));
    let mut auth = SessionAuth::new("test");
    auth.principal_key = Some("opaque-principal".into());
    session.auth = Some(auth);
    session.inbound_peer_identity = Some(Arc::from("authenticated-peer"));
    session.process_id = Some(71);
    let pending = services.clone().with_business_identity(&session).unwrap();
    assert!(
        pending.execution().invocation.is_none(),
        "unallocated Session0 cannot become business facts"
    );
    proxy
        .engine()
        .prepare_session(&mut session, "incoming")
        .unwrap();
    let role = services.execution().ingress.unwrap().role;
    let projected = services
        .with_business_identity(&session)
        .unwrap()
        .execution()
        .invocation
        .unwrap();
    assert!(projected.deadline.is_none());
    let RequestIdentity::Business(facts) = projected.identity else {
        panic!("business identity required")
    };
    assert_eq!(facts.session, SessionId(session.id));
    assert_ne!(facts.session.0, 0);
    assert_eq!(facts.inbound, role.role);
    assert_eq!(facts.subject.as_ref().unwrap().owner, role.component);
    assert_eq!(
        facts.peer.as_ref().unwrap().0.as_ref(),
        "authenticated-peer"
    );
    assert_eq!(
        facts.effective_source.unwrap(),
        "198.51.100.7:12345".parse().unwrap()
    );
    assert!(facts.original_source.is_none());
    assert_eq!(
        facts.local_endpoint.unwrap(),
        "127.0.0.1:1234".parse().unwrap()
    );
    assert_eq!(facts.process.as_ref().unwrap().id, Some(71));
}

#[tokio::test]
async fn business_direct_uses_its_registered_role_and_cannot_fall_back_after_revocation() {
    let proxy = proxy();
    let config = proxy.engine().config();
    proxy
        .protocols
        .start_components(config.clone(), Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let services = proxy.tcp_runtime_services().for_inbound("incoming".into());
    let mut session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        listener.local_addr().unwrap().port(),
        Network::Tcp,
        ProtocolType::new("direct"),
    );
    proxy
        .engine()
        .prepare_session(&mut session, "incoming")
        .unwrap();
    let plan = services.execution().components.unwrap();
    let slot = plan.direct(None).unwrap();
    proxy
        .protocols
        .drive_component_supervision(slot.stream(Instant::now() + Duration::from_secs(1)))
        .await
        .unwrap();
    slot.revoke();
    let (resolved, _plan) = proxy
        .engine()
        .resolve_route_decision(zero_engine::RouteDecision::Direct)
        .unwrap();
    let result = dispatch_tcp_outbound(
        services,
        &session,
        resolved,
        zero_engine::RouteMode::Flow,
        TcpDispatchIntent::Traffic,
    )
    .await;
    assert!(
        result.is_err(),
        "revoked native Direct must not invoke the legacy dialer"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}
