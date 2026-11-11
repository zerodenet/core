#![cfg(feature = "socks5")]
use super::*;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

#[cfg(feature = "mieru")]
#[tokio::test]
async fn standard_outbound_failed_actual_catalog_cannot_select_legacy_pooled_udp() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = serde_json::json!({
        "outbounds":[{"tag":"node","protocol":{
            "type":"mieru","server":"127.0.0.1","port":listener.local_addr().unwrap().port(),
            "username":"test-user","password":"test-password"
        }}],
        "route":{"final":{"type":"route","outbound":"node"}}
    });
    let proxy =
        crate::Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap();
    let mut session = zero_core::Session::new(
        0,
        zero_core::Address::Ipv4([203, 0, 113, 9]),
        53,
        zero_core::Network::Udp,
        zero_core::ProtocolType::new("test"),
    );
    proxy
        .engine()
        .prepare_session(&mut session, "incoming")
        .unwrap();
    let mut services = proxy
        .tcp_runtime_services()
        .for_inbound("incoming".into())
        .with_business_identity(&session)
        .unwrap();
    // Inject a negative planning result only; this supplies no cleanup proof.
    let original = Arc::new(ChannelError::new(
        Stage::Prepare,
        Reason::InvalidConfig,
        Impact::Component,
    ));
    services.execution.components = Err(original.clone());
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(zero_engine::RouteDecision::Route("node".into()))
        .unwrap();
    let context = crate::protocol_registry::UdpAdapterContext::new(
        services.config(),
        crate::protocol_registry::UdpRuntimeServices::new(services.clone()),
    );
    let failure = match proxy.protocols.prepare_udp_outbound(
        context,
        &session,
        &resolved,
        b"query",
        zero_engine::RouteMode::Flow,
    ) {
        Ok(_) => panic!("failed actual catalog selected the compatibility Mieru pool"),
        Err(failure) => failure,
    };
    assert_eq!(failure.stage, "prepare_component");
    let channel = crate::transport::channel_failure(&failure.error).unwrap();
    assert_eq!(channel.reason, Reason::InvalidConfig);
    let source = std::error::Error::source(channel)
        .unwrap()
        .downcast_ref::<Arc<ChannelError>>()
        .unwrap();
    assert!(Arc::ptr_eq(source, &original));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(listener.poll_accept(&mut context).is_pending());
}

#[tokio::test]
async fn a_failed_actual_catalog_result_cannot_select_legacy_tcp_preparation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = serde_json::json!({"outbounds":[{"tag":"node","protocol":{"type":"socks5","server":"127.0.0.1","port":listener.local_addr().unwrap().port()}}],"route":{"final":{"type":"route","outbound":"node"}}});
    let proxy =
        crate::Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap();
    let mut services = proxy.tcp_runtime_services().with_service_invocation(
        crate::component::direct_bridge::ServiceInvocation {
            caller: None,
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            purpose: zero_core::request::ServicePurpose::DiagnosticProbe,
        },
    );
    // Negative preparation-error fixture only. It certifies no resource close.
    let original = Arc::new(ChannelError::new(
        Stage::Prepare,
        Reason::InvalidConfig,
        Impact::Component,
    ));
    services.execution.components = Err(original.clone());
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(zero_engine::RouteDecision::Route("node".into()))
        .unwrap();
    let failure = match services.prepare_tcp_outbound(&resolved, zero_engine::RouteMode::Flow) {
        Ok(_) => panic!("failed catalog silently selected legacy TCP"),
        Err(failure) => failure,
    };
    assert_eq!(failure.stage, "prepare_component");
    let channel = crate::transport::channel_failure(&failure.error).unwrap();
    assert_eq!(channel.stage, Stage::Prepare);
    assert_eq!(channel.reason, Reason::InvalidConfig);
    let source = std::error::Error::source(channel)
        .unwrap()
        .downcast_ref::<Arc<ChannelError>>()
        .unwrap();
    assert!(Arc::ptr_eq(source, &original));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(listener.poll_accept(&mut cx).is_pending());
}

#[cfg(feature = "tls-ech-runtime")]
#[tokio::test]
async fn independent_services_borrow_the_same_actual_host_ech_cache_owner() {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = crate::Proxy::new(config.clone()).unwrap();
    let source = proxy.current_service_source().upgrade().unwrap();
    let first = source.runtime_services().unwrap().upstream().ech_resolver();
    let second = source.runtime_services().unwrap().upstream().ech_resolver();
    let ordinary = proxy.tcp_runtime_services().upstream().ech_resolver();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(&first, &ordinary));
    let other = crate::Proxy::new(config).unwrap();
    let unrelated = other.tcp_runtime_services().upstream().ech_resolver();
    assert!(
        !Arc::ptr_eq(&first, &unrelated),
        "different actual hosts cannot share this cache owner"
    );
}
