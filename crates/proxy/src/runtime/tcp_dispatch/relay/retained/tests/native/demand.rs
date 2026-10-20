//! A real host consumer for the neutral per-demand connector. This explicit
//! concrete native prefix has no pool, background task or Datagram grant.
use super::*;
use crate::{
    component::instance::invalid,
    inventory::service::{ServiceNode, ServicePlan},
};
use std::{io, task::Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{
    request::{Destination, ServicePurpose},
    Address,
};
use zero_platform_tokio::contract::{
    network::{CarrierContext, UpstreamBinding},
    CancellationToken, Reason,
};
use zero_transport::{relay_connector::RelayStreamConnector, RuntimeError};

/// This callback retains only the actual selected node, static dependency and
/// Weak host source. Every connect supplies a fresh real demand; no initial
/// query deadline, cancellation token, Execution or snapshot is retained.
fn connector(proxy: &Proxy) -> RelayStreamConnector {
    let services = proxy.tcp_runtime_services();
    let target = services.snapshot().plan().target_id("first").unwrap();
    let (resolved, _) = services
        .engine()
        .resolve_target_id_in_snapshot(services.snapshot(), target)
        .unwrap();
    let plan = proxy
        .protocols
        .prepare_service_stream(
            OutboundAdapterContext::new(services.config()),
            &resolved,
            services.components.as_ref().unwrap(),
        )
        .unwrap_or_else(|failure| panic!("{}", failure.error))
        .unwrap();
    let ServicePlan::Single(node) = plan else {
        panic!("explicit concrete native prefix")
    };
    let node = Arc::new(node);
    let dependency = services
        .snapshot()
        .plan()
        .outbound_static_binding("first")
        .unwrap();
    let generation = services.upstream().network_generation();
    let binding = UpstreamBinding {
        prefix: vec![node.slot.role_id()],
        dial: node.dial.clone(),
        network_generation: zero_core::component::NetworkGeneration(generation),
    };
    let weak = proxy.current_service_source();
    RelayStreamConnector::new_service_bound(
        "actual-native-prefix".into(),
        binding,
        ("127.0.0.1".into(), 45873),
        Arc::new(move |host, port, demand| {
            let node = node.clone();
            let dependency = dependency.clone();
            let weak = weak.clone();
            Box::pin(async move {
                let source = weak.upgrade().ok_or_else(|| {
                    RuntimeError::Io(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "actual host source is gone",
                    ))
                })?;
                let services = source.runtime_services()?.execution();
                let current = services
                    .components
                    .as_ref()
                    .map_err(|source| {
                        io::Error::other(crate::component::instance::retained(source.clone()))
                    })?
                    .configured("first")
                    .ok_or_else(|| {
                        io::Error::other(invalid(Reason::StalePlan, "actual selected slot is gone"))
                    })?;
                if !Arc::ptr_eq(&current, &node.slot)
                    || !dependency.matches(services.snapshot().plan())
                    || services.upstream().network_generation() != generation
                {
                    return Err(io::Error::other(invalid(
                        Reason::StalePlan,
                        "actual prefix dependency changed",
                    ))
                    .into());
                }
                node.slot
                    .service_admission()
                    .ok_or_else(|| {
                        io::Error::other(invalid(
                            Reason::Revoked,
                            "actual admission is unavailable",
                        ))
                    })?
                    .check_current()
                    .map_err(io::Error::other)?;
                if let Some((reason, endpoint)) =
                    zero_engine::EndpointAdmission::from_snapshot(services.snapshot())
                        .outbound_denial("first")
                {
                    return Err(io::Error::other(invalid(
                        Reason::Revoked,
                        format!("{reason}: {endpoint}"),
                    ))
                    .into());
                }
                let selected = ServiceNode {
                    slot: node.slot.clone(),
                    tag: node.tag.clone(),
                    endpoint: node.endpoint.clone(),
                    wire_endpoint: node.wire_endpoint.clone(),
                    role: node.role,
                    dial: node.dial.clone(),
                };
                // This actual invocation is a diagnostic Service demand, never
                // converted from a compatibility Session or arbitrary SessionId.
                let connected = crate::runtime::tcp_dispatch::service::connect(
                    services,
                    Destination::new(Address::Domain(host), port),
                    ServicePlan::Single(selected),
                    ServicePurpose::DiagnosticProbe,
                    demand,
                )
                .await
                .map_err(io::Error::other)?;
                Ok(connected.stream)
            })
        }),
    )
}
fn demand() -> CarrierContext {
    CarrierContext {
        session: None,
        deadline: Some(deadline()),
        cancel: CancellationToken::new(),
    }
}
fn channel_reason(error: &RuntimeError) -> Option<Reason> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<zero_platform_tokio::contract::ChannelError>() {
            return Some(error.reason);
        }
        current = error.source();
    }
    None
}
#[tokio::test]
async fn real_weak_prefix_cancel_at_socks_greeting_then_new_demand_and_retirement() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = proxy(listener.local_addr().unwrap().port());
    start(&proxy).await;
    let connector = connector(&proxy);
    let binding = connector.service_binding().unwrap().clone();
    let first = demand();
    let cancelled = first.cancel.clone();
    let mut work = connector.connect_with_context(&binding, "127.0.0.1".into(), 45873, first);
    let (mut peer, _) = tokio::select! {
        accepted = listener.accept() => accepted.unwrap(),
        result = &mut work => panic!("native prefix finished before actual peer: {}",result.err().unwrap()),
    };
    let mut greeting = [0; 3];
    tokio::select! {
        result = peer.read_exact(&mut greeting) => { result.unwrap(); },
        result = &mut work => panic!("native prefix finished before greeting: {}",result.err().unwrap()),
    }
    assert_eq!(greeting, [5, 1, 0]);
    std::future::poll_fn(|cx| {
        assert!(work.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    cancelled.cancel();
    let error = work.await.err().unwrap();
    assert_eq!(channel_reason(&error), Some(Reason::Cancelled));
    drop(peer);
    // This is a new explicit demand. The old cancelled query is never silently
    // extended or reused by the callback or the underlying native Service path.
    let mut second = demand();
    second.deadline = Some(tokio::time::Instant::now() + std::time::Duration::from_secs(7));
    let opened = connector.connect_with_context(&binding, "127.0.0.1".into(), 45873, second);
    let client = async {
        let mut io = opened.await.unwrap();
        io.write_all(&[41]).await.unwrap();
        assert_eq!(io.read_u8().await.unwrap(), 41);
    };
    let remote = async {
        let (mut peer, _) = listener.accept().await.unwrap();
        let mut greeting = [0; 3];
        peer.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [5, 1, 0]);
        peer.write_all(&[5, 0]).await.unwrap();
        let mut request = [0; 4];
        peer.read_exact(&mut request).await.unwrap();
        assert_eq!(request, [5, 1, 0, 3]);
        let len = peer.read_u8().await.unwrap();
        let mut host = vec![0; len as usize];
        peer.read_exact(&mut host).await.unwrap();
        assert_eq!(host, b"127.0.0.1");
        assert_eq!(peer.read_u16().await.unwrap(), 45873);
        peer.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
            .await
            .unwrap();
        assert_eq!(peer.read_u8().await.unwrap(), 41);
        peer.write_all(&[41]).await.unwrap();
    };
    tokio::time::timeout_at(deadline(), async {
        tokio::join!(client, remote);
    })
    .await
    .unwrap();
    assert!(cancelled.is_cancelled());
    assert!(proxy.engine().active_sessions().is_empty());
    assert!(proxy.engine().completed_sessions().is_empty());
    proxy
        .protocols
        .shutdown_components(deadline())
        .await
        .unwrap();
    let error = connector
        .connect_with_context(&binding, "127.0.0.1".into(), 45873, demand())
        .await
        .err()
        .unwrap();
    assert_eq!(
        channel_reason(&error),
        Some(Reason::StalePlan),
        "source alive is not a retired owner's grant"
    );
}
#[tokio::test]
async fn retained_contextual_prefix_does_not_keep_source_engine_config_or_dns_alive() {
    let proxy = proxy(34567);
    start(&proxy).await;
    let connector = connector(&proxy);
    let binding = connector.service_binding().unwrap().clone();
    let weak = proxy.current_service_source();
    let config = Arc::downgrade(&proxy.engine().config());
    let dns = Arc::downgrade(&proxy.resolver);
    drop(proxy);
    assert!(weak.upgrade().is_none());
    assert!(config.upgrade().is_none());
    assert!(dns.upgrade().is_none());
    let error = connector
        .connect_with_context(&binding, "127.0.0.1".into(), 45873, demand())
        .await
        .err()
        .unwrap();
    assert!(
        matches!(error, RuntimeError::Io(ref source) if source.kind()==io::ErrorKind::NotConnected)
    );
}
