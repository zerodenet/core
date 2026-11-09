//! Tagged Direct DNS uses its declared original and the actual dial policy.
use super::*;
use std::{sync::Arc, time::Duration};
use zero_dns::{DnsOutboundConnectFuture, DnsOutboundConnector};

#[derive(Debug)]
struct NoLegacy;
impl DnsOutboundConnector for NoLegacy {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> DnsOutboundConnectFuture {
        Box::pin(async { panic!("ordinary Direct DNS must use its host-owned original") })
    }
}

#[tokio::test]
async fn constrained_direct_dns_uses_its_declared_original_and_live_source_policy() {
    direct(false).await;
}

#[tokio::test]
async fn constrained_direct_dns_family_rejection_cannot_retry_a_bare_carrier() {
    direct(true).await;
}

async fn direct(reject: bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let socket = tokio::net::UdpSocket::bind(endpoint).await.unwrap();
        let dial = if reject {
            serde_json::json!({"address_family":"only_ipv6"})
        } else {
            serde_json::json!({"address_family":"only_ipv4","source_ip":"127.0.0.1"})
        };
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[{"tag":"dns-direct","protocol":{"type":"direct"},"dial":dial}],
            "runtime":{"direct_max_originals":1,"direct_max_tasks":1,"dns":{"servers":{
                "node":{"type":"system"},
                "query":{"type":"udp","host":"127.0.0.1","port":endpoint.port(),"detour":"dns-direct"}
            },"default_server":"query","policy":{"node_server":"node","timeout_ms":3000}}},
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        proxy.resolver.set_outbound_connector(Arc::new(NoLegacy));
        let end = || tokio::time::Instant::now()+Duration::from_secs(3);
        let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(config,Duration::from_secs(3)).await.unwrap();
        let mut query = tokio::spawn({
            let resolver=proxy.resolver.clone();
            async move {resolver.resolve_real_type("constrained.direct.dns.test",1).await}
        });
        if reject {
            assert!(query.await.unwrap().is_err());
            std::future::poll_fn(|cx| {
                assert!(listener.poll_accept(cx).is_pending());
                let mut bytes=[0;1]; let mut target=tokio::io::ReadBuf::new(&mut bytes);
                assert!(socket.poll_recv_from(cx, &mut target).is_pending());
                std::task::Poll::Ready(())
            }).await;
        } else {
            let mut bytes=vec![0;65536];
            let (size,source)=tokio::select! {
                tcp = listener.accept() => panic!("UDP Direct must not use the retained TCP peer: {tcp:?}"),
                received = socket.recv_from(&mut bytes) => received.unwrap(),
                result = &mut query => panic!("Direct DNS returned before actual UDP reception: {result:?}"),
            };
            assert_eq!(source.ip(),endpoint.ip());
            bytes.truncate(size);
            let observed=tokio::time::timeout_at(end(),async {
                loop {
                    if let Some(query)=proxy.resolver.native_query_observations().into_iter().find(|query|query.io_pending&&query.original.is_some()) {break query;}
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
            let original=observed.original.as_ref().unwrap();
            let donor=original.parent().expect("DNS child retains the actual selected Direct original");
            assert_ne!(donor.instance(),original.instance());
            assert!(donor.parent().is_none() && donor.driver().is_none());
            assert!(original.closed().is_none() && donor.closed().is_none());
            let plan=proxy.tcp_runtime_services().components.clone().unwrap();
            let slot=plan.component(donor.instance().component).unwrap();
            assert_eq!(slot.instance_id(),donor.instance());
            let owner=slot.original_carrier_owner(0).expect("same actual Direct native owner");
            assert!(donor.owner().same_owner(&owner));
            assert_eq!(owner.limit().get(),1);
            let held=owner.observe().unwrap();assert_eq!(held.retained(),1);
            assert_eq!(held.slots()[0].native().unwrap().kind(), zero_platform_tokio::controlled_carrier::CarrierNativeKind::Datagram);
            assert!(!held.slots()[0].native().unwrap().is_closed());
            let auxiliary=held.slots()[0].auxiliary().expect("actual Direct preparation").clone();
            let joined=auxiliary.try_joined().expect("Direct preparation joined before delivery");
            assert!(auxiliary.matches_joined(&joined) && joined.registration_released());
            assert!(auxiliary.scope_identity().same_scope(&owner.bound().unwrap().scope_identity()));
            joined.result().unwrap();
            let blocked = proxy.protocols.drive_component_supervision(Box::pin(proxy.resolver.resolve_real_type("blocked.same-direct.dns.test", 1))).await;
            assert!(blocked.is_err(), "same Direct native N1 rejects a second original without harming the first");
            let blocked_stream = proxy.protocols.drive_component_supervision(Box::pin(zero_dns::test_support::open_native_detour(proxy.resolver.as_ref(), "dns-direct".into(), endpoint, end()))).await;
            assert!(blocked_stream.is_err(), "Stream and Datagram use the same actual Direct N1, not separate native pools");
            assert_eq!(owner.observe().unwrap().retained(), 1);
            assert!(donor.closed().is_none() && !query.is_finished());
            std::future::poll_fn(|cx| {
                assert!(listener.poll_accept(cx).is_pending());
                let mut bytes=[0;1]; let mut target=tokio::io::ReadBuf::new(&mut bytes);
                assert!(socket.poll_recv_from(cx, &mut target).is_pending());
                std::task::Poll::Ready(())
            }).await;
            let answer=zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([198,51,100,27])]);
            assert_eq!(socket.send_to(&answer,source).await.unwrap(),answer.len());
            assert_eq!(proxy.protocols.drive_component_supervision(Box::pin(query)).await.unwrap().unwrap(),vec![IpAddress::V4([198,51,100,27])]);
            proxy.protocols.drive_component_supervision(Box::pin(observed.task.wait())).await;
            let closed=original.closed().expect("same original native controller receipt");
            assert!(closed.matches(original) && !closed.had_native_acquisition(), "the DNS child returns its actual logical loan");
            let native=donor.closed().expect("same Direct original native controller receipt");
            assert!(native.matches(donor) && native.had_native_acquisition());
            assert!(auxiliary.try_joined().unwrap().registration_released());
            let query_ack = proxy.protocols.drive_component_supervision(Box::pin(observed.task.wait_registration_released())).await;
            assert!(observed.task.matches_joined(&query_ack) && query_ack.registration_released());
            std::future::poll_fn(|cx| { assert!(listener.poll_accept(cx).is_pending()); std::task::Poll::Ready(()) }).await;
        }
        assert!(proxy.engine().active_sessions().is_empty());
        assert!(proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();
    }).await.unwrap();
}

#[path = "direct/datagram_cancel.rs"]
mod datagram_cancel;
