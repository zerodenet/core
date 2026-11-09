//! A real deferred device warm consumes only its exact pending wire candidate.
use crate::{Proxy, ProxyHandle};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use std::{sync::Arc, time::Duration};
use zero_config::RuntimeConfig;
use zero_traits::packet_metadata::PacketMetadataLayout;
fn config(
    dns: &crate::dns_fixture::Dns,
    bytes: usize,
    peer: Option<std::net::SocketAddr>,
) -> RuntimeConfig {
    let mut document = serde_json::json!({
        "runtime":{"dns_wire_total_max_buffered_bytes":bytes,"dns":{
            "servers":{"node":dns.server()},"default_server":"node","cache":null,
            "policy":{"node_server":"node","address_family":"ipv4_only"}}},
        "route":{"final":{"type":"direct"}}
    });
    if let Some(peer) = peer {
        document["outbounds"] = serde_json::json!([{"tag":"wg","protocol":{
            "type":"wireguard","private_key":STANDARD.encode([121;32]),
            "addresses":["10.0.0.1/32"],"peers":[{
                "public_key":STANDARD.encode([122;32]),
                "endpoint":format!("wire-budget-warm.test:{}",peer.port()),
                "allowed_ips":["0.0.0.0/0"]}]}}]);
    }
    RuntimeConfig::parse(&document.to_string()).unwrap()
}
#[tokio::test]
async fn actual_target_node_warm_uses_provisional_growth_while_old_callers_and_held_backing_keep_the_same_book(
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut dns = crate::dns_fixture::Dns::new().await;
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let initial = config(&dns, 1024, None);
        let proxy = Proxy::new(initial.clone()).unwrap();
        let originals = proxy.protocols.runtime_host().original_budgets();
        let book = originals.dns_wire.clone();
        let running = proxy.clone().spawn();
        let handle = ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(initial, Duration::from_secs(5)).await.unwrap();
        let original_input = proxy.resolver.prepare_wire_input(256).unwrap();
        let original_capacity = original_input.len();
        let target = config(&dns, 262144, Some(peer.local_addr().unwrap()));
        let mut applying = Box::pin(handle.apply_runtime_config_and_wait(target.clone(), Duration::from_secs(5)));
        let request = tokio::select! {
            request = dns.request() => request,
            result = &mut applying => panic!("target Source must resolve its actual domain before publication: {result:?}"),
        };
        assert!(!request.tcp);
        assert_eq!(book.snapshot().limit, PacketMetadataLayout::new(1024, 1024));
        assert!(book.snapshot().occupied.bytes() > 1024,
            "the actual native UDP receive backing was prepaid on the same target lane");
        let error = proxy.resolver.prepare_wire_input(1).err().unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock,
            "old caller cannot consume target growth before the exact commit");
        assert_eq!(original_input.len(), original_capacity);
        request.answer(false, false);
        applying.await.unwrap();
        assert_eq!(book.snapshot().limit, PacketMetadataLayout::new(262144, 262144));
        assert!(Arc::ptr_eq(&book, &proxy.protocols.runtime_host().original_budgets().dns_wire));
        let roots = proxy.protocols.test_packet_source_origins(proxy.engine().config().as_ref());
        assert_eq!(roots.len(), 1, "one actual standalone WG wire was assembled");
        let actual = roots[0].clone();
        assert!(actual.closed().is_none());
        let driver = actual.driver().unwrap().clone();
        assert!(!driver.is_joined());
        let held = proxy.resolver.prepare_wire_input(65536).unwrap();
        let accepted = proxy.engine().runtime_snapshot();
        let held_bytes = book.snapshot().occupied;
        let small = config(&dns, 1024, Some(peer.local_addr().unwrap()));
        assert!(handle.apply_runtime_config_and_wait(small.clone(), Duration::from_secs(5)).await.is_err());
        assert!(Arc::ptr_eq(&accepted, &proxy.engine().runtime_snapshot()));
        assert_eq!(book.snapshot().limit, PacketMetadataLayout::new(262144, 262144));
        assert_eq!(book.snapshot().occupied, held_bytes,
            "a rejected shrink cannot refund or evict the actual held input");
        assert!(actual.closed().is_none() && !driver.is_joined());
        drop(held);
        handle.apply_runtime_config_and_wait(small, Duration::from_secs(5)).await.unwrap();
        assert_eq!(book.snapshot().limit, PacketMetadataLayout::new(1024, 1024));
        assert!(actual.closed().is_none() && !driver.is_joined(),
            "pure wire-cap publication cannot retire the existing physical device");
        drop(original_input);
        running.shutdown().await.unwrap();
        let closed = actual.closed().expect("same WG native original closed");
        assert!(closed.matches(&actual) && closed.had_native_acquisition());
        let joined = driver.try_joined().expect("actual same device Scope consumed its driver");
        assert!(driver.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap();
        assert_eq!(originals.carrier.snapshot().charged, 0);
        drop(peer);
        dns.close().await;
    }).await.unwrap();
}

mod failure;
mod response;
