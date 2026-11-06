//! Normal Proxy DNS consumes the same canonical WG device's actual loans.
use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::{sync::Arc, time::Duration};
use zero_platform_tokio::controlled_carrier::CarrierOrigin;
mod peer;
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}
#[derive(Debug)]
struct NoLegacy;
impl zero_dns::DnsOutboundConnector for NoLegacy {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async { panic!("ordinary WG DNS must consume the current Host device role") })
    }
    fn exchange_datagram(
        &self,
        _: String,
        _: std::net::SocketAddr,
        _: Vec<u8>,
    ) -> zero_dns::DnsOutboundDatagramFuture {
        Box::pin(async { panic!("ordinary WG DNS cannot use the old RawIp exchange bridge") })
    }
}
async fn start(peer: &peer::Peer, dns: &crate::dns_fixture::Dns) -> (Proxy, crate::RunningProxy) {
    start_server(peer, dns.server()).await
}
async fn start_server(
    peer: &peer::Peer,
    backend: zero_config::DnsServerConfig,
) -> (Proxy, crate::RunningProxy) {
    start_server_mode(peer, backend, true).await
}
async fn start_server_mode(
    peer: &peer::Peer,
    backend: zero_config::DnsServerConfig,
    linked: bool,
) -> (Proxy, crate::RunningProxy) {
    start_server_with_business(peer, backend, linked, false).await
}
async fn start_server_with_business(
    peer: &peer::Peer,
    mut backend: zero_config::DnsServerConfig,
    linked: bool,
    business: bool,
) -> (Proxy, crate::RunningProxy) {
    match &mut backend {
        zero_config::DnsServerConfig::Udp { host, detour, .. }
        | zero_config::DnsServerConfig::Dot { host, detour, .. }
        | zero_config::DnsServerConfig::Doh { host, detour, .. } => {
            *host = "10.0.0.2".into();
            *detour = Some("wg".into());
        }
        _ => panic!("fixture requires an existing UDP/DoT/DoH WG detour"),
    }
    let mut document = serde_json::json!({
        "runtime":{"dns":{"servers":{"node":{"type":"system"},"query":backend},"default_server":"query","policy":{"node_server":"node","address_family":"ipv4_only"}}},
        "inbounds":[{"tag":"wg-source","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"wireguard","private_key":STANDARD.encode([121;32]),"addresses":["10.0.0.1/32"],"peers":[{"public_key":peer::public(122),"allowed_ips":["0.0.0.0/0"]}]}}],
        "outbounds":[{"tag":"wg","protocol":{"type":"wireguard","private_key":STANDARD.encode([121;32]),"addresses":["10.0.0.1/32"],"inbound_tag":"wg-source","peers":[{"public_key":peer::public(122),"endpoint":peer.address.to_string(),"allowed_ips":["0.0.0.0/0"]}]}}],
        "route":{"final":{"type":"direct"}}
    });
    if !linked {
        document.as_object_mut().unwrap().remove("inbounds");
        document["outbounds"][0]["protocol"]
            .as_object_mut()
            .unwrap()
            .remove("inbound_tag");
    }
    if business {
        let inbounds = document
            .as_object_mut()
            .unwrap()
            .entry("inbounds")
            .or_insert_with(|| serde_json::json!([]))
            .as_array_mut()
            .unwrap();
        inbounds.push(serde_json::json!({"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}));
        document["route"]["rules"] = serde_json::json!([{"condition":{"type":"inbound","values":["entry"]},"action":{"type":"route","outbound":"wg"}}]);
    }
    let config = zero_config::RuntimeConfig::parse(&document.to_string()).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    proxy.resolver.set_outbound_connector(Arc::new(NoLegacy));
    let running = proxy.spawn();
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    (proxy, running)
}
fn query(proxy: &Proxy, name: &str) -> tokio::task::JoinHandle<std::io::Result<Vec<IpAddress>>> {
    let resolver = proxy.resolver.clone();
    let name = name.to_owned();
    tokio::spawn(async move { resolver.resolve_real_type(&name, 1).await })
}
async fn pending(proxy: &Proxy) -> zero_dns::OwnedDnsQueryObservation {
    tokio::time::timeout_at(end(), async {
        loop {
            if let Some(actual) = proxy
                .resolver
                .native_query_observations()
                .into_iter()
                .find(|actual| actual.io_pending && actual.original.is_some())
            {
                return actual;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
fn returned(original: &CarrierOrigin) {
    let receipt = original
        .closed()
        .expect("actual query logical socket closed");
    assert!(receipt.matches(original) && !receipt.had_native_acquisition());
}
fn native_pending(child: &CarrierOrigin, root: &CarrierOrigin) {
    let donor = child.parent().expect("actual device socket lender");
    assert!(donor.native_origin().same_original(root));
    assert!(root.parent().is_none() && root.closed().is_none());
    assert!(!root.driver().unwrap().is_joined());
}
async fn close(
    proxy: &Proxy,
    running: crate::RunningProxy,
    root: &CarrierOrigin,
    children: &[CarrierOrigin],
) {
    assert!(
        proxy.engine().active_sessions().is_empty()
            && proxy.engine().completed_sessions().is_empty()
    );
    let source = proxy
        .system_tun_source_registry()
        .owner("wg-source")
        .unwrap();
    running.shutdown().await.unwrap();
    let closed = root.closed().expect("same native listener finally closed");
    assert!(closed.matches(root) && closed.had_native_acquisition());
    let driver = root.driver().unwrap();
    assert!(driver.is_joined());
    driver.try_joined().unwrap().result().unwrap();
    for child in children {
        returned(child);
        assert!(child.owner().closed().unwrap().matches(child.owner()));
        assert!(child.owner().users_closed().unwrap().matches(child.owner()));
    }
    let terminal = source.terminal_for_test().unwrap();
    assert_eq!(terminal.identity(), source.identity());
    assert!(
        terminal.failure().is_none()
            && terminal.task_failures().is_empty()
            && terminal.native_bound_failures().is_empty()
    );
    assert!(terminal.joined_tasks() >= 3 && terminal.closed_io_receipts() > 0);
}
#[tokio::test]
async fn ordinary_linked_wireguard_dns_udp_cancel_returns_only_the_actual_query_socket() {
    tokio::time::timeout(Duration::from_secs(25),async {
        let mut dns=crate::dns_fixture::Dns::new().await;
        let peer=peer::Peer::new(dns.address).await;
        let (proxy,running)=start(&peer,&dns).await;
        let root=proxy.protocols.test_packet_source_origins(proxy.engine().config().as_ref()).remove(0);
        let mut children=Vec::new();
        for (index,cancel) in [false,true,false].into_iter().enumerate() {
            let mut query=query(&proxy,&format!("{index}.linked.wg.dns.test"));
            let request=tokio::select! {request=dns.request()=>request,result=&mut query=>panic!("WG DNS failed before real inner UDP delivery: {result:?}")};
            assert!(!request.tcp);
            let actual=pending(&proxy).await;let child=actual.original.as_ref().unwrap();
            native_pending(child,&root);
            assert!(!actual.task.scope_identity().same_scope(&root.driver().unwrap().scope_identity()));
            if cancel {query.abort();assert!(query.await.unwrap_err().is_cancelled());drop(request);}
            else {request.answer(false,false);assert_eq!(query.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);}
            tokio::time::timeout_at(end(),actual.task.wait()).await.unwrap().result().unwrap();
            returned(child);native_pending(child,&root);children.push(child.clone());
        }
        close(&proxy,running,&root,&children).await;peer.close().await;dns.close().await;
    }).await.unwrap();
}
#[tokio::test]
async fn ordinary_linked_wireguard_truncated_dns_retires_udp_before_actual_framed_tcp() {
    tokio::time::timeout(Duration::from_secs(25),async {
        let mut dns=crate::dns_fixture::Dns::new().await;
        let peer=peer::Peer::new(dns.address).await;
        let (proxy,running)=start(&peer,&dns).await;
        let root=proxy.protocols.test_packet_source_origins(proxy.engine().config().as_ref()).remove(0);
        let mut query=query(&proxy,"truncated.linked.wg.dns.test");
        let request=tokio::select! {request=dns.request()=>request,result=&mut query=>panic!("WG UDP failed before delivery: {result:?}")};
        assert!(!request.tcp);
        let udp=pending(&proxy).await;let first=udp.original.as_ref().unwrap();native_pending(first,&root);
        request.answer(true,false);
        let request=tokio::select! {request=dns.request()=>request,result=&mut query=>panic!("WG TCP fallback failed before framed DNS delivery: {result:?}")};
        assert!(request.tcp);returned(first);
        let tcp=pending(&proxy).await;let next=tcp.original.as_ref().unwrap();
        assert!(tcp.task.same_registration(&udp.task));assert!(!next.same_original(first));
        native_pending(next,&root);
        request.answer(false,false);assert_eq!(query.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);
        tokio::time::timeout_at(end(),tcp.task.wait()).await.unwrap().result().unwrap();
        returned(next);native_pending(next,&root);
        close(&proxy,running,&root,&[first.clone(),next.clone()]).await;peer.close().await;dns.close().await;
    }).await.unwrap();
}

mod encrypted;

mod standalone;

#[cfg(feature = "socks5")]
mod warm;
