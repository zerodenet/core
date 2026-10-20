//! End-to-end DNS bytes cross actual native Stream roles without a business
//! Session. Every fixture retains its bound listener through the real open.
use super::*;
use std::{net::SocketAddr, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use zero_platform_tokio::contract::{
    dns::{DnsQueryContext, DnsQueryRole, DnsRequest, DnsService},
    CancellationToken, Reason,
};

fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}
async fn start(proxy: &Proxy) {
    proxy
        .protocols
        .start_components(proxy.engine.runtime_snapshot().config().clone(), deadline())
        .await
        .unwrap();
}
fn config(endpoint: SocketAddr, ports: &[u16]) -> zero_config::RuntimeConfig {
    let nodes:Vec<_>=ports.iter().enumerate().map(|(i,port)|serde_json::json!({"tag":format!("socks-{i}"),"protocol":{"type":"socks5","server":"127.0.0.1","port":port}})).collect();
    let tags: Vec<_> = (0..ports.len()).map(|i| format!("socks-{i}")).collect();
    let detour = if ports.len() == 1 {
        "socks-0"
    } else {
        "dns-chain"
    };
    let mut config = serde_json::json!({"outbounds":nodes,"runtime":{"dns":{"servers":{"node":{"type":"udp","host":"127.0.0.1","port":endpoint.port()},"query":{"type":"udp","host":"127.0.0.1","port":endpoint.port(),"detour":detour}},"default_server":"query","policy":{"node_server":"node","timeout_ms":2000}}},"route":{"final":{"type":"direct"}}});
    if ports.len() > 1 {
        config["outbound_groups"] =
            serde_json::json!([{"tag":"dns-chain","type":"relay","proxies":tags}]);
    }
    zero_config::RuntimeConfig::parse(&config.to_string()).unwrap()
}
async fn socks(mut client: TcpStream, expected: SocketAddr, named: bool) {
    let mut greeting = [0; 3];
    client.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting, [5, 1, 0]);
    client.write_all(&[5, 0]).await.unwrap();
    let mut header = [0; 4];
    client.read_exact(&mut header).await.unwrap();
    assert_eq!(&header[..3], &[5, 1, 0]);
    let host = match header[3] {
        1 => {
            assert!(!named);
            let mut ip = [0; 4];
            client.read_exact(&mut ip).await.unwrap();
            std::net::Ipv4Addr::from(ip).to_string()
        }
        3 => {
            assert!(named);
            let len = client.read_u8().await.unwrap();
            let mut host = vec![0; len as usize];
            client.read_exact(&mut host).await.unwrap();
            String::from_utf8(host).unwrap()
        }
        other => panic!("unexpected address type {other}"),
    };
    let port = client.read_u16().await.unwrap();
    assert_eq!(host, expected.ip().to_string());
    assert_eq!(port, expected.port());
    let mut upstream = TcpStream::connect(expected).await.unwrap();
    client
        .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
        .await
        .unwrap();
    tokio::io::copy_bidirectional(&mut client, &mut upstream)
        .await
        .unwrap();
}

#[tokio::test]
async fn native_selected_socks_prefix_transports_actual_dns_without_allocating_business_sessions() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = dns.local_addr().unwrap();
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_port = first.local_addr().unwrap().port();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_endpoint = second.local_addr().unwrap();
    let proxy = Proxy::new(config(endpoint, &[first_port, second_endpoint.port()])).unwrap();
    start(&proxy).await;
    let services = proxy.tcp_runtime_services();
    let target = services.snapshot().plan().target_id("dns-chain").unwrap();
    let (resolved, _) = proxy
        .engine
        .resolve_target_id_in_snapshot(services.snapshot(), target)
        .unwrap();
    assert!(
        matches!(super::super::service::prepare(&proxy.protocols,&services,&resolved).unwrap_or_else(|failure|panic!("{}: {}",failure.stage,failure.error)),Some(crate::inventory::service::ServicePlan::Relay(ref nodes)) if nodes.len()==2)
    );
    let mut servers = tokio::task::JoinSet::new();
    servers.spawn(async move {
        let (stream, _) = first.accept().await.unwrap();
        socks(stream, second_endpoint, true).await;
    });
    servers.spawn(async move {
        let (stream, _) = second.accept().await.unwrap();
        socks(stream, endpoint, false).await;
    });
    servers.spawn(async move {
        let (mut io, _) = dns.accept().await.unwrap();
        let size = io.read_u16().await.unwrap();
        let mut query = vec![0; size as usize];
        io.read_exact(&mut query).await.unwrap();
        let response =
            zero_dns::udp::build_dns_response(&query, &[IpAddress::V4([198, 51, 100, 61])]);
        io.write_u16(response.len() as u16).await.unwrap();
        io.write_all(&response).await.unwrap();
    });
    let answer = tokio::time::timeout_at(
        deadline(),
        proxy.resolver.resolve_real_type("native-detour.test", 1),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(answer, vec![IpAddress::V4([198, 51, 100, 61])]);
    assert!(proxy.engine.active_sessions().is_empty());
    assert!(proxy.engine.completed_sessions().is_empty());
    while let Some(result) = tokio::time::timeout_at(deadline(), servers.join_next())
        .await
        .unwrap()
    {
        result.unwrap();
    }
    proxy.resolver.shutdown_queries(deadline()).await.unwrap();
    proxy
        .protocols
        .shutdown_components(deadline())
        .await
        .unwrap();
}

#[tokio::test]
async fn native_dns_service_cancels_an_actual_pending_socks_handshake_without_legacy_retry() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socks = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let next = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = Proxy::new(config(
        dns.local_addr().unwrap(),
        &[
            socks.local_addr().unwrap().port(),
            next.local_addr().unwrap().port(),
        ],
    ))
    .unwrap();
    start(&proxy).await;
    let (entered, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut io, _) = socks.accept().await.unwrap();
        let mut greeting = [0; 3];
        io.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [5, 1, 0]);
        entered.send(()).unwrap();
        let mut next = [0; 1];
        assert_eq!(
            io.read(&mut next).await.unwrap(),
            0,
            "cancellation must actually close the pending native carrier"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), socks.accept())
                .await
                .is_err(),
            "native failure cannot fall back to another legacy connection"
        );
    });
    let cancel = CancellationToken::new();
    let query = {
        let resolver = proxy.resolver.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            DnsService::resolve(
                resolver.as_ref(),
                DnsRequest {
                    name: "cancel-native.test".into(),
                    role: DnsQueryRole::Default,
                    family: zero_traits::AddressFamily::OnlyIpv4,
                },
                DnsQueryContext {
                    deadline: Some(deadline()),
                    cancel,
                    expected_generation: None,
                },
            )
            .await
        })
    };
    tokio::time::timeout_at(deadline(), received)
        .await
        .unwrap()
        .unwrap();
    assert!(!query.is_finished());
    cancel.cancel();
    assert_eq!(query.await.unwrap().unwrap_err().reason, Reason::Cancelled);
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
    assert!(proxy.engine.active_sessions().is_empty());
    assert!(proxy
        .resolver
        .inspect_cache("cancel-native.test")
        .await
        .is_none());
    proxy.resolver.shutdown_queries(deadline()).await.unwrap();
    proxy
        .protocols
        .shutdown_components(deadline())
        .await
        .unwrap();
}
