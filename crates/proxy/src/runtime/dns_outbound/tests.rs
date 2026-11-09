use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::IpAddress;

use crate::runtime::Proxy;

#[test]
fn retained_dns_cannot_keep_outbound_pools_alive_after_proxy_drop() {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let _retained_resolver = proxy.resolver.clone();
    let source = proxy.current_service_source();
    assert!(source.upgrade().unwrap().runtime_services().is_ok());
    drop(proxy);
    // The resolver's actual native sources are weak. Keeping the resolver
    // after its Proxy cannot revive its outbound inventory or original pools.
    assert!(source.upgrade().is_none());
}

#[tokio::test]
async fn proxy_executes_dns_detour_in_its_actual_host_without_recursive_resolution() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind DNS TCP server");
    let endpoint = listener.local_addr().expect("DNS TCP endpoint");
    let socket = tokio::net::UdpSocket::bind(endpoint)
        .await
        .expect("same configured DNS UDP endpoint");
    let server = tokio::spawn(async move {
        let mut query = vec![0_u8; 65536];
        let (size, peer) = tokio::select! { biased;
            tcp = listener.accept() => panic!("UDP DNS detour must not steal the retained TCP peer: {tcp:?}"),
            query = socket.recv_from(&mut query) => query.expect("receive actual UDP DNS"),
        };
        query.truncate(size);
        let response =
            zero_dns::udp::build_dns_response(&query, &[IpAddress::V4([198, 51, 100, 53])]);
        assert_eq!(
            socket
                .send_to(&response, peer)
                .await
                .expect("write DNS UDP response"),
            response.len()
        );
        std::future::poll_fn(|cx| {
            assert!(listener.poll_accept(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    });
    let config = zero_config::RuntimeConfig::parse(&format!(
        r#"{{
            "runtime": {{
                "dns": {{
                    "servers": {{
                        "bootstrap": {{ "type": "udp", "host": "1.1.1.1" }},
                        "proxy": {{
                            "type": "udp",
                            "host": "{}",
                            "port": {},
                            "detour": "dns-out"
                        }}
                    }},
                    "default_server": "proxy",
                    "policy": {{ "node_server": "bootstrap" }}
                }}
            }},
            "outbounds": [{{ "tag": "dns-out", "protocol": {{ "type": "direct" }} }}],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#,
        endpoint.ip(),
        endpoint.port()
    ))
    .expect("parse DNS detour config");
    let proxy = Proxy::new(config).expect("build proxy");
    proxy
        .protocols
        .start_components(
            proxy.engine.runtime_snapshot().config().clone(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(3),
        )
        .await
        .unwrap();

    assert_eq!(
        proxy
            .protocols
            .drive_component_supervision(Box::pin(
                proxy.resolver.resolve_real_type("detour.example", 1)
            ))
            .await
            .unwrap(),
        vec![IpAddress::V4([198, 51, 100, 53])]
    );
    let attempts =
        proxy
            .resolver
            .recent_query_attempts("detour.example", zero_dns::DnsQueryRole::Default, 1);
    assert_eq!(attempts[0].outbound, "dns-out");
    server.await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    proxy
        .protocols
        .drive_component_supervision(Box::pin(proxy.resolver.shutdown_queries(deadline)))
        .await
        .unwrap();
    proxy.protocols.shutdown_components(deadline).await.unwrap();
}

#[cfg(feature = "socks5")]
mod service;

#[cfg(feature = "socks5")]
mod doh_binding;

mod direct;

#[cfg(feature = "vmess")]
mod vmess_grpc;

#[cfg(feature = "vless")]
mod ech;

#[cfg(feature = "vless")]
mod vless_grpc;

#[cfg(all(feature = "wireguard", feature = "raw-ip-runtime"))]
mod wireguard;

#[cfg(feature = "vless")]
mod vless_stream;

#[cfg(all(feature = "shadowsocks", target_os = "linux"))]
mod sip;

#[cfg(any(feature = "socks5", feature = "shadowsocks"))]
mod native_datagram;
