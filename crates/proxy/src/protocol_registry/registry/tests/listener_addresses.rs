#[cfg(feature = "udp-runtime")]
use std::net::SocketAddr;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::protocol_registry::bind_tcp_listener;
fn ipv6_available() -> bool {
    match std::net::TcpListener::bind((Ipv6Addr::LOCALHOST, 0)) {
        Ok(_) => true,
        Err(error) => {
            eprintln!("SKIP IPv6 listener test: IPv6 loopback bind failed: {error}");
            false
        }
    }
}

async fn exercise_tcp(listener: &zero_platform_tokio::TokioListener) {
    let mut address = listener.local_addr().unwrap();
    if address.ip().is_unspecified() {
        address.set_ip(if address.is_ipv6() {
            Ipv6Addr::LOCALHOST.into()
        } else {
            Ipv4Addr::LOCALHOST.into()
        });
    }
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(b"ipv6").await.unwrap();
    let (mut accepted, _) = timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut bytes = [0; 4];
    timeout(Duration::from_secs(3), accepted.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes, b"ipv6");
}

#[tokio::test]
async fn tcp_listeners_accept_bare_and_bracketed_ipv6() {
    if !ipv6_available() {
        return;
    }
    for host in ["::1", "[::1]", "0:0:0:0:0:0:0:1", "::", "[::]"] {
        let listener = bind_tcp_listener(host, 0).await.expect(host);
        assert!(listener.local_addr().unwrap().is_ipv6());
        exercise_tcp(&listener).await;
    }
}

#[tokio::test]
async fn tcp_listeners_preserve_ipv4_and_hostname_support() {
    for host in ["127.0.0.1", "0.0.0.0", "localhost"] {
        let listener = bind_tcp_listener(host, 0).await.expect(host);
        exercise_tcp(&listener).await;
    }
}

#[cfg(feature = "udp-runtime")]
async fn exercise_udp(socket: &tokio::net::UdpSocket) {
    let mut address = socket.local_addr().unwrap();
    let loopback = if address.is_ipv6() {
        Ipv6Addr::LOCALHOST.into()
    } else {
        Ipv4Addr::LOCALHOST.into()
    };
    if address.ip().is_unspecified() {
        address.set_ip(loopback);
    }
    let client = tokio::net::UdpSocket::bind(SocketAddr::new(loopback, 0))
        .await
        .unwrap();
    client.send_to(b"udp", address).await.unwrap();
    let mut bytes = [0; 8];
    let (size, peer) = timeout(Duration::from_secs(3), socket.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..size], b"udp");
    socket.send_to(&bytes[..size], peer).await.unwrap();
    let size = timeout(Duration::from_secs(3), client.recv(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..size], b"udp");
}

#[cfg(feature = "udp-runtime")]
#[tokio::test]
async fn datagram_listeners_accept_ipv6_and_preserve_ipv4_hostname_support() {
    let ipv6 = ipv6_available();
    for host in ["127.0.0.1", "localhost", "::1", "[::1]", "::", "[::]"] {
        if host.contains(':') && !ipv6 {
            continue;
        }
        let socket = crate::protocol_registry::bind_datagram_listener(host, 0)
            .await
            .expect(host);
        exercise_udp(&socket).await;
    }
}

#[tokio::test]
async fn ipv6_wildcard_records_os_default_ipv4_acceptance() {
    if !ipv6_available() {
        return;
    }
    let listener = bind_tcp_listener("::", 0).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let connects = timeout(
        Duration::from_secs(1),
        TcpStream::connect((Ipv4Addr::LOCALHOST, port)),
    )
    .await;
    let ipv4_accepted = match connects {
        Ok(Ok(_client)) => timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap()
            .is_ok(),
        _ => false,
    };
    eprintln!("IPv6 wildcard TCP OS-default IPv4 acceptance: {ipv4_accepted}");
    // IPv6 must always work; IPv4 acceptance is deliberately platform-dependent.
    exercise_tcp(&listener).await;
    #[cfg(feature = "udp-runtime")]
    {
        let socket = crate::protocol_registry::bind_datagram_listener("::", 0)
            .await
            .unwrap();
        let client = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        client
            .send_to(
                b"v4",
                (Ipv4Addr::LOCALHOST, socket.local_addr().unwrap().port()),
            )
            .await
            .unwrap();
        let mut bytes = [0; 2];
        let received = timeout(Duration::from_millis(250), socket.recv_from(&mut bytes)).await;
        let ipv4_accepted = matches!(received, Ok(Ok((2, _))));
        eprintln!("IPv6 wildcard UDP OS-default IPv4 acceptance: {ipv4_accepted}");
        exercise_udp(&socket).await;
    }
}

#[tokio::test]
async fn registered_tcp_adapters_bind_ipv6_literals() {
    if !ipv6_available() {
        return;
    }
    #[cfg(any(feature = "trojan", feature = "vmess"))]
    let material = {
        let material = tempfile::tempdir().unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(material.path().join("ca.pem"), cert.cert.pem()).unwrap();
        std::fs::write(
            material.path().join("key.pem"),
            cert.signing_key.serialize_pem(),
        )
        .unwrap();
        material
    };
    let registry = crate::register::protocol_registry();
    for mut protocol in super::fixtures::compiled_in_inbound_configs() {
        #[cfg(any(feature = "trojan", feature = "vmess"))]
        if matches!(
            &protocol,
            zero_config::InboundProtocolConfig::Trojan { .. }
                | zero_config::InboundProtocolConfig::Vmess { .. }
        ) {
            let mut value = serde_json::to_value(&protocol).unwrap();
            value["tls"] = serde_json::json!({"cert_path":material.path().join("ca.pem"),
                "key_path":material.path().join("key.pem")});
            protocol = serde_json::from_value(value).unwrap();
        }
        if matches!(
            protocol,
            zero_config::InboundProtocolConfig::Hysteria2 { .. }
                | zero_config::InboundProtocolConfig::Wireguard { .. }
        ) {
            continue;
        }
        for address in ["::1", "[::1]", "::", "[::]"] {
            let mut inbound = zero_config::InboundConfig {
                tag: "tcp-ipv6".to_owned(),
                listen: zero_config::ListenConfig {
                    address: address.to_owned(),
                    port: 0,
                },
                protocol: protocol.clone(),
                udp: Default::default(),
                idle_timeout_secs: None,
            };
            inbound.udp.enabled = false;
            if registry
                .inbounds
                .has_factory_for(inbound.protocol.protocol_name(), &inbound)
            {
                crate::runtime::inbound_owners::test_standard_native_listener(inbound).await;
                continue;
            }
            panic!("every compiled inbound must have a standard factory");
        }
    }
}

#[cfg(any(feature = "mieru", feature = "wireguard", feature = "vless"))]
#[tokio::test]
async fn registered_packet_adapters_bind_ipv6_literals() {
    use zero_config::InboundProtocolConfig;
    if !ipv6_available() {
        return;
    }
    let registry = crate::register::protocol_registry();
    for mut protocol in super::fixtures::compiled_in_inbound_configs() {
        match &mut protocol {
            InboundProtocolConfig::Wireguard { .. } => {}
            InboundProtocolConfig::Mieru { transport, .. } => {
                *transport = zero_config::MieruTransport::Udp;
            }
            InboundProtocolConfig::Vless { mkcp, .. } => {
                *mkcp = Some(Box::default());
            }
            _ => continue,
        }
        for address in ["::1", "[::1]", "::", "[::]"] {
            let inbound = zero_config::InboundConfig {
                tag: "datagram-ipv6".to_owned(),
                listen: zero_config::ListenConfig {
                    address: address.to_owned(),
                    port: 0,
                },
                protocol: protocol.clone(),
                udp: Default::default(),
                idle_timeout_secs: None,
            };
            if registry
                .inbounds
                .has_factory_for(inbound.protocol.protocol_name(), &inbound)
            {
                crate::runtime::inbound_owners::test_standard_datagram_listener(inbound).await;
                continue;
            }
            panic!("every compiled packet inbound must have a standard factory");
        }
    }
}
