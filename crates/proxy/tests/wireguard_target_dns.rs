#![cfg(all(feature = "wireguard", feature = "socks5", feature = "dns"))]

use crate::{support, wireguard_traffic::peer};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    time::{timeout, Duration},
};
use zero_core::Address;
use zero_traits::IpAddress;

async fn dns_server(answer: IpAddress) -> (u16, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let count = Arc::new(AtomicUsize::new(0));
    let queries = count.clone();
    let task = tokio::spawn(async move {
        let mut request = [0; 4096];
        loop {
            let (size, source) = socket.recv_from(&mut request).await.unwrap();
            queries.fetch_add(1, Ordering::Relaxed);
            let reply = zero_dns::udp::build_dns_response(&request[..size], &[answer]);
            socket.send_to(&reply, source).await.unwrap();
        }
    });
    (port, count, task)
}

async fn udp_echo(port: u16, domain: &str) {
    let mut control = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    control.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    control.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    control
        .write_all(&[5, 3, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut response = [0; 10];
    control.read_exact(&mut response).await.unwrap();
    assert_eq!(response[1], 0);
    let relay = u16::from_be_bytes([response[8], response[9]]);
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let request =
        support::build_udp_packet(&Address::Domain(domain.into()), 8000, b"company-udp").unwrap();
    client
        .send_to(&request, ("127.0.0.1", relay))
        .await
        .unwrap();
    let mut buffer = [0; 2048];
    let (size, _) = client.recv_from(&mut buffer).await.unwrap();
    let packet = support::parse_udp_packet(&buffer[..size]).unwrap();
    assert_eq!(packet.target, Address::Ipv4([192, 168, 1, 235]));
    assert_eq!(packet.port, 8000);
    assert_eq!(packet.payload, b"company-udp");
}

#[tokio::test]
async fn company_target_uses_tunnel_dns_for_tcp_and_udp_while_peer_uses_node_dns() {
    let (remote, peer_task) = peer::echo_peer_with_dns(IpAddress::V4([192, 168, 1, 235])).await;
    let (public_port, public_queries, public_task) =
        dns_server(IpAddress::V4([203, 0, 113, 7])).await;
    let (node_port, node_queries, node_task) = dns_server(IpAddress::V4([127, 0, 0, 1])).await;
    let socks_port = support::free_port();
    let config = zero_config::RuntimeConfig::parse(&json!({
        "runtime":{"dns":{
            "servers":{
                "company-dns":{"type":"udp","host":"192.168.1.180","detour":"company"},
                "public":{"type":"udp","host":"127.0.0.1","port":public_port},
                "node":{"type":"udp","host":"127.0.0.1","port":node_port}
            },
            "default_server":"public",
            "dispatch":[{"condition":{"type":"domain","values":["starmerx.com"]},"server":"company-dns"}],
            "answer":{"type":"fake_ip","cidr":"198.18.0.0/15","ttl_seconds":60,"max_entries":16},
            "policy":{"direct_server":"public","node_server":"node","address_family":"ipv4_only","timeout_ms":1000}
        }},
        "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks_port},"protocol":{"type":"socks5"}}],
        "endpoints":[{"tag":"company","directions":{"inbound":false,"outbound":true},"protocol":{
            "type":"wireguard","private_key":STANDARD.encode([121;32]),"addresses":["10.0.0.1/32"],
            "peers":[{"public_key":peer::public(122),"endpoint":format!("peer.example:{}",remote.port()),"allowed_ips":["192.168.1.0/24"]}]
        }}],
        "route":{"rules":[],"final":{"type":"route","outbound":"company"}}
    }).to_string()).unwrap();
    let running = support::spawn_engine(zero_proxy::Proxy::new(config).unwrap());
    support::wait_for_listener(socks_port).await;
    let domain = "jms.yt.starmerx.com";
    timeout(Duration::from_secs(5), async {
        let mut stream = TcpStream::connect(("127.0.0.1", socks_port)).await.unwrap();
        stream.write_all(&[5, 1, 0]).await.unwrap();
        let mut auth = [0; 2];
        stream.read_exact(&mut auth).await.unwrap();
        assert_eq!(auth, [5, 0]);
        let mut request = vec![5, 1, 0, 3, domain.len() as u8];
        request.extend_from_slice(domain.as_bytes());
        request.extend_from_slice(&443u16.to_be_bytes());
        stream.write_all(&request).await.unwrap();
        let mut response = [0; 10];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(
            response[1], 0,
            "private target must match company AllowedIPs"
        );
        stream.write_all(b"company-tcp").await.unwrap();
        let mut echoed = [0; 11];
        stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"company-tcp");
    })
    .await
    .expect("company TCP DNS/echo timed out");
    timeout(Duration::from_secs(5), udp_echo(socks_port, domain))
        .await
        .expect("company UDP DNS/echo timed out");
    assert!(
        node_queries.load(Ordering::Relaxed) > 0,
        "outer peer must use Node DNS"
    );
    assert_eq!(
        public_queries.load(Ordering::Relaxed),
        0,
        "business target must not use Direct/default public DNS"
    );
    running.shutdown().await.unwrap();
    peer_task.abort();
    public_task.abort();
    node_task.abort();
}
