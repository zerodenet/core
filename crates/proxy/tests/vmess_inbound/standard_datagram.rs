//! Actual TLS + AEAD Stream/Datagram, packet/raw detection and payload accounting.
use super::*;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
#[path = "standard_datagram/mux.rs"]
mod mux;

fn packet(target: SocketAddr, payload: &[u8]) -> Vec<u8> {
    let std::net::IpAddr::V4(ip) = target.ip() else {
        panic!("IPv4 fixture")
    };
    let mut body = vec![1];
    body.extend(ip.octets());
    body.extend(target.port().to_be_bytes());
    body.extend(payload);
    let mut wire = (body.len() as u16).to_be_bytes().to_vec();
    wire.extend(body);
    wire
}
async fn packet_reply(client: &mut (impl tokio::io::AsyncRead + Unpin)) -> (SocketAddr, Vec<u8>) {
    let size = client.read_u16().await.unwrap();
    let mut body = vec![0; size as usize];
    client.read_exact(&mut body).await.unwrap();
    assert_eq!(body[0], 1);
    let target = SocketAddr::from((
        [body[1], body[2], body[3], body[4]],
        u16::from_be_bytes([body[5], body[6]]),
    ));
    (target, body[7..].to_vec())
}
async fn udp_client(
    fixture: &Fixture,
    target: SocketAddr,
) -> (
    impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    SocketAddr,
) {
    let (tls, source) = fixture.connect().await;
    let mut carrier = zero_platform_tokio::TcpRelayStream::new(tls);
    let session = zero_core::Session::new(
        901,
        zero_core::Address::Ipv4([127, 0, 0, 1]),
        target.port(),
        zero_core::Network::Udp,
        zero_core::ProtocolType::new("client"),
    );
    // This Session belongs to the independent client request builder, never the host.
    let state = ::vmess::outbound::VmessOutbound
        .establish_udp_packet_session(
            &mut carrier,
            &session,
            &::vmess::parse_uuid(USER_ID).unwrap(),
            ::vmess::VmessCipher::Chacha20Poly1305,
        )
        .await
        .unwrap();
    (
        ::vmess::stream::VmessAeadStream::outbound(carrier, state).unwrap(),
        source,
    )
}
#[tokio::test]
async fn standard_udp_packet_mode_keeps_targets_empty_payload_auth_and_accounting_with_relative_tls(
) {
    timeout(Duration::from_secs(12), async {
        let first = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = first.local_addr().unwrap();
        let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let alternate = second.local_addr().unwrap();
        let fixture = Fixture::datagram();
        let (mut client, source) = udp_client(&fixture, address).await;
        client.write_all(&packet(address, b"one")).await.unwrap();
        client.flush().await.unwrap();
        let mut bytes = [0; 128];
        let (size, peer) = first.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], b"one");
        // A response can arrive while the standard reader waits for another real AEAD frame.
        first.send_to(b"one", peer).await.unwrap();
        assert_eq!(packet_reply(&mut client).await, (address, b"one".to_vec()));
        client.write_all(&packet(alternate, b"two")).await.unwrap();
        client.flush().await.unwrap();
        let (size, other) = second.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], b"two");
        second.send_to(b"two", other).await.unwrap();
        assert_eq!(
            packet_reply(&mut client).await,
            (alternate, b"two".to_vec())
        );
        client.write_all(&packet(address, b"")).await.unwrap();
        client.flush().await.unwrap();
        let (size, same) = first.recv_from(&mut bytes).await.unwrap();
        assert_eq!(size, 0);
        assert_eq!(same, peer);
        first.send_to(b"", peer).await.unwrap();
        assert_eq!(packet_reply(&mut client).await, (address, Vec::new()));
        fixture.running.shutdown().await.unwrap();
        let completed = fixture.probe.completed_sessions();
        assert_eq!(completed.len(), 2);
        for flow in &completed {
            assert_eq!(flow.protocol.as_str(), "vmess");
            assert_eq!(flow.source_port, Some(source.port()));
            assert_eq!(
                flow.auth.as_ref().unwrap().principal_key.as_deref(),
                Some("vmess:datagram")
            );
        }
        let flow = completed
            .iter()
            .find(|flow| flow.port == address.port())
            .unwrap();
        assert_eq!((flow.bytes_up, flow.bytes_down), (3, 21));
        let flow = completed
            .iter()
            .find(|flow| flow.port == alternate.port())
            .unwrap();
        assert_eq!((flow.bytes_up, flow.bytes_down), (3, 12));
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn standard_udp_raw_mode_stays_raw_when_a_later_payload_looks_like_a_packet() {
    timeout(Duration::from_secs(10), async {
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let fixture = Fixture::datagram();
        let (mut client, source) = udp_client(&fixture, address).await;
        let opaque = packet(SocketAddr::from(([127, 0, 0, 1], 9)), b"opaque");
        let mut bytes = [0; 128];
        let mut native = None;
        for payload in [b"raw".as_slice(), opaque.as_slice()] {
            client.write_all(payload).await.unwrap();
            client.flush().await.unwrap();
            let (size, peer) = target.recv_from(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], payload);
            if let Some(previous) = native {
                assert_eq!(previous, peer);
            }
            native = Some(peer);
            target.send_to(payload, peer).await.unwrap();
            let mut reply = vec![0; payload.len()];
            client.read_exact(&mut reply).await.unwrap();
            assert_eq!(reply, payload);
        }
        fixture.running.shutdown().await.unwrap();
        let completed = fixture.probe.completed_sessions();
        assert_eq!(completed.len(), 1);
        let flow = &completed[0];
        assert_eq!(flow.source_port, Some(source.port()));
        assert_eq!(
            (flow.bytes_up, flow.bytes_down),
            (3 + opaque.len() as u64, 3 + opaque.len() as u64)
        );
    })
    .await
    .unwrap();
}
