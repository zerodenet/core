use super::*;
#[test]
fn xicmp_rejects_kernel_echo_and_wrong_identity_then_restores_carrier_peer() {
    let peer: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let mut client = codec::Client::new(1234, false);
    let wire = client.query(b"request", peer).unwrap();
    let echo = codec::parse(&wire, false, false).unwrap();
    assert_eq!(echo.id, 1234);
    assert_eq!(echo.sequence, 1);
    let reflected = codec::encode(false, true, 1234, 1, b"request").unwrap();
    assert!(client.response(&reflected, peer.ip()).unwrap().is_none());
    let wrong = codec::reply(false, 4321, 1, Some(b'r'), b"response").unwrap();
    assert!(client.response(&wrong, peer.ip()).unwrap().is_none());
    let reply = codec::reply(false, 1234, 1, Some(b'r'), b"response").unwrap();
    assert!(client
        .response(&reply, "127.0.0.2".parse().unwrap())
        .unwrap()
        .is_none());
    assert_eq!(
        client.response(&reply, peer.ip()).unwrap(),
        Some((b"response".to_vec(), peer))
    );
    assert!(client.response(&reply, peer.ip()).unwrap().is_none());
    let mut invalid = reply;
    invalid[3] ^= 1;
    assert!(codec::parse(&invalid, false, true).is_err());
}
#[test]
fn xicmp_sequence_window_expires_old_requests_and_wraps_without_zero() {
    let peer: SocketAddr = "[::1]:9000".parse().unwrap();
    let mut client = codec::Client::new(10, true);
    for _ in 0..65535 {
        let wire = client.query(&[], peer).unwrap();
        assert_ne!(codec::parse(&wire, true, false).unwrap().sequence, 0);
    }
    let stale = codec::reply(true, 10, 1, None, b"old").unwrap();
    assert!(client.response(&stale, peer.ip()).unwrap().is_none());
    let recent = codec::reply(true, 10, 65535, None, b"last").unwrap();
    assert_eq!(
        client.response(&recent, peer.ip()).unwrap(),
        Some((b"last".to_vec(), peer))
    );
    let wire = client.query(&[], peer).unwrap();
    assert_eq!(codec::parse(&wire, true, false).unwrap().sequence, 1);
    let reply = codec::reply(true, 10, 1, None, b"new").unwrap();
    assert_eq!(
        client.response(&reply, peer.ip()).unwrap(),
        Some((b"new".to_vec(), peer))
    );
}
#[test]
fn xicmp_requires_outermost_placement_and_valid_bind_address() {
    use crate::finalmask::{
        udp::{self, Mask},
        Profile,
    };
    assert!(udp::validate(&[
        Mask::Wireguard,
        Mask::Xicmp {
            ip: "127.0.0.1".into(),
            id: 0
        }
    ])
    .is_err());
    assert!(Profile::from_udp(vec![Mask::Xicmp {
        ip: "host.invalid".into(),
        id: 0
    }])
    .is_err());
    assert!(Profile::from_udp(vec![
        Mask::Xicmp {
            ip: "::".into(),
            id: 0
        },
        Mask::MkcpOriginal
    ])
    .is_ok());
}
#[tokio::test]
#[ignore = "requires OS permission to create raw ICMP sockets"]
async fn raw_xicmp_carrier_exchanges_packets_over_loopback() {
    use crate::finalmask::{packet_socket, udp::Mask};
    let masks = [
        Mask::Xicmp {
            ip: "127.0.0.1".into(),
            id: 54321,
        },
        Mask::MkcpOriginal,
    ];
    let server = packet_socket::wrap(
        std::net::UdpSocket::bind("127.0.0.1:0").unwrap(),
        &masks,
        true,
    )
    .unwrap();
    let client = packet_socket::wrap(
        std::net::UdpSocket::bind("127.0.0.1:0").unwrap(),
        &masks,
        false,
    )
    .unwrap();
    let destination = SocketAddr::new("127.0.0.1".parse().unwrap(), 9000);
    tokio::time::timeout(Duration::from_secs(5), async {
        client.send_to(b"request", destination).await.unwrap();
        let mut bytes = [0; 1024];
        let (n, peer) = server.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..n], b"request");
        assert_eq!(peer.port(), 54321);
        server.send_to(b"reply", peer).await.unwrap();
        let (n, peer) = client.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..n], b"reply");
        assert_eq!(peer, destination);
    })
    .await
    .unwrap();
}
