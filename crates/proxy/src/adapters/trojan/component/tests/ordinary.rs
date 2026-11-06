//! One real Source and same per-connection grant for TLS/WS/gRPC TCP and UDP.
#[path = "fixture.rs"]
mod fixture;
use fixture::*;
use std::time::Duration;
#[tokio::test]
async fn ordinary_trojan_carriers_drain_large_half_closed_tcp_and_real_datagrams_without_unowned_workers(
) {
    tokio::time::timeout(Duration::from_secs(40), async {
        for kind in ["plain", "ws", "grpc-single", "grpc-multi"] {
            let directory = tempfile::tempdir().unwrap();
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let cert_path = directory.path().join("server.pem");
            let key_path = directory.path().join("server.key");
            std::fs::write(&cert_path, cert.cert.pem()).unwrap();
            std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
            let remote = server(kind, &cert_path, &key_path).await;
            let client = client(kind, remote.address).await;
            let (owner, original) = echo(&client).await;
            wait_returned(&owner, &original).await;
            let (udp_owner, udp_original) = datagrams(&client).await;
            assert!(owner.same_owner(&udp_owner));
            assert!(!original.same_original(&udp_original));
            client.close().await;
            let closed = udp_original.closed().unwrap();
            assert!(udp_original.matches_closed(&closed) && closed.had_native_acquisition());
            assert!(owner.closed().unwrap().matches(&owner));
            assert!(owner.users_closed().unwrap().matches(&owner));
            remote.close().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_trojan_mux_full_transports_reuse_the_same_original_and_driver_for_tcp_udp_and_network_retirement(
) {
    tokio::time::timeout(Duration::from_secs(40), async {
        for kind in ["mux-plain", "mux-ws", "mux-grpc-single", "mux-grpc-multi"] {
            let directory = tempfile::tempdir().unwrap();
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let certificate = directory.path().join("server.pem");
            let key = directory.path().join("server.key");
            std::fs::write(&certificate, cert.cert.pem()).unwrap();
            std::fs::write(&key, cert.signing_key.serialize_pem()).unwrap();
            let remote = server(kind, &certificate, &key).await;
            let client = client(kind, remote.address).await;
            let (owner, original) = echo(&client).await;
            let first_driver = cached_driver(&owner, &original);
            let (same, reused) = echo(&client).await;
            assert!(owner.same_owner(&same) && original.same_original(&reused));
            let (udp_owner, udp) = datagrams(&client).await;
            assert!(owner.same_owner(&udp_owner) && original.same_original(&udp));
            assert!(original.closed().is_none() && !first_driver.is_joined());
            client.proxy.egress_interface.invalidate_network();
            let (next_owner, next) = echo(&client).await;
            assert!(!original.same_original(&next));
            let old = original.closed().unwrap();
            assert!(original.matches_closed(&old) && old.had_native_acquisition());
            let second_driver = cached_driver(&next_owner, &next);
            client.close().await;
            for driver in [first_driver, second_driver] {
                let joined = driver.try_joined().unwrap();
                assert!(driver.matches_joined(&joined) && joined.registration_released());
            }
            for (owner, original) in [(owner, original), (next_owner, next)] {
                assert!(owner.closed().unwrap().matches(&owner));
                assert!(owner.users_closed().unwrap().matches(&owner));
                assert!(original.matches_closed(&original.closed().unwrap()));
            }
            remote.close().await;
        }
    })
    .await
    .unwrap();
}

async fn datagrams(
    client: &Running,
) -> (
    zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration,
    zero_platform_tokio::controlled_carrier::CarrierOriginObservation,
) {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpStream, UdpSocket},
    };
    use zero_core::Address;
    let mut control = TcpStream::connect(client.address).await.unwrap();
    control.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    control.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    control
        .write_all(&[5, 3, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut reply = [0; 10];
    control.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..4], &[5, 0, 0, 1]);
    let relay =
        std::net::SocketAddr::from(([127, 0, 0, 1], u16::from_be_bytes([reply[8], reply[9]])));
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let codec = socks5::udp::Socks5InboundUdpCodec;
    let mut original = None;
    for payload in [&b"first"[..], &b""[..], &b"reuse"[..]] {
        let wire = codec
            .encode_response_to_client(&Address::Ipv4([127, 0, 0, 1]), target.port(), payload)
            .unwrap();
        socket.send_to(&wire, relay).await.unwrap();
        let mut buffer = [0; 65536];
        let (size, sender) =
            tokio::time::timeout(Duration::from_secs(4), peer.recv_from(&mut buffer))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&buffer[..size], payload);
        let observed = capture(&client.proxy);
        if let Some((owner, native)) = &original {
            assert!(observed.0.same_owner(owner));
            assert!(observed.1.same_original(native));
        } else {
            original = Some(observed);
        }
        peer.send_to(payload, sender).await.unwrap();
        let (size, _) = tokio::time::timeout(Duration::from_secs(4), socket.recv_from(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        let (address, port, received) =
            codec.decode_response(&buffer[..size]).unwrap().into_parts();
        assert_eq!(address, Address::Ipv4([127, 0, 0, 1]));
        assert_eq!(port, target.port());
        assert_eq!(received.as_slice(), payload);
    }
    drop(socket);
    drop(control);
    original.unwrap()
}
