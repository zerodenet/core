//! Same ordinary Source/transport/native owner for real UDP packet views.
use super::fixture::*;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    time::timeout,
};
use zero_core::Address;

async fn associate(endpoint: SocketAddr) -> (TcpStream, UdpSocket, SocketAddr) {
    let mut control = TcpStream::connect(endpoint).await.unwrap();
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
    let relay = SocketAddr::from(([127, 0, 0, 1], u16::from_be_bytes([reply[8], reply[9]])));
    (
        control,
        UdpSocket::bind("127.0.0.1:0").await.unwrap(),
        relay,
    )
}

#[tokio::test]
async fn ordinary_h2_and_upgrade_datagrams_return_exact_payloads_and_keep_one_original() {
    timeout(Duration::from_secs(30), async {
        for kind in ["h2", "http_upgrade"] {
            for backend in ["rustls", "openssl"] {
                let directory = tempfile::tempdir().unwrap();
                let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
                let cert_path = directory.path().join("server.pem");
                let key_path = directory.path().join("server.key");
                let ca_path = directory.path().join("client.pem");
                std::fs::write(&cert_path, cert.cert.pem()).unwrap();
                std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
                std::fs::write(&ca_path, cert.cert.pem()).unwrap();
                let remote = server(kind, &cert_path, &key_path).await;
                let client = client(kind, backend, remote.address, &ca_path).await;
                let (control, socket, relay) = associate(client.address).await;
                let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let target = peer.local_addr().unwrap();
                let codec = socks5::udp::Socks5InboundUdpCodec;
                let mut original = None;
                let mut previous_sender = None;
                for payload in [&b"first"[..], &b""[..], &b"same-association"[..]] {
                    let wire = codec
                        .encode_response_to_client(
                            &Address::Ipv4([127, 0, 0, 1]),
                            target.port(),
                            payload,
                        )
                        .unwrap();
                    socket.send_to(&wire, relay).await.unwrap();
                    let mut buffer = [0; 65536];
                    let (size, sender) =
                        timeout(Duration::from_secs(4), peer.recv_from(&mut buffer))
                            .await
                            .unwrap()
                            .unwrap();
                    assert_eq!(&buffer[..size], payload);
                    if let Some(previous) = previous_sender {
                        assert_eq!(previous, sender);
                    }
                    previous_sender = Some(sender);
                    let captured = capture(&client.proxy);
                    if let Some((owner, native)) = &original {
                        assert!(captured.0.same_owner(owner));
                        assert!(captured.1.same_original(native));
                    } else {
                        original = Some(captured);
                    }
                    peer.send_to(payload, sender).await.unwrap();
                    let (size, _) = timeout(Duration::from_secs(4), socket.recv_from(&mut buffer))
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
                client.close().await;
                let (owner, native) = original.unwrap();
                let closed = native
                    .closed()
                    .expect("the same UDP transport original actually closed");
                assert!(native.matches_closed(&closed) && closed.had_native_acquisition());
                assert!(owner.closed().unwrap().matches(&owner));
                assert!(owner.users_closed().unwrap().matches(&owner));
                remote.close().await;
            }
        }
    })
    .await
    .unwrap();
}
