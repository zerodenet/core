use super::*;
use original::Original;
async fn open(client: &Running, target: SocketAddr) -> TcpStream {
    let mut io = TcpStream::connect(client.address).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend(target.port().to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    if reply[1] != 0 {
        crate::component::stream_bridge::capacity_debug::books(&client.proxy);
    }
    assert_eq!(reply[1], 0);
    io
}
pub(super) async fn tcp(client: &Running, borrowed: bool) -> Original {
    timeout(Duration::from_secs(8), async {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let payload: Vec<_> = (0..262144).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();
        let peer = tokio::spawn(async move {
            let (mut io, _) = target.accept().await.unwrap();
            let mut body = Vec::new();
            io.read_to_end(&mut body).await.unwrap();
            assert_eq!(body, expected);
            io.write_all(&body).await.unwrap();
            io.write_all(&[73; 1025]).await.unwrap();
            io.shutdown().await.unwrap();
        });
        let mut io = open(client, address).await;
        io.write_all(&payload).await.unwrap();
        io.flush().await.unwrap();
        let original = Original::capture(client, borrowed).await;
        // No application reads are needed to drive QUIC transmission/credit.
        io.shutdown().await.unwrap();
        let mut body = Vec::new();
        io.read_to_end(&mut body).await.unwrap();
        assert_eq!(body.len(), payload.len() + 1025);
        assert_eq!(&body[..payload.len()], &payload);
        assert_eq!(&body[payload.len()..], &[73; 1025]);
        drop(io);
        peer.await.unwrap();
        assert!(client.proxy.engine().stats_snapshot().bytes_up >= 262144);
        assert!(client.proxy.engine().stats_snapshot().bytes_down >= 263169);
        original
    })
    .await
    .unwrap()
}
pub(super) async fn udp(client: &Running, borrowed: bool) {
    timeout(Duration::from_secs(8), async {
        let (control, socket, relay) = super::super::udp::associate(client.address).await;
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = peer.local_addr().unwrap();
        let codec = socks5::udp::Socks5InboundUdpCodec;
        let mut original = None;
        let mut previous = None;
        for payload in [&b""[..], &b"same-original"[..], &b""[..]] {
            let wire = codec
                .encode_response_to_client(
                    &zero_core::Address::Ipv4([127, 0, 0, 1]),
                    target.port(),
                    payload,
                )
                .unwrap();
            socket.send_to(&wire, relay).await.unwrap();
            let mut bytes = [0; 65536];
            let (size, sender) = peer.recv_from(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], payload);
            if let Some(previous) = previous {
                assert_eq!(previous, sender);
            }
            previous = Some(sender);
            let actual = Original::capture(client, borrowed).await;
            if let Some(original) = &original {
                assert!(actual.same(original));
            } else {
                original = Some(actual);
            }
            peer.send_to(payload, sender).await.unwrap();
            let (size, _) = socket.recv_from(&mut bytes).await.unwrap();
            let (address, port, received) =
                codec.decode_response(&bytes[..size]).unwrap().into_parts();
            assert_eq!(address, zero_core::Address::Ipv4([127, 0, 0, 1]));
            assert_eq!(port, target.port());
            assert_eq!(received.as_slice(), payload);
        }
        drop(socket);
        drop(control);
        original.unwrap().returned().await;
    })
    .await
    .unwrap();
}
