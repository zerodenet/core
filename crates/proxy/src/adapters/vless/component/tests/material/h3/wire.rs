//! Real upload EOF followed by full reverse bytes, with no application reads.
use super::*;
pub(super) struct Exchange {
    io: TcpStream,
    peer: tokio::task::JoinHandle<()>,
    payload: Vec<u8>,
}
impl Exchange {
    pub(super) async fn open(client: &Running) -> Self {
        Self::open_with_reply(client, true).await
    }
    pub(super) async fn discarded(client: &Running) -> Self {
        Self::open_with_reply(client, false).await
    }
    async fn open_with_reply(client: &Running, reply: bool) -> Self {
        timeout(Duration::from_secs(8), async {
            let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = target.local_addr().unwrap();
            let payload: Vec<_> = (0..262144).map(|i| (i % 251) as u8).collect();
            let expected = payload.clone();
            let (accepted, received) = tokio::sync::oneshot::channel();
            let peer = tokio::spawn(async move {
                let (mut io, _) = target.accept().await.unwrap();
                let mut body = vec![0; expected.len()];
                io.read_exact(&mut body).await.unwrap();
                assert!(
                    body == expected,
                    "actual complete target upload differs at {:?}",
                    body.iter().zip(&expected).position(|(a, b)| a != b)
                );
                accepted.send(()).unwrap();
                let mut tail = Vec::new();
                io.read_to_end(&mut tail).await.unwrap();
                assert!(tail.is_empty());
                if reply {
                    io.write_all(&body).await.unwrap();
                    io.write_all(&[73; 1025]).await.unwrap();
                    io.shutdown().await.unwrap();
                }
            });
            let mut io = open(client, address).await;
            io.write_all(&payload).await.unwrap();
            io.flush().await.unwrap();
            received.await.unwrap();
            Self { io, peer, payload }
        })
        .await
        .unwrap()
    }
    pub(super) async fn abandon(self) {
        drop(self.io);
        timeout(Duration::from_secs(8), self.peer)
            .await
            .unwrap()
            .unwrap();
    }
    pub(super) async fn finish(mut self) {
        timeout(Duration::from_secs(8), async {
            self.io.shutdown().await.unwrap();
            let mut body = Vec::new();
            self.io.read_to_end(&mut body).await.unwrap();
            assert_eq!(body.len(), self.payload.len() + 1025);
            assert!(
                body[..self.payload.len()] == self.payload,
                "actual reverse DATA first difference {:?}",
                body.iter().zip(&self.payload).position(|(a, b)| a != b)
            );
            assert!(body[self.payload.len()..] == [73; 1025]);
            drop(self.io);
            self.peer.await.unwrap();
        })
        .await
        .unwrap();
    }
}
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
    assert_eq!(reply[1], 0);
    io
}
pub(super) async fn udp(client: &Running, original: &Original, borrowed: bool) {
    timeout(Duration::from_secs(8), async {
        let (control, socket, relay) = super::super::udp::associate(client.address).await;
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = peer.local_addr().unwrap();
        let codec = socks5::udp::Socks5InboundUdpCodec;
        let mut previous = None;
        for payload in [
            b"".as_slice(),
            b"same real H3 original".as_slice(),
            b"".as_slice(),
        ] {
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
                assert_eq!(sender, previous);
            }
            previous = Some(sender);
            assert!(Original::capture(client, borrowed).await.same(original));
            peer.send_to(payload, sender).await.unwrap();
            let (size, _) = socket.recv_from(&mut bytes).await.unwrap();
            let (address, port, body) = codec.decode_response(&bytes[..size]).unwrap().into_parts();
            assert_eq!(address, zero_core::Address::Ipv4([127, 0, 0, 1]));
            assert_eq!(port, target.port());
            assert_eq!(body.as_slice(), payload);
        }
        drop(socket);
        drop(control);
    })
    .await
    .unwrap();
}
