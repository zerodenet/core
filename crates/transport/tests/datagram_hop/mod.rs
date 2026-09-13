use super::*;
use crate::OutboundDatagramSocketFactory;
use std::io::IoSliceMut;

#[tokio::test]
async fn port_hopping_rotates_sockets_keeps_previous_replies_and_releases_on_drop() {
    let server = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = server.local_addr().unwrap();
    let profile = Profile::new(OptionsRef {
        ports: &[peer.port()],
        interval_min_secs: 5,
        interval_max_secs: 5,
    })
    .unwrap();
    let socket = OutboundDatagramSocketFactory::new(Default::default())
        .with_hopping(Some(profile))
        .open_socket(peer)
        .await
        .unwrap();
    async fn send(
        socket: &Arc<dyn quinn::AsyncUdpSocket>,
        server: &tokio::net::UdpSocket,
        peer: SocketAddr,
    ) -> SocketAddr {
        datagram_queue::send(
            socket,
            Packet {
                bytes: b"ping".to_vec(),
                peer,
            },
        )
        .await
        .unwrap();
        let mut bytes = [0; 16];
        let (size, source) =
            tokio::time::timeout(Duration::from_secs(1), server.recv_from(&mut bytes))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&bytes[..size], b"ping");
        source
    }
    async fn receive(socket: &Arc<dyn quinn::AsyncUdpSocket>, peer: SocketAddr) {
        let mut bytes = [0; 32];
        let mut meta = [quinn::udp::RecvMeta::default()];
        tokio::time::timeout(
            Duration::from_secs(1),
            std::future::poll_fn(|cx| {
                socket.poll_recv(cx, &mut [IoSliceMut::new(&mut bytes)], &mut meta)
            }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(meta[0].addr, peer);
        assert_eq!(&bytes[..meta[0].len], b"late reply");
    }
    let first = send(&socket, &server, peer).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(5020)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();
    let second = send(&socket, &server, peer).await;
    assert_ne!(first.port(), second.port());
    server.send_to(b"late reply", first).await.unwrap();
    receive(&socket, peer).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(5020)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();
    let third = send(&socket, &server, peer).await;
    assert_ne!(second.port(), third.port());
    assert!(
        std::net::UdpSocket::bind(first).is_ok(),
        "only one previous socket is retained"
    );
    drop(socket);
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(std::net::UdpSocket::bind(second).is_ok());
    assert!(std::net::UdpSocket::bind(third).is_ok());
}
#[tokio::test]
async fn hopping_rejects_a_relay_and_invalid_interval_before_opening_any_socket() {
    assert!(Profile::new(OptionsRef {
        ports: &[1],
        interval_min_secs: 4,
        interval_max_secs: 6
    })
    .is_err());
    assert!(Profile::new(OptionsRef {
        ports: &[0],
        ..Default::default()
    })
    .is_err());
    let profile = Profile::new(OptionsRef {
        ports: &[443],
        ..Default::default()
    })
    .unwrap();
    let factory = OutboundDatagramSocketFactory::new(Default::default())
        .with_relay(Arc::new(|_| panic!("relay must not be opened")))
        .with_hopping(Some(profile));
    assert_eq!(
        factory
            .open_socket("127.0.0.1:443".parse().unwrap())
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
}
