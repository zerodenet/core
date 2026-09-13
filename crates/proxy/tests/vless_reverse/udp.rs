use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
};
use zero_core::Address;

pub(super) async fn burst_and_unsolicited(socks: u16) {
    let mut control = TcpStream::connect(("127.0.0.1", socks)).await.unwrap();
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
    assert_eq!(reply[1], 0);
    let relay = ("127.0.0.1", u16::from_be_bytes([reply[8], reply[9]]));
    let remote = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let remote_port = remote.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let mut buffer = [0; 2048];
        let mut received = Vec::new();
        let mut peer = None;
        for _ in 0..8 {
            let (len, from) = remote.recv_from(&mut buffer).await.unwrap();
            received.push(buffer[..len].to_vec());
            peer = Some(from);
        }
        // Reply after collecting the whole burst, then send an extra packet.
        // A subscription per request would repeat the first response eight times.
        for payload in received {
            remote.send_to(&payload, peer.unwrap()).await.unwrap();
        }
        remote.send_to(b"unsolicited", peer.unwrap()).await.unwrap();
    });
    let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    for n in 0u8..8 {
        let packet =
            crate::support::build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), remote_port, &[n; 64])
                .unwrap();
        client.send_to(&packet, relay).await.unwrap();
    }
    let mut replies = Vec::new();
    let mut buffer = [0; 2048];
    for _ in 0..9 {
        let (len, _) = client.recv_from(&mut buffer).await.unwrap();
        let response = crate::support::parse_udp_packet(&buffer[..len]).unwrap();
        assert_eq!(response.port, remote_port);
        replies.push(response.payload);
    }
    replies.sort();
    let mut expected: Vec<_> = (0u8..8).map(|n| vec![n; 64]).collect();
    expected.push(b"unsolicited".to_vec());
    expected.sort();
    assert_eq!(replies, expected);
    server.await.unwrap();
}
