//! Concurrent real Stream and packet/raw Datagram over one TLS/AEAD MUX.
use super::*;
fn frame(sid: u16, status: u8, target: Option<(SocketAddr, u8)>, payload: &[u8]) -> Vec<u8> {
    let mut meta = sid.to_be_bytes().to_vec();
    meta.push(status);
    meta.push(u8::from(!payload.is_empty()));
    if let Some((target, network)) = target {
        let std::net::IpAddr::V4(ip) = target.ip() else {
            panic!("IPv4 fixture")
        };
        meta.push(network);
        meta.extend(target.port().to_be_bytes());
        meta.push(1);
        meta.extend(ip.octets());
    }
    let mut bytes = (meta.len() as u16).to_be_bytes().to_vec();
    bytes.extend(meta);
    if !payload.is_empty() {
        bytes.extend((payload.len() as u16).to_be_bytes());
        bytes.extend(payload);
    }
    bytes
}
async fn response(client: &mut (impl tokio::io::AsyncRead + Unpin)) -> (u16, u8, Vec<u8>) {
    let length = client.read_u16().await.unwrap();
    let mut meta = vec![0; length as usize];
    client.read_exact(&mut meta).await.unwrap();
    assert!(meta.len() >= 4);
    let mut payload = Vec::new();
    if meta[3] & 1 != 0 {
        let length = client.read_u16().await.unwrap();
        payload.resize(length as usize, 0);
        client.read_exact(&mut payload).await.unwrap();
    }
    (u16::from_be_bytes([meta[0], meta[1]]), meta[2], payload)
}
#[tokio::test]
async fn standard_mux_preserves_packet_and_raw_state_and_reuses_actual_custody_while_tcp_stays_live(
) {
    let progress = std::cell::Cell::new((0_u16, "TLS connect"));
    timeout(Duration::from_secs(25), async {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_address = tcp.local_addr().unwrap();
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let fixture = Fixture::datagram();
        let request = zero_core::Session::new(
            902,
            zero_core::Address::Domain("v1.mux.cool".into()),
            666,
            zero_core::Network::Tcp,
            zero_core::ProtocolType::new("client"),
        );
        let (tls, _) = fixture.connect().await;
        let (mut client, _) = ::vmess::outbound::PreparedVmessOutboundRequestBundle::from_config(
            USER_ID,
            "chacha20-poly1305",
            None,
        )
        .unwrap()
        .establish_tcp_outbound_stream(zero_platform_tokio::TcpRelayStream::new(tls), &request)
        .await
        .unwrap();
        client
            .write_all(&frame(1, 1, Some((tcp_address, 1)), b"tcp"))
            .await
            .unwrap();
        client.flush().await.unwrap();
        progress.set((1, "TCP accept"));
        let (mut actual, _) = tcp.accept().await.unwrap();
        let mut tcp_bytes = [0; 3];
        progress.set((1, "TCP upload"));
        actual.read_exact(&mut tcp_bytes).await.unwrap();
        assert_eq!(&tcp_bytes, b"tcp");
        // Keep TCP genuinely pending throughout repeated UDP logical streams.
        let mut seen = 0;
        for sid in 2..302 {
            let wire = if sid % 2 == 0 {
                packet(address, b"u")
            } else {
                b"u".to_vec()
            };
            client
                .write_all(&frame(sid, 1, Some((address, 2)), &wire))
                .await
                .unwrap();
            client.flush().await.unwrap();
            let mut bytes = [0; 8];
            progress.set((sid, "UDP ingress"));
            let (size, sender) = target.recv_from(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], b"u");
            target.send_to(b"u", sender).await.unwrap();
            progress.set((sid, "UDP response"));
            loop {
                let (id, status, payload) = response(&mut client).await;
                if id == sid && status == 2 {
                    assert_eq!(payload, wire);
                    break;
                }
            }
            client.write_all(&frame(sid, 3, None, b"")).await.unwrap();
            client.flush().await.unwrap();
            seen += 1;
        }
        assert_eq!(
            seen, 300,
            "more than the finite per-entry custody count, no lifetime growth"
        );
        progress.set((1, "TCP reverse"));
        actual.write_all(b"tcp").await.unwrap();
        loop {
            let (id, status, payload) = response(&mut client).await;
            if id == 1 && status == 2 {
                assert_eq!(payload, b"tcp");
                break;
            }
        }
        client.write_all(&frame(1, 3, None, b"")).await.unwrap();
        client.flush().await.unwrap();
        progress.set((1, "TCP END"));
        assert_eq!(
            actual.read(&mut [0]).await.unwrap(),
            0,
            "wire END closes actual upstream without waiting for reverse EOF"
        );
        drop(actual);
        progress.set((0, "host shutdown"));
        fixture.running.shutdown().await.unwrap();
        let completed = fixture.probe.completed_sessions();
        assert_eq!(
            completed.len(),
            256,
            "preserve the existing bounded history"
        );
        assert!(
            completed
                .iter()
                .all(|flow| flow.protocol.as_str() == "vmess"),
            "no synthetic protocol labels in public completed flows"
        );
        let stream = completed
            .iter()
            .find(|flow| flow.network == zero_core::Network::Tcp && flow.port == tcp_address.port())
            .unwrap();
        assert_eq!(
            stream.protocol.as_str(),
            "vmess",
            "preserve the existing public protocol label"
        );
        assert_eq!(
            (stream.bytes_up, stream.bytes_down),
            (3, 3),
            "logical TCP payload accounting excludes MUX frames"
        );
    })
    .await
    .unwrap_or_else(|error| panic!("{error} at actual {:?}", progress.get()));
}
