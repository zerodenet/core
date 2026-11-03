//! Real TLS, independent wire vectors, concurrent MUX I/O and original cleanup.
use super::*;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

fn udp_packet(target: SocketAddr, payload: &[u8]) -> Vec<u8> {
    let std::net::IpAddr::V4(ip) = target.ip() else {
        panic!("IPv4 fixture")
    };
    let mut bytes = vec![ATYP_IPV4];
    bytes.extend(ip.octets());
    bytes.extend(target.port().to_be_bytes());
    bytes.extend((payload.len() as u16).to_be_bytes());
    bytes.extend(b"\r\n");
    bytes.extend(payload);
    bytes
}
async fn udp_reply(client: &mut (impl tokio::io::AsyncRead + Unpin)) -> (SocketAddr, Vec<u8>) {
    let mut header = [0; 11];
    client.read_exact(&mut header).await.unwrap();
    assert_eq!(header[0], ATYP_IPV4);
    assert_eq!(&header[9..], b"\r\n");
    let target = SocketAddr::from((
        [header[1], header[2], header[3], header[4]],
        u16::from_be_bytes([header[5], header[6]]),
    ));
    let mut payload = vec![0; u16::from_be_bytes([header[7], header[8]]) as usize];
    client.read_exact(&mut payload).await.unwrap();
    (target, payload)
}
#[tokio::test]
async fn standard_udp_relative_tls_preserves_empty_packets_partial_reads_targets_and_accounting() {
    timeout(Duration::from_secs(15), async {
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let fixture = Fixture::relative_tls();
        let (mut client, source) = fixture.connect().await;
        client
            .write_all(&request(PASSWORD, 3, address))
            .await
            .unwrap();
        client
            .write_all(&udp_packet(address, b"one"))
            .await
            .unwrap();
        let mut bytes = [0; 32];
        let (size, sender) = target.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], b"one");
        let next = udp_packet(address, b"two");
        client.write_all(&next[..5]).await.unwrap(); // actual second frame is incomplete
        target.send_to(b"one", sender).await.unwrap();
        assert_eq!(udp_reply(&mut client).await, (address, b"one".to_vec()));
        client.write_all(&next[5..]).await.unwrap();
        let (size, second) = target.recv_from(&mut bytes).await.unwrap();
        assert_eq!(second, sender, "same real dispatcher/egress");
        assert_eq!(&bytes[..size], b"two");
        target.send_to(b"two", sender).await.unwrap();
        assert_eq!(udp_reply(&mut client).await, (address, b"two".to_vec()));
        client.write_all(&udp_packet(address, b"")).await.unwrap();
        let (size, empty) = target.recv_from(&mut bytes).await.unwrap();
        assert_eq!(empty, sender);
        assert_eq!(size, 0);
        target.send_to(b"", sender).await.unwrap();
        assert_eq!(udp_reply(&mut client).await, (address, Vec::new()));
        fixture.running.shutdown().await.unwrap();
        let completed = fixture.probe.completed_sessions();
        let flow = completed
            .iter()
            .find(|flow| flow.source_port == Some(source.port()))
            .unwrap();
        assert_eq!(
            (flow.bytes_up, flow.bytes_down),
            (6, 39),
            "existing payload upload and framed response accounting"
        );
    })
    .await
    .unwrap();
}

fn mux_request() -> Vec<u8> {
    let mut bytes = request(PASSWORD, CMD_TCP, SocketAddr::from(([127, 0, 0, 1], 9)));
    bytes.truncate(58);
    bytes.extend([CMD_TCP, 3, 11]);
    bytes.extend(b"v1.mux.cool");
    bytes.extend(666_u16.to_be_bytes());
    bytes.extend(b"\r\n");
    bytes
}
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
        meta.push(ATYP_IPV4);
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
async fn standard_mux_keeps_tcp_and_udp_concurrent_and_reuses_udp_custody_on_one_tls_connection() {
    let progress = std::cell::Cell::new((0_u16, "TLS connect"));
    timeout(Duration::from_secs(25), async {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_address = tcp.local_addr().unwrap();
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let fixture = Fixture::new(None, false);
        let (mut client, _) = fixture.connect().await;
        client.write_all(&mux_request()).await.unwrap();
        client
            .write_all(&frame(1, 1, Some((tcp_address, 1)), b"tcp"))
            .await
            .unwrap();
        progress.set((1, "TCP accept"));
        let (mut actual, _) = tcp.accept().await.unwrap();
        let mut tcp_bytes = [0; 3];
        progress.set((1, "TCP upload"));
        actual.read_exact(&mut tcp_bytes).await.unwrap();
        assert_eq!(&tcp_bytes, b"tcp");
        // Keep TCP genuinely pending throughout repeated UDP logical streams.
        let mut seen = 0;
        for sid in 2..302 {
            client
                .write_all(&frame(sid, 1, Some((address, 2)), b"u"))
                .await
                .unwrap();
            let mut bytes = [0; 8];
            progress.set((sid, "UDP ingress"));
            let (size, sender) = target.recv_from(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], b"u");
            target.send_to(b"u", sender).await.unwrap();
            progress.set((sid, "UDP response"));
            loop {
                let (id, status, payload) = response(&mut client).await;
                if id == sid && status == 2 {
                    assert_eq!(payload, b"u");
                    break;
                }
            }
            client.write_all(&frame(sid, 3, None, b"")).await.unwrap();
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
                .all(|flow| flow.protocol.as_str() == "trojan"),
            "no synthetic protocol labels in public completed flows"
        );
        let stream = completed
            .iter()
            .find(|flow| flow.port == tcp_address.port())
            .unwrap();
        assert_eq!(
            stream.protocol.as_str(),
            "trojan",
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
