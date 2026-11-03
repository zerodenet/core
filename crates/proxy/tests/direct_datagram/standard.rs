//! Actual paired standard sources, traffic accounting and confirmed resource replacement.
use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use zero_engine::EngineHandle;
use zero_proxy::ProxyHandle;

fn config(host: &str, port: u16, target: std::net::SocketAddr) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"entry","listen":{"address":host,"port":port},
            "protocol":{"type":"direct","target":target.ip().to_string(),"port":target.port()}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
async fn connect(host: &str, port: u16) -> TcpStream {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(stream) = TcpStream::connect((host.trim_matches(['[', ']']), port)).await {
                return stream;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("standard paired source ready")
}
async fn exchange(
    client: &UdpSocket,
    entry: std::net::SocketAddr,
    target: &UdpSocket,
    payload: &[u8],
) -> std::net::SocketAddr {
    client.send_to(payload, entry).await.unwrap();
    let mut bytes = [0; 64];
    let (size, upstream) =
        tokio::time::timeout(Duration::from_secs(3), target.recv_from(&mut bytes))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&bytes[..size], payload);
    target.send_to(payload, upstream).await.unwrap();
    let (size, sender) = tokio::time::timeout(Duration::from_secs(3), client.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..size], payload);
    assert_eq!(sender, entry, "same actual TCP/UDP listening endpoint");
    upstream
}

#[tokio::test]
async fn standard_direct_pair_preserves_empty_packets_sources_and_accounting_for_literal_and_domain_listens(
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        for host in ["127.0.0.1", "localhost", "[::1]"] {
            let tcp_target = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let target = tcp_target.local_addr().unwrap();
            let udp_target = UdpSocket::bind(target).await.unwrap();
            let reserve = TcpListener::bind(if host == "[::1]" {
                "[::1]:0"
            } else {
                "127.0.0.1:0"
            })
            .await
            .unwrap();
            let port = reserve.local_addr().unwrap().port();
            let proxy = Proxy::new(config(host, port, target)).unwrap();
            let observe = proxy.clone();
            drop(reserve);
            let running = spawn_engine(proxy);
            let mut tcp_client = connect(host, port).await;
            let entry = tcp_client.peer_addr().unwrap();
            let tcp_source = tcp_client.local_addr().unwrap();
            tcp_client.write_all(b"tcp").await.unwrap();
            let (mut tcp_peer, _) = tcp_target.accept().await.unwrap();
            let mut payload = [0; 3];
            tcp_peer.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, b"tcp");
            tcp_peer.write_all(b"back").await.unwrap();
            let mut reply = [0; 4];
            tcp_client.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"back");
            let udp_client = UdpSocket::bind((entry.ip(), 0)).await.unwrap();
            let udp_source = udp_client.local_addr().unwrap();
            let first = exchange(&udp_client, entry, &udp_target, b"").await;
            let next = exchange(&udp_client, entry, &udp_target, b"udp").await;
            assert_eq!(first, next, "empty packet keeps its real peer mapping");
            running.shutdown().await.unwrap();
            let sessions = observe.completed_sessions();
            let tcp = sessions
                .iter()
                .find(|s| s.source_port == Some(tcp_source.port()))
                .expect("TCP session");
            let udp = sessions
                .iter()
                .find(|s| s.source_port == Some(udp_source.port()))
                .expect("UDP session");
            assert_eq!((tcp.bytes_up, tcp.bytes_down), (3, 4));
            assert_eq!((udp.bytes_up, udp.bytes_down), (3, 3));
            assert_eq!(sessions.len(), 2, "one business identity per actual flow");
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn standard_direct_pair_replacement_retires_old_udp_mapping_and_tcp_before_new_target() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let old_tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let old_address = old_tcp.local_addr().unwrap();
        let old_udp = UdpSocket::bind(old_address).await.unwrap();
        let new_tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let new_address = new_tcp.local_addr().unwrap();
        let new_udp = UdpSocket::bind(new_address).await.unwrap();
        let reserve = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let entry = reserve.local_addr().unwrap();
        let proxy = Proxy::new(config("127.0.0.1", entry.port(), old_address)).unwrap();
        let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
        drop(reserve);
        let running = spawn_engine(proxy);
        let mut old = connect("127.0.0.1", entry.port()).await;
        old.write_all(b"old").await.unwrap();
        let (mut peer, _) = old_tcp.accept().await.unwrap();
        let mut bytes = [0; 3];
        peer.read_exact(&mut bytes).await.unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let old_mapping = exchange(&client, entry, &old_udp, b"old").await;
        // The real old TCP read is pending at I/O, not just a delay or marker.
        {
            use std::{future::Future, task::Poll};
            let pending = old.read(&mut bytes);
            tokio::pin!(pending);
            std::future::poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        handle
            .apply_config_and_wait(
                config("127.0.0.1", entry.port(), new_address),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert_eq!(old.read(&mut bytes).await.unwrap(), 0);
        assert_eq!(peer.read(&mut bytes).await.unwrap(), 0);
        old_udp.send_to(b"late", old_mapping).await.unwrap();
        exchange(&client, entry, &new_udp, b"new").await;
        // A successful new exchange alone is not native-close evidence. Native
        // providers' original receipts are tested separately by owner regressions.
        running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
