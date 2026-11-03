//! Actual standard inbound framing, one-native egress budget and multi-target I/O.
use crate::support::{
    build_udp_packet, parse_udp_packet, spawn_engine, wait_for, wait_for_listener,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::{timeout, Duration},
};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_proxy::Proxy;

#[tokio::test]
async fn standard_socks_and_mixed_udp_keep_zero_packets_targets_reuse_and_recoverable_frame_errors()
{
    for protocol in [
        "socks5",
        #[cfg(feature = "mixed")]
        "mixed",
    ] {
        timeout(Duration::from_secs(8), async {
            let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let targets = [a.local_addr().unwrap(), b.local_addr().unwrap()];
            let echo = |socket: UdpSocket| tokio::spawn(async move {
                let mut bytes = [0;128];
                let (size, source) = socket.recv_from(&mut bytes).await.unwrap();
                socket.send_to(&bytes[..size], source).await.unwrap();
                source
            });
            let (a, b) = (echo(a), echo(b));
            let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = reservation.local_addr().unwrap().port();
            let config = RuntimeConfig::parse(&serde_json::json!({
                "runtime":{"udp_direct_max_sockets":1},
                "inbounds":[{"tag":"standard", "listen":{"address":"127.0.0.1","port":port}, "protocol":{"type":protocol}}],
                "route":{"final":{"type":"direct"}}
            }).to_string()).unwrap();
            let proxy = Proxy::new(config).unwrap(); let probe = proxy.clone();
            drop(reservation);
            let running = spawn_engine(proxy);
            wait_for_listener(port).await;
            let mut control = TcpStream::connect(("127.0.0.1",port)).await.unwrap();
            // Retain a real socket on the preferred source port before the
            // server binds anything. The one-native owner must confirm the
            // failed preferred original before its ephemeral fallback attempt.
            let client = UdpSocket::bind(control.local_addr().unwrap()).await.unwrap();
            control.write_all(&[5,1,0]).await.unwrap();
            let mut method = [0;2]; control.read_exact(&mut method).await.unwrap(); assert_eq!(method,[5,0]);
            control.write_all(&[5,3,0,1,0,0,0,0,0,0]).await.unwrap();
            let mut reply = [0;10]; control.read_exact(&mut reply).await.unwrap(); assert_eq!(&reply[..4], &[5,0,0,1]);
            let relay = std::net::SocketAddr::from(([127,0,0,1],u16::from_be_bytes([reply[8],reply[9]])));
            client.send_to(&[0,0,1],relay).await.unwrap(); // A malformed packet must not end the association.
            for (target,payload) in targets.into_iter().zip([&b""[..],&b"second"[..]]) {
                client.send_to(&build_udp_packet(&Address::Ipv4([127,0,0,1]),target.port(),payload).unwrap(),relay).await.unwrap();
                let mut buffer = [0;128]; let (size,_) = client.recv_from(&mut buffer).await.unwrap();
                let response = parse_udp_packet(&buffer[..size]).unwrap();
                assert_eq!(response.target,Address::Ipv4([127,0,0,1])); assert_eq!(response.port,target.port()); assert_eq!(response.payload,payload);
            }
            assert_eq!(a.await.unwrap(),b.await.unwrap(),"different targets reuse the same actual native socket");
            drop(control);
            wait_for("both actual UDP flows complete", || probe.completed_sessions().len()==2).await;
            let completed = probe.completed_sessions();
            assert_eq!(completed.iter().map(|flow|flow.bytes_up).sum::<u64>(),39);
            assert_eq!(completed.iter().map(|flow|flow.bytes_down).sum::<u64>(),38);
            assert!(completed.iter().all(|flow| flow.source_port==Some(client.local_addr().unwrap().port())));
            running.shutdown().await.unwrap(); // Actual host/native/Scope confirmation.
        }).await.unwrap();
    }
}
