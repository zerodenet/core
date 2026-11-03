//! Authentication survives the association boundary and cached UDP dispatch.
use crate::support::{
    build_udp_packet, parse_udp_packet, spawn_engine, wait_for, wait_for_listener,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::{timeout, Duration},
};
use zero_api::{event_type, EventFilter, EventSource};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_proxy::Proxy;

const PROTOCOLS: &[&str] = &[
    "socks5",
    #[cfg(feature = "mixed")]
    "mixed",
];

#[tokio::test]
async fn authenticated_socks_and_mixed_udp_keep_principal_rates_and_single_control_accounting() {
    for protocol in PROTOCOLS {
        for principal in [None, Some("configured:udp-alice")] {
            exercise(protocol, true, principal).await;
        }
    }
}
#[tokio::test]
async fn unauthenticated_socks_and_mixed_udp_keep_existing_wire_and_accounting() {
    for protocol in PROTOCOLS {
        exercise(protocol, false, None).await;
    }
}
async fn exercise(protocol: &str, authenticated: bool, principal: Option<&str>) {
    timeout(Duration::from_secs(6), async {
        let echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = echo.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut buf = [0;128];
            let mut original = None;
            for _ in 0..2 {
                let (size, peer) = echo.recv_from(&mut buf).await.unwrap();
                if let Some(previous) = original { assert_eq!(previous, peer, "same admitted flow reuses its actual UDP socket"); }
                original = Some(peer);
                echo.send_to(&buf[..size], peer).await.unwrap();
            }
        });
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = reservation.local_addr().unwrap().port();
        let users = if authenticated { serde_json::json!([{
            "username":"alice", "password":"secret", "principal_key":principal,
            "up_bps":65536, "down_bps":32768
        }]) } else { serde_json::json!([]) };
        let config = RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"udp-in", "listen":{"address":"127.0.0.1","port":port},
                "protocol": if protocol == "mixed" { serde_json::json!({"type":"mixed","socks5_users":users}) }
                    else { serde_json::json!({"type":"socks5","users":users}) }}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let probe = proxy.clone();
        drop(reservation);
        let running = spawn_engine(proxy);
        wait_for_listener(port).await;
        let mut control = TcpStream::connect(("127.0.0.1",port)).await.unwrap();
        control.write_all(&[5,1, if authenticated {2} else {0}]).await.unwrap();
        let mut selected = [0;2];
        control.read_exact(&mut selected).await.unwrap();
        assert_eq!(selected, [5,if authenticated {2} else {0}]);
        if authenticated {
            control.write_all(b"\x01\x05alice\x06secret").await.unwrap();
            control.read_exact(&mut selected).await.unwrap();
            assert_eq!(selected, [1,0]);
        }
        control.write_all(&[5,3,0,1,0,0,0,0,0,0]).await.unwrap();
        let mut associated = [0;10];
        control.read_exact(&mut associated).await.unwrap();
        assert_eq!(&associated[..4], &[5,0,0,1]);
        let relay = std::net::SocketAddr::from(([127,0,0,1], u16::from_be_bytes([associated[8],associated[9]])));
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for payload in [b"ping",b"pong"] {
            let wire = build_udp_packet(&Address::Ipv4([127,0,0,1]), target.port(), payload).unwrap();
            client.send_to(&wire,relay).await.unwrap();
            let mut buf = [0;128];
            let (size,_) = client.recv_from(&mut buf).await.unwrap();
            let reply = parse_udp_packet(&buf[..size]).unwrap();
            assert_eq!(reply.target,Address::Ipv4([127,0,0,1]));
            assert_eq!(reply.port,target.port());
            assert_eq!(reply.payload,payload);
        }
        drop(control);
        server.await.unwrap();
        wait_for("UDP association settles its actual flow", || !probe.completed_sessions().is_empty()).await;
        let completed = probe.completed_sessions();
        assert_eq!(completed.len(),1);
        let flow = &completed[0];
        assert_eq!(flow.protocol.as_str(),"socks5");
        assert_eq!(flow.source_port,Some(client.local_addr().unwrap().port()));
        assert_eq!(flow.auth.is_some(),authenticated);
        if let Some(auth) = &flow.auth {
            assert_eq!(auth.scheme,"socks5");
            assert_eq!(auth.principal_key.as_deref(),principal);
            assert_eq!((auth.up_bps,auth.down_bps),(Some(65536),Some(32768)));
        }
        // Two 4-byte payloads, two 10-byte UDP headers, and the actual
        // negotiation counted once on the first admitted flow.
        assert_eq!(flow.bytes_up, 28 + if authenticated {27} else {13});
        assert_eq!(flow.bytes_down, 28 + if authenticated {14} else {12});
        let events = probe.latest(usize::MAX,EventFilter {
            event_types:vec![event_type::FLOW_STARTED.into(),event_type::FLOW_COMPLETED.into()],
            ..Default::default()
        }).unwrap();
        assert_eq!(events.len(),2);
        assert!(events.iter().all(|event| event.principal_key.as_deref()==principal));
        running.shutdown().await.unwrap();
    }).await.unwrap();
}
