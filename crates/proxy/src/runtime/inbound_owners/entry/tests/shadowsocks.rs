//! Real native TCP/UDP owners and Scope receipts across acknowledged replacement.
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};
use zero_traits::DatagramCodec;

#[tokio::test]
async fn standard_native_source_confirms_originals_before_replacement_and_closes_real_tcp_udp() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let reservation = crate::socket_fixture::tcp_udp();
        let endpoint = reservation.0.local_addr().unwrap();
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"ss-native", "listen":{"address":"127.0.0.1","port":endpoint.port()},
                "protocol":{"type":"shadowsocks","cipher":"aes-128-gcm","users":[{"password":"native-user","principal_key":"native-principal","policy_revision":7}]}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = crate::Proxy::new(config).unwrap();
        drop(reservation);
        let running = proxy.spawn();
        let original = loop {
            if let Some(owner) = proxy.inbound_owners.owner("ss-native") { break owner; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        original.wait_ready().await.unwrap();
        let identity = original.identity();
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let codec = ::shadowsocks::udp::ShadowsocksDatagramCodec::new(::shadowsocks::CipherKind::Aes128Gcm, "native-user");
        let wire = codec.encode(&zero_core::Address::Ipv4([127,0,0,1]), address.port(), b"before").unwrap();
        client.send_to(&wire, endpoint).await.unwrap();
        let mut bytes = [0;1024];
        let (size, peer) = target.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], b"before");
        target.send_to(b"reply", peer).await.unwrap();
        let size = client.recv(&mut bytes).await.unwrap();
        let encoded_reply_bytes = size as u64;
        assert_eq!(codec.decode(&bytes[..size]).unwrap().2, b"reply");
        // Keep the live UDP business peer open while applying replacement.
        let recv = target.recv_from(&mut bytes); tokio::pin!(recv);
        std::future::poll_fn(|cx| { assert!(std::future::Future::poll(recv.as_mut(),cx).is_pending()); std::task::Poll::Ready(()) }).await;
        assert!(original.terminal().is_none());
        let reserved = crate::socket_fixture::tcp_udp();
        let replacement = reserved.0.local_addr().unwrap();
        let mut candidate = (*proxy.engine().config()).clone();
        candidate.inbounds[0].listen.port = replacement.port();
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        drop(reserved);
        handle.apply_runtime_config_and_wait(candidate, Duration::from_secs(5)).await.unwrap();
        let closed = original.terminal().expect("original native provider receipts before apply ACK");
        assert!(closed.failure().is_none()); assert!(closed.task_failures().is_empty());
        assert_eq!(closed.joined_tasks(), 2, "source and actual UDP peer driver");
        assert_eq!(closed.closed_io_receipts(), 3, "original TCP/UDP listeners and Direct UDP provider");
        let completed = proxy.completed_sessions();
        assert_eq!(completed.len(), 1, "one actual UDP flow, no listener-only business session");
        assert_eq!((completed[0].bytes_up, completed[0].bytes_down), (6, encoded_reply_bytes));
        assert_eq!(completed[0].outbound_rx_bytes, 5, "decoded return payload remains separate");
        assert_eq!(completed[0].inbound_rx_bytes, 6, "preserve decoded-payload ingress accounting");
        assert_eq!(completed[0].inbound_tx_bytes, encoded_reply_bytes, "actual encoded local submission");
        let auth = completed[0].auth.as_ref().unwrap();
        assert_eq!(auth.principal_key.as_deref(), Some("native-principal"));
        assert_eq!(auth.policy_revision, Some(7));
        let current = proxy.inbound_owners.owner("ss-native").unwrap();
        assert_ne!(current.identity(), identity); assert!(current.is_running());
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap(); let destination = target.local_addr().unwrap();
        let socket = TcpStream::connect(replacement).await.unwrap();
        let connect = ::shadowsocks::ShadowsocksTcpConnectConfig::from_config("aes-128-gcm", "native-user").unwrap();
        let mut socket = zero_platform_tokio::TokioSocket::new(socket);
        let session = connect.establish_tcp_to(&mut socket, &zero_core::request::Destination::new(zero_core::Address::Ipv4([127,0,0,1]), destination.port())).await.unwrap();
        let mut stream = connect.wrap_outbound_stream(socket, session);
        stream.write_all(b"after").await.unwrap(); stream.flush().await.unwrap();
        let (mut accepted, _) = target.accept().await.unwrap(); let mut payload = [0;5]; accepted.read_exact(&mut payload).await.unwrap(); assert_eq!(&payload,b"after");
        let read = accepted.read(&mut payload); tokio::pin!(read);
        std::future::poll_fn(|cx| { assert!(std::future::Future::poll(read.as_mut(),cx).is_pending()); std::task::Poll::Ready(()) }).await;
        running.shutdown().await.unwrap();
        let closed = current.terminal().expect("replacement original retained through shutdown");
        assert!(closed.failure().is_none()); assert!(closed.task_failures().is_empty());
        assert_eq!(closed.closed_io_receipts(), 4, "both listener originals, accepted TCP and real egress original");
    }).await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn sip003_inbound_keeps_actual_compatibility_process_and_tcp_udp_while_native_source_migrates(
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let material = tempfile::tempdir().unwrap();
        let record = material.path().join("child.json");
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../protocols/shadowsocks/tests/support/sip003.py");
        let reserved = crate::socket_fixture::tcp_udp();
        let endpoint = reserved.0.local_addr().unwrap();
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"ss-plugin", "listen":{"address":"127.0.0.1","port":endpoint.port()},
                "protocol":{"type":"shadowsocks","cipher":"aes-128-gcm","password":"plugin-user",
                    "plugin":{"command":"python3","args":[script,"server","tcp_and_udp",record],"options":"test=options;escaped=value","mode":"tcp_and_udp"}}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = crate::Proxy::new(config).unwrap();
        drop(reserved); let running = proxy.spawn();
        let owner = loop {
            if let Some(owner) = proxy.inbound_owners.owner("ss-plugin") { break owner; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        owner.wait_ready().await.unwrap();
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap(); let destination = target.local_addr().unwrap();
        let socket = loop {
            match TcpStream::connect(endpoint).await {
                Ok(socket) => break socket,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        let connect = ::shadowsocks::ShadowsocksTcpConnectConfig::from_config("aes-128-gcm", "plugin-user").unwrap();
        let mut socket = zero_platform_tokio::TokioSocket::new(socket);
        let session = connect.establish_tcp_to(&mut socket, &zero_core::request::Destination::new(zero_core::Address::Ipv4([127,0,0,1]), destination.port())).await.unwrap();
        let mut stream = connect.wrap_outbound_stream(socket, session);
        stream.write_all(b"tcp").await.unwrap(); stream.flush().await.unwrap();
        let (mut accepted, _) = target.accept().await.unwrap(); let mut bytes = [0;3]; accepted.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"tcp");
        accepted.write_all(b"yes").await.unwrap(); stream.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"yes");
        // SIP003u has no UDP readiness handshake: retransmit real client data.
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let codec = ::shadowsocks::udp::ShadowsocksDatagramCodec::new(::shadowsocks::CipherKind::Aes128Gcm, "plugin-user");
        let packet = codec.encode(&zero_core::Address::Ipv4([127,0,0,1]), target.local_addr().unwrap().port(), b"udp").unwrap();
        let mut buffer = [0;1024];
        let peer = loop {
            client.send_to(&packet, endpoint).await.unwrap();
            if let Ok(result) = tokio::time::timeout(Duration::from_millis(50), target.recv_from(&mut buffer)).await {
                let (size, peer) = result.unwrap(); assert_eq!(&buffer[..size],b"udp"); break peer;
            }
        };
        target.send_to(b"yes",peer).await.unwrap(); let size = client.recv(&mut buffer).await.unwrap(); assert_eq!(codec.decode(&buffer[..size]).unwrap().2,b"yes");
        let child: serde_json::Value = serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
        let mut candidate = (*proxy.engine().config()).clone();
        let zero_config::InboundProtocolConfig::Shadowsocks { plugin, .. } = &mut candidate.inbounds[0].protocol else { unreachable!() };
        *plugin = None;
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(candidate, Duration::from_secs(5)).await.unwrap();
        let closed = owner.terminal().expect("same legacy owner closed before native replacement ACK");
        assert!(closed.failure().is_none()); assert!(closed.task_failures().is_empty());
        let status = std::process::Command::new("kill").args(["-0", &child["pid"].as_u64().unwrap().to_string()]).stderr(std::process::Stdio::null()).status().unwrap();
        assert!(!status.success(), "same actual SIP003 child must be reaped before native replacement ACK");
        let current = proxy.inbound_owners.owner("ss-plugin").unwrap(); assert_ne!(current.identity(), owner.identity());
        let native_target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let packet = codec.encode(&zero_core::Address::Ipv4([127,0,0,1]),native_target.local_addr().unwrap().port(),b"native").unwrap();
        client.send_to(&packet,endpoint).await.unwrap();
        let (size, peer) = native_target.recv_from(&mut buffer).await.unwrap(); assert_eq!(&buffer[..size],b"native");
        native_target.send_to(b"native-reply",peer).await.unwrap(); let size = client.recv(&mut buffer).await.unwrap(); assert_eq!(codec.decode(&buffer[..size]).unwrap().2,b"native-reply");
        drop(stream); drop(accepted);
        running.shutdown().await.unwrap();
        let closed = current.terminal().expect("same replacement native owner"); assert!(closed.failure().is_none()); assert!(closed.task_failures().is_empty()); assert_eq!(closed.closed_io_receipts(),3);
    }).await.unwrap();
}
