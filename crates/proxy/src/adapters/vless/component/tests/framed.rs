//! Real standard production callers retain their original framed carriers.
use base64::Engine;
use std::{net::SocketAddr, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn encryption() -> (String, String) {
    let server = format!(
        "mlkem768x25519plus.native.600s.100-35-35.{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([7; 32])
    );
    let keys = ::vless::encryption::EncryptionServer::new(
        ::vless::encryption::config::EncryptionConfig::server(&server)
            .unwrap()
            .unwrap(),
    )
    .unwrap()
    .public_keys();
    let client = format!(
        "mlkem768x25519plus.native.0rtt.100-35-35.{}",
        keys.iter()
            .map(|key| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key))
            .collect::<Vec<_>>()
            .join(".")
    );
    (client, server)
}
pub(super) async fn start(
    config: serde_json::Value,
) -> (crate::Proxy, crate::RunningProxy, SocketAddr) {
    let config = zero_config::RuntimeConfig::parse(&config.to_string()).unwrap();
    let proxy = crate::Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(10))
        .await
        .unwrap();
    let source = proxy.system_tun_source_registry().owner("entry").unwrap();
    source.wait_ready().await.unwrap();
    let endpoint = *source.original_listener_address.get().unwrap();
    (proxy, running, endpoint)
}
pub(super) async fn authenticate(endpoint: SocketAddr) -> tokio::net::TcpStream {
    let mut io = tokio::net::TcpStream::connect(endpoint).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut reply = [0; 2];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [5, 0]);
    io
}
pub(super) async fn exercise_tcp(endpoint: SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_port = listener.local_addr().unwrap().port();
    let tcp = tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4];
        io.read_exact(&mut bytes).await.unwrap();
        io.write_all(&bytes).await.unwrap();
        io.shutdown().await.unwrap();
    });
    let mut io = authenticate(endpoint).await;
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend(tcp_port.to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..2], &[5, 0]);
    io.write_all(b"wire").await.unwrap();
    let mut echo = [0; 4];
    io.read_exact(&mut echo).await.unwrap();
    assert_eq!(&echo, b"wire");
    drop(io);
    tcp.await.unwrap();
}
pub(super) async fn exercise(endpoint: SocketAddr) {
    exercise_tcp(endpoint).await;
    let mut reply = [0; 10];
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_port = udp.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let mut bytes = [0; 128];
        let (size, peer) = udp.recv_from(&mut bytes).await.unwrap();
        udp.send_to(&bytes[..size], peer).await.unwrap();
    });
    let mut control = authenticate(endpoint).await;
    control
        .write_all(&[5, 3, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    control.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..4], &[5, 0, 0, 1]);
    let relay = SocketAddr::from(([127, 0, 0, 1], u16::from_be_bytes([reply[8], reply[9]])));
    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut packet = vec![0, 0, 0, 1, 127, 0, 0, 1];
    packet.extend(udp_port.to_be_bytes());
    packet.extend(b"xudp");
    client.send_to(&packet, relay).await.unwrap();
    let mut echo = [0; 128];
    let size = client.recv(&mut echo).await.unwrap();
    assert_eq!(&echo[..size], packet);
    drop(control);
    server.await.unwrap();
}
#[tokio::test]
async fn encryption_vision_framed_tcp_and_xudp_use_real_standard_owners() {
    for carrier in [
        serde_json::json!({}),
        serde_json::json!({"ws":{"path":"/vision"}}),
        serde_json::json!({"h2":{"path":"/vision"}}),
        serde_json::json!({"http_upgrade":{"path":"/vision"}}),
        serde_json::json!({"grpc":{"service_names":["vision/Tun"]}}),
        serde_json::json!({"split_http":{"path":"/vision/","mode":"stream-one"}}),
    ] {
        let (encryption, decryption) = encryption();
        let mut inbound = serde_json::json!({"type":"vless","decryption":decryption,
            "users":[{"id":"encrypted-vision","flow":"xtls-rprx-vision"}]});
        inbound
            .as_object_mut()
            .unwrap()
            .extend(carrier.as_object().unwrap().clone());
        let (remote, remote_running, endpoint) = start(serde_json::json!({
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":inbound}],
            "route":{"final":{"type":"direct"}}
        })).await;
        let mut outbound = serde_json::json!({"type":"vless","server":"127.0.0.1","port":endpoint.port(),
            "id":"encrypted-vision","encryption":encryption,"flow":"xtls-rprx-vision"});
        outbound
            .as_object_mut()
            .unwrap()
            .extend(carrier.as_object().unwrap().clone());
        let (client, client_running, socks) = start(serde_json::json!({
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
            "outbounds":[{"tag":"upstream","protocol":outbound}],
            "route":{"final":{"type":"route","outbound":"upstream"}}
        })).await;
        let owner = client.system_tun_source_registry().owner("entry").unwrap();
        let original_remote = remote.system_tun_source_registry().owner("entry").unwrap();
        let original_scope = original_remote.original_scope_for_test();
        let cold = remote
            .protocols
            .prepare_inbound_bind(remote.engine().config().inbounds[0].clone(), None)
            .unwrap()
            .unwrap();
        assert_eq!(
            cold.into_recipe().spec().task_limit.unwrap().get(),
            1,
            "exact resumable decryption Source declares one maintenance task before any handshake"
        );
        let result = tokio::time::timeout(Duration::from_secs(30), async {
            exercise(socks).await;
            exercise(socks).await;
        })
        .await;
        client_running.shutdown().await.unwrap();
        remote_running.shutdown().await.unwrap();
        assert!(result.is_ok(), "actual encrypted Vision carrier: {carrier}");
        assert!(
            !original_scope.resource_open(),
            "that same original Source Scope retired, not a replacement observer"
        );
        for original in [owner, original_remote] {
            let closed = original.terminal_for_test().unwrap();
            assert!(closed.failure().is_none() && closed.closed_io_receipts() > 0);
            assert!(
                closed.joined_tasks() > 1
                    && closed.task_failures().is_empty()
                    && closed.native_bound_failures().is_empty()
            );
        }
    }
}

#[tokio::test]
async fn zero_aead_existing_tcp_wire_uses_standard_roles_and_closes_originals() {
    for carrier in [
        serde_json::json!({}),
        serde_json::json!({"ws":{"path":"/compat"}}),
        serde_json::json!({"h2":{"path":"/compat"}}),
        serde_json::json!({"http_upgrade":{"path":"/compat"}}),
        serde_json::json!({"grpc":{"service_names":["compat/Tun"]}}),
        serde_json::json!({"split_http":{"path":"/compat/","mode":"stream-one"}}),
    ] {
        let mut inbound = serde_json::json!({"type":"vless", "users":[{"id":"compat-aead","flow":"zero-aead-v1"}]});
        inbound
            .as_object_mut()
            .unwrap()
            .extend(carrier.as_object().unwrap().clone());
        let (remote, remote_running, endpoint) = start(serde_json::json!({
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":inbound}],
            "route":{"final":{"type":"direct"}}
        })).await;
        let mut outbound = serde_json::json!({"type":"vless","server":"127.0.0.1","port":endpoint.port(),
            "id":"compat-aead","flow":"zero-aead-v1"});
        outbound
            .as_object_mut()
            .unwrap()
            .extend(carrier.as_object().unwrap().clone());
        let (client, client_running, socks) = start(serde_json::json!({
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
            "outbounds":[{"tag":"upstream","protocol":outbound}],
            "route":{"final":{"type":"route","outbound":"upstream"}}
        })).await;
        // Production inventory must expose both real executable roles, rather
        // than silently selecting a compatibility leaf after cold selection.
        let plan = client.tcp_runtime_services().components.clone().unwrap();
        let selected = plan.configured("upstream").unwrap();
        assert!(selected
            .stream(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .is_ok());
        assert!(selected
            .datagram(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .is_ok());
        let original_client = client.system_tun_source_registry().owner("entry").unwrap();
        let original_remote = remote.system_tun_source_registry().owner("entry").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(15), exercise_tcp(socks)).await;
        client_running.shutdown().await.unwrap();
        remote_running.shutdown().await.unwrap();
        assert!(result.is_ok(), "actual compatibility carrier: {carrier}");
        for original in [original_client, original_remote] {
            let closed = original.terminal_for_test().unwrap();
            assert!(closed.failure().is_none() && closed.closed_io_receipts() > 0);
            assert!(closed.task_failures().is_empty());
        }
    }
}
