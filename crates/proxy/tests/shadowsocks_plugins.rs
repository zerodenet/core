#![cfg(all(unix, feature = "shadowsocks", feature = "socks5"))]
mod support;
use serde_json::json;
use support::interop::{socks5_tcp_echo, spawn_tcp_echo, spawn_udp_echo};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;
#[tokio::test]
async fn sip003_and_sip003u_proxy_paths_use_the_plugin_environment_and_close_processes() {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../protocols/shadowsocks/tests/support/sip003.py");
    for mode in ["tcp_only", "udp_only", "tcp_and_udp"] {
        let port = free_port();
        let local_port = free_port();
        let records =
            std::env::temp_dir().join(format!("zero-sip003-{}-{port}", std::process::id()));
        std::fs::create_dir_all(&records).unwrap();
        let plugin = |role: &str| json!({"command":"python3","args":[fixture,role,mode,records.join(role)],"options":"test=options;escaped=value","mode":mode});
        let server = RuntimeConfig::parse(&json!({"inbounds":[{"tag":"ss","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"shadowsocks","cipher":"aes-128-gcm","password":"plugin-test","plugin":plugin("server")}}],"route":{"rules":[],"final":{"type":"direct"}}}).to_string()).unwrap();
        let server = spawn_engine(Proxy::new(server).unwrap());
        wait_for_listener(port).await;
        let client = RuntimeConfig::parse(&json!({"inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":local_port},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"ss","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":port,"cipher":"aes-128-gcm","password":"plugin-test","plugin":plugin("client")}}],"route":{"rules":[],"final":{"type":"route","outbound":"ss"}}}).to_string()).unwrap();
        let client = spawn_engine(Proxy::new(client).unwrap());
        wait_for_listener(local_port).await;
        let tcp_port = free_port();
        let tcp = spawn_tcp_echo(tcp_port, 5).await;
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(15),
                socks5_tcp_echo(local_port, tcp_port, b"hello")
            )
            .await
            .expect("plugin TCP deadline"),
            b"hello",
            "{mode}"
        );
        let udp_port = free_udp_port();
        let udp = spawn_udp_echo(udp_port, 5).await;
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(15),
                udp_echo_with_startup_retry(local_port, udp_port, b"world")
            )
            .await
            .expect("plugin UDP deadline"),
            b"world",
            "{mode}"
        );
        client.shutdown().await.unwrap();
        server.shutdown().await.unwrap();
        tcp.abort();
        udp.abort();
        for role in ["server", "client"] {
            let record: serde_json::Value =
                serde_json::from_slice(&std::fs::read(records.join(role)).unwrap()).unwrap();
            let pid = record["pid"].as_u64().unwrap();
            // A terminated child must disappear without waiting for another network request.
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
            loop {
                let status = std::process::Command::new("kill")
                    .args(["-0", &pid.to_string()])
                    .stderr(std::process::Stdio::null())
                    .status()
                    .unwrap();
                if !status.success() {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "plugin {role} leaked after shutdown"
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
        std::fs::remove_dir_all(records).unwrap();
    }
}

// SIP003u intentionally has no UDP startup readiness handshake. Retransmit at
// the test client while the child starts; the kernel must not invent packets.
async fn udp_echo_with_startup_retry(proxy: u16, target: u16, payload: &[u8]) -> Vec<u8> {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpStream, UdpSocket},
    };
    let mut control = TcpStream::connect(("127.0.0.1", proxy)).await.unwrap();
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
    let relay = u16::from_be_bytes([reply[8], reply[9]]);
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let packet =
        support::build_udp_packet(&zero_core::Address::Ipv4([127, 0, 0, 1]), target, payload)
            .unwrap();
    let mut retry = tokio::time::interval(std::time::Duration::from_millis(200));
    let mut bytes = [0; 2048];
    loop {
        tokio::select! {
            _ = retry.tick() => { socket.send_to(&packet, ("127.0.0.1", relay)).await.unwrap(); }
            result = socket.recv_from(&mut bytes) => {
                let (size, _) = result.unwrap();
                let reply = support::parse_udp_packet(&bytes[..size]).unwrap();
                assert_eq!(reply.port, target);
                return reply.payload.to_vec();
            }
        }
    }
}
