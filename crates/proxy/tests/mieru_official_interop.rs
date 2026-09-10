#![cfg(all(feature = "mieru", feature = "socks5"))]
mod support;

use std::time::Duration;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, UdpSocket},
    time::timeout,
};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

#[tokio::test]
#[ignore = "requires MIERU_REFERENCE_BIN built by scripts/prepare-mieru-interop.py"]
async fn official_tcp_underlay_multiplexes_tcp_udp_and_isolates_stalled_sessions() {
    inbound_interop("tcp", "").await;
}

#[tokio::test]
#[ignore = "requires MIERU_REFERENCE_BIN"]
async fn official_udp_underlay_multiplexes_tcp_udp_and_isolates_stalled_sessions() {
    inbound_interop("udp", "").await;
}

async fn inbound_interop(mode: &str, options: &str) {
    support::interop::init_logs("zero_proxy=warn");
    let binary = std::env::var("MIERU_REFERENCE_BIN")
        .expect("set MIERU_REFERENCE_BIN; explicit interop must not silently skip");
    let targets = targets().await;
    let tcp_port = targets.tcp;
    let udp_port = targets.udp;
    let port = support::free_port();
    let config=RuntimeConfig::parse(&format!(r#"{{
        "inbounds":[{{"tag":"mieru-in","listen":{{"address":"127.0.0.1","port":{port}}},
            "protocol":{{"type":"mieru","transport":"{mode}","users":[{{"username":"mux-user","password":"mux-password"}}]{options}}}}}],
        "outbounds":[],"route":{{"rules":[],"final":{{"type":"direct"}}}}
    }}"#)).unwrap();
    let proxy = support::spawn_engine(Proxy::new(config).unwrap());
    if mode == "tcp" {
        support::wait_for_listener(port).await;
    }
    let material = support::interop::TempMaterial::new("mieru-multiplex-reference");
    let args = [
        port.to_string(),
        tcp_port.to_string(),
        udp_port.to_string(),
        mode.to_owned(),
    ];
    let mut reference = support::interop::ExternalProcess::start(
        binary,
        &args.iter().map(String::as_str).collect::<Vec<_>>(),
        &material,
        "official",
    );
    let outcome = timeout(Duration::from_secs(90), reference.wait()).await;
    reference.kill();
    proxy.shutdown().await.unwrap();

    let logs = reference.logs();
    let status = outcome
        .unwrap_or_else(|_| panic!("reference timed out: {logs}"))
        .expect("reference wait");
    assert!(status.success(), "reference failed: {logs}");
    assert!(
        logs.contains(&format!("one {mode} underlay")),
        "missing reference assertion: {logs}"
    );
    println!("{logs}");
}

#[tokio::test]
#[ignore = "requires MIERU_REFERENCE_BIN"]
async fn zero_outbound_tcp_pool_interoperates_with_official_server() {
    outbound_interop("tcp", "").await;
}
#[tokio::test]
#[ignore = "requires MIERU_REFERENCE_BIN"]
async fn zero_outbound_udp_pool_interoperates_with_official_server() {
    outbound_interop("udp", "").await;
}

async fn outbound_interop(mode: &str, options: &str) {
    let binary = std::env::var("MIERU_REFERENCE_BIN").expect("set MIERU_REFERENCE_BIN");
    let material = support::interop::TempMaterial::new("mieru-official-server");
    let upstream_port = support::free_port();
    let port_string = upstream_port.to_string();
    let mut reference = support::interop::ExternalProcess::start(
        binary,
        &["server", mode, &port_string],
        &material,
        "server",
    );
    timeout(Duration::from_secs(30), async {
        while !reference.logs().contains("READY") {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("reference startup: {}", reference.logs()));
    let targets = targets().await;
    let tcp_port = targets.tcp;
    let udp_port = targets.udp;
    let local = support::free_port();
    let config=RuntimeConfig::parse(&format!(r#"{{
        "inbounds":[{{"tag":"socks","listen":{{"address":"127.0.0.1","port":{local}}},"protocol":{{"type":"socks5"}}}}],
        "outbounds":[{{"tag":"mieru","protocol":{{"type":"mieru","transport":"{mode}","server":"127.0.0.1","port":{upstream_port},"username":"mux-user","password":"mux-password"{options}}}}}],
        "route":{{"rules":[],"final":{{"type":"route","outbound":"mieru"}}}}
    }}"#)).unwrap();
    let proxy = support::spawn_engine(Proxy::new(config).unwrap());
    support::wait_for_listener(local).await;
    let work = async {
        let mut tasks = tokio::task::JoinSet::new();
        for byte in 0..6u8 {
            tasks.spawn(async move {
                let size = if byte < 4 { 131073 } else { 1600 };
                let payload = vec![byte; size];
                let received = if byte < 4 {
                    support::interop::socks5_tcp_echo_once(local, tcp_port, &payload)
                        .await
                        .unwrap()
                } else {
                    support::interop::socks5_udp_echo(local, udp_port, &payload).await
                };
                assert_eq!(received, payload);
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        let payload = vec![7; 131073];
        assert_eq!(
            support::interop::socks5_tcp_echo_once(local, tcp_port, &payload)
                .await
                .unwrap(),
            payload
        );
    };
    let result = timeout(Duration::from_secs(60), work).await;
    proxy.shutdown().await.unwrap();
    reference.kill();
    let logs = reference.logs();
    assert!(result.is_ok(), "outbound timeout: {logs}");
    assert_eq!(
        logs.lines()
            .filter(|line| line.starts_with("UNDERLAY "))
            .count(),
        1,
        "pool did not share one carrier: {logs}"
    );
    println!(
        "PASS: Zero outbound {mode}; one official underlay; TCP/UDP and subsequent reuse\n{logs}"
    );
}

struct Targets {
    tcp: u16,
    udp: u16,
    tasks: [tokio::task::JoinHandle<()>; 2],
}
impl Drop for Targets {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
async fn targets() -> Targets {
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_port = tcp.local_addr().unwrap().port();
    let tcp_task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = tcp.accept() => {
                    let (mut stream,_) = accepted.unwrap();
                    tasks.spawn(async move {
                        let (mut read,mut write)=stream.split();
                        let _=tokio::io::copy(&mut read,&mut write).await;
                        let _=write.shutdown().await;
                    });
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
    });
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_port = udp.local_addr().unwrap().port();
    let udp_task = tokio::spawn(async move {
        let mut buffer = [0; 65536];
        loop {
            let (n, peer) = udp.recv_from(&mut buffer).await.unwrap();
            udp.send_to(&buffer[..n], peer).await.unwrap();
        }
    });
    Targets {
        tcp: tcp_port,
        udp: udp_port,
        tasks: [tcp_task, udp_task],
    }
}

const CUSTOM_OPTIONS: &str = r#", "mtu":1280,"traffic_pattern":{"seed":3330,"tcp_fragment":{"enable":true,"max_sleep_ms":0},"nonce":{"type":"fixed","apply_to_all_udp_packet":true,"custom_hex_strings":["474554202f"]}}"#;

#[tokio::test]
#[ignore = "requires MIERU_REFERENCE_BIN"]
async fn official_tcp_accepts_configured_traffic_pattern() {
    inbound_interop("tcp", CUSTOM_OPTIONS).await;
}
#[tokio::test]
#[ignore = "requires MIERU_REFERENCE_BIN"]
async fn official_udp_accepts_configured_mtu_and_traffic_pattern() {
    inbound_interop("udp", CUSTOM_OPTIONS).await;
}
#[tokio::test]
#[ignore = "requires MIERU_REFERENCE_BIN"]
async fn zero_tcp_outbound_applies_configured_traffic_pattern() {
    outbound_interop("tcp", CUSTOM_OPTIONS).await;
}
#[tokio::test]
#[ignore = "requires MIERU_REFERENCE_BIN"]
async fn zero_udp_outbound_applies_configured_mtu_and_traffic_pattern() {
    outbound_interop("udp", CUSTOM_OPTIONS).await;
}
