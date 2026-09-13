#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
#[path = "vless_reverse/udp.rs"]
mod udp;
use serde_json::json;
use support::interop::{
    socks5_udp_echo_sequence, spawn_tcp_echo, spawn_udp_echo_count, ExternalProcess, TcpResetProxy,
    TempMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{timeout, Duration},
};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;
const ID: &str = "01234567-89ab-cdef-0123-456789abcdef";

#[tokio::test]
async fn reverse_native_tcp_udp_routes_and_reconnects_without_a_bridge_listener() {
    timeout(Duration::from_secs(30), run_case(None, false))
        .await
        .unwrap();
}
#[tokio::test]
async fn reverse_interoperates_with_pinned_official_portal_and_bridge() {
    let Some(bin) = support::interop::require_env("XRAY_BIN") else {
        return;
    };
    timeout(Duration::from_secs(30), run_case(Some(bin.clone()), false))
        .await
        .unwrap();
    timeout(Duration::from_secs(30), run_case(Some(bin), true))
        .await
        .unwrap();
}
async fn run_case(official: Option<String>, official_portal: bool) {
    support::interop::init_logs("vless=debug");
    let portal_port = free_port();
    let carrier_port = free_port();
    let socks_port = free_port();
    let tcp_port = free_port();
    let udp_port = free_udp_port();
    let material = TempMaterial::new("vless-rvs");
    let native_portal = json!({
        "inbounds":[
            {"tag":"rvs","listen":{"address":"127.0.0.1","port":portal_port},"protocol":{"type":"vless","users":[{"id":ID,"reverse_tag":"portal"}]}},
            {"tag":"socks","listen":{"address":"127.0.0.1","port":socks_port},"protocol":{"type":"socks5"}}
        ], "outbounds":[{"tag":"portal","protocol":{"type":"vless_reverse"}}],
        "route":{"final":{"type":"route","outbound":"portal"}}
    });
    let native_bridge = json!({
        "outbounds":[{"tag":"link","protocol":{"type":"vless","server":"127.0.0.1","port":carrier_port,"id":ID,"reverse_tag":"bridge"}}],
        "route":{"rules":[{"condition":{"type":"inbound","values":["bridge"]},"action":{"type":"direct"}}],"final":{"type":"reject"}}
    });
    let xray_portal = json!({"log":{"loglevel":"debug"},
        "inbounds":[
            {"tag":"rvs","listen":"127.0.0.1","port":portal_port,"protocol":"vless","settings":{"decryption":"none","clients":[{"id":ID,"reverse":{"tag":"portal"}}]}},
            {"tag":"socks","listen":"127.0.0.1","port":socks_port,"protocol":"socks","settings":{"udp":true}}
        ],"outbounds":[{"tag":"direct","protocol":"freedom"}],
        "routing":{"rules":[{"type":"field","inboundTag":["socks"],"outboundTag":"portal"}]}
    });
    let xray_bridge = json!({"log":{"loglevel":"debug"},
        "outbounds":[
            {"tag":"link","protocol":"vless","settings":{"address":"127.0.0.1","port":carrier_port,"id":ID,"encryption":"none","reverse":{"tag":"bridge"}}},
            {"tag":"direct","protocol":"freedom"}
        ],"routing":{"rules":[{"type":"field","inboundTag":["bridge"],"outboundTag":"direct"}]}
    });
    let mut processes = Vec::new();
    let mut engines = Vec::new();
    if official.is_some() && official_portal {
        processes.push(start_official(
            official.as_ref().unwrap(),
            &material,
            "portal",
            xray_portal,
        ));
    } else {
        engines.push(spawn_engine(
            Proxy::new(RuntimeConfig::parse(&native_portal.to_string()).unwrap()).unwrap(),
        ));
    }
    wait_for_listener(portal_port).await;
    wait_for_listener(socks_port).await;
    let carrier = TcpResetProxy::start(carrier_port, portal_port).await;
    if official.is_some() && !official_portal {
        processes.push(start_official(
            official.as_ref().unwrap(),
            &material,
            "bridge",
            xray_bridge,
        ));
    } else {
        engines.push(spawn_engine(
            Proxy::new(RuntimeConfig::parse(&native_bridge.to_string()).unwrap()).unwrap(),
        ));
    }
    let payload = vec![0x95; 65_537];
    for round in 0..2 {
        wait_for_reverse_path(socks_port).await;
        let echo = spawn_tcp_echo(tcp_port, payload.len()).await;
        let mut stream = timeout(
            Duration::from_secs(8),
            connect_when_ready(socks_port, tcp_port),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Rvs worker did not become ready: {}",
                processes
                    .iter()
                    .map(ExternalProcess::logs)
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        });
        timeout(Duration::from_secs(5), async {
            stream.write_all(&payload).await.unwrap();
            let mut received = vec![0; payload.len()];
            stream.read_exact(&mut received).await.unwrap_or_else(|error| {
                panic!("reverse TCP read: {error}; round={round}, official_portal={official_portal}; {}", processes.iter().map(ExternalProcess::logs).collect::<Vec<_>>().join("\n"))
            });
            assert_eq!(received, payload);
        })
        .await
        .unwrap();
        drop(stream);
        echo.await.unwrap();
        let udp = spawn_udp_echo_count(udp_port, 3).await;
        let packets: &[&[u8]] = &[b"first", b"second", b"last"];
        assert_eq!(
            socks5_udp_echo_sequence(socks_port, udp_port, packets).await,
            packets.iter().map(|p| p.to_vec()).collect::<Vec<_>>()
        );
        udp.await.unwrap();
        timeout(
            Duration::from_secs(5),
            udp::burst_and_unsolicited(socks_port),
        )
        .await
        .expect("reverse UDP burst response deadline");
        if round == 0 {
            carrier.reset_connections();
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }
    for engine in engines {
        engine.shutdown().await.unwrap();
    }
    drop(processes);
    carrier.shutdown().await;
}
fn start_official(
    bin: &str,
    material: &TempMaterial,
    name: &str,
    config: serde_json::Value,
) -> ExternalProcess {
    let path = material.path(&format!("{name}.json"));
    std::fs::write(&path, config.to_string()).unwrap();
    ExternalProcess::start(
        bin.to_owned(),
        &["run", "-c", path.to_str().unwrap()],
        material,
        name,
    )
}
async fn connect_when_ready(socks: u16, target: u16) -> TcpStream {
    loop {
        let mut stream = TcpStream::connect(("127.0.0.1", socks)).await.unwrap();
        stream.write_all(&[5, 1, 0]).await.unwrap();
        let mut hello = [0; 2];
        stream.read_exact(&mut hello).await.unwrap();
        assert_eq!(hello, [5, 0]);
        let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
        request.extend_from_slice(&target.to_be_bytes());
        stream.write_all(&request).await.unwrap();
        let mut response = [0; 10];
        if stream.read_exact(&mut response).await.is_ok() && response[1] == 0 {
            return stream;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

// Official SOCKS acknowledges CONNECT before routing. Probe actual payload on a
// separate target so a not-yet-registered portal cannot consume the test echo.
async fn wait_for_reverse_path(socks: u16) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let echo = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut byte = [0];
                if stream.read_exact(&mut byte).await.is_ok() {
                    let _ = stream.write_all(&byte).await;
                }
            });
        }
    });
    let result = timeout(Duration::from_secs(8), async {
        loop {
            let ready = timeout(Duration::from_millis(500), async {
                let mut stream = connect_when_ready(socks, port).await;
                stream.write_all(&[0x72]).await?;
                let mut reply = [0];
                stream.read_exact(&mut reply).await?;
                Ok::<_, std::io::Error>(reply == [0x72])
            })
            .await;
            if matches!(ready, Ok(Ok(true))) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    echo.abort();
    result.expect("reverse data path did not become ready");
}
