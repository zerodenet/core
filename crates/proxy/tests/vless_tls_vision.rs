#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use serde_json::json;
use support::interop::*;
use support::tls_echo::{socks5_tls_echo, spawn_tls_echo};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

async fn exercise(socks: u16) -> std::io::Result<()> {
    let port = free_port();
    let payload = vec![153; 65537];
    let (echo, cert) = spawn_tls_echo(port, payload.len()).await;
    assert_eq!(socks5_tls_echo(socks, port, &payload, cert).await?, payload);
    echo.await.unwrap();
    let port = free_udp_port();
    let echo = spawn_udp_echo_count(port, 2).await;
    assert_eq!(
        socks5_udp_echo_targets(socks, &[(port, b"one"), (port, b"two")]).await,
        vec![b"one".to_vec(), b"two".to_vec()]
    );
    echo.await.unwrap();
    Ok(())
}

// mode: native, Zero outbound, or Zero inbound.
async fn run(mode: u8) {
    init_logs("zero_proxy=trace,zero_transport=trace,vless=trace");
    let material = TempMaterial::new("vless-tls-vision");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_path = material.path("cert.pem");
    let key_path = material.path("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let port = free_port();
    let socks = free_port();
    let inbound = json!({"inbounds":[{"tag":"vless","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"vless","users":[{"id":"tls-vision","flow":"xtls-rprx-vision"}],"tls":{"cert_path":cert_path,"key_path":key_path}}}],"route":{"rules":[],"final":{"type":"direct"}}});
    let outbound = json!({"inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"up","protocol":{"type":"vless","server":"127.0.0.1","port":port,"id":"tls-vision","flow":"xtls-rprx-vision","tls":{"server_name":"localhost","ca_cert_path":cert_path}}}],"route":{"rules":[],"final":{"type":"route","outbound":"up"}}});
    let mut zeros = Vec::new();
    if mode != 1 {
        zeros.push(spawn_engine(
            Proxy::new(RuntimeConfig::parse(&inbound.to_string()).unwrap()).unwrap(),
        ));
    }
    if mode != 2 {
        zeros.push(spawn_engine(
            Proxy::new(RuntimeConfig::parse(&outbound.to_string()).unwrap()).unwrap(),
        ));
    }
    let mut xray = if mode == 0 {
        None
    } else {
        let binary = std::env::var("XRAY_BIN").expect("official v26.3.27 binary required");
        let version = std::process::Command::new(&binary)
            .arg("version")
            .output()
            .unwrap();
        let version = String::from_utf8_lossy(&version.stdout);
        assert!(version.contains("26.3.27") && version.contains("d2758a0"));
        let config = if mode == 1 {
            json!({"log":{"loglevel":"debug"},"inbounds":[{"listen":"127.0.0.1","port":port,"protocol":"vless","settings":{"clients":[{"id":"tls-vision","flow":"xtls-rprx-vision"}],"decryption":"none"},"streamSettings":{"network":"raw","security":"tls","tlsSettings":{"certificates":[{"certificateFile":cert_path,"keyFile":key_path}]}}}],"outbounds":[{"protocol":"freedom"}]})
        } else {
            json!({"log":{"loglevel":"debug"},"inbounds":[{"listen":"127.0.0.1","port":socks,"protocol":"socks","settings":{"udp":true}}],"outbounds":[{"protocol":"vless","settings":{"address":"127.0.0.1","port":port,"id":"tls-vision","flow":"xtls-rprx-vision","encryption":"none"},"streamSettings":{"network":"raw","security":"tls","tlsSettings":{"serverName":"localhost","certificates":[{"certificateFile":cert_path,"usage":"verify"}]}}}]})
        };
        let path = material.path("xray.json");
        std::fs::write(&path, config.to_string()).unwrap();
        Some(XrayProcess::start(binary, &path, &material))
    };
    wait_for_listener(port).await;
    wait_for_listener(socks).await;
    let result = tokio::time::timeout(std::time::Duration::from_secs(20), exercise(socks)).await;
    if let Some(xray) = &mut xray {
        xray.kill();
    }
    for zero in zeros {
        zero.shutdown().await.unwrap();
    }
    assert!(
        matches!(result, Ok(Ok(()))),
        "mode={mode}: {result:?}; {}",
        xray.map(|x| x.logs()).unwrap_or_default()
    );
}
#[tokio::test]
async fn ordinary_tls13_vision_tcp_direct_and_xudp_native() {
    run(0).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn ordinary_tls13_vision_official_both_directions() {
    run(1).await;
    run(2).await;
}
