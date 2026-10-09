//! Independent ingress routes retain the selected Direct socket requirements.
use crate::support::{free_port, spawn_engine, wait_for_listener};
use std::{net::IpAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;
fn config(first: u16, second: u16, target: u16) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "runtime":{"udp":{"enabled":false}},
        "inbounds":[{"tag":"v4-entry","listen":{"address":"127.0.0.1","port":first},"protocol":{"type":"direct","target":"localhost","port":target}},
        {"tag":"v6-entry","listen":{"address":"127.0.0.1","port":second},"protocol":{"type":"direct","target":"localhost","port":target}}],
        "outbounds":[{"tag":"v4-exit","protocol":{"type":"direct"},"dial":{"address_family":"only_ipv4","source_ip":"127.0.0.1"}},
        {"tag":"v6-exit","protocol":{"type":"direct"},"dial":{"address_family":"only_ipv6","source_ip":"::1"}}],
        "route":{"rules":[{"condition":{"type":"inbound","values":["v4-entry"]},"action":{"type":"route","outbound":"v4-exit"}},
        {"condition":{"type":"inbound","values":["v6-entry"]},"action":{"type":"route","outbound":"v6-exit"}}],"final":{"type":"reject"}}
    }).to_string()).unwrap()
}
async fn echo(listener: TcpListener, expected: IpAddr) {
    loop {
        let (mut stream, peer) = listener.accept().await.unwrap();
        assert_eq!(peer.ip(), expected);
        tokio::spawn(async move {
            let mut bytes = [0; 4];
            while stream.read_exact(&mut bytes).await.is_ok() {
                bytes[0] = if expected.is_ipv6() { 6 } else { 4 };
                stream.write_all(&bytes).await.unwrap();
            }
        });
    }
}
async fn exchange(stream: &mut TcpStream, value: &[u8; 4], family: u8) {
    stream.write_all(value).await.unwrap();
    let mut response = [0; 4];
    tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response[0], family);
    assert_eq!(&response[1..], &value[1..]);
}
#[tokio::test]
async fn two_inbounds_keep_independent_direct_families_and_old_tcp_survives_reload() {
    let ipv4 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = ipv4.local_addr().unwrap().port();
    let ipv6 = TcpListener::bind(("::1", target))
        .await
        .expect("IPv6 loopback required");
    let v4_server = tokio::spawn(echo(ipv4, "127.0.0.1".parse().unwrap()));
    let v6_server = tokio::spawn(echo(ipv6, "::1".parse().unwrap()));
    let first = free_port();
    let second = free_port();
    let mut configuration = config(first, second, target);
    let handle = spawn_engine(Proxy::new(configuration.clone()).unwrap());
    wait_for_listener(first).await;
    wait_for_listener(second).await;
    let mut v4 = TcpStream::connect(("127.0.0.1", first)).await.unwrap();
    let mut v6 = TcpStream::connect(("127.0.0.1", second)).await.unwrap();
    exchange(&mut v4, b"four", 4).await;
    exchange(&mut v6, b"six!", 6).await;
    configuration.outbounds[0].dial = configuration.outbounds[1].dial.clone();
    handle
        .apply_config_and_wait(configuration, Duration::from_secs(5))
        .await
        .unwrap();
    exchange(&mut v4, b"kept", 4).await;
    let mut changed = TcpStream::connect(("127.0.0.1", first)).await.unwrap();
    exchange(&mut changed, b"new!", 6).await;
    handle.shutdown().await.unwrap();
    v4_server.abort();
    v6_server.abort();
}
#[test]
fn prepare_rejects_missing_local_interface_and_source() {
    for dial in [
        serde_json::json!({"interface":"zero-missing-dial-interface"}),
        serde_json::json!({"source_ip":"192.0.2.197"}),
    ] {
        let config=RuntimeConfig::parse(&serde_json::json!({"outbounds":[{"tag":"limited","protocol":{"type":"direct"},"dial":dial}],"route":{"final":{"type":"route","outbound":"limited"}}}).to_string()).unwrap();
        zero_proxy::validate_config(&config)
            .expect("structural validation must not inspect the host");
        assert!(Proxy::new(config).is_err());
    }
}
