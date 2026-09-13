#![cfg(all(feature = "socks5", feature = "vless"))]

mod support;

use base64::Engine;
use serde_json::{json, Value};
use support::interop::{
    socks5_tcp_echo_once, socks5_udp_echo_sequence, spawn_tcp_echo, spawn_udp_echo_count,
    ExternalProcess, TempMaterial, TestTlsMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::time::{timeout, Duration};

const ID: &str = "01234567-89ab-cdef-0123-456789abcdef";
const ECH_CONFIG: &str =
    "ADn+DQA1agAgACBtuySC1pphjFlGYKTaSm2KWNg7GQVRS8uAYvLTm5QlGwAEAAEAAQAGZWcuY29tAAA=";

#[tokio::test]
async fn native_rustls_client_and_openssl_server_carry_vless_h3_tcp_and_udp() {
    run(None, false).await;
}

#[tokio::test]
async fn vless_h3_ech_matches_pinned_official_in_both_directions() {
    let Some(binary) = support::interop::require_env("XRAY_BIN") else {
        return;
    };
    assert_pinned_xray(&binary);
    for official_server in [false, true] {
        run(Some(&binary), official_server).await;
    }
}

fn server_keys() -> String {
    let private = base64::engine::general_purpose::STANDARD
        .decode("MC4CAQAwBQYDK2VuBCIEIKBC3rocwIF5tGY+/TaYQrCxY+ULsch94ja9DojkcvlT")
        .unwrap();
    let config = base64::engine::general_purpose::STANDARD
        .decode(ECH_CONFIG)
        .unwrap();
    let mut keys = vec![0, 32];
    keys.extend_from_slice(&private[private.len() - 32..]);
    keys.extend_from_slice(&config);
    base64::engine::general_purpose::STANDARD.encode(keys)
}

async fn run(binary: Option<&str>, official_server: bool) {
    support::interop::init_logs("zero_transport=debug,zero_proxy=debug,vless=debug");
    let material = TempMaterial::new("vless-ech-h3");
    let tls = material.tls();
    let tunnel = free_udp_port();
    let ready = free_port();
    let socks = free_port();
    let native_server = native_server(tunnel, ready, &tls);
    let native_client = native_client(tunnel, socks, &tls);
    let xray_server = xray_server(tunnel, ready, &tls);
    let xray_client = xray_client(tunnel, socks, &tls);

    let mut engines = Vec::new();
    let mut processes = Vec::new();
    for (name, native, official, external) in [
        (
            "server",
            native_server,
            xray_server,
            binary.is_some() && official_server,
        ),
        (
            "client",
            native_client,
            xray_client,
            binary.is_some() && !official_server,
        ),
    ] {
        if external {
            let path = material.path(&format!("{name}.json"));
            std::fs::write(&path, official.to_string()).unwrap();
            processes.push(ExternalProcess::start(
                binary.unwrap().to_owned(),
                &["run", "-c", path.to_str().unwrap()],
                &material,
                name,
            ));
        } else {
            engines.push(start_native(native));
        }
    }

    let mut traffic = tokio::spawn(roundtrip(ready, socks));
    match timeout(Duration::from_secs(30), &mut traffic).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!(
            "VLESS H3 ECH traffic failed (official_server={official_server}): {error}; {}",
            external_logs(&processes)
        ),
        Err(error) => {
            traffic.abort();
            panic!(
                "VLESS H3 ECH traffic timed out (official_server={official_server}): {error}; {}",
                external_logs(&processes)
            );
        }
    }
    for engine in engines {
        engine.shutdown().await.unwrap();
    }
}

async fn roundtrip(ready: u16, socks: u16) {
    wait_for_listener(ready).await;
    wait_for_listener(socks).await;

    let tcp_port = free_port();
    let payload = vec![0x5a; 32_769];
    let echo = spawn_tcp_echo(tcp_port, payload.len()).await;
    assert_eq!(
        socks5_tcp_echo_once(socks, tcp_port, &payload)
            .await
            .unwrap(),
        payload
    );
    echo.await.unwrap();

    let udp_port = free_udp_port();
    let echo = spawn_udp_echo_count(udp_port, 3).await;
    let packets: &[&[u8]] = &[b"first", &[0x65; 1600], b"last"];
    assert_eq!(
        socks5_udp_echo_sequence(socks, udp_port, packets).await,
        packets
            .iter()
            .map(|packet| packet.to_vec())
            .collect::<Vec<_>>()
    );
    echo.await.unwrap();
}

fn native_server(tunnel: u16, ready: u16, tls: &TestTlsMaterial) -> Value {
    json!({
        "inbounds": [
            {"tag":"vless","listen":{"address":"127.0.0.1","port":tunnel},"protocol":{
                "type":"vless","users":[{"id":ID}],
                "quic":{"cert_path":tls.cert_path,"key_path":tls.key_path,"server_options":{
                    "backend":"openssl","ech_server_keys":server_keys(),"one_time_loading":true
                }},
                "split_http":{"path":"/ech/","mode":"auto"}
            }},
            {"tag":"ready","listen":{"address":"127.0.0.1","port":ready},"protocol":{"type":"socks5"}}
        ],
        "route":{"final":{"type":"direct"}}
    })
}

fn native_client(tunnel: u16, socks: u16, tls: &TestTlsMaterial) -> Value {
    json!({
        "inbounds":[{"tag":"local","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],
        "outbounds":[{"tag":"node","protocol":{
            "type":"vless","server":"127.0.0.1","port":tunnel,"id":ID,
            "quic":{"server_name":"localhost","ca_cert_path":tls.cert_path,"client_options":{
                "backend":"rustls","ech_config_list":ECH_CONFIG
            }},
            "split_http":{"path":"/ech/","mode":"stream-up","xmux":{"max_connections":1,"h_max_request_times":3}}
        }}],
        "route":{"final":{"type":"route","outbound":"node"}}
    })
}

fn xray_server(tunnel: u16, ready: u16, tls: &TestTlsMaterial) -> Value {
    json!({
        "log":{"loglevel":"warning"},
        "inbounds":[
            {"listen":"127.0.0.1","port":tunnel,"protocol":"vless","settings":{
                "decryption":"none","clients":[{"id":ID}]
            },"streamSettings":{
                "network":"xhttp","security":"tls",
                "tlsSettings":{"alpn":["h3"],"echServerKeys":server_keys(),"certificates":[{
                    "certificateFile":tls.cert_path,"keyFile":tls.key_path
                }]},
                "xhttpSettings":{"path":"/ech/","mode":"auto"}
            }},
            {"listen":"127.0.0.1","port":ready,"protocol":"socks"}
        ],
        "outbounds":[{"protocol":"freedom"}]
    })
}

fn xray_client(tunnel: u16, socks: u16, tls: &TestTlsMaterial) -> Value {
    json!({
        "log":{"loglevel":"warning"},
        "inbounds":[{"listen":"127.0.0.1","port":socks,"protocol":"socks","settings":{"udp":true}}],
        "outbounds":[{"protocol":"vless","settings":{"vnext":[{
            "address":"127.0.0.1","port":tunnel,"users":[{"id":ID,"encryption":"none"}]
        }]},"streamSettings":{
            "network":"xhttp","security":"tls",
            "tlsSettings":{"serverName":"localhost","pinnedPeerCertSha256":tls.cert_sha256_hex,"echConfigList":ECH_CONFIG,"alpn":["h3"]},
            "xhttpSettings":{"path":"/ech/","mode":"stream-up"}
        }}]
    })
}

fn start_native(config: Value) -> zero_proxy::RunningProxy {
    spawn_engine(
        zero_proxy::Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap())
            .unwrap(),
    )
}

fn external_logs(processes: &[ExternalProcess]) -> String {
    processes
        .iter()
        .map(ExternalProcess::logs)
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_pinned_xray(binary: &str) {
    let output = std::process::Command::new(binary)
        .arg("version")
        .output()
        .expect("read XRAY_BIN version");
    let version = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && version.contains("26.3.27") && version.contains("d2758a0"),
        "XRAY_BIN must be official v26.3.27 d2758a0: {version}; {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
