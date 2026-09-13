#![cfg(all(feature = "socks5", feature = "vless"))]
#[path = "vless_finalmask/cases.rs"]
mod cases;
mod support;
use serde_json::{json, Value};
use support::interop::{
    socks5_tcp_echo_once, socks5_udp_echo_sequence, spawn_tcp_echo, spawn_udp_echo_count,
    ExternalProcess, TempMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::time::{timeout, Duration};
const ID: &str = "01234567-89ab-cdef-0123-456789abcdef";
#[tokio::test]
async fn native_finalmask_carriers_preserve_tcp_udp_and_tls_fragmentation() {
    for case in cases::all()
        .into_iter()
        .filter(|case| std::env::var("VLESS_FINALMASK_CASE").map_or(true, |name| name == case.name))
    {
        run(case, None, false).await;
    }
}
#[tokio::test]
async fn finalmask_interoperates_with_pinned_official_client_and_server() {
    let Some(bin) = support::interop::require_env("XRAY_BIN") else {
        return;
    };
    for case in cases::all()
        .into_iter()
        .filter(|case| std::env::var("VLESS_FINALMASK_CASE").map_or(true, |name| name == case.name))
    {
        run(case.clone(), Some(bin.clone()), false).await;
        run(case, Some(bin.clone()), true).await;
    }
}
async fn run(case: cases::Case, official: Option<String>, official_server: bool) {
    support::interop::init_logs("zero_transport=debug,vless=debug,rustls=debug");
    eprintln!(
        "FinalMask {} official={official:?} server={official_server}",
        case.name
    );
    let tunnel = if case.carrier == "tcp" || case.carrier == "tls" {
        free_port()
    } else {
        free_udp_port()
    };
    let ready = free_port();
    let socks = free_port();
    let target_tcp = free_port();
    let target_udp = free_udp_port();
    let material = TempMaterial::new("vless-finalmask");
    let tls = material.tls();
    let mut inbound = json!({"type":"vless","users":[{"id":ID}],"final_mask":case.native});
    let mut outbound =
        json!({"type":"vless","server":"127.0.0.1","port":tunnel,"id":ID,"final_mask":case.native});
    let mut server_stream = json!({"network":"raw","finalmask":case.reference});
    let mut client_stream = server_stream.clone();
    match case.carrier {
        "mkcp" | "mkcp_tls" => {
            inbound["mkcp"] = json!({"mtu":case.mtu});
            outbound["mkcp"] = json!({"mtu":case.mtu});
            server_stream["network"] = json!("mkcp");
            server_stream["kcpSettings"] = json!({"mtu":case.mtu});
            client_stream = server_stream.clone();
        }
        "hysteria" => {
            inbound["hysteria"] = json!({"auth":"secret"});
            outbound["hysteria"] = json!({"auth":"secret"});
            inbound["quic"] = json!({"cert_path":tls.cert_path,"key_path":tls.key_path});
            outbound["quic"] = json!({"server_name":"localhost","ca_cert_path":tls.cert_path});
            server_stream["network"] = json!("hysteria");
            server_stream["hysteriaSettings"] = json!({"version":2,"auth":"secret"});
            client_stream = server_stream.clone();
        }
        "tls" => {
            inbound["tls"] = json!({"cert_path":tls.cert_path,"key_path":tls.key_path});
            outbound["tls"] = json!({"server_name":"localhost","ca_cert_path":tls.cert_path});
        }
        _ => {}
    }
    if matches!(case.carrier, "tls" | "mkcp_tls" | "hysteria") {
        let alpn = if case.carrier == "hysteria" {
            json!(["h3"])
        } else {
            json!(["h2", "http/1.1"])
        };
        server_stream["security"] = json!("tls");
        server_stream["tlsSettings"] = json!({"alpn":alpn,"certificates":[{"certificateFile":tls.cert_path,"keyFile":tls.key_path}]});
        client_stream["security"] = json!("tls");
        client_stream["tlsSettings"] = json!({"alpn":alpn,"serverName":"localhost","pinnedPeerCertSha256":tls.cert_sha256_hex});
        if matches!(case.carrier, "tls" | "mkcp_tls") {
            inbound["tls"] = json!({"cert_path":tls.cert_path,"key_path":tls.key_path});
            outbound["tls"] = json!({"server_name":"localhost","ca_cert_path":tls.cert_path});
            outbound["tls"]["alpn"] = alpn.clone();
            inbound["tls"]["alpn"] = alpn;
        }
    }
    let server = json!({"inbounds":[{"tag":"tunnel","listen":{"address":"127.0.0.1","port":tunnel},"protocol":inbound},{"tag":"ready","listen":{"address":"127.0.0.1","port":ready},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}});
    let client = json!({"inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"node","protocol":outbound}],"route":{"final":{"type":"route","outbound":"node"}}});
    let reference_server = json!({"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":tunnel,"protocol":"vless","settings":{"decryption":"none","clients":[{"id":ID}]},"streamSettings":server_stream},{"listen":"127.0.0.1","port":ready,"protocol":"socks"}],"outbounds":[{"protocol":"freedom"}]});
    let reference_client = json!({"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":socks,"protocol":"socks","settings":{"udp":true}}],"outbounds":[{"protocol":"vless","settings":{"address":"127.0.0.1","port":tunnel,"id":ID,"encryption":"none"},"streamSettings":client_stream}]});
    let mut engines = Vec::new();
    let mut processes = Vec::new();
    for (name, native, reference, is_official) in [
        (
            "server",
            server,
            reference_server,
            official.is_some() && official_server,
        ),
        (
            "client",
            client,
            reference_client,
            official.is_some() && !official_server,
        ),
    ] {
        if is_official {
            let path = material.path(&format!("{name}.json"));
            std::fs::write(&path, reference.to_string()).unwrap();
            processes.push(ExternalProcess::start(
                official.clone().unwrap(),
                &["run", "-c", path.to_str().unwrap()],
                &material,
                name,
            ));
        } else {
            engines.push(spawn_engine(
                zero_proxy::Proxy::new(
                    zero_config::RuntimeConfig::parse(&native.to_string()).unwrap(),
                )
                .unwrap(),
            ));
        }
    }
    wait_for_listener(ready).await;
    wait_for_listener(socks).await;
    timeout(Duration::from_secs(30), async {
        let payload = vec![0x67; 32769];
        let echo = spawn_tcp_echo(target_tcp, payload.len()).await;
        assert_eq!(
            socks5_tcp_echo_once(socks, target_tcp, &payload)
                .await
                .unwrap_or_else(|error| panic!(
                    "{}: {error}; {}",
                    case.name,
                    processes
                        .iter()
                        .map(ExternalProcess::logs)
                        .collect::<Vec<_>>()
                        .join("\n")
                )),
            payload
        );
        echo.await.unwrap();
        let echo = spawn_udp_echo_count(target_udp, 3).await;
        let packets: &[&[u8]] = &[b"first", &[0x35; 1600], b"last"];
        assert_eq!(
            socks5_udp_echo_sequence(socks, target_udp, packets).await,
            packets.iter().map(|p| p.to_vec()).collect::<Vec<_>>()
        );
        echo.await.unwrap();
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "FinalMask {} timed out: {}",
            case.name,
            processes
                .iter()
                .map(ExternalProcess::logs)
                .collect::<Vec<_>>()
                .join("\n")
        )
    });
    for engine in engines {
        engine.shutdown().await.unwrap();
    }
}
