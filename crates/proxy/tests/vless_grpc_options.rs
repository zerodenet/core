#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use serde_json::json;
use support::interop::{
    socks5_tcp_echo_once, socks5_udp_echo_sequence, spawn_tcp_echo, spawn_udp_echo_count,
    ExternalProcess, TempMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for, wait_for_listener};
use tokio::time::{timeout, Duration};
const ID: &str = "01234567-89ab-cdef-0123-456789abcdef";
#[tokio::test]
async fn grpc_multi_carries_native_vless_tcp_and_udp() {
    run(None, false, false).await;
}
#[tokio::test]
async fn grpc_multi_interoperates_with_pinned_official_client_and_server() {
    let Some(bin) = support::interop::require_env("XRAY_BIN") else {
        return;
    };
    run(Some(bin.clone()), false, false).await;
    run(Some(bin), true, false).await;
}
#[tokio::test]
async fn grpc_multi_keeps_tcp_and_udp_inside_its_relay_prefix() {
    run(None, false, true).await;
    if let Some(bin) = support::interop::require_env("XRAY_BIN") {
        run(Some(bin), true, true).await;
    }
}
async fn run(official: Option<String>, official_server: bool, relay: bool) {
    support::interop::init_logs("vless=debug,zero_transport=debug");
    let tunnel = free_port();
    let socks = free_port();
    let ready = free_port();
    let target_tcp = free_port();
    let target_udp = free_udp_port();
    let material = TempMaterial::new("vless-grpc-multi");
    let mut processes = Vec::new();
    let mut engines = Vec::new();
    let prefix_port = free_port();
    let prefix = if relay {
        let config = json!({"inbounds":[{"tag":"prefix","listen":{"address":"127.0.0.1","port":prefix_port},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}});
        let engine = spawn_engine(
            zero_proxy::Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap())
                .unwrap(),
        );
        wait_for_listener(prefix_port).await;
        Some(engine)
    } else {
        None
    };
    let server = json!({"inbounds":[
        {"tag":"tunnel","listen":{"address":"127.0.0.1","port":tunnel},"protocol":{"type":"vless","users":[{"id":ID}],"grpc":{"service_names":"/suite/one|many","multi_mode":true,"authority":"front.example","initial_window_size":262144,"idle_timeout_secs":10,"health_check_timeout_secs":2,"user_agent":"firefox"}}},
        {"tag":"ready","listen":{"address":"127.0.0.1","port":ready},"protocol":{"type":"socks5"}}
    ],"route":{"final":{"type":"direct"}}});
    let mut client = json!({"inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],
        "outbounds":[{"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":tunnel,"id":ID,"grpc":{"service_names":"/suite/one|many","multi_mode":true,"authority":"front.example","initial_window_size":262144,"idle_timeout_secs":10,"health_check_timeout_secs":2,"user_agent":"firefox"}}}],
        "route":{"final":{"type":"route","outbound":"node"}}});
    if relay {
        assert!(!official.is_some() || official_server);
        client["outbounds"].as_array_mut().unwrap().push(json!({"tag":"prefix","protocol":{"type":"socks5","server":"127.0.0.1","port":prefix_port}}));
        client["outbound_groups"] =
            json!([{"tag":"chain","type":"relay","proxies":["prefix","node"]}]);
        client["route"]["final"]["outbound"] = json!("chain");
    }
    let xray_server = json!({"log":{"loglevel":"warning"},"inbounds":[
        {"port":tunnel,"listen":"127.0.0.1","protocol":"vless","settings":{"decryption":"none","clients":[{"id":ID}]},"streamSettings":{"network":"grpc","grpcSettings":{"serviceName":"/suite/one|many","multiMode":true,"authority":"front.example","initial_windows_size":262144,"idle_timeout":10,"health_check_timeout":2,"user_agent":"firefox"}}},
        {"port":ready,"listen":"127.0.0.1","protocol":"socks"}],"outbounds":[{"protocol":"freedom"}]});
    let xray_client = json!({"log":{"loglevel":"warning"},"inbounds":[{"port":socks,"listen":"127.0.0.1","protocol":"socks","settings":{"udp":true}}],
        "outbounds":[{"protocol":"vless","settings":{"address":"127.0.0.1","port":tunnel,"id":ID,"encryption":"none"},"streamSettings":{"network":"grpc","grpcSettings":{"serviceName":"/suite/one|many","multiMode":true,"authority":"front.example","initial_windows_size":262144,"idle_timeout":10,"health_check_timeout":2,"user_agent":"firefox"}}}]});
    for (name, native, reference, is_official) in [
        (
            "server",
            server,
            xray_server,
            official.is_some() && official_server,
        ),
        (
            "client",
            client,
            xray_client,
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
    timeout(Duration::from_secs(20), async {
        for _ in 0..2 {
            let payload = vec![0x37; 131073];
            let echo = spawn_tcp_echo(target_tcp, payload.len()).await;
            assert_eq!(
                socks5_tcp_echo_once(socks, target_tcp, &payload)
                    .await
                    .unwrap(),
                payload
            );
            echo.await.unwrap();
        }
        let echo = spawn_udp_echo_count(target_udp, 3).await;
        let packets: &[&[u8]] = &[b"first", &[0x72; 1600], b"last"];
        assert_eq!(
            socks5_udp_echo_sequence(socks, target_udp, packets).await,
            packets
                .iter()
                .map(|packet| packet.to_vec())
                .collect::<Vec<_>>()
        );
        echo.await.unwrap();
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "gRPC transfer timed out: {}",
            processes
                .iter()
                .map(ExternalProcess::logs)
                .collect::<Vec<_>>()
                .join("\n")
        )
    });
    if let Some(prefix) = &prefix {
        wait_for("gRPC carrier uses the configured relay prefix", || {
            prefix
                .active_sessions()
                .iter()
                .any(|session| session.port == tunnel)
                || prefix
                    .completed_sessions()
                    .iter()
                    .any(|session| session.port == tunnel)
        })
        .await;
        assert_eq!(
            prefix
                .active_sessions()
                .iter()
                .filter(|session| session.port == tunnel)
                .count()
                + prefix
                    .completed_sessions()
                    .iter()
                    .filter(|session| session.port == tunnel)
                    .count(),
            1,
            "the two TCP routes and UDP route should reuse one relayed gRPC connection"
        );
    }
    for engine in engines {
        engine.shutdown().await.unwrap();
    }
    if let Some(prefix) = prefix {
        prefix.shutdown().await.unwrap();
    }
}
