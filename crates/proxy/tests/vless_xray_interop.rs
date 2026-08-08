#![cfg(all(feature = "socks5", feature = "vless"))]

mod support;
use tokio::time::{timeout, Duration};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::interop::*;
use support::{free_port, free_udp_port, spawn_engine, wait_for, wait_for_listener};

const USER_ID: &str = "11111111-2222-3333-4444-555555555555";
const WRONG_USER_ID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const XRAY_WS_PATH: &str = "/zero-vless-ws";
const XRAY_GRPC_SERVICE_NAME: &str = "zero.vless.grpc";
const ZERO_GRPC_SERVICE_PATH: &str = "/zero.vless.grpc/Tun";
const XRAY_XHTTP_PATH: &str = "/zero-vless-xhttp/";

#[path = "vless_xray_interop/reality_vision.rs"]
mod reality_vision;

#[derive(Debug, Clone, Copy)]
enum VlessTransport {
    Tcp,
    Ws,
    Grpc,
    XhttpStreamOne,
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vless_outbound_interops_with_xray_vless_inbound_tcp() {
    init_logs("vless=debug");
    let material = TempMaterial::new("zero-xray-vless-tcp-out");
    let xray_port = free_port();
    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_vless_inbound_config(xray_port, VlessTransport::Tcp),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;

    exercise_vless_tcp(xray_port, USER_ID, VlessTransport::Tcp, "xray", || {
        xray.logs()
    })
    .await;
    xray.kill();
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vless_outbound_interops_with_xray_vless_inbound_ws_tcp() {
    exercise_xray_vless_tcp_transport(VlessTransport::Ws).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vless_outbound_interops_with_xray_vless_inbound_grpc_tcp() {
    exercise_xray_vless_tcp_transport(VlessTransport::Grpc).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vless_outbound_interops_with_xray_vless_inbound_xhttp_stream_one_tcp() {
    exercise_xray_vless_tcp_transport(VlessTransport::XhttpStreamOne).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vless_outbound_interops_with_xray_vless_inbound_udp() {
    init_logs("vless=debug");
    let material = TempMaterial::new("zero-xray-vless-udp-out");
    let xray_port = free_port();
    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_vless_inbound_config(xray_port, VlessTransport::Tcp),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;

    exercise_vless_udp(xray_port, VlessTransport::Tcp, "xray", || xray.logs()).await;
    xray.kill();
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_xudp_interops_with_xray_vless_inbound() {
    init_logs("vless=debug");
    let material = TempMaterial::new("zero-xray-vless-xudp-out");
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let first_echo_port = free_udp_port();
    let second_echo_port = free_udp_port();
    let payloads: [&[u8]; 3] = [
        b"zero-xray-xudp-new",
        b"zero-xray-xudp-keep-target-change",
        b"zero-xray-xudp-keep-target-return",
    ];
    let packets = [
        (first_echo_port, payloads[0]),
        (second_echo_port, payloads[1]),
        (first_echo_port, payloads[2]),
    ];
    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_vless_inbound_config(xray_port, VlessTransport::Tcp),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;
    let zero = spawn_vless_xudp_outbound(zero_socks_port, xray_port).await;

    let first_echo = spawn_udp_echo_count(first_echo_port, 2).await;
    let second_echo = spawn_udp_echo_count(second_echo_port, 1).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_udp_echo_targets(zero_socks_port, &packets),
    )
    .await
    .unwrap_or_else(|error| panic!("Zero XUDP -> Xray timed out: {error}; logs={}", xray.logs()));
    assert_eq!(echoed, payloads, "xray logs={}", xray.logs());
    exercise_concurrent_xudp_associations(zero_socks_port).await;

    shutdown_zero(zero).await;
    xray.kill();
    wait_for_echo(first_echo).await;
    wait_for_echo(second_echo).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vless_outbound_interops_with_xray_vless_inbound_xhttp_stream_one_udp() {
    init_logs("vless=debug");
    let material = TempMaterial::new("zero-xray-vless-xhttp-stream-one-udp-out");
    let xray_port = free_port();
    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_vless_inbound_config(xray_port, VlessTransport::XhttpStreamOne),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;

    exercise_vless_udp(xray_port, VlessTransport::XhttpStreamOne, "xray", || {
        xray.logs()
    })
    .await;
    xray.kill();
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vless_xhttp_stream_one_final_hop_interops_with_xray_over_socks_relay() {
    init_logs("vless=debug");
    let material = TempMaterial::new("zero-xray-vless-xhttp-stream-one-relay");
    let xray_port = free_port();
    let first_hop_port = free_port();
    let outer_port = free_port();
    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_vless_inbound_config(xray_port, VlessTransport::XhttpStreamOne),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;

    let first_hop = spawn_direct_socks5(first_hop_port).await;
    let outer_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "outer-socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "first-socks",
                    "protocol": {{
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": {first_hop_port}
                    }}
                }},
                {{
                    "tag": "final-vless",
                    "protocol": {{
                        "type": "vless",
                        "server": "127.0.0.1",
                        "port": {xray_port},
                        "id": "{USER_ID}",
                        "split_http": {{
                            "path": "{XRAY_XHTTP_PATH}",
                            "mode": "stream-one"
                        }}
                    }}
                }}
            ],
            "outbound_groups": [
                {{
                    "tag": "relay-chain",
                    "type": "relay",
                    "proxies": ["first-socks", "final-vless"]
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "relay-chain" }}
            }}
        }}"#
    ))
    .expect("parse relay-chain config");
    let outer = spawn_engine(Engine::new(outer_config).expect("build relay-chain engine"));
    wait_for_listener(outer_port).await;

    let tcp_echo_port = free_port();
    let tcp_payload = b"xhttp-relay-tcp";
    let tcp_echo = spawn_tcp_echo(tcp_echo_port, tcp_payload.len()).await;
    let tcp_result = timeout(
        Duration::from_secs(10),
        socks5_tcp_echo_once(outer_port, tcp_echo_port, tcp_payload),
    )
    .await
    .unwrap_or_else(|error| panic!("relay TCP timed out: {error}; logs={}", xray.logs()))
    .unwrap_or_else(|error| panic!("relay TCP failed: {error:?}; logs={}", xray.logs()));
    assert_eq!(tcp_result, tcp_payload, "xray logs={}", xray.logs());
    wait_for_echo(tcp_echo).await;

    let udp_echo_port = free_udp_port();
    let udp_payload = b"xhttp-relay-udp";
    let udp_echo = spawn_udp_echo(udp_echo_port, udp_payload.len()).await;
    let udp_result = timeout(
        Duration::from_secs(10),
        socks5_udp_echo(outer_port, udp_echo_port, udp_payload),
    )
    .await
    .unwrap_or_else(|error| panic!("relay UDP timed out: {error}; logs={}", xray.logs()));
    assert_eq!(udp_result, udp_payload, "xray logs={}", xray.logs());
    wait_for_echo(udp_echo).await;

    shutdown_zero(outer).await;
    shutdown_zero(first_hop).await;
    xray.kill();
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_vless_outbound_interops_with_zero_vless_inbound_xhttp_stream_one_tcp() {
    init_logs("vless=debug");
    let material = TempMaterial::new("xray-zero-vless-xhttp-stream-one-tcp");
    let zero_vless_port = free_port();
    let xray_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"xray-zero-xhttp-tcp";

    let zero = spawn_vless_xhttp_inbound(zero_vless_port).await;
    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        xray_vless_outbound_config(xray_socks_port, zero_vless_port, false, false, -1),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        shutdown_zero(zero).await;
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_socks_port).await;

    let echo = spawn_tcp_echo(echo_port, payload.len()).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_tcp_echo_once(xray_socks_port, echo_port, payload),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "Xray -> Zero VLESS/XHTTP TCP timed out: {error}; logs={}",
            xray.logs()
        )
    })
    .unwrap_or_else(|error| {
        panic!(
            "Xray -> Zero VLESS/XHTTP TCP failed: {error:?}; logs={}",
            xray.logs()
        )
    });
    assert_eq!(echoed, payload, "xray logs={}", xray.logs());

    xray.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_vless_outbound_interops_with_zero_vless_inbound_xhttp_stream_one_udp() {
    init_logs("vless=debug");
    let material = TempMaterial::new("xray-zero-vless-xhttp-stream-one-udp");
    let zero_vless_port = free_port();
    let xray_socks_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"xray-zero-xhttp-udp";

    let zero = spawn_vless_xhttp_inbound(zero_vless_port).await;
    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        xray_vless_outbound_config(xray_socks_port, zero_vless_port, true, false, -1),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        shutdown_zero(zero).await;
        return;
    };
    // Xray enables its proprietary XUDP/Mux.Cool aggregation globally by
    // default. Disable it so this case isolates the standard VLESS UDP path
    // over XHTTP; XUDP has a separate wire contract and capability status.
    let mut xray = XrayProcess::start_with_env(
        xray_bin,
        &xray_config,
        &[("XRAY_CONE_DISABLED", "true")],
        &material,
    );
    wait_for_listener(xray_socks_port).await;

    let echo = spawn_udp_echo(echo_port, payload.len()).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_udp_echo(xray_socks_port, echo_port, payload),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "Xray -> Zero VLESS/XHTTP UDP timed out: {error}; logs={}",
            xray.logs()
        )
    });
    assert_eq!(echoed, payload, "xray logs={}", xray.logs());

    xray.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_xudp_interops_with_zero_vless_inbound_xhttp_stream_one() {
    init_logs("vless=debug");
    let material = TempMaterial::new("xray-zero-vless-xhttp-stream-one-xudp");
    let zero_vless_port = free_port();
    let xray_socks_port = free_port();
    let first_echo_port = free_udp_port();
    let second_echo_port = free_udp_port();
    let payloads: [&[u8]; 3] = [
        b"xray-zero-xudp-new",
        b"xray-zero-xudp-keep-target-change",
        b"xray-zero-xudp-keep-target-return",
    ];
    let packets = [
        (first_echo_port, payloads[0]),
        (second_echo_port, payloads[1]),
        (first_echo_port, payloads[2]),
    ];

    let zero = spawn_vless_xhttp_inbound(zero_vless_port).await;
    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        xray_vless_outbound_config(xray_socks_port, zero_vless_port, true, false, 8),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        shutdown_zero(zero).await;
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_socks_port).await;

    let first_echo = spawn_udp_echo_count(first_echo_port, 2).await;
    let second_echo = spawn_udp_echo_count(second_echo_port, 1).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_udp_echo_targets(xray_socks_port, &packets),
    )
    .await
    .unwrap_or_else(|error| panic!("Xray XUDP -> Zero timed out: {error}; logs={}", xray.logs()));
    assert_eq!(echoed, payloads, "xray logs={}", xray.logs());
    exercise_concurrent_xudp_associations(xray_socks_port).await;

    xray.kill();
    shutdown_zero(zero).await;
    wait_for_echo(first_echo).await;
    wait_for_echo(second_echo).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_xudp_reattaches_zero_dispatch_after_carrier_reset() {
    init_logs("vless=debug");
    let material = TempMaterial::new("xray-zero-vless-xudp-carrier-reset");
    let zero_vless_port = free_port();
    let reset_proxy_port = free_port();
    let xray_socks_port = free_port();
    let echo_port = free_udp_port();
    let payloads: [&[u8]; 2] = [b"xudp-before-carrier-reset", b"xudp-after-carrier-reset"];
    let packets = [(echo_port, payloads[0]), (echo_port, payloads[1])];

    let zero = spawn_vless_xhttp_inbound_with_udp_timeout(zero_vless_port, 1).await;
    let reset_proxy = TcpResetProxy::start(reset_proxy_port, zero_vless_port).await;
    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        xray_vless_outbound_config(xray_socks_port, reset_proxy_port, true, false, 8),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        reset_proxy.shutdown().await;
        shutdown_zero(zero).await;
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_socks_port).await;

    let echo = spawn_udp_echo_count(echo_port, payloads.len()).await;
    let mut first_session_id = None;
    let echoed = timeout(
        Duration::from_secs(15),
        socks5_udp_echo_targets_after_first(xray_socks_port, &packets, || {
            let active = zero.active_sessions();
            assert_eq!(active.len(), 1, "expected one active XUDP flow");
            first_session_id = Some(active[0].id);
            reset_proxy.reset_connections();
        }),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "Xray XUDP carrier recovery timed out: {error}; logs={}",
            xray.logs()
        )
    });
    assert_eq!(echoed, payloads, "xray logs={}", xray.logs());
    let active = zero.active_sessions();
    assert_eq!(active.len(), 1, "reattach created or lost the UDP flow");
    assert_eq!(Some(active[0].id), first_session_id);

    xray.kill();
    reset_proxy.shutdown().await;
    wait_for("recovered XUDP flow settlement", || {
        zero.active_sessions().is_empty()
    })
    .await;
    let completed = zero.completed_sessions();
    assert_eq!(completed.len(), 1);
    assert_eq!(Some(completed[0].id), first_session_id);
    assert_eq!(
        completed[0].bytes_up,
        payloads
            .iter()
            .map(|payload| payload.len() as u64)
            .sum::<u64>()
    );
    assert_eq!(
        completed[0].bytes_down,
        payloads
            .iter()
            .map(|payload| payload.len() as u64)
            .sum::<u64>()
    );
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_mux_tcp_interops_with_zero_vless_inbound_xhttp_stream_one() {
    init_logs("vless=debug");
    let material = TempMaterial::new("xray-zero-vless-xhttp-stream-one-mux-tcp");
    let zero_vless_port = free_port();
    let xray_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"xray-zero-mux-tcp";

    let zero = spawn_vless_xhttp_inbound(zero_vless_port).await;
    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        xray_vless_outbound_config(xray_socks_port, zero_vless_port, false, true, -1),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        shutdown_zero(zero).await;
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_socks_port).await;

    let echo = spawn_tcp_echo(echo_port, payload.len()).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_tcp_echo_once(xray_socks_port, echo_port, payload),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "Xray MUX TCP -> Zero timed out: {error}; logs={}",
            xray.logs()
        )
    })
    .unwrap_or_else(|error| {
        panic!(
            "Xray MUX TCP -> Zero failed: {error:?}; logs={}",
            xray.logs()
        )
    });
    assert_eq!(echoed, payload, "xray logs={}", xray.logs());

    xray.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_mux_tcp_interops_with_xray_vless_inbound() {
    init_logs("vless=debug");
    let material = TempMaterial::new("zero-xray-vless-mux-tcp-out");
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"zero-xray-mux-tcp";
    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_vless_inbound_config(xray_port, VlessTransport::Tcp),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;
    let zero = spawn_vless_xudp_outbound(zero_socks_port, xray_port).await;

    let echo = spawn_tcp_echo(echo_port, payload.len()).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_tcp_echo_once(zero_socks_port, echo_port, payload),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "Zero MUX TCP -> Xray timed out: {error}; logs={}",
            xray.logs()
        )
    })
    .unwrap_or_else(|error| {
        panic!(
            "Zero MUX TCP -> Xray failed: {error:?}; logs={}",
            xray.logs()
        )
    });
    assert_eq!(echoed, payload, "xray logs={}", xray.logs());

    shutdown_zero(zero).await;
    xray.kill();
    wait_for_echo(echo).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_vless_inbound_rejects_wrong_zero_user() {
    init_logs("vless=debug");
    let material = TempMaterial::new("zero-xray-vless-wrong-user");
    let xray_port = free_port();
    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_vless_inbound_config(xray_port, VlessTransport::Tcp),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;

    exercise_vless_rejection(xray_port, WRONG_USER_ID, || xray.logs()).await;
    xray.kill();
}

#[tokio::test]
#[ignore = "requires SING_BOX_BIN pointing to a sing-box executable"]
async fn zero_vless_outbound_interops_with_sing_box_vless_inbound_tcp() {
    init_logs("vless=debug");
    let material = TempMaterial::new("zero-sing-vless-tcp-out");
    let sing_port = free_port();
    let sing_config = material.path("sing-box-server.json");
    std::fs::write(&sing_config, sing_box_vless_inbound_config(sing_port))
        .expect("write sing-box config");
    let mut sing_box = ExternalProcess::start(
        sing_box_bin("vless"),
        &[
            "run",
            "-c",
            sing_config.to_str().expect("sing-box config path"),
        ],
        &material,
        "sing-box",
    );
    wait_for_listener(sing_port).await;

    exercise_vless_tcp(sing_port, USER_ID, VlessTransport::Tcp, "sing-box", || {
        sing_box.logs()
    })
    .await;
    sing_box.kill();
}

#[tokio::test]
#[ignore = "requires SING_BOX_BIN pointing to a sing-box executable"]
async fn zero_vless_outbound_interops_with_sing_box_vless_inbound_udp() {
    init_logs("vless=debug");
    let material = TempMaterial::new("zero-sing-vless-udp-out");
    let sing_port = free_port();
    let sing_config = material.path("sing-box-server.json");
    std::fs::write(&sing_config, sing_box_vless_inbound_config(sing_port))
        .expect("write sing-box config");
    let mut sing_box = ExternalProcess::start(
        sing_box_bin("vless"),
        &[
            "run",
            "-c",
            sing_config.to_str().expect("sing-box config path"),
        ],
        &material,
        "sing-box",
    );
    wait_for_listener(sing_port).await;

    exercise_vless_udp(sing_port, VlessTransport::Tcp, "sing-box", || {
        sing_box.logs()
    })
    .await;
    sing_box.kill();
}

async fn exercise_vless_tcp(
    server_port: u16,
    user_id: &str,
    transport: VlessTransport,
    peer: &str,
    logs: impl Fn() -> String,
) {
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"external-vless-tcp";
    let zero = spawn_vless_outbound(zero_socks_port, server_port, user_id, transport).await;

    let echo = spawn_tcp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_tcp_echo_once(zero_socks_port, echo_port, payload),
    )
    .await
    {
        Ok(Ok(echoed)) => echoed,
        Ok(Err(error)) => panic!("zero -> {peer} VLESS failed: {error:?}; logs={}", logs()),
        Err(error) => panic!("zero -> {peer} VLESS timed out: {error}; logs={}", logs()),
    };
    assert_eq!(echoed, payload, "{peer} logs={}", logs());

    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

async fn exercise_vless_udp(
    server_port: u16,
    transport: VlessTransport,
    peer: &str,
    logs: impl Fn() -> String,
) {
    let zero_socks_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"external-vless-udp";
    let zero = spawn_vless_outbound(zero_socks_port, server_port, USER_ID, transport).await;

    let echo = spawn_udp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_udp_echo(zero_socks_port, echo_port, payload),
    )
    .await
    {
        Ok(echoed) => echoed,
        Err(error) => panic!(
            "zero -> {peer} VLESS UDP timed out: {error}; logs={}",
            logs()
        ),
    };
    assert_eq!(echoed, payload, "{peer} logs={}", logs());

    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

async fn exercise_vless_rejection(server_port: u16, user_id: &str, logs: impl Fn() -> String) {
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"wrong-vless-user";
    let zero =
        spawn_vless_outbound(zero_socks_port, server_port, user_id, VlessTransport::Tcp).await;
    let echo = spawn_tcp_echo(echo_port, payload.len()).await;

    let result = timeout(
        Duration::from_secs(5),
        socks5_tcp_echo_once(zero_socks_port, echo_port, payload),
    )
    .await;
    assert!(
        !matches!(result, Ok(Ok(echoed)) if echoed == payload),
        "Xray accepted an unknown VLESS user; logs={}",
        logs()
    );

    shutdown_zero(zero).await;
    echo.abort();
}

async fn spawn_vless_outbound(
    zero_socks_port: u16,
    server_port: u16,
    user_id: &str,
    transport: VlessTransport,
) -> zero_proxy::RunningProxy {
    let transport_config = zero_vless_outbound_transport_config(transport);
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_socks_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "vless-out",
                    "protocol": {{
                        "type": "vless",
                        "server": "127.0.0.1",
                        "port": {server_port},
                        "id": "{user_id}"{transport_config}
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "vless-out" }} }}
        }}"#
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(config).expect("build zero engine"));
    wait_for_listener(zero_socks_port).await;
    zero
}

async fn spawn_vless_xudp_outbound(
    zero_socks_port: u16,
    server_port: u16,
) -> zero_proxy::RunningProxy {
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_socks_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "vless-out",
                    "protocol": {{
                        "type": "vless",
                        "server": "127.0.0.1",
                        "port": {server_port},
                        "id": "{USER_ID}",
                        "mux_concurrency": 8,
                        "xudp_concurrency": 8
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "vless-out" }} }}
        }}"#
    ))
    .expect("parse Zero XUDP config");
    let zero = spawn_engine(Engine::new(config).expect("build Zero XUDP engine"));
    wait_for_listener(zero_socks_port).await;
    zero
}

async fn spawn_direct_socks5(port: u16) -> zero_proxy::RunningProxy {
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse direct SOCKS5 config");
    let zero = spawn_engine(Engine::new(config).expect("build direct SOCKS5 engine"));
    wait_for_listener(port).await;
    zero
}

async fn spawn_vless_xhttp_inbound(port: u16) -> zero_proxy::RunningProxy {
    spawn_vless_xhttp_inbound_with_udp_timeout(port, 120).await
}

async fn spawn_vless_xhttp_inbound_with_udp_timeout(
    port: u16,
    udp_upstream_idle_timeout_seconds: u64,
) -> zero_proxy::RunningProxy {
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "runtime": {{
                "udp_upstream_idle_timeout_seconds": {udp_upstream_idle_timeout_seconds}
            }},
            "inbounds": [
                {{
                    "tag": "vless-in",
                    "listen": {{ "address": "127.0.0.1", "port": {port} }},
                    "protocol": {{
                        "type": "vless",
                        "users": [
                            {{ "id": "{USER_ID}", "principal_key": "interop:xray" }}
                        ],
                        "split_http": {{
                            "path": "{XRAY_XHTTP_PATH}",
                            "mode": "stream-one"
                        }}
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse Zero VLESS/XHTTP inbound config");
    let zero = spawn_engine(Engine::new(config).expect("build Zero VLESS/XHTTP inbound"));
    wait_for_listener(port).await;
    zero
}

async fn shutdown_zero(zero: zero_proxy::RunningProxy) {
    timeout(Duration::from_secs(5), zero.shutdown())
        .await
        .expect("zero shutdown timed out")
        .expect("shutdown zero");
}

async fn wait_for_echo(echo: tokio::task::JoinHandle<()>) {
    timeout(Duration::from_secs(5), echo)
        .await
        .expect("echo task timed out")
        .expect("echo task");
}

async fn exercise_concurrent_xudp_associations(proxy_port: u16) {
    let first_echo_port = free_udp_port();
    let second_echo_port = free_udp_port();
    let first_payloads: [&[u8]; 2] = [b"xudp-concurrent-first-new", b"xudp-concurrent-first-keep"];
    let second_payloads: [&[u8]; 2] = [
        b"xudp-concurrent-second-new",
        b"xudp-concurrent-second-keep",
    ];
    let first_echo = spawn_udp_echo_count(first_echo_port, first_payloads.len()).await;
    let second_echo = spawn_udp_echo_count(second_echo_port, second_payloads.len()).await;

    let (first_result, second_result) = timeout(Duration::from_secs(10), async {
        tokio::join!(
            socks5_udp_echo_sequence(proxy_port, first_echo_port, &first_payloads),
            socks5_udp_echo_sequence(proxy_port, second_echo_port, &second_payloads),
        )
    })
    .await
    .expect("concurrent XUDP associations timed out");

    assert_eq!(first_result, first_payloads);
    assert_eq!(second_result, second_payloads);
    wait_for_echo(first_echo).await;
    wait_for_echo(second_echo).await;
}

async fn exercise_xray_vless_tcp_transport(transport: VlessTransport) {
    init_logs("vless=debug");
    let material = TempMaterial::new("zero-xray-vless-transport-out");
    let xray_port = free_port();
    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_vless_inbound_config(xray_port, transport),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;

    exercise_vless_tcp(xray_port, USER_ID, transport, "xray", || xray.logs()).await;
    xray.kill();
}

fn zero_vless_outbound_transport_config(transport: VlessTransport) -> String {
    match transport {
        VlessTransport::Tcp => String::new(),
        VlessTransport::Ws => format!(r#", "ws": {{ "path": "{XRAY_WS_PATH}" }}"#),
        VlessTransport::Grpc => {
            format!(r#", "grpc": {{ "service_names": ["{ZERO_GRPC_SERVICE_PATH}"] }}"#)
        }
        VlessTransport::XhttpStreamOne => {
            format!(r#", "split_http": {{ "path": "{XRAY_XHTTP_PATH}", "mode": "stream-one" }}"#)
        }
    }
}

fn xray_vless_inbound_config(port: u16, transport: VlessTransport) -> String {
    let stream_settings = match transport {
        VlessTransport::Tcp => r#"{ "network": "tcp", "security": "none" }"#.to_owned(),
        VlessTransport::Ws => format!(
            r#"{{ "network": "ws", "security": "none", "wsSettings": {{ "path": "{XRAY_WS_PATH}" }} }}"#
        ),
        VlessTransport::Grpc => format!(
            r#"{{ "network": "grpc", "security": "none", "grpcSettings": {{ "serviceName": "{XRAY_GRPC_SERVICE_NAME}" }} }}"#
        ),
        VlessTransport::XhttpStreamOne => format!(
            r#"{{ "network": "xhttp", "security": "none", "xhttpSettings": {{ "path": "{XRAY_XHTTP_PATH}", "mode": "stream-one" }} }}"#
        ),
    };
    format!(
        r#"{{
            "log": {{ "loglevel": "debug" }},
            "inbounds": [
                {{
                    "listen": "127.0.0.1",
                    "port": {port},
                    "protocol": "vless",
                    "settings": {{
                        "clients": [{{ "id": "{USER_ID}" }}],
                        "decryption": "none"
                    }},
                    "streamSettings": {stream_settings}
                }}
            ],
            "outbounds": [{{ "protocol": "freedom", "settings": {{}} }}]
        }}"#
    )
}

fn xray_vless_outbound_config(
    socks_port: u16,
    zero_vless_port: u16,
    socks_udp: bool,
    mux_enabled: bool,
    xudp_concurrency: i32,
) -> String {
    format!(
        r#"{{
            "log": {{ "loglevel": "debug" }},
            "inbounds": [
                {{
                    "listen": "127.0.0.1",
                    "port": {socks_port},
                    "protocol": "socks",
                    "settings": {{ "auth": "noauth", "udp": {socks_udp} }}
                }}
            ],
            "outbounds": [
                {{
                    "protocol": "vless",
                    "settings": {{
                        "vnext": [
                            {{
                                "address": "127.0.0.1",
                                "port": {zero_vless_port},
                                "users": [
                                    {{
                                        "id": "{USER_ID}",
                                        "encryption": "none"
                                    }}
                                ]
                            }}
                        ]
                    }},
                    "streamSettings": {{
                        "network": "xhttp",
                        "security": "none",
                        "xhttpSettings": {{
                            "path": "{XRAY_XHTTP_PATH}",
                            "mode": "stream-one"
                        }}
                    }},
                    "mux": {{
                        "enabled": {mux_enabled},
                        "concurrency": 8,
                        "xudpConcurrency": {xudp_concurrency}
                    }}
                }}
            ]
        }}"#
    )
}

fn sing_box_vless_inbound_config(port: u16) -> String {
    format!(
        r#"{{
            "log": {{ "level": "debug" }},
            "inbounds": [
                {{
                    "type": "vless",
                    "tag": "vless-in",
                    "listen": "127.0.0.1",
                    "listen_port": {port},
                    "users": [{{ "uuid": "{USER_ID}" }}]
                }}
            ],
            "outbounds": [{{ "type": "direct", "tag": "direct" }}],
            "route": {{ "final": "direct" }}
        }}"#
    )
}
