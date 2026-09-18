#![cfg(all(feature = "socks5", feature = "vmess"))]

mod support;

use std::path::Path;

use tokio::time::{timeout, Duration};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::interop::*;
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};

const USER_ID: &str = "11111111-2222-3333-4444-555555555555";
const XRAY_WS_PATH: &str = "/zero-vmess-ws";
const XRAY_GRPC_SERVICE_NAME: &str = "zero.vmess.grpc";
const ZERO_GRPC_SERVICE_PATH: &str = "/zero.vmess.grpc/Tun";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum XrayTransport {
    Tcp,
    Ws,
    Grpc,
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vmess_outbound_interops_with_xray_vmess_inbound_tcp() {
    zero_vmess_outbound_interops_with_xray_vmess_inbound_tcp_cipher("aes-128-gcm").await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vmess_outbound_interops_with_xray_vmess_inbound_ws_tcp() {
    zero_vmess_outbound_interops_with_xray_vmess_inbound_tcp_transport(XrayTransport::Ws).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vmess_outbound_interops_with_xray_vmess_inbound_grpc_tcp() {
    zero_vmess_outbound_interops_with_xray_vmess_inbound_tcp_transport(XrayTransport::Grpc).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vmess_outbound_none_interops_with_xray_vmess_inbound_tcp() {
    zero_vmess_outbound_interops_with_xray_vmess_inbound_tcp_cipher("none").await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vmess_outbound_zero_interops_with_xray_vmess_inbound_tcp() {
    zero_vmess_outbound_interops_with_xray_vmess_inbound_tcp_cipher("zero").await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vmess_outbound_interops_with_xray_vmess_inbound_udp() {
    init_logs("vmess=debug");
    let material = TempMaterial::new("zero-xray-vmess-udp-out");
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"xray-vmess-udp";

    let xray_config = material.path("xray-server.json");
    std::fs::write(&xray_config, xray_vmess_inbound_config(xray_port)).expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;

    let zero_config = RuntimeConfig::parse(&format!(
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
                    "tag": "vmess-out",
                    "protocol": {{
                        "type": "vmess",
                        "server": "127.0.0.1",
                        "port": {xray_port},
                        "id": "{USER_ID}",
                        "cipher": "aes-128-gcm"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "vmess-out" }} }}
        }}"#
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
    wait_for_listener(zero_socks_port).await;

    let echo = spawn_udp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_udp_echo(zero_socks_port, echo_port, payload),
    )
    .await
    {
        Ok(echoed) => echoed,
        Err(error) => panic!(
            "zero -> xray UDP interop timed out: {error}; xray={}",
            xray.logs()
        ),
    };
    assert_eq!(echoed, payload, "xray={}", xray.logs());

    zero.shutdown().await.expect("shutdown zero");
    xray.kill();
    echo.await.expect("echo task");
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vmess_outbound_zero_plus_is_rejected_by_xray_vmess_inbound_tcp() {
    init_logs("vmess=debug");
    let material = TempMaterial::new("zero-xray-vmess-zero-out");
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"xray-vmess-zero-tcp";

    let xray_config = material.path("xray-server.json");
    std::fs::write(&xray_config, xray_vmess_inbound_config(xray_port)).expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;

    let zero_config = RuntimeConfig::parse(&format!(
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
                    "tag": "vmess-out",
                    "protocol": {{
                        "type": "vmess",
                        "server": "127.0.0.1",
                        "port": {xray_port},
                        "id": "{USER_ID}",
                        "cipher": "zero-plus"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "vmess-out" }} }}
        }}"#
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
    wait_for_listener(zero_socks_port).await;

    let echo = spawn_tcp_echo(echo_port, payload.len()).await;
    let result = timeout(
        Duration::from_secs(5),
        socks5_tcp_echo_once(zero_socks_port, echo_port, payload),
    )
    .await;
    assert!(
        !matches!(result, Ok(Ok(echoed)) if echoed == payload),
        "Xray unexpectedly accepted VMess cipher zero-plus; xray={}",
        xray.logs()
    );

    zero.shutdown().await.expect("shutdown zero");
    xray.kill();
    echo.abort();
}

async fn zero_vmess_outbound_interops_with_xray_vmess_inbound_tcp_cipher(cipher: &str) {
    zero_vmess_outbound_interops_with_xray_vmess_inbound_tcp_inner(cipher, XrayTransport::Tcp)
        .await;
}

async fn zero_vmess_outbound_interops_with_xray_vmess_inbound_tcp_transport(
    transport: XrayTransport,
) {
    zero_vmess_outbound_interops_with_xray_vmess_inbound_tcp_inner("aes-128-gcm", transport).await;
}

async fn zero_vmess_outbound_interops_with_xray_vmess_inbound_tcp_inner(
    cipher: &str,
    transport: XrayTransport,
) {
    init_logs("vmess=debug");
    let material = TempMaterial::new("zero-xray-vmess-out");
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"xray-vmess-tcp";
    let tls = material.tls();

    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_vmess_inbound_config_with_transport(xray_port, transport, Some(&tls)),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;

    let outbound_transport = zero_vmess_outbound_transport_config(transport, &tls.cert_path);
    let zero_config = RuntimeConfig::parse(&format!(
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
                    "tag": "vmess-out",
                    "protocol": {{
                        "type": "vmess",
                        "server": "127.0.0.1",
                        "port": {xray_port},
                        "id": "{USER_ID}",
                        "cipher": "{cipher}"{outbound_transport}
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "vmess-out" }} }}
        }}"#
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
    wait_for_listener(zero_socks_port).await;

    let echo = spawn_tcp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_tcp_echo_once(zero_socks_port, echo_port, payload),
    )
    .await
    {
        Ok(Ok(echoed)) => echoed,
        Ok(Err(error)) => panic!(
            "zero -> xray interop failed: {error:?}; xray={}",
            xray.logs()
        ),
        Err(error) => panic!(
            "zero -> xray interop timed out: {error}; xray={}",
            xray.logs()
        ),
    };
    assert_eq!(echoed, payload, "xray={}", xray.logs());

    zero.shutdown().await.expect("shutdown zero");
    xray.kill();
    echo.await.expect("echo task");
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_vmess_outbound_interops_with_zero_vmess_inbound_tcp() {
    xray_vmess_outbound_interops_with_zero_vmess_inbound_tcp_security("aes-128-gcm").await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_vmess_outbound_interops_with_zero_vmess_inbound_ws_tcp() {
    xray_vmess_outbound_interops_with_zero_vmess_inbound_tcp_inner(
        "aes-128-gcm",
        XrayTransport::Ws,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_vmess_outbound_interops_with_zero_vmess_inbound_grpc_tcp() {
    xray_vmess_outbound_interops_with_zero_vmess_inbound_tcp_inner(
        "aes-128-gcm",
        XrayTransport::Grpc,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_vmess_outbound_none_interops_with_zero_vmess_inbound_tcp() {
    xray_vmess_outbound_interops_with_zero_vmess_inbound_tcp_security("none").await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_vmess_outbound_zero_interops_with_zero_vmess_inbound_tcp() {
    xray_vmess_outbound_interops_with_zero_vmess_inbound_tcp_security("zero").await;
}

async fn xray_vmess_outbound_interops_with_zero_vmess_inbound_tcp_security(security: &str) {
    xray_vmess_outbound_interops_with_zero_vmess_inbound_tcp_inner(security, XrayTransport::Tcp)
        .await;
}

async fn xray_vmess_outbound_interops_with_zero_vmess_inbound_tcp_inner(
    security: &str,
    transport: XrayTransport,
) {
    init_logs("vmess=debug");
    let material = TempMaterial::new("xray-zero-vmess-out");
    let xray_socks_port = free_port();
    let zero_vmess_port = free_port();
    let echo_port = free_port();
    let payload = b"zero-vmess-tcp";
    let tls = material.tls();

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "vmess-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_vmess_port} }},
                    "protocol": {{
                        "type": "vmess",
                        "users": [{{ "id": "{USER_ID}", "cipher": "aes-128-gcm" }}],
                        "tls": {{
                            "cert_path": "{}",
                            "key_path": "{}"
                        }}{}
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#,
        escape_json_path(&tls.cert_path),
        escape_json_path(&tls.key_path),
        zero_vmess_inbound_transport_config(transport),
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
    wait_for_listener(zero_vmess_port).await;

    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        xray_vmess_outbound_tls_config(
            xray_socks_port,
            zero_vmess_port,
            &tls.cert_sha256_hex,
            security,
            false,
            transport,
        ),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_socks_port).await;

    let echo = spawn_tcp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_tcp_echo(xray_socks_port, echo_port, payload),
    )
    .await
    {
        Ok(echoed) => echoed,
        Err(error) => panic!(
            "xray -> zero interop timed out: {error}; xray={}",
            xray.logs()
        ),
    };
    assert_eq!(echoed, payload);

    xray.kill();
    zero.shutdown().await.expect("shutdown zero");
    echo.await.expect("echo task");
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_vmess_outbound_interops_with_zero_vmess_inbound_udp() {
    init_logs("vmess=debug");
    let material = TempMaterial::new("xray-zero-vmess-udp-out");
    let xray_socks_port = free_port();
    let zero_vmess_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"zero-vmess-udp";
    let tls = material.tls();

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "vmess-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_vmess_port} }},
                    "protocol": {{
                        "type": "vmess",
                        "users": [{{ "id": "{USER_ID}", "cipher": "aes-128-gcm" }}],
                        "tls": {{
                            "cert_path": "{}",
                            "key_path": "{}"
                        }}
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#,
        escape_json_path(&tls.cert_path),
        escape_json_path(&tls.key_path),
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
    wait_for_listener(zero_vmess_port).await;

    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        xray_vmess_outbound_tls_config(
            xray_socks_port,
            zero_vmess_port,
            &tls.cert_sha256_hex,
            "aes-128-gcm",
            true,
            XrayTransport::Tcp,
        ),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_socks_port).await;

    let echo = spawn_udp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_udp_echo(xray_socks_port, echo_port, payload),
    )
    .await
    {
        Ok(echoed) => echoed,
        Err(error) => panic!(
            "xray -> zero UDP interop timed out: {error}; xray={}",
            xray.logs()
        ),
    };
    assert_eq!(echoed, payload, "xray={}", xray.logs());

    xray.kill();
    zero.shutdown().await.expect("shutdown zero");
    echo.await.expect("echo task");
}

#[tokio::test]
#[ignore = "requires SING_BOX_BIN or downloaded sing-box under temp interop dir"]
async fn zero_vmess_outbound_interops_with_sing_box_vmess_inbound_tcp() {
    init_logs("vmess=debug");
    let material = TempMaterial::new("zero-sing-vmess-out");
    let sing_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"sing-box-vmess-tcp";

    let sing_config = material.path("sing-box-server.json");
    std::fs::write(&sing_config, sing_box_vmess_inbound_config(sing_port))
        .expect("write sing-box config");
    let mut sing_box = ExternalProcess::start(
        sing_box_bin("vmess"),
        &[
            "run",
            "-c",
            sing_config.to_str().expect("sing-box config path"),
        ],
        &material,
        "sing-box",
    );
    wait_for_listener(sing_port).await;

    let zero_config = RuntimeConfig::parse(&format!(
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
                    "tag": "vmess-out",
                    "protocol": {{
                        "type": "vmess",
                        "server": "127.0.0.1",
                        "port": {sing_port},
                        "id": "{USER_ID}",
                        "cipher": "aes-128-gcm"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "vmess-out" }} }}
        }}"#
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
    wait_for_listener(zero_socks_port).await;

    let echo = spawn_tcp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_tcp_echo_once(zero_socks_port, echo_port, payload),
    )
    .await
    {
        Ok(Ok(echoed)) => echoed,
        Ok(Err(error)) => panic!(
            "zero -> sing-box interop failed: {error:?}; sing-box={}",
            sing_box.logs()
        ),
        Err(error) => panic!(
            "zero -> sing-box interop timed out: {error}; sing-box={}",
            sing_box.logs()
        ),
    };
    assert_eq!(echoed, payload, "sing-box={}", sing_box.logs());

    zero.shutdown().await.expect("shutdown zero");
    sing_box.kill();
    echo.await.expect("echo task");
}

#[tokio::test]
#[ignore = "requires SING_BOX_BIN or downloaded sing-box under temp interop dir"]
async fn zero_vmess_outbound_interops_with_sing_box_vmess_inbound_udp() {
    init_logs("vmess=debug");
    let material = TempMaterial::new("zero-sing-vmess-udp-out");
    let sing_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"sing-box-vmess-udp";

    let sing_config = material.path("sing-box-server.json");
    std::fs::write(&sing_config, sing_box_vmess_inbound_config(sing_port))
        .expect("write sing-box config");
    let mut sing_box = ExternalProcess::start(
        sing_box_bin("vmess"),
        &[
            "run",
            "-c",
            sing_config.to_str().expect("sing-box config path"),
        ],
        &material,
        "sing-box",
    );
    wait_for_listener(sing_port).await;

    let zero_config = RuntimeConfig::parse(&format!(
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
                    "tag": "vmess-out",
                    "protocol": {{
                        "type": "vmess",
                        "server": "127.0.0.1",
                        "port": {sing_port},
                        "id": "{USER_ID}",
                        "cipher": "aes-128-gcm"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "vmess-out" }} }}
        }}"#
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
    wait_for_listener(zero_socks_port).await;

    let echo = spawn_udp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_udp_echo(zero_socks_port, echo_port, payload),
    )
    .await
    {
        Ok(echoed) => echoed,
        Err(error) => panic!(
            "zero -> sing-box UDP interop timed out: {error}; sing-box={}",
            sing_box.logs()
        ),
    };
    assert_eq!(echoed, payload, "sing-box={}", sing_box.logs());

    zero.shutdown().await.expect("shutdown zero");
    sing_box.kill();
    echo.await.expect("echo task");
}

#[tokio::test]
#[ignore = "requires MIHOMO_BIN or downloaded mihomo under temp interop dir"]
async fn mihomo_vmess_outbound_interops_with_zero_vmess_inbound_tcp() {
    init_logs("vmess=debug");
    let material = TempMaterial::new("mihomo-zero-vmess-out");
    let mihomo_mixed_port = free_port();
    let zero_vmess_port = free_port();
    let echo_port = free_port();
    let payload = b"mihomo-vmess-tcp";
    let tls = material.tls();

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "vmess-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_vmess_port} }},
                    "protocol": {{
                        "type": "vmess",
                        "users": [{{ "id": "{USER_ID}", "cipher": "aes-128-gcm" }}],
                        "tls": {{
                            "cert_path": "{}",
                            "key_path": "{}"
                        }}
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#,
        escape_json_path(&tls.cert_path),
        escape_json_path(&tls.key_path),
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
    wait_for_listener(zero_vmess_port).await;

    let mihomo_config = material.path("mihomo-client.yaml");
    std::fs::write(
        &mihomo_config,
        mihomo_vmess_outbound_config(mihomo_mixed_port, zero_vmess_port),
    )
    .expect("write mihomo config");
    let mut mihomo = ExternalProcess::start(
        mihomo_bin("vmess"),
        &[
            "-f",
            mihomo_config.to_str().expect("mihomo config path"),
            "-d",
            material.dir.to_str().expect("mihomo work dir"),
        ],
        &material,
        "mihomo",
    );
    wait_for_listener(mihomo_mixed_port).await;

    let echo = spawn_tcp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_tcp_echo(mihomo_mixed_port, echo_port, payload),
    )
    .await
    {
        Ok(echoed) => echoed,
        Err(error) => panic!(
            "mihomo -> zero interop timed out: {error}; mihomo={}",
            mihomo.logs()
        ),
    };
    assert_eq!(echoed, payload, "mihomo={}", mihomo.logs());

    mihomo.kill();
    zero.shutdown().await.expect("shutdown zero");
    echo.await.expect("echo task");
}

#[tokio::test]
#[ignore = "requires MIHOMO_BIN or downloaded mihomo under temp interop dir"]
async fn mihomo_vmess_outbound_interops_with_zero_vmess_inbound_udp() {
    init_logs("vmess=debug");
    let material = TempMaterial::new("mihomo-zero-vmess-udp-out");
    let mihomo_mixed_port = free_port();
    let zero_vmess_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"mihomo-vmess-udp";
    let tls = material.tls();

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "vmess-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_vmess_port} }},
                    "protocol": {{
                        "type": "vmess",
                        "users": [{{ "id": "{USER_ID}", "cipher": "aes-128-gcm" }}],
                        "tls": {{
                            "cert_path": "{}",
                            "key_path": "{}"
                        }}
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#,
        escape_json_path(&tls.cert_path),
        escape_json_path(&tls.key_path),
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
    wait_for_listener(zero_vmess_port).await;

    let mihomo_config = material.path("mihomo-client.yaml");
    std::fs::write(
        &mihomo_config,
        mihomo_vmess_outbound_config(mihomo_mixed_port, zero_vmess_port),
    )
    .expect("write mihomo config");
    let mut mihomo = ExternalProcess::start(
        mihomo_bin("vmess"),
        &[
            "-f",
            mihomo_config.to_str().expect("mihomo config path"),
            "-d",
            material.dir.to_str().expect("mihomo work dir"),
        ],
        &material,
        "mihomo",
    );
    wait_for_listener(mihomo_mixed_port).await;

    let echo = spawn_udp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_udp_echo(mihomo_mixed_port, echo_port, payload),
    )
    .await
    {
        Ok(echoed) => echoed,
        Err(error) => panic!(
            "mihomo -> zero UDP interop timed out: {error}; mihomo={}",
            mihomo.logs()
        ),
    };
    assert_eq!(echoed, payload, "mihomo={}", mihomo.logs());

    mihomo.kill();
    zero.shutdown().await.expect("shutdown zero");
    echo.await.expect("echo task");
}

fn xray_vmess_inbound_config(port: u16) -> String {
    xray_vmess_inbound_config_with_transport(port, XrayTransport::Tcp, None)
}

fn xray_vmess_inbound_config_with_transport(
    port: u16,
    transport: XrayTransport,
    tls: Option<&TestTlsMaterial>,
) -> String {
    let stream_settings = xray_inbound_stream_settings(transport, tls);
    format!(
        r#"{{
            "log": {{ "loglevel": "debug" }},
            "inbounds": [
                {{
                    "listen": "127.0.0.1",
                    "port": {port},
                    "protocol": "vmess",
                    "settings": {{
                        "clients": [{{ "id": "{USER_ID}", "alterId": 0, "security": "auto" }}]
                    }},
                    "streamSettings": {stream_settings}
                }}
            ],
            "outbounds": [{{ "protocol": "freedom", "settings": {{}} }}]
        }}"#
    )
}

fn xray_inbound_stream_settings(transport: XrayTransport, tls: Option<&TestTlsMaterial>) -> String {
    let (network, extra) = match transport {
        XrayTransport::Tcp => ("tcp", String::new()),
        XrayTransport::Ws => (
            "ws",
            format!(r#", "wsSettings": {{ "path": "{XRAY_WS_PATH}" }}"#),
        ),
        XrayTransport::Grpc => (
            "grpc",
            format!(r#", "grpcSettings": {{ "serviceName": "{XRAY_GRPC_SERVICE_NAME}" }}"#),
        ),
    };

    match tls {
        Some(tls) => format!(
            r#"{{
                "network": "{network}",
                "security": "tls",
                "tlsSettings": {{
                    "serverName": "localhost",
                    "certificates": [
                        {{
                            "certificateFile": "{}",
                            "keyFile": "{}"
                        }}
                    ]
                }}{extra}
            }}"#,
            escape_json_path(&tls.cert_path),
            escape_json_path(&tls.key_path),
        ),
        None => format!(r#"{{ "network": "{network}", "security": "none"{extra} }}"#),
    }
}

fn zero_vmess_outbound_transport_config(transport: XrayTransport, ca_cert_path: &Path) -> String {
    let transport_config = match transport {
        XrayTransport::Tcp => String::new(),
        XrayTransport::Ws => format!(r#", "ws": {{ "path": "{XRAY_WS_PATH}" }}"#),
        XrayTransport::Grpc => {
            format!(r#", "grpc": {{ "service_names": ["{ZERO_GRPC_SERVICE_PATH}"] }}"#)
        }
    };
    let alpn_config = match transport {
        XrayTransport::Grpc => {
            r#",
                            "alpn": ["h2"]"#
        }
        XrayTransport::Tcp | XrayTransport::Ws => "",
    };
    format!(
        r#",
                        "tls": {{
                            "server_name": "localhost",
                            "ca_cert_path": "{}"{alpn_config}
                        }}{transport_config}"#,
        escape_json_path(ca_cert_path),
    )
}

fn zero_vmess_inbound_transport_config(transport: XrayTransport) -> String {
    match transport {
        XrayTransport::Tcp => String::new(),
        XrayTransport::Ws => format!(r#", "ws": {{ "path": "{XRAY_WS_PATH}" }}"#),
        XrayTransport::Grpc => {
            format!(r#", "grpc": {{ "service_names": ["{ZERO_GRPC_SERVICE_PATH}"] }}"#)
        }
    }
}

fn sing_box_vmess_inbound_config(port: u16) -> String {
    format!(
        r#"{{
            "log": {{ "level": "debug" }},
            "inbounds": [
                {{
                    "type": "vmess",
                    "tag": "vmess-in",
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

fn mihomo_vmess_outbound_config(mixed_port: u16, vmess_port: u16) -> String {
    format!(
        r#"mixed-port: {mixed_port}
allow-lan: false
mode: global
log-level: debug
ipv6: false
proxies:
  - name: zero-vmess
    type: vmess
    server: 127.0.0.1
    port: {vmess_port}
    uuid: {USER_ID}
    alterId: 0
    cipher: auto
    udp: true
    tls: true
    servername: localhost
    skip-cert-verify: true
proxy-groups:
  - name: GLOBAL
    type: select
    proxies:
      - zero-vmess
rules:
  - MATCH,zero-vmess
"#
    )
}

fn xray_vmess_outbound_tls_config(
    socks_port: u16,
    vmess_port: u16,
    cert_sha256_hex: &str,
    security: &str,
    socks_udp: bool,
    transport: XrayTransport,
) -> String {
    let stream_settings = xray_outbound_stream_settings(transport, cert_sha256_hex);
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
                    "protocol": "vmess",
                    "settings": {{
                        "vnext": [
                            {{
                                "address": "127.0.0.1",
                                "port": {vmess_port},
                                "users": [{{ "id": "{USER_ID}", "alterId": 0, "security": "{security}" }}]
                            }}
                        ]
                    }},
                    "streamSettings": {stream_settings}
                }}
            ]
        }}"#,
    )
}

fn xray_outbound_stream_settings(transport: XrayTransport, cert_sha256_hex: &str) -> String {
    let (network, extra) = match transport {
        XrayTransport::Tcp => ("tcp", String::new()),
        XrayTransport::Ws => (
            "ws",
            format!(r#", "wsSettings": {{ "path": "{XRAY_WS_PATH}" }}"#),
        ),
        XrayTransport::Grpc => (
            "grpc",
            format!(r#", "grpcSettings": {{ "serviceName": "{XRAY_GRPC_SERVICE_NAME}" }}"#),
        ),
    };
    format!(
        r#"{{
            "network": "{network}",
            "security": "tls",
            "tlsSettings": {{
                "serverName": "localhost",
                "pinnedPeerCertSha256": "{cert_sha256_hex}"
            }}{extra}
        }}"#
    )
}
