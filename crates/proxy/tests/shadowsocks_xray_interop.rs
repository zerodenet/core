#![cfg(all(feature = "socks5", feature = "shadowsocks"))]

mod support;

use tokio::time::{timeout, Duration};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::interop::*;
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};

const PASSWORD: &str = "test-ss-password";
// 2022 Blake3 ciphers require base64-encoded master keys:
// MDEyMzQ1Njc4OWFiY2RlZg== decodes to 16 bytes ("0123456789abcdef") → aes-128
// MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY= decodes to 32 bytes → aes-256 / chacha20
const PASSWORD_2022_AES_128: &str = "MDEyMzQ1Njc4OWFiY2RlZg==";
const PASSWORD_2022_AES_256: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
const PASSWORD_2022_CHACHA20: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";

const ALL_CIPHERS: &[&str] = &[
    "aes-128-gcm",
    "aes-256-gcm",
    "chacha20-ietf-poly1305",
    "2022-blake3-aes-128-gcm",
    "2022-blake3-aes-256-gcm",
    "2022-blake3-chacha20-poly1305",
];

// ── Zero → Xray interop ──────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_ss_outbound_interops_with_xray_ss_inbound_tcp_aes_128_gcm() {
    init_logs("shadowsocks=debug");
    let material = TempMaterial::new("zero-xray-ss-tcp-out");
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"xray-ss-tcp-aes128";

    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_ss_inbound_config(xray_port, "aes-128-gcm", PASSWORD),
    )
    .expect("write xray config");
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
                    "tag": "ss-out",
                    "protocol": {{
                        "type": "shadowsocks",
                        "server": "127.0.0.1",
                        "port": {xray_port},
                        "password": "{PASSWORD}",
                        "cipher": "aes-128-gcm"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "ss-out" }} }}
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
            "zero -> xray SS interop failed: {error:?}; xray={}",
            xray.logs()
        ),
        Err(error) => panic!(
            "zero -> xray SS interop timed out: {error}; xray={}",
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
async fn zero_ss_outbound_interops_with_xray_ss_inbound_udp_aes_128_gcm() {
    init_logs("shadowsocks=debug");
    let material = TempMaterial::new("zero-xray-ss-udp-out");
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"xray-ss-udp-aes128";

    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_ss_inbound_config(xray_port, "aes-128-gcm", PASSWORD),
    )
    .expect("write xray config");
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
                    "tag": "ss-out",
                    "protocol": {{
                        "type": "shadowsocks",
                        "server": "127.0.0.1",
                        "port": {xray_port},
                        "password": "{PASSWORD}",
                        "cipher": "aes-128-gcm"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "ss-out" }} }}
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
            "zero -> xray SS UDP interop timed out: {error}; xray={}",
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
async fn zero_ss_outbound_interops_with_xray_ss_inbound_tcp_2022_blake3_aes_128_gcm() {
    let cipher = "2022-blake3-aes-128-gcm";
    let password = password_for_cipher(cipher);
    init_logs("shadowsocks=debug");
    let material = TempMaterial::new("zero-xray-ss2022-tcp-out");
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"xray-ss2022-tcp";

    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_ss_inbound_config(xray_port, cipher, password),
    )
    .expect("write xray config");
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
                    "tag": "ss-out",
                    "protocol": {{
                        "type": "shadowsocks",
                        "server": "127.0.0.1",
                        "port": {xray_port},
                        "password": "{password}",
                        "cipher": "{cipher}"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "ss-out" }} }}
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
            "zero -> xray SS 2022 interop failed: {error:?}; xray={}",
            xray.logs()
        ),
        Err(error) => panic!(
            "zero -> xray SS 2022 interop timed out: {error}; xray={}",
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
async fn zero_ss_outbound_interops_with_xray_ss_inbound_udp_2022_blake3_aes_128_gcm() {
    let cipher = "2022-blake3-aes-128-gcm";
    let password = password_for_cipher(cipher);
    init_logs("shadowsocks=debug");
    let material = TempMaterial::new("zero-xray-ss2022-udp-out");
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"xray-ss2022-udp";

    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_ss_inbound_config(xray_port, cipher, password),
    )
    .expect("write xray config");
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
                    "tag": "ss-out",
                    "protocol": {{
                        "type": "shadowsocks",
                        "server": "127.0.0.1",
                        "port": {xray_port},
                        "password": "{password}",
                        "cipher": "{cipher}"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "ss-out" }} }}
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
            "zero -> xray SS 2022 UDP interop timed out: {error}; xray={}",
            xray.logs()
        ),
    };
    assert_eq!(echoed, payload, "xray={}", xray.logs());

    zero.shutdown().await.expect("shutdown zero");
    xray.kill();
    echo.await.expect("echo task");
}

// ── Xray → Zero interop ─────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_ss_outbound_interops_with_zero_ss_inbound_tcp_aes_128_gcm() {
    init_logs("shadowsocks=debug");
    let material = TempMaterial::new("xray-zero-ss-tcp-in");
    let zero_port = free_port();
    let xray_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"zero-ss-tcp-aes128";

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "ss-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_port} }},
                    "protocol": {{
                        "type": "shadowsocks",
                        "password": "{PASSWORD}",
                        "cipher": "aes-128-gcm"
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
    wait_for_listener(zero_port).await;

    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        xray_ss_outbound_config(xray_socks_port, zero_port, "aes-128-gcm", PASSWORD, false),
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
        socks5_tcp_echo_once(xray_socks_port, echo_port, payload),
    )
    .await
    {
        Ok(Ok(echoed)) => echoed,
        Ok(Err(error)) => panic!(
            "xray -> zero SS interop failed: {error:?}; xray={}",
            xray.logs()
        ),
        Err(error) => panic!(
            "xray -> zero SS interop timed out: {error}; xray={}",
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
async fn xray_ss_outbound_interops_with_zero_ss_inbound_udp_aes_128_gcm() {
    init_logs("shadowsocks=debug");
    let material = TempMaterial::new("xray-zero-ss-udp-in");
    let zero_port = free_port();
    let xray_socks_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"zero-ss-udp-aes128";

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "ss-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_port} }},
                    "protocol": {{
                        "type": "shadowsocks",
                        "password": "{PASSWORD}",
                        "cipher": "aes-128-gcm"
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
    wait_for_listener(zero_port).await;

    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        xray_ss_outbound_config(xray_socks_port, zero_port, "aes-128-gcm", PASSWORD, true),
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
            "xray -> zero SS UDP interop timed out: {error}; xray={}",
            xray.logs()
        ),
    };
    assert_eq!(echoed, payload, "xray={}", xray.logs());

    zero.shutdown().await.expect("shutdown zero");
    xray.kill();
    echo.await.expect("echo task");
}

// ── Zero → sing-box interop ─────────────────────────────────────────

#[tokio::test]
#[ignore = "requires SING_BOX_BIN or downloaded sing-box under temp interop dir"]
async fn zero_ss_outbound_interops_with_sing_box_ss_inbound_tcp() {
    init_logs("shadowsocks=debug");
    let material = TempMaterial::new("zero-sing-ss-tcp-out");
    let sing_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"sing-box-ss-tcp";

    let sing_config = material.path("sing-box-server.json");
    std::fs::write(
        &sing_config,
        sing_box_ss_inbound_config(sing_port, "aes-128-gcm", PASSWORD),
    )
    .expect("write sing-box config");
    let mut sing_box = ExternalProcess::start(
        sing_box_bin("shadowsocks"),
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
                    "tag": "ss-out",
                    "protocol": {{
                        "type": "shadowsocks",
                        "server": "127.0.0.1",
                        "port": {sing_port},
                        "password": "{PASSWORD}",
                        "cipher": "aes-128-gcm"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "ss-out" }} }}
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
            "zero -> sing-box SS interop failed: {error:?}; sing-box={}",
            sing_box.logs()
        ),
        Err(error) => panic!(
            "zero -> sing-box SS interop timed out: {error}; sing-box={}",
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
async fn zero_ss_outbound_interops_with_sing_box_ss_inbound_udp() {
    init_logs("shadowsocks=debug");
    let material = TempMaterial::new("zero-sing-ss-udp-out");
    let sing_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"sing-box-ss-udp";

    let sing_config = material.path("sing-box-server.json");
    std::fs::write(
        &sing_config,
        sing_box_ss_inbound_config(sing_port, "aes-128-gcm", PASSWORD),
    )
    .expect("write sing-box config");
    let mut sing_box = ExternalProcess::start(
        sing_box_bin("shadowsocks"),
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
                    "tag": "ss-out",
                    "protocol": {{
                        "type": "shadowsocks",
                        "server": "127.0.0.1",
                        "port": {sing_port},
                        "password": "{PASSWORD}",
                        "cipher": "aes-128-gcm"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "ss-out" }} }}
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
            "zero -> sing-box SS UDP interop timed out: {error}; sing-box={}",
            sing_box.logs()
        ),
    };
    assert_eq!(echoed, payload, "sing-box={}", sing_box.logs());

    zero.shutdown().await.expect("shutdown zero");
    sing_box.kill();
    echo.await.expect("echo task");
}

// ── Zero → shadowsocks-rust interop ─────────────────────────────────

#[tokio::test]
#[ignore = "requires SHADOWSOCKS_RUST_BIN or ssserver in PATH"]
async fn zero_ss_outbound_interops_with_ssrust_inbound_tcp_aes_256_gcm() {
    init_logs("shadowsocks=debug");
    let material = TempMaterial::new("zero-ssrust-ss-tcp-out");
    let ssrust_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"ssrust-ss-tcp-aes256";

    let mut ssrust = ExternalProcess::start(
        ssrust_bin(),
        &ssrust_server_args("aes-256-gcm", PASSWORD, ssrust_port, false)
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        &material,
        "ssserver",
    );
    wait_for_listener(ssrust_port).await;

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
                    "tag": "ss-out",
                    "protocol": {{
                        "type": "shadowsocks",
                        "server": "127.0.0.1",
                        "port": {ssrust_port},
                        "password": "{PASSWORD}",
                        "cipher": "aes-256-gcm"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "ss-out" }} }}
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
            "zero -> ssrust SS interop failed: {error:?}; ssrust={}",
            ssrust.logs()
        ),
        Err(error) => panic!(
            "zero -> ssrust SS interop timed out: {error}; ssrust={}",
            ssrust.logs()
        ),
    };
    assert_eq!(echoed, payload, "ssrust={}", ssrust.logs());

    zero.shutdown().await.expect("shutdown zero");
    ssrust.kill();
    echo.await.expect("echo task");
}

#[tokio::test]
#[ignore = "requires SHADOWSOCKS_RUST_BIN or ssserver in PATH"]
async fn zero_ss_outbound_interops_with_ssrust_inbound_udp_all_ciphers() {
    init_logs("shadowsocks=debug");
    for cipher in ALL_CIPHERS {
        let material = TempMaterial::new(&format!("zero-ssrust-ss-udp-{cipher}"));
        let password = password_for_cipher(cipher);
        let ssrust_port = free_port();
        let zero_socks_port = free_port();
        let echo_port = free_udp_port();
        let payload = format!("ssrust-udp:{cipher}");

        let args: Vec<String> = ssrust_server_args(cipher, password, ssrust_port, true);
        let arg_strs: Vec<&str> = args.iter().map(String::as_str).collect();
        let mut ssrust = ExternalProcess::start(ssrust_bin(), &arg_strs, &material, "ssserver");
        wait_for_listener(ssrust_port).await;

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
                        "tag": "ss-out",
                        "protocol": {{
                            "type": "shadowsocks",
                            "server": "127.0.0.1",
                            "port": {ssrust_port},
                            "password": "{password}",
                            "cipher": "{cipher}"
                        }}
                    }}
                ],
                "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "ss-out" }} }}
            }}"#
        ))
        .expect("parse zero config");
        let zero = spawn_engine(Engine::new(zero_config).expect("build zero engine"));
        wait_for_listener(zero_socks_port).await;

        let echo = spawn_udp_echo(echo_port, payload.len()).await;
        let echoed = match timeout(
            Duration::from_secs(10),
            socks5_udp_echo(zero_socks_port, echo_port, payload.as_bytes()),
        )
        .await
        {
            Ok(echoed) => echoed,
            Err(error) => panic!(
                "zero -> ssrust UDP interop timed out for cipher {cipher}: {error}; ssrust={}",
                ssrust.logs()
            ),
        };
        assert_eq!(
            echoed,
            payload.as_bytes(),
            "cipher={cipher}; ssrust={}",
            ssrust.logs()
        );

        timeout(Duration::from_secs(10), zero.shutdown())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "zero shutdown timed out for cipher {cipher}: {error}; ssrust={}",
                    ssrust.logs()
                )
            })
            .expect("shutdown zero");
        ssrust.kill();
        timeout(Duration::from_secs(5), echo)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "UDP echo task did not finish for cipher {cipher}: {error}; ssrust={}",
                    ssrust.logs()
                )
            })
            .expect("echo task");
    }
}

// ── External config builders ────────────────────────────────────────

/// Build an Xray Shadowsocks inbound config (server-side).
///
/// The `network` field enables both TCP and UDP so the same config
/// works for both test modes.
fn xray_ss_inbound_config(port: u16, cipher: &str, password: &str) -> String {
    format!(
        r#"{{
            "log": {{ "loglevel": "debug" }},
            "inbounds": [
                {{
                    "listen": "127.0.0.1",
                    "port": {port},
                    "protocol": "shadowsocks",
                    "settings": {{
                        "method": "{cipher}",
                        "password": "{password}",
                        "network": "tcp,udp"
                    }}
                }}
            ],
            "outbounds": [{{ "protocol": "freedom", "settings": {{}} }}]
        }}"#
    )
}

/// Build an Xray Shadowsocks outbound config (client-side) with a SOCKS5 inbound.
///
/// When `socks_udp` is true the SOCKS inbound advertises UDP ASSOCIATE support.
fn xray_ss_outbound_config(
    socks_port: u16,
    ss_port: u16,
    cipher: &str,
    password: &str,
    socks_udp: bool,
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
                    "protocol": "shadowsocks",
                    "settings": {{
                        "servers": [
                            {{
                                "address": "127.0.0.1",
                                "port": {ss_port},
                                "method": "{cipher}",
                                "password": "{password}"
                            }}
                        ]
                    }}
                }}
            ]
        }}"#
    )
}

/// Build a sing-box Shadowsocks inbound config (server-side).
fn sing_box_ss_inbound_config(port: u16, cipher: &str, password: &str) -> String {
    format!(
        r#"{{
            "log": {{ "level": "debug" }},
            "inbounds": [
                {{
                    "type": "shadowsocks",
                    "tag": "ss-in",
                    "listen": "127.0.0.1",
                    "listen_port": {port},
                    "method": "{cipher}",
                    "password": "{password}"
                }}
            ],
            "outbounds": [{{ "type": "direct", "tag": "direct" }}],
            "route": {{ "final": "direct" }}
        }}"#
    )
}

/// Build `ssserver` (shadowsocks-rust) command-line arguments.
///
/// Returns `Vec<String>` so callers can borrow `&[&str]` for `ExternalProcess::start`.
fn ssrust_server_args(cipher: &str, password: &str, port: u16, udp: bool) -> Vec<String> {
    let mut args = vec![
        "-s".to_owned(),
        format!("127.0.0.1:{port}"),
        "-m".to_owned(),
        cipher.to_owned(),
        "-k".to_owned(),
        password.to_owned(),
    ];
    if udp {
        // shadowsocks-rust uses `-u` for UDP_ONLY and `-U` for
        // TCP_AND_UDP. This test uses the TCP listener as its readiness
        // barrier before exercising UDP, so it requires the combined mode.
        args.push("-U".to_owned());
    }
    args
}

// ── Helpers ─────────────────────────────────────────────────────────

fn password_for_cipher(cipher: &str) -> &'static str {
    match cipher {
        "2022-blake3-aes-128-gcm" => PASSWORD_2022_AES_128,
        "2022-blake3-aes-256-gcm" => PASSWORD_2022_AES_256,
        "2022-blake3-chacha20-poly1305" => PASSWORD_2022_CHACHA20,
        _ => PASSWORD,
    }
}

fn ssrust_bin() -> String {
    std::env::var("SHADOWSOCKS_RUST_BIN").unwrap_or_else(|_| "ssserver".to_owned())
}
