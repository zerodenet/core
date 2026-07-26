#![cfg(all(feature = "socks5", feature = "trojan"))]

mod support;

use tokio::time::{timeout, Duration};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::interop::*;
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};

const PASSWORD: &str = "test-trojan-password";

async fn wait_for_echo(echo: tokio::task::JoinHandle<()>) {
    timeout(Duration::from_secs(5), echo)
        .await
        .expect("echo task timed out")
        .expect("echo task");
}

async fn shutdown_zero(zero: zero_proxy::RunningProxy) {
    timeout(Duration::from_secs(5), zero.shutdown())
        .await
        .expect("zero shutdown timed out")
        .expect("shutdown zero");
}

// ── Zero → Xray interop ─────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_trojan_outbound_interops_with_xray_trojan_inbound_tcp() {
    init_logs("trojan=debug");
    let material = TempMaterial::new("zero-xray-trojan-tcp-out");
    let tls = material.tls();
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"xray-trojan-tcp-echo";

    let xray_config = material.path("xray-server.json");
    std::fs::write(&xray_config, xray_trojan_inbound_config(xray_port, &tls))
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
                    "tag": "trojan-out",
                    "protocol": {{
                        "type": "trojan",
                        "server": "127.0.0.1",
                        "port": {xray_port},
                        "password": "{PASSWORD}",
                        "sni": "localhost",
                        "insecure": true
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "trojan-out" }} }}
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
            "zero -> xray trojan interop failed: {error:?}; xray={}",
            xray.logs()
        ),
        Err(error) => panic!(
            "zero -> xray trojan interop timed out: {error}; xray={}",
            xray.logs()
        ),
    };
    assert_eq!(echoed, payload, "xray={}", xray.logs());

    xray.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

/// Same as the TCP interop above, but sets `client_fingerprint: "chrome"` on
/// Zero's trojan outbound. This verifies that the fingerprint-selected cipher
/// and ALPN profile remains wire-compatible with a stock Xray TLS server.
#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_trojan_outbound_with_fingerprint_interops_with_xray_trojan_inbound_tcp() {
    init_logs("trojan=debug");
    let material = TempMaterial::new("zero-xray-trojan-tcp-out-fp");
    let tls = material.tls();
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"xray-trojan-tcp-fp";

    let xray_config = material.path("xray-server.json");
    std::fs::write(&xray_config, xray_trojan_inbound_config(xray_port, &tls))
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
                    "tag": "trojan-out",
                    "protocol": {{
                        "type": "trojan",
                        "server": "127.0.0.1",
                        "port": {xray_port},
                        "password": "{PASSWORD}",
                        "sni": "localhost",
                        "insecure": true,
                        "client_fingerprint": "chrome"
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "trojan-out" }} }}
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
            "zero -> xray trojan fingerprint interop failed: {error:?}; xray={}",
            xray.logs()
        ),
        Err(error) => panic!(
            "zero -> xray trojan fingerprint interop timed out: {error}; xray={}",
            xray.logs()
        ),
    };
    assert_eq!(echoed, payload, "xray={}", xray.logs());

    xray.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_trojan_outbound_interops_with_xray_trojan_inbound_udp() {
    init_logs("trojan=debug");
    let material = TempMaterial::new("zero-xray-trojan-udp-out");
    let tls = material.tls();
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"xray-trojan-udp";

    let xray_config = material.path("xray-server.json");
    std::fs::write(&xray_config, xray_trojan_inbound_config(xray_port, &tls))
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
                    "tag": "trojan-out",
                    "protocol": {{
                        "type": "trojan",
                        "server": "127.0.0.1",
                        "port": {xray_port},
                        "password": "{PASSWORD}",
                        "sni": "localhost",
                        "insecure": true
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "trojan-out" }} }}
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
            "zero -> xray trojan UDP interop timed out: {error}; xray={}",
            xray.logs()
        ),
    };
    assert_eq!(echoed, payload, "xray={}", xray.logs());

    xray.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

// ── Xray → Zero interop ──────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_trojan_outbound_interops_with_zero_trojan_inbound_tcp() {
    init_logs("trojan=debug");
    let material = TempMaterial::new("xray-zero-trojan-tcp-in");
    let tls = material.tls();
    let zero_port = free_port();
    let xray_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"zero-trojan-tcp-echo";

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "trojan-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_port} }},
                    "protocol": {{
                        "type": "trojan",
                        "password": "{PASSWORD}",
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
    wait_for_listener(zero_port).await;

    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        xray_trojan_outbound_config(xray_socks_port, zero_port, &tls),
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
            "xray -> zero trojan interop failed: {error:?}; xray={}",
            xray.logs()
        ),
        Err(error) => panic!(
            "xray -> zero trojan interop timed out: {error}; xray={}",
            xray.logs()
        ),
    };
    assert_eq!(echoed, payload, "xray={}", xray.logs());

    xray.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn xray_trojan_outbound_interops_with_zero_trojan_inbound_udp() {
    init_logs("trojan=debug");
    let material = TempMaterial::new("xray-zero-trojan-udp-in");
    let tls = material.tls();
    let zero_port = free_port();
    let xray_socks_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"zero-trojan-udp";

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "trojan-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_port} }},
                    "protocol": {{
                        "type": "trojan",
                        "password": "{PASSWORD}",
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
    wait_for_listener(zero_port).await;

    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        xray_trojan_outbound_config(xray_socks_port, zero_port, &tls),
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
            "xray -> zero trojan UDP interop timed out: {error}; xray={}",
            xray.logs()
        ),
    };
    assert_eq!(echoed, payload, "xray={}", xray.logs());

    xray.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

// ── Zero → sing-box interop ──────────────────────────────────────────

#[tokio::test]
#[ignore = "requires SING_BOX_BIN or downloaded sing-box under temp interop dir"]
async fn zero_trojan_outbound_interops_with_sing_box_trojan_inbound_tcp() {
    init_logs("trojan=debug");
    let material = TempMaterial::new("zero-sing-trojan-tcp-out");
    let tls = material.tls();
    let sing_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"sing-box-trojan-tcp";

    let sing_config = material.path("sing-box-server.json");
    std::fs::write(
        &sing_config,
        sing_box_trojan_inbound_config(sing_port, &tls),
    )
    .expect("write sing-box config");
    let mut sing_box = ExternalProcess::start(
        sing_box_bin("trojan"),
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
                    "tag": "trojan-out",
                    "protocol": {{
                        "type": "trojan",
                        "server": "127.0.0.1",
                        "port": {sing_port},
                        "password": "{PASSWORD}",
                        "sni": "localhost",
                        "insecure": true
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "trojan-out" }} }}
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
            "zero -> sing-box trojan interop failed: {error:?}; sing-box={}",
            sing_box.logs()
        ),
        Err(error) => panic!(
            "zero -> sing-box trojan interop timed out: {error}; sing-box={}",
            sing_box.logs()
        ),
    };
    assert_eq!(echoed, payload, "sing-box={}", sing_box.logs());

    sing_box.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

#[tokio::test]
#[ignore = "requires SING_BOX_BIN or downloaded sing-box under temp interop dir"]
async fn zero_trojan_outbound_interops_with_sing_box_trojan_inbound_udp() {
    init_logs("trojan=debug");
    let material = TempMaterial::new("zero-sing-trojan-udp-out");
    let tls = material.tls();
    let sing_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"sing-box-trojan-udp";

    let sing_config = material.path("sing-box-server.json");
    std::fs::write(
        &sing_config,
        sing_box_trojan_inbound_config(sing_port, &tls),
    )
    .expect("write sing-box config");
    let mut sing_box = ExternalProcess::start(
        sing_box_bin("trojan"),
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
                    "tag": "trojan-out",
                    "protocol": {{
                        "type": "trojan",
                        "server": "127.0.0.1",
                        "port": {sing_port},
                        "password": "{PASSWORD}",
                        "sni": "localhost",
                        "insecure": true
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "trojan-out" }} }}
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
            "zero -> sing-box trojan UDP interop timed out: {error}; sing-box={}",
            sing_box.logs()
        ),
    };
    assert_eq!(echoed, payload, "sing-box={}", sing_box.logs());

    sing_box.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

// ── Mihomo → Zero interop ────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires MIHOMO_BIN pointing to a Mihomo executable"]
async fn mihomo_trojan_outbound_interops_with_zero_trojan_inbound_tcp() {
    init_logs("trojan=debug");
    let material = TempMaterial::new("mihomo-zero-trojan-tcp");
    let tls = material.tls();
    let zero_port = free_port();
    let mihomo_port = free_port();
    let echo_port = free_port();
    let payload = b"mihomo-trojan-tcp";

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "trojan-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_port} }},
                    "protocol": {{
                        "type": "trojan",
                        "password": "{PASSWORD}",
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
    wait_for_listener(zero_port).await;

    let mihomo_config = material.path("mihomo.yaml");
    std::fs::write(
        &mihomo_config,
        mihomo_trojan_outbound_config(mihomo_port, zero_port, &tls),
    )
    .expect("write mihomo config");
    let mut mihomo = ExternalProcess::start(
        mihomo_bin("trojan"),
        &["-f", mihomo_config.to_str().expect("mihomo config path")],
        &material,
        "mihomo",
    );
    wait_for_listener(mihomo_port).await;

    let echo = spawn_tcp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_tcp_echo_once(mihomo_port, echo_port, payload),
    )
    .await
    {
        Ok(Ok(echoed)) => echoed,
        Ok(Err(error)) => panic!(
            "mihomo -> zero trojan interop failed: {error:?}; mihomo={}",
            mihomo.logs()
        ),
        Err(error) => panic!(
            "mihomo -> zero trojan interop timed out: {error}; mihomo={}",
            mihomo.logs()
        ),
    };
    assert_eq!(echoed, payload, "mihomo={}", mihomo.logs());

    mihomo.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

#[tokio::test]
#[ignore = "requires MIHOMO_BIN pointing to a Mihomo executable"]
async fn mihomo_trojan_outbound_interops_with_zero_trojan_inbound_udp() {
    init_logs("trojan=debug");
    let material = TempMaterial::new("mihomo-zero-trojan-udp");
    let tls = material.tls();
    let zero_port = free_port();
    let mihomo_port = free_port();
    let echo_port = free_udp_port();
    let payload = b"mihomo-trojan-udp";

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "trojan-in",
                    "listen": {{ "address": "127.0.0.1", "port": {zero_port} }},
                    "protocol": {{
                        "type": "trojan",
                        "password": "{PASSWORD}",
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
    wait_for_listener(zero_port).await;

    let mihomo_config = material.path("mihomo.yaml");
    std::fs::write(
        &mihomo_config,
        mihomo_trojan_outbound_config(mihomo_port, zero_port, &tls),
    )
    .expect("write mihomo config");
    let mut mihomo = ExternalProcess::start(
        mihomo_bin("trojan"),
        &["-f", mihomo_config.to_str().expect("mihomo config path")],
        &material,
        "mihomo",
    );
    wait_for_listener(mihomo_port).await;

    let echo = spawn_udp_echo(echo_port, payload.len()).await;
    let echoed = match timeout(
        Duration::from_secs(10),
        socks5_udp_echo(mihomo_port, echo_port, payload),
    )
    .await
    {
        Ok(echoed) => echoed,
        Err(error) => panic!(
            "mihomo -> zero trojan UDP interop timed out: {error}; mihomo={}",
            mihomo.logs()
        ),
    };
    assert_eq!(echoed, payload, "mihomo={}", mihomo.logs());

    mihomo.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

// ── External config builders ─────────────────────────────────────────

fn xray_trojan_inbound_config(port: u16, tls: &TestTlsMaterial) -> String {
    format!(
        r#"{{
            "log": {{ "loglevel": "debug" }},
            "inbounds": [
                {{
                    "listen": "127.0.0.1",
                    "port": {port},
                    "protocol": "trojan",
                    "settings": {{
                        "clients": [
                            {{ "password": "{PASSWORD}" }}
                        ]
                    }},
                    "streamSettings": {{
                        "network": "tcp",
                        "security": "tls",
                        "tlsSettings": {{
                            "serverName": "localhost",
                            "certificates": [
                                {{
                                    "certificateFile": "{}",
                                    "keyFile": "{}"
                                }}
                            ]
                        }}
                    }}
                }}
            ],
            "outbounds": [{{ "protocol": "freedom", "settings": {{}} }}]
        }}"#,
        escape_json_path(&tls.cert_path),
        escape_json_path(&tls.key_path),
    )
}

fn xray_trojan_outbound_config(socks_port: u16, trojan_port: u16, tls: &TestTlsMaterial) -> String {
    format!(
        r#"{{
            "log": {{ "loglevel": "debug" }},
            "inbounds": [
                {{
                    "listen": "127.0.0.1",
                    "port": {socks_port},
                    "protocol": "socks",
                    "settings": {{ "auth": "noauth", "udp": true }}
                }}
            ],
            "outbounds": [
                {{
                    "protocol": "trojan",
                    "settings": {{
                        "servers": [
                            {{
                                "address": "127.0.0.1",
                                "port": {trojan_port},
                                "password": "{PASSWORD}"
                            }}
                        ]
                    }},
                    "streamSettings": {{
                        "network": "tcp",
                        "security": "tls",
                        "tlsSettings": {{
                            "serverName": "localhost",
                            "pinnedPeerCertSha256": "{}",
                            "fingerprint": "chrome"
                        }}
                    }}
                }}
            ]
        }}"#,
        tls.cert_sha256_hex
    )
}

fn sing_box_trojan_inbound_config(port: u16, tls: &TestTlsMaterial) -> String {
    format!(
        r#"{{
            "log": {{ "level": "debug" }},
            "inbounds": [
                {{
                    "type": "trojan",
                    "tag": "trojan-in",
                    "listen": "127.0.0.1",
                    "listen_port": {port},
                    "users": [
                        {{ "password": "{PASSWORD}" }}
                    ],
                    "tls": {{
                        "enabled": true,
                        "certificate_path": "{}",
                        "key_path": "{}"
                    }}
                }}
            ],
            "outbounds": [
                {{ "type": "direct" }}
            ]
        }}"#,
        escape_json_path(&tls.cert_path),
        escape_json_path(&tls.key_path),
    )
}

fn mihomo_trojan_outbound_config(
    mixed_port: u16,
    trojan_port: u16,
    _tls: &TestTlsMaterial,
) -> String {
    format!(
        r#"mixed-port: {mixed_port}
mode: rule
log-level: debug
proxies:
  - name: trojan-zero
    type: trojan
    server: 127.0.0.1
    port: {trojan_port}
    password: {PASSWORD}
    sni: localhost
    skip-cert-verify: true
rules:
  - MATCH,trojan-zero
"#
    )
}
