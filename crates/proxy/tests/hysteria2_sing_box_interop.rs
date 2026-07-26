#![cfg(all(feature = "socks5", feature = "hysteria2"))]

mod support;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout, Duration};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy as Engine, ProxyHandle};

use support::interop::*;
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};

const PASSWORD: &str = "zero-hysteria2-interop-password";

#[tokio::test]
#[ignore = "requires SING_BOX_BIN pointing to a sing-box executable"]
async fn zero_hysteria2_outbound_interops_with_sing_box_tcp() {
    init_logs("hysteria2=debug");
    let material = TempMaterial::new("zero-sing-hysteria2-tcp-out");
    let tls = write_tls(&material);
    let sing_port = free_udp_port();
    let sing_config = material.path("sing-box-server.json");
    std::fs::write(
        &sing_config,
        sing_box_hysteria2_inbound_config(sing_port, &tls),
    )
    .expect("write sing-box config");
    let mut sing_box = start_sing_box(&material, &sing_config);
    sleep(Duration::from_millis(300)).await;

    let zero_socks_port = free_port();
    let zero = spawn_hysteria2_outbound(zero_socks_port, sing_port).await;
    let echo_port = free_port();
    let payload = b"zero-hysteria2-sing-box-tcp";
    let echo = spawn_tcp_echo(echo_port, payload.len()).await;

    let echoed = timeout(
        Duration::from_secs(10),
        socks5_tcp_echo(zero_socks_port, echo_port, payload),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "zero -> sing-box Hysteria2 TCP timed out: {error}; logs={}",
            sing_box.logs()
        )
    });
    assert_eq!(echoed, payload, "sing-box logs={}", sing_box.logs());

    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
    sing_box.kill();
}

#[tokio::test]
#[ignore = "requires SING_BOX_BIN pointing to a sing-box executable"]
async fn zero_hysteria2_outbound_interops_with_sing_box_udp() {
    init_logs("hysteria2=debug");
    let material = TempMaterial::new("zero-sing-hysteria2-udp-out");
    let tls = write_tls(&material);
    let sing_port = free_udp_port();
    let sing_config = material.path("sing-box-server.json");
    std::fs::write(
        &sing_config,
        sing_box_hysteria2_inbound_config(sing_port, &tls),
    )
    .expect("write sing-box config");
    let mut sing_box = start_sing_box(&material, &sing_config);
    sleep(Duration::from_millis(300)).await;

    let zero_socks_port = free_port();
    let zero = spawn_hysteria2_outbound(zero_socks_port, sing_port).await;
    let echo_port = free_udp_port();
    let payload = (0..1_600)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let echo = spawn_udp_echo(echo_port, payload.len()).await;

    let echoed = timeout(
        Duration::from_secs(10),
        socks5_udp_echo(zero_socks_port, echo_port, &payload),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "zero -> sing-box Hysteria2 UDP timed out: {error}; logs={}",
            sing_box.logs()
        )
    });
    assert_eq!(echoed, payload, "sing-box logs={}", sing_box.logs());

    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
    sing_box.kill();
}

#[tokio::test]
#[ignore = "requires SING_BOX_BIN pointing to a sing-box executable"]
async fn sing_box_hysteria2_outbound_interops_with_zero_tcp() {
    init_logs("hysteria2=debug");
    let material = TempMaterial::new("sing-zero-hysteria2-tcp-in");
    let tls = write_tls(&material);
    let zero_port = free_udp_port();
    let zero = spawn_hysteria2_inbound(zero_port, &tls).await;
    let sing_socks_port = free_port();
    let sing_config = material.path("sing-box-client.json");
    std::fs::write(
        &sing_config,
        sing_box_hysteria2_outbound_config(sing_socks_port, zero_port, PASSWORD),
    )
    .expect("write sing-box config");
    let mut sing_box = start_sing_box(&material, &sing_config);
    wait_for_listener(sing_socks_port).await;

    let echo_port = free_port();
    let payload = b"sing-box-hysteria2-zero-tcp";
    let echo = spawn_tcp_echo(echo_port, payload.len()).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_tcp_echo(sing_socks_port, echo_port, payload),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "sing-box -> zero Hysteria2 TCP timed out: {error}; logs={}",
            sing_box.logs()
        )
    });
    assert_eq!(echoed, payload, "sing-box logs={}", sing_box.logs());

    sing_box.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

#[tokio::test]
#[ignore = "requires SING_BOX_BIN pointing to a sing-box executable"]
async fn sing_box_hysteria2_outbound_interops_with_zero_udp() {
    init_logs("hysteria2=debug");
    let material = TempMaterial::new("sing-zero-hysteria2-udp-in");
    let tls = write_tls(&material);
    let zero_port = free_udp_port();
    let zero = spawn_hysteria2_inbound(zero_port, &tls).await;
    let sing_socks_port = free_port();
    let sing_config = material.path("sing-box-client.json");
    std::fs::write(
        &sing_config,
        sing_box_hysteria2_outbound_config(sing_socks_port, zero_port, PASSWORD),
    )
    .expect("write sing-box config");
    let mut sing_box = start_sing_box(&material, &sing_config);
    wait_for_listener(sing_socks_port).await;

    let echo_port = free_udp_port();
    let payload = (0..1_600)
        .map(|index| (250 - (index % 251)) as u8)
        .collect::<Vec<_>>();
    let echo = spawn_udp_echo(echo_port, payload.len()).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_udp_echo(sing_socks_port, echo_port, &payload),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "sing-box -> zero Hysteria2 UDP timed out: {error}; logs={}",
            sing_box.logs()
        )
    });
    assert_eq!(echoed, payload, "sing-box logs={}", sing_box.logs());

    sing_box.kill();
    shutdown_zero(zero).await;
    wait_for_echo(echo).await;
}

#[tokio::test]
#[ignore = "requires SING_BOX_BIN pointing to a sing-box executable"]
async fn zero_hysteria2_inbound_rejects_wrong_sing_box_password() {
    init_logs("hysteria2=debug");
    let material = TempMaterial::new("sing-zero-hysteria2-wrong-password");
    let tls = write_tls(&material);
    let zero_port = free_udp_port();
    let zero = spawn_hysteria2_inbound(zero_port, &tls).await;
    let sing_socks_port = free_port();
    let sing_config = material.path("sing-box-client.json");
    std::fs::write(
        &sing_config,
        sing_box_hysteria2_outbound_config(
            sing_socks_port,
            zero_port,
            "definitely-not-the-node-password",
        ),
    )
    .expect("write sing-box config");
    let mut sing_box = start_sing_box(&material, &sing_config);
    wait_for_listener(sing_socks_port).await;

    let target = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind protected target");
    let target_port = target.local_addr().expect("target address").port();
    let status = timeout(
        Duration::from_secs(5),
        socks5_tcp_connect_status(sing_socks_port, target_port),
    )
    .await;
    assert!(
        !matches!(status, Ok(Ok(0x00))),
        "wrong Hysteria2 password unexpectedly opened a SOCKS tunnel; logs={}",
        sing_box.logs()
    );
    assert!(
        timeout(Duration::from_millis(750), target.accept())
            .await
            .is_err(),
        "wrong Hysteria2 password reached the protected target; logs={}",
        sing_box.logs()
    );

    sing_box.kill();
    shutdown_zero(zero).await;
}

#[tokio::test]
#[ignore = "requires SING_BOX_BIN pointing to a sing-box executable"]
async fn hysteria2_authentication_reload_revokes_existing_connection_and_accepts_new_credential() {
    init_logs("hysteria2=debug");
    let material = TempMaterial::new("sing-zero-hysteria2-credential-reload");
    let tls = write_tls(&material);
    let zero_port = free_udp_port();
    let proxy = Engine::new(hysteria2_inbound_config(
        zero_port,
        &tls,
        PASSWORD,
        "account:old",
    ))
    .expect("build zero engine");
    let command = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    let zero = spawn_engine(proxy);
    sleep(Duration::from_millis(200)).await;

    let old_socks_port = free_port();
    let old_config = material.path("sing-box-old-client.json");
    std::fs::write(
        &old_config,
        sing_box_hysteria2_outbound_config(old_socks_port, zero_port, PASSWORD),
    )
    .expect("write old sing-box config");
    let mut old_sing_box = start_sing_box(&material, &old_config);
    wait_for_listener(old_socks_port).await;

    let initial_echo_port = free_port();
    let initial_payload = b"hysteria2-user-before-sync";
    let initial_echo = spawn_tcp_echo(initial_echo_port, initial_payload.len()).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_tcp_echo(old_socks_port, initial_echo_port, initial_payload),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "old Hysteria2 user could not establish the initial session: {error}; logs={}",
            old_sing_box.logs()
        )
    });
    assert_eq!(echoed, initial_payload);
    wait_for_echo(initial_echo).await;

    let active_target = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind active target");
    let active_target_port = active_target
        .local_addr()
        .expect("active target address")
        .port();
    let mut active_client = socks5_tcp_connect(old_socks_port, active_target_port)
        .await
        .expect("open active Hysteria2 stream");
    let (mut active_upstream, _) = timeout(Duration::from_secs(5), active_target.accept())
        .await
        .expect("active target was not reached")
        .expect("accept active target");
    active_client
        .write_all(b"before-sync")
        .await
        .expect("write active stream");
    let mut before_sync = [0_u8; 11];
    active_upstream
        .read_exact(&mut before_sync)
        .await
        .expect("read active stream");
    assert_eq!(&before_sync, b"before-sync");

    let mut updated = (*command.engine_handle().inner().config()).clone();
    let zero_config::InboundProtocolConfig::Hysteria2 { users, .. } =
        &mut updated.inbounds[0].protocol
    else {
        panic!("expected Hysteria2 inbound");
    };
    *users = vec![zero_config::Hysteria2UserConfig {
        password: "new-hysteria2-password".to_owned(),
        principal_key: Some("account:new".to_owned()),
        up_bps: None,
        down_bps: None,
        device_limit: None,
        quota_remaining_bytes: None,
        policy_revision: Some(2),
    }];
    command
        .apply_runtime_config_with_principal_impact_and_wait(
            updated,
            vec!["account:old".to_owned()],
            Vec::new(),
            Duration::from_secs(5),
        )
        .await
        .expect("apply acknowledged Hysteria2 credential reload");
    let mut closed = [0_u8; 1];
    let active_read = timeout(Duration::from_secs(5), active_client.read(&mut closed))
        .await
        .expect("revoked active Hysteria2 stream remained open");
    assert!(
        matches!(active_read, Ok(0) | Err(_)),
        "revoked active Hysteria2 stream still delivered bytes"
    );

    let protected = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind protected target");
    let protected_port = protected.local_addr().expect("protected address").port();
    let old_status = timeout(
        Duration::from_secs(5),
        socks5_tcp_connect_status(old_socks_port, protected_port),
    )
    .await;
    assert!(
        !matches!(old_status, Ok(Ok(0x00))),
        "revoked Hysteria2 user unexpectedly opened a new stream; logs={}",
        old_sing_box.logs()
    );
    assert!(
        timeout(Duration::from_millis(750), protected.accept())
            .await
            .is_err(),
        "revoked Hysteria2 user reached the protected target; logs={}",
        old_sing_box.logs()
    );
    old_sing_box.kill();

    let new_socks_port = free_port();
    let new_config = material.path("sing-box-new-client.json");
    std::fs::write(
        &new_config,
        sing_box_hysteria2_outbound_config(new_socks_port, zero_port, "new-hysteria2-password"),
    )
    .expect("write new sing-box config");
    let mut new_sing_box = start_sing_box(&material, &new_config);
    wait_for_listener(new_socks_port).await;

    let new_echo_port = free_port();
    let new_payload = b"hysteria2-user-after-sync";
    let new_echo = spawn_tcp_echo(new_echo_port, new_payload.len()).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_tcp_echo(new_socks_port, new_echo_port, new_payload),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "new Hysteria2 user could not establish a session: {error}; logs={}",
            new_sing_box.logs()
        )
    });
    assert_eq!(echoed, new_payload);

    new_sing_box.kill();
    shutdown_zero(zero).await;
    wait_for_echo(new_echo).await;
}

async fn spawn_hysteria2_outbound(
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
                    "tag": "hysteria2-out",
                    "protocol": {{
                        "type": "hysteria2",
                        "server": "127.0.0.1",
                        "port": {server_port},
                        "password": "{PASSWORD}",
                        "insecure": true
                    }}
                }}
            ],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "hysteria2-out" }} }}
        }}"#
    ))
    .expect("parse zero config");
    let zero = spawn_engine(Engine::new(config).expect("build zero engine"));
    wait_for_listener(zero_socks_port).await;
    zero
}

async fn spawn_hysteria2_inbound(listen_port: u16, tls: &TlsPaths) -> zero_proxy::RunningProxy {
    let config = hysteria2_inbound_config(listen_port, tls, PASSWORD, "account:interop");
    let zero = spawn_engine(Engine::new(config).expect("build zero engine"));
    sleep(Duration::from_millis(200)).await;
    zero
}

fn hysteria2_inbound_config(
    listen_port: u16,
    tls: &TlsPaths,
    password: &str,
    principal_key: &str,
) -> RuntimeConfig {
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "hysteria2-in",
                    "listen": {{ "address": "127.0.0.1", "port": {listen_port} }},
                    "protocol": {{
                        "type": "hysteria2",
                        "users": [{{
                            "password": "{password}",
                            "principal_key": "{principal_key}"
                        }}],
                        "cert_path": "{}",
                        "key_path": "{}"
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#,
        escape_json_path(&tls.cert_path),
        escape_json_path(&tls.key_path),
    ))
    .expect("parse zero inbound config");
    config
}

fn start_sing_box(material: &TempMaterial, config: &std::path::Path) -> ExternalProcess {
    ExternalProcess::start(
        sing_box_bin("hysteria2"),
        &["run", "-c", config.to_str().expect("sing-box config path")],
        material,
        "sing-box",
    )
}

struct TlsPaths {
    cert_path: std::path::PathBuf,
    key_path: std::path::PathBuf,
}

fn write_tls(material: &TempMaterial) -> TlsPaths {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate self-signed cert");
    let cert_path = material.path("server.crt");
    let key_path = material.path("server.key");
    std::fs::write(&cert_path, certified.cert.pem()).expect("write cert");
    std::fs::write(&key_path, certified.signing_key.serialize_pem()).expect("write key");
    TlsPaths {
        cert_path,
        key_path,
    }
}

fn sing_box_hysteria2_inbound_config(port: u16, tls: &TlsPaths) -> String {
    format!(
        r#"{{
            "log": {{ "level": "debug" }},
            "inbounds": [
                {{
                    "type": "hysteria2",
                    "tag": "hysteria2-in",
                    "listen": "127.0.0.1",
                    "listen_port": {port},
                    "users": [{{ "name": "zero", "password": "{PASSWORD}" }}],
                    "tls": {{
                        "enabled": true,
                        "certificate_path": "{}",
                        "key_path": "{}"
                    }}
                }}
            ],
            "outbounds": [{{ "type": "direct", "tag": "direct" }}],
            "route": {{ "final": "direct" }}
        }}"#,
        escape_json_path(&tls.cert_path),
        escape_json_path(&tls.key_path),
    )
}

fn sing_box_hysteria2_outbound_config(socks_port: u16, server_port: u16, password: &str) -> String {
    format!(
        r#"{{
            "log": {{ "level": "debug" }},
            "inbounds": [
                {{
                    "type": "socks",
                    "tag": "socks-in",
                    "listen": "127.0.0.1",
                    "listen_port": {socks_port}
                }}
            ],
            "outbounds": [
                {{
                    "type": "hysteria2",
                    "tag": "hysteria2-out",
                    "server": "127.0.0.1",
                    "server_port": {server_port},
                    "password": "{password}",
                    "tls": {{
                        "enabled": true,
                        "server_name": "localhost",
                        "insecure": true
                    }}
                }}
            ],
            "route": {{ "final": "hysteria2-out" }}
        }}"#
    )
}

async fn socks5_tcp_connect_status(proxy_port: u16, target_port: u16) -> std::io::Result<u8> {
    let mut stream = TcpStream::connect(("127.0.0.1", proxy_port)).await?;
    stream.write_all(&[0x05, 0x01, 0x00]).await?;
    let mut auth = [0_u8; 2];
    stream.read_exact(&mut auth).await?;
    if auth != [0x05, 0x00] {
        return Ok(auth[1]);
    }

    let mut request = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request).await?;
    let mut response = [0_u8; 10];
    stream.read_exact(&mut response).await?;
    Ok(response[1])
}

async fn socks5_tcp_connect(proxy_port: u16, target_port: u16) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect(("127.0.0.1", proxy_port)).await?;
    stream.write_all(&[0x05, 0x01, 0x00]).await?;
    let mut auth = [0_u8; 2];
    stream.read_exact(&mut auth).await?;
    if auth != [0x05, 0x00] {
        return Err(std::io::Error::other(format!(
            "SOCKS authentication failed: {auth:?}"
        )));
    }

    let mut request = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request).await?;
    let mut response = [0_u8; 10];
    stream.read_exact(&mut response).await?;
    if response[1] != 0x00 {
        return Err(std::io::Error::other(format!(
            "SOCKS connect failed: {response:?}"
        )));
    }
    Ok(stream)
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
