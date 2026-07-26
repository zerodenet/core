mod support;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use support::{
    acquire_port_lock, free_port, http_get, http_post_json, remove_temp_file, wait_for_port,
    write_temp_config,
};

#[test]
fn local_status_listener_exposes_live_runtime_view() {
    let _lock = acquire_port_lock();
    let socks_port = free_port();
    let status_port = free_port();
    // With `grpc-api` enabled the run command also binds status_port + 1.
    // Ephemeral ports returned by Windows are commonly consecutive, so avoid
    // accidentally assigning that companion port to the echo server.
    let mut echo_port = free_port();
    while echo_port == status_port.saturating_add(1) {
        echo_port = free_port();
    }

    let config = format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {socks_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#
    );
    let config_path = write_temp_config(&config, "live-status");

    let (ready_tx, ready_rx) = mpsc::channel();
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let echo_thread = thread::spawn(move || {
        let listener = TcpListener::bind(("127.0.0.1", echo_port)).expect("bind echo");
        ready_tx.send(()).expect("notify echo ready");
        let (mut stream, _) = listener.accept().expect("accept echo");
        accepted_tx.send(()).expect("notify echo accepted");
        let _ = release_rx.recv();
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).expect("read echo");
        stream.write_all(&buf).expect("write echo");
    });
    ready_rx.recv().expect("wait for echo listener");

    let mut child = Command::new(env!("CARGO_BIN_EXE_zero"))
        .args([
            "run",
            "--status-listen",
            &format!("127.0.0.1:{status_port}"),
            config_path.to_str().expect("utf-8 config path"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn zero run");

    wait_for_port(status_port);
    wait_for_port(socks_port);

    let mut client = TcpStream::connect(("127.0.0.1", socks_port)).expect("connect socks5");
    client
        .write_all(&[0x05, 0x01, 0x00])
        .expect("write socks auth");

    let mut auth = [0_u8; 2];
    client.read_exact(&mut auth).expect("read socks auth");
    assert_eq!(auth, [0x05, 0x00]);

    let request = [
        0x05,
        0x01,
        0x00,
        0x01,
        127,
        0,
        0,
        1,
        ((echo_port >> 8) & 0xff) as u8,
        (echo_port & 0xff) as u8,
    ];
    client.write_all(&request).expect("write socks request");

    let mut response = [0_u8; 10];
    client
        .read_exact(&mut response)
        .expect("read socks response");
    assert_eq!(response[1], 0x00);

    accepted_rx.recv().expect("wait for echo accept");

    let runtime_response = http_get(status_port, "/api/v1/runtime");
    let runtime_body = runtime_response
        .split("\r\n\r\n")
        .nth(1)
        .expect("http body");
    let runtime: serde_json::Value =
        serde_json::from_str(runtime_body).expect("parse runtime json");
    assert_eq!(runtime["result"]["stats"]["active_sessions"], 1);
    assert_eq!(
        runtime["result"]["active_sessions"][0]["inbound_tag"],
        "socks-in"
    );
    assert_eq!(
        runtime["result"]["active_sessions"][0]["outbound_tag"],
        "direct"
    );
    assert_eq!(runtime["result"]["active_sessions"][0]["network"], "tcp");
    assert_eq!(runtime["result"]["active_sessions"][0]["mode"], "rule");

    let config_response = http_get(status_port, "/api/v1/config");
    let config_body = config_response.split("\r\n\r\n").nth(1).expect("http body");
    let config_json: serde_json::Value =
        serde_json::from_str(config_body).expect("parse config json");
    assert_eq!(config_json["result"]["rule_count"], 0);
    assert_eq!(
        config_json["result"]["listeners"][0]["listen_port"],
        socks_port
    );

    let status_response = http_get(status_port, "/api/v1/runtime");
    let status_body = status_response.split("\r\n\r\n").nth(1).expect("http body");
    let status_json: serde_json::Value =
        serde_json::from_str(status_body).expect("parse status json");
    assert_eq!(status_json["result"]["stats"]["active_sessions"], 1);

    release_tx.send(()).expect("release echo");
    client.write_all(b"ping").expect("write echo payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).expect("read echo payload");
    assert_eq!(&echoed, b"ping");
    drop(client);

    thread::sleep(Duration::from_millis(100));

    let runtime_after_response = http_get(status_port, "/api/v1/runtime");
    let runtime_after_body = runtime_after_response
        .split("\r\n\r\n")
        .nth(1)
        .expect("http body");
    let runtime_after: serde_json::Value =
        serde_json::from_str(runtime_after_body).expect("parse runtime json");
    assert_eq!(runtime_after["result"]["stats"]["active_sessions"], 0);

    child.kill().expect("kill zero process");
    let _ = child.wait();
    let _ = echo_thread.join();
    remove_temp_file(&config_path);
}

#[test]
fn local_status_listener_can_switch_selector_group() {
    let _lock = acquire_port_lock();
    let socks_port = free_port();
    let status_port = free_port();
    let mut echo_port = free_port();
    while echo_port == status_port.saturating_add(1) {
        echo_port = free_port();
    }

    let config = format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {socks_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "direct",
                    "protocol": {{ "type": "direct" }}
                }},
                {{
                    "tag": "block",
                    "protocol": {{ "type": "block" }}
                }}
            ],
            "outbound_groups": [
                {{
                    "tag": "proxy",
                    "type": "selector",
                    "outbounds": ["block", "direct"],
                    "selected": "block"
                }}
            ],
            "mode": {{
                "type": "global",
                "outbound": "proxy"
            }},
            "route": {{
                "rules": [],
                "final": {{ "type": "reject" }}
            }}
        }}"#
    );
    let config_path = write_temp_config(&config, "selector-control");

    let (ready_tx, ready_rx) = mpsc::channel();
    let echo_thread = thread::spawn(move || {
        let listener = TcpListener::bind(("127.0.0.1", echo_port)).expect("bind echo");
        ready_tx.send(()).expect("notify echo ready");
        let (mut stream, _) = listener.accept().expect("accept echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).expect("read echo");
        stream.write_all(&buf).expect("write echo");
    });
    ready_rx.recv().expect("wait for echo listener");

    let mut child = Command::new(env!("CARGO_BIN_EXE_zero"))
        .args([
            "run",
            "--status-listen",
            &format!("127.0.0.1:{status_port}"),
            config_path.to_str().expect("utf-8 config path"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn zero run");

    wait_for_port(status_port);
    wait_for_port(socks_port);

    let initial_config = http_get(status_port, "/api/v1/config");
    let initial_body = initial_config.split("\r\n\r\n").nth(1).expect("http body");
    let initial_json: serde_json::Value =
        serde_json::from_str(initial_body).expect("parse config json");
    assert_eq!(
        initial_json["result"]["outbound_groups"][0]["selected"],
        "block"
    );

    let mut blocked = TcpStream::connect(("127.0.0.1", socks_port)).expect("connect socks5");
    blocked
        .write_all(&[0x05, 0x01, 0x00])
        .expect("write socks auth");
    let mut auth = [0_u8; 2];
    blocked.read_exact(&mut auth).expect("read auth");
    assert_eq!(auth, [0x05, 0x00]);

    let request = [
        0x05,
        0x01,
        0x00,
        0x01,
        127,
        0,
        0,
        1,
        ((echo_port >> 8) & 0xff) as u8,
        (echo_port & 0xff) as u8,
    ];
    blocked.write_all(&request).expect("write socks request");
    let mut blocked_response = [0_u8; 10];
    blocked
        .read_exact(&mut blocked_response)
        .expect("read blocked response");
    assert_eq!(blocked_response[1], 0x02);
    drop(blocked);

    let update_response = http_post_json(
        status_port,
        "/api/v1/commands",
        r#"{
            "method": "policies.select",
            "params": {
                "policy_tag": "proxy",
                "target_tag": "direct"
            }
        }"#,
    );
    assert!(update_response.starts_with("HTTP/1.1 200 OK"));
    let update_body = update_response.split("\r\n\r\n").nth(1).expect("http body");
    let update_json: serde_json::Value =
        serde_json::from_str(update_body).expect("parse command json");
    assert_eq!(update_json["result"]["result"]["selected"], "direct");

    let config_after = http_get(status_port, "/api/v1/config");
    let config_after_body = config_after.split("\r\n\r\n").nth(1).expect("http body");
    let config_after_json: serde_json::Value =
        serde_json::from_str(config_after_body).expect("parse config json");
    assert_eq!(
        config_after_json["result"]["outbound_groups"][0]["selected"],
        "direct"
    );

    let mut client = TcpStream::connect(("127.0.0.1", socks_port)).expect("connect socks5");
    client
        .write_all(&[0x05, 0x01, 0x00])
        .expect("write socks auth");
    client.read_exact(&mut auth).expect("read auth");
    assert_eq!(auth, [0x05, 0x00]);
    client.write_all(&request).expect("write socks request");

    let mut response = [0_u8; 10];
    client
        .read_exact(&mut response)
        .expect("read socks response");
    assert_eq!(response[1], 0x00);

    client.write_all(b"ping").expect("write echo payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).expect("read echo payload");
    assert_eq!(&echoed, b"ping");
    drop(client);

    child.kill().expect("kill zero process");
    let _ = child.wait();
    let _ = echo_thread.join();
    remove_temp_file(&config_path);
}

#[test]
fn local_status_commands_endpoint_selects_policy() {
    let _lock = acquire_port_lock();
    let socks_port = free_port();
    let status_port = free_port();

    let config = format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {socks_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "direct",
                    "protocol": {{ "type": "direct" }}
                }},
                {{
                    "tag": "block",
                    "protocol": {{ "type": "block" }}
                }}
            ],
            "outbound_groups": [
                {{
                    "tag": "proxy",
                    "type": "selector",
                    "outbounds": ["block", "direct"],
                    "selected": "block"
                }}
            ],
            "mode": {{
                "type": "global",
                "outbound": "proxy"
            }},
            "route": {{
                "rules": [],
                "final": {{ "type": "reject" }}
            }}
        }}"#
    );
    let config_path = write_temp_config(&config, "commands-control");

    let mut child = Command::new(env!("CARGO_BIN_EXE_zero"))
        .args([
            "run",
            "--status-listen",
            &format!("127.0.0.1:{status_port}"),
            config_path.to_str().expect("utf-8 config path"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn zero run");

    wait_for_port(status_port);
    wait_for_port(socks_port);

    let command = r#"{
        "method": "policies.select",
        "params": {
            "policy_tag": "proxy",
            "target_tag": "direct"
        }
    }"#;
    let response = http_post_json(status_port, "/api/v1/commands", command);
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    let body = response.split("\r\n\r\n").nth(1).expect("http body");
    let body: serde_json::Value = serde_json::from_str(body).expect("parse command response");
    assert_eq!(body["result"]["accepted"], true);
    assert_eq!(body["result"]["result"]["selected"], "direct");

    let config_after = http_get(status_port, "/api/v1/config");
    let config_after_body = config_after.split("\r\n\r\n").nth(1).expect("http body");
    let config_after_json: serde_json::Value =
        serde_json::from_str(config_after_body).expect("parse config json");
    assert_eq!(
        config_after_json["result"]["outbound_groups"][0]["selected"],
        "direct"
    );

    child.kill().expect("kill zero process");
    let _ = child.wait();
    remove_temp_file(&config_path);
}

#[test]
fn configured_control_api_requires_api_key() {
    let _lock = acquire_port_lock();
    let socks_port = free_port();
    let status_port = free_port();

    let config = format!(
        r#"{{
            "api": {{
                "control": {{
                    "enabled": true,
                    "listen": {{ "address": "127.0.0.1", "port": {status_port} }},
                    "api_key_env": "ZERO_NODE_API_KEY"
                }}
            }},
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {socks_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#
    );
    let config_path = write_temp_config(&config, "configured-control");

    let mut child = Command::new(env!("CARGO_BIN_EXE_zero"))
        .args(["run", config_path.to_str().expect("utf-8 config path")])
        .env("ZERO_NODE_API_KEY", "node-secret")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn zero run");

    wait_for_port(status_port);
    wait_for_port(socks_port);

    let unauthorized = http_get(status_port, "/api/v1/runtime");
    assert!(unauthorized.starts_with("HTTP/1.1 401 Unauthorized"));

    let authorized = http_get_with_api_key(status_port, "/api/v1/runtime", "node-secret");
    assert!(authorized.starts_with("HTTP/1.1 200 OK"));

    child.kill().expect("kill zero process");
    let _ = child.wait();
    remove_temp_file(&config_path);
}

fn http_get_with_api_key(port: u16, path: &str, api_key: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect status port");
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: Bearer {api_key}\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .expect("write http request");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read http response");
    response
}
