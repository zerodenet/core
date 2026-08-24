mod support;

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use support::{
    acquire_port_lock, create_temp_dir, free_port, http_tunnel, remove_temp_dir, remove_temp_file,
    socks5_connect, socks5_connect_ipv4, spawn_zero, stop_child, wait_for_port, write_temp_config,
};

#[test]
fn zero_binary_relays_tcp_through_socks5_direct() {
    let _lock = acquire_port_lock();
    let proxy_port = free_port();
    let echo_port = free_port();

    let config = format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
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
    let config_path = write_temp_config(&config, "proxy-smoke-socks5-direct");

    let echo_thread = spawn_echo_server(echo_port);
    let mut child = spawn_zero(&["run", config_path.to_str().expect("utf-8 config path")]);

    wait_for_port(proxy_port);

    let mut stream = socks5_connect_ipv4(proxy_port, [127, 0, 0, 1], echo_port);
    stream.write_all(b"ping").expect("write payload");

    let mut echoed = [0_u8; 4];
    stream.read_exact(&mut echoed).expect("read payload");
    assert_eq!(&echoed, b"ping");

    stop_child(&mut child);
    let _ = echo_thread.join();
    remove_temp_file(&config_path);
}

#[test]
fn zero_binary_relays_tcp_through_http() {
    let _lock = acquire_port_lock();
    let proxy_port = free_port();
    let echo_port = free_port();

    let config = format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "http-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                    "protocol": {{ "type": "http" }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#
    );
    let config_path = write_temp_config(&config, "proxy-smoke-http");

    let echo_thread = spawn_echo_server(echo_port);
    let mut child = spawn_zero(&["run", config_path.to_str().expect("utf-8 config path")]);

    wait_for_port(proxy_port);

    let mut stream = http_tunnel(proxy_port, &format!("127.0.0.1:{echo_port}"));
    stream.write_all(b"pong").expect("write payload");

    let mut echoed = [0_u8; 4];
    stream.read_exact(&mut echoed).expect("read payload");
    assert_eq!(&echoed, b"pong");

    stop_child(&mut child);
    let _ = echo_thread.join();
    remove_temp_file(&config_path);
}

#[test]
fn zero_binary_relays_tcp_through_chained_socks5_outbound() {
    let _lock = acquire_port_lock();
    let outer_port = free_port();
    let upstream_port = free_port();
    let echo_port = free_port();

    let upstream_config = format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "upstream-socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {upstream_port} }},
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
    let upstream_config_path = write_temp_config(&upstream_config, "proxy-smoke-upstream");

    let outer_config = format!(
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
                    "tag": "chain",
                    "protocol": {{
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": {upstream_port}
                    }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "chain" }}
            }}
        }}"#
    );
    let outer_config_path = write_temp_config(&outer_config, "proxy-smoke-outer");

    let echo_thread = spawn_echo_server(echo_port);
    let mut upstream = spawn_zero(&[
        "run",
        upstream_config_path.to_str().expect("utf-8 config path"),
    ]);
    wait_for_port(upstream_port);

    let mut outer = spawn_zero(&[
        "run",
        outer_config_path.to_str().expect("utf-8 config path"),
    ]);
    wait_for_port(outer_port);

    let mut stream = socks5_connect(outer_port, "127.0.0.1", echo_port);
    stream.write_all(b"mesh").expect("write payload");

    let mut echoed = [0_u8; 4];
    stream.read_exact(&mut echoed).expect("read payload");
    assert_eq!(&echoed, b"mesh");

    stop_child(&mut outer);
    stop_child(&mut upstream);
    let _ = echo_thread.join();
    remove_temp_file(&outer_config_path);
    remove_temp_file(&upstream_config_path);
}

#[test]
fn zero_binary_applies_file_backed_rule_sets() {
    let _lock = acquire_port_lock();
    let project_dir = create_temp_dir("proxy-smoke-rule-sets");
    let rules_dir = project_dir.join("rules");
    fs::create_dir_all(&rules_dir).expect("create rules dir");
    fs::write(
        rules_dir.join("ads.zero.json"),
        r#"{
            "version": 1,
            "name": "ads",
            "rules": [
                { "type": "domain_suffix", "value": "blocked.example" },
                { "type": "domain_suffix", "value": "ads.local" }
            ]
        }"#,
    )
    .expect("write domain Zero Rule IR");
    fs::write(
        rules_dir.join("lan.zero.json"),
        r#"{
            "version": 1,
            "name": "lan",
            "rules": [
                { "type": "ipv4_cidr", "value": "127.0.0.0/8" }
            ]
        }"#,
    )
    .expect("write CIDR Zero Rule IR");

    let proxy_port = free_port();
    let echo_port = free_port();
    let config_path = project_dir.join("config.json");
    let config = format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "mixed-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                    "protocol": {{ "type": "mixed" }}
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
            "route": {{
                "rule_sets": [
                    {{
                        "tag": "ads",
                        "type": "file",
                        "path": "rules/ads.zero.json",
                        "format": "zero_rule_ir"
                    }},
                    {{
                        "tag": "lan",
                        "type": "file",
                        "path": "rules/lan.zero.json",
                        "format": "zero_rule_ir"
                    }}
                ],
                "rules": [
                    {{
                        "condition": {{ "type": "rule_set", "tag": "ads" }},
                        "action": {{ "type": "reject" }}
                    }},
                    {{
                        "condition": {{ "type": "rule_set", "tag": "lan" }},
                        "action": {{ "type": "route", "outbound": "direct" }}
                    }}
                ],
                "final": {{ "type": "route", "outbound": "block" }}
            }}
        }}"#
    );
    fs::write(&config_path, config).expect("write config");

    let echo_thread = spawn_echo_server(echo_port);
    let mut child = spawn_zero(&["run", config_path.to_str().expect("utf-8 config path")]);

    wait_for_port(proxy_port);

    let mut blocked =
        std::net::TcpStream::connect(("127.0.0.1", proxy_port)).expect("connect proxy");
    blocked
        .write_all(&[0x05, 0x01, 0x00])
        .expect("write socks auth");
    let mut auth = [0_u8; 2];
    blocked.read_exact(&mut auth).expect("read socks auth");
    assert_eq!(auth, [0x05, 0x00]);

    let mut blocked_request = vec![0x05, 0x01, 0x00, 0x03, 0x0f];
    blocked_request.extend_from_slice(b"blocked.example");
    blocked_request.extend_from_slice(&443_u16.to_be_bytes());
    blocked
        .write_all(&blocked_request)
        .expect("write blocked request");

    let mut blocked_response = [0_u8; 10];
    blocked
        .read_exact(&mut blocked_response)
        .expect("read blocked response");
    assert_eq!(
        blocked_response[1], 0x02,
        "unexpected socks5 blocked reply: {:?}",
        blocked_response
    );

    let mut stream = socks5_connect_ipv4(proxy_port, [127, 0, 0, 1], echo_port);
    stream.write_all(b"rule").expect("write payload");
    let mut echoed = [0_u8; 4];
    stream.read_exact(&mut echoed).expect("read payload");
    assert_eq!(&echoed, b"rule");

    stop_child(&mut child);
    let _ = echo_thread.join();
    remove_temp_dir(&project_dir);
}

fn spawn_echo_server(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind echo");
        let (mut stream, _) = listener.accept().expect("accept echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).expect("read echo");
        stream.write_all(&buf).expect("write echo");
    })
}
