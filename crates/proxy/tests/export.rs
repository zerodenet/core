mod support;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use zero_api::{
    event_type, ApiErrorCode, CapabilitiesQuery, CommandRequest, CommandService, ConfigQuery,
    ConfigValidateCommand, EventFilter, EventSource, FlowCloseCommand, FlowFilter, FlowGetQuery,
    FlowListQuery, PoliciesQuery, PolicySelectCommand, QueryRequest, QueryResponse, QueryService,
};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::Proxy as Engine;
use zero_proxy::ProxyHandle;

use serde_json::json;
use support::{free_port, spawn_engine, wait_for, wait_for_listener};

#[tokio::test]
async fn exports_serializable_engine_status_view() {
    let echo_port = free_port();
    let proxy_port = free_port();

    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind echo");
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read echo");
        stream.write_all(&buf).await.expect("write echo");
    });

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "chain",
                    "protocol": {{
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": 2080
                    }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#
    ))
    .expect("parse config");

    let engine = Engine::new(config).expect("build engine");
    let handle = spawn_engine(engine);

    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write auth");

    let mut auth = [0_u8; 2];
    client.read_exact(&mut auth).await.expect("read auth");

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
    client.write_all(&request).await.expect("write request");

    let mut response = [0_u8; 10];
    client
        .read_exact(&mut response)
        .await
        .expect("read response");
    assert_eq!(response[1], 0x00);

    let exported = handle.export_status();
    assert_eq!(exported.config.mode.kind, "rule");
    assert_eq!(exported.config.rule_count, 0);
    assert_eq!(exported.config.listeners.len(), 1);
    assert_eq!(exported.config.listeners[0].tag, "socks-in");
    assert_eq!(exported.config.listeners[0].protocol, "socks5");
    assert!(exported.config.listeners[0].udp_enabled);
    assert_eq!(exported.config.outbounds.len(), 1);
    assert_eq!(exported.config.outbounds[0].tag, "chain");
    assert_eq!(exported.config.outbounds[0].protocol, "socks5");
    assert!(exported.config.outbounds[0].udp_enabled);
    assert!(exported.config.outbound_groups.is_empty());
    assert_eq!(exported.runtime.udp_upstream_idle_timeout_seconds, 30);
    assert!(exported.runtime.udp_enabled);
    assert_eq!(exported.runtime.active_sessions.len(), 1);
    assert_eq!(exported.runtime.active_sessions[0].target.family, "ipv4");
    assert_eq!(exported.runtime.active_sessions[0].protocol, "socks5");
    assert_eq!(exported.runtime.active_sessions[0].network, "tcp");
    assert_eq!(exported.runtime.active_sessions[0].mode, "rule");
    assert!(exported.runtime.recent_completed_sessions.is_empty());

    let json = serde_json::to_value(&exported).expect("serialize export");
    assert_eq!(json["config"]["mode"]["kind"], "rule");
    assert_eq!(json["config"]["listeners"][0]["tag"], "socks-in");
    assert_eq!(json["config"]["outbounds"][0]["server"], "127.0.0.1");
    assert_eq!(json["runtime"]["udp_upstream_idle_timeout_seconds"], 30);
    assert_eq!(json["runtime"]["active_sessions"][0]["network"], "tcp");
    assert_eq!(json["runtime"]["active_sessions"][0]["mode"], "rule");
    assert_eq!(
        json["runtime"]["active_sessions"][0]["target"]["family"],
        "ipv4"
    );

    client.write_all(b"ping").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"ping");

    drop(client);
    wait_for("completed flow event", || {
        !handle.completed_sessions().is_empty()
    })
    .await;

    let events = handle
        .latest(usize::MAX, EventFilter::default())
        .expect("read event history");
    let completed = events
        .iter()
        .find(|event| event.event_type == event_type::FLOW_COMPLETED)
        .expect("flow completed event");
    assert!(
        completed.sequence.unwrap() >= 1,
        "sequence should be >=1 (engine.started takes seq 1)"
    );
    assert_eq!(completed.payload["flow_id"], "1");
    assert_eq!(completed.payload["network"], "tcp");
    assert_eq!(completed.payload["inbound"]["tag"], "socks-in");
    assert_eq!(completed.payload["outbound"]["protocol"], "direct");
    assert!(completed.payload["traffic"]["bytes_up"].as_u64().unwrap() >= 4);
    assert!(completed.payload["traffic"]["bytes_down"].as_u64().unwrap() >= 4);
    let record = &completed.payload["record"];
    assert_eq!(record["flow_id"], "1");
    assert_eq!(record["state"], "completed");
    assert_eq!(record["source"]["ip"], "127.0.0.1");
    assert_eq!(record["target"]["host"], "127.0.0.1");
    assert_eq!(record["target"]["port"], echo_port);
    assert_eq!(record["route"]["mode"], "rule");
    assert_eq!(record["route"]["action"], "direct");
    assert_eq!(record["route"]["selection_chain"][0], "direct");
    assert_eq!(record["path"]["outbound"]["tag"], "direct");
    assert_eq!(record["path"]["outbound"]["protocol"], "direct");
    assert_eq!(record["result"]["outcome"], "direct_relayed");
    assert!(record["timing"]["ended_at_unix_ms"].as_u64().is_some());

    let routed = events
        .iter()
        .find(|event| event.event_type == event_type::FLOW_ROUTED)
        .expect("flow routed event");
    assert_eq!(routed.payload["record"]["state"], "active");
    assert!(
        routed.payload["record"]["revision"].as_u64().unwrap()
            < record["revision"].as_u64().unwrap()
    );

    let filtered = handle
        .latest(
            usize::MAX,
            EventFilter {
                inbound_tags: vec!["socks-in".to_owned()],
                ..EventFilter::default()
            },
        )
        .expect("read filtered event history");
    assert!(
        !filtered.is_empty(),
        "should have at least 1 flow.completed event"
    );

    handle.shutdown().await.expect("shutdown engine");
    let _ = echo_task.await;
}

#[test]
fn exports_custom_udp_upstream_idle_timeout_from_config() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "udp_upstream_idle_timeout_seconds": 9
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("parse config");

    let engine = Engine::new(config).expect("build engine");
    let exported = engine.export_runtime();

    assert_eq!(exported.udp_upstream_idle_timeout_seconds, 9);
}

#[test]
fn exports_udp_policy_from_config() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": { "udp": { "enabled": false } },
            "inbounds": [
                {
                    "tag": "socks-in",
                    "listen": { "address": "127.0.0.1", "port": 1080 },
                    "udp": { "enabled": false },
                    "protocol": { "type": "socks5" }
                }
            ],
            "outbounds": [
                {
                    "tag": "direct",
                    "udp": { "enabled": false },
                    "protocol": { "type": "direct" }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "direct" }
            }
        }"#,
    )
    .expect("parse config");

    let engine = Engine::new(config).expect("build engine");
    let exported = engine.export_status();

    assert!(!exported.runtime.udp_enabled);
    assert!(!exported.config.listeners[0].udp_enabled);
    assert!(!exported.config.outbounds[0].udp_enabled);
}

#[test]
fn selector_group_update_is_reflected_in_exported_config() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "direct", "protocol": { "type": "direct" } },
                { "tag": "block", "protocol": { "type": "block" } }
            ],
            "outbound_groups": [
                {
                    "tag": "proxy",
                    "type": "selector",
                    "outbounds": ["block", "direct"],
                    "selected": "block"
                }
            ],
            "mode": {
                "type": "global",
                "outbound": "proxy"
            },
            "route": {
                "rules": [],
                "final": { "type": "reject" }
            }
        }"#,
    )
    .expect("parse config");

    let engine = Engine::new(config).expect("build engine");
    assert_eq!(
        engine.export_config().outbound_groups[0]
            .selected
            .as_deref(),
        Some("block")
    );

    engine
        .set_selector_target("proxy", "direct")
        .expect("update selector");

    assert_eq!(
        engine.export_config().outbound_groups[0]
            .selected
            .as_deref(),
        Some("direct")
    );
}

#[test]
fn selector_group_can_switch_to_group_target_in_exported_config() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "direct", "protocol": { "type": "direct" } },
                { "tag": "block", "protocol": { "type": "block" } }
            ],
            "outbound_groups": [
                {
                    "tag": "fallback-proxy",
                    "type": "fallback",
                    "outbounds": ["block", "direct"]
                },
                {
                    "tag": "proxy",
                    "type": "selector",
                    "outbounds": ["direct", "fallback-proxy"],
                    "selected": "direct"
                }
            ],
            "mode": {
                "type": "global",
                "outbound": "proxy"
            },
            "route": {
                "rules": [],
                "final": { "type": "reject" }
            }
        }"#,
    )
    .expect("parse config");

    let engine = Engine::new(config).expect("build engine");
    engine
        .set_selector_target("proxy", "fallback-proxy")
        .expect("update selector");

    let exported = engine.export_config();
    let group = exported
        .outbound_groups
        .iter()
        .find(|group| group.tag == "proxy")
        .expect("find selector group");
    assert_eq!(group.selected.as_deref(), Some("fallback-proxy"));
    assert_eq!(
        group.effective_chains,
        vec![
            vec![
                "proxy".to_owned(),
                "fallback-proxy".to_owned(),
                "block".to_owned(),
            ],
            vec![
                "proxy".to_owned(),
                "fallback-proxy".to_owned(),
                "direct".to_owned(),
            ],
        ]
    );
}

#[test]
fn engine_command_service_validates_config_and_selects_policy() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "direct", "protocol": { "type": "direct" } },
                { "tag": "block", "protocol": { "type": "block" } }
            ],
            "outbound_groups": [
                {
                    "tag": "proxy",
                    "type": "selector",
                    "outbounds": ["block", "direct"],
                    "selected": "block"
                }
            ],
            "mode": {
                "type": "global",
                "outbound": "proxy"
            },
            "route": {
                "rules": [],
                "final": { "type": "reject" }
            }
        }"#,
    )
    .expect("parse config");

    let engine = Engine::new(config).expect("build engine");
    let validation = engine
        .execute(CommandRequest::ConfigValidate(ConfigValidateCommand {
            config: json!({
                "route": {
                    "rules": [],
                    "final": { "type": "direct" }
                }
            }),
        }))
        .expect("validate config");
    assert!(validation.accepted);
    assert_eq!(validation.result.expect("result")["valid"], true);

    let selected = engine
        .execute(CommandRequest::PolicySelect(PolicySelectCommand {
            policy_tag: "proxy".to_owned(),
            target_tag: "direct".to_owned(),
        }))
        .expect("select policy");
    assert!(selected.accepted);
    assert_eq!(
        engine.export_config().outbound_groups[0]
            .selected
            .as_deref(),
        Some("direct")
    );

    let missing = engine
        .execute(CommandRequest::PolicySelect(PolicySelectCommand {
            policy_tag: "missing".to_owned(),
            target_tag: "direct".to_owned(),
        }))
        .expect_err("missing policy should fail");
    assert_eq!(missing.code, ApiErrorCode::NotFound);

    let not_found = engine
        .execute(CommandRequest::FlowClose(FlowCloseCommand {
            flow_id: "999999".to_owned(),
        }))
        .expect_err("closing non-existent flow should fail");
    assert_eq!(not_found.code, ApiErrorCode::NotFound);
}

#[test]
fn engine_query_service_exposes_config_and_policy_snapshots() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "direct", "protocol": { "type": "direct" } },
                { "tag": "block", "protocol": { "type": "block" } }
            ],
            "outbound_groups": [
                {
                    "tag": "proxy",
                    "type": "selector",
                    "outbounds": ["block", "direct"],
                    "selected": "block"
                }
            ],
            "mode": {
                "type": "global",
                "outbound": "proxy"
            },
            "route": {
                "rules": [],
                "final": { "type": "reject" }
            }
        }"#,
    )
    .expect("parse config");

    let engine = Engine::new(config).expect("build engine");

    let config = engine
        .query(QueryRequest::Config(ConfigQuery))
        .expect("query config");
    let QueryResponse::Config(config) = config else {
        panic!("expected config query response");
    };
    assert_eq!(config.mode.kind, "global");
    assert_eq!(config.mode.outbound.as_deref(), Some("proxy"));

    let policies = engine
        .query(QueryRequest::Policies(PoliciesQuery))
        .expect("query policies");
    let QueryResponse::Policies(policies) = policies else {
        panic!("expected policies query response");
    };
    assert_eq!(policies.len(), 1);
    assert_eq!(policies[0].tag, "proxy");
    assert_eq!(policies[0].selected.as_deref(), Some("block"));
}

#[test]
fn capabilities_use_snake_case_wire_names() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("parse config");

    let engine = Engine::new(config).expect("build engine");
    let response = engine
        .query(QueryRequest::Capabilities(CapabilitiesQuery))
        .expect("query capabilities");
    let QueryResponse::Capabilities(capabilities) = response else {
        panic!("expected capabilities query response");
    };

    assert_eq!(capabilities.adapters[0].kind, "in_process");
    assert!(capabilities
        .features
        .iter()
        .all(|feature| !feature.contains('-')));
    assert!(capabilities
        .features
        .contains(&"config_snapshot".to_owned()));
    assert!(
        capabilities.protocols.is_empty(),
        "engine capabilities should not own the protocol capability matrix"
    );

    let json = serde_json::to_value(&capabilities).expect("serialize capabilities");
    assert_snake_case_capability_strings(&json);
}

#[test]
fn proxy_handle_capabilities_use_protocol_inventory() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("parse config");

    let proxy = Engine::new(config).expect("build proxy");
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy);
    let response = handle
        .query(QueryRequest::Capabilities(CapabilitiesQuery))
        .expect("query capabilities");
    let QueryResponse::Capabilities(capabilities) = response else {
        panic!("expected capabilities query response");
    };

    assert!(capabilities
        .protocols
        .iter()
        .any(|p| p.protocol == "direct"));
    assert!(capabilities.protocols.iter().any(|p| p.protocol == "block"));
    assert!(capabilities.protocols.iter().any(|p| p.protocol == "mixed"));
    assert!(capabilities
        .protocols
        .iter()
        .any(|p| p.protocol == "socks5"));
    assert!(capabilities
        .protocols
        .iter()
        .all(|protocol| protocol.compiled));

    let socks5 = capabilities
        .protocols
        .iter()
        .find(|protocol| protocol.protocol == "socks5")
        .expect("socks5 capability");
    assert_eq!(socks5.status, "supported");
    assert!(socks5.inbound.tcp.supported);
    assert!(socks5.inbound.udp.supported);
    assert!(socks5.outbound.tcp.supported);
    assert!(socks5.outbound.udp.supported);

    let http = capabilities
        .protocols
        .iter()
        .find(|protocol| protocol.protocol == "http")
        .expect("http capability");
    assert_eq!(http.inbound.udp.level, "not_applicable");
    assert_eq!(http.outbound.tcp.level, "unsupported");

    let vless = capabilities
        .protocols
        .iter()
        .find(|protocol| protocol.protocol == "vless")
        .expect("vless capability");
    assert_eq!(vless.status, "supported");
    assert_eq!(vless.inbound.tcp.level, "supported");
    assert_eq!(vless.inbound.udp.level, "supported");
    assert_eq!(vless.outbound.tcp.level, "supported");
    assert_eq!(vless.outbound.udp.level, "supported");
    assert_eq!(vless.mux.level, "supported");

    if let Some(mieru) = capabilities
        .protocols
        .iter()
        .find(|protocol| protocol.protocol == "mieru")
    {
        assert_eq!(mieru.status, "supported");
        assert_eq!(mieru.outbound.tcp.level, "supported");
        assert_eq!(mieru.outbound.udp.level, "supported");
        assert_eq!(mieru.inbound.tcp.level, "supported");
        assert_eq!(mieru.inbound.udp.level, "supported");
        assert!(mieru.limitations.is_empty());
        assert!(!mieru
            .limitations
            .contains(&"relay_chain_hop_is_not_supported".to_owned()));
        assert!(!mieru
            .limitations
            .contains(&"udp_relay_chain_is_not_supported".to_owned()));
    }

    if let Some(shadowsocks) = capabilities
        .protocols
        .iter()
        .find(|protocol| protocol.protocol == "shadowsocks")
    {
        assert_eq!(shadowsocks.status, "partial");
        assert_eq!(
            shadowsocks.compatibility_baseline,
            "shadowsocks_rust_sip022_sip023"
        );
        assert!(shadowsocks.inbound.tcp.supported);
        assert_eq!(shadowsocks.inbound.tcp.level, "supported");
        assert!(shadowsocks.inbound.udp.supported);
        assert_eq!(shadowsocks.inbound.udp.level, "supported");
        assert!(shadowsocks.outbound.tcp.supported);
        assert_eq!(shadowsocks.outbound.tcp.level, "supported");
        assert!(shadowsocks.outbound.udp.supported);
        assert_eq!(shadowsocks.outbound.udp.level, "supported");
        assert_eq!(shadowsocks.transports, vec!["tcp", "udp"]);
        assert_eq!(shadowsocks.mux.level, "unsupported");
        assert!(shadowsocks
            .limitations
            .contains(&"shadowsocks_2022_hardening_not_externally_validated".to_owned()));
        assert!(!shadowsocks
            .limitations
            .contains(&"shadowsocks_2022_tcp_header_is_not_implemented".to_owned()));
        assert!(!shadowsocks.limitations.contains(
            &"shadowsocks_2022_udp_server_response_context_is_not_implemented".to_owned()
        ));
        assert!(!shadowsocks
            .limitations
            .contains(&"udp_relay_chain_packet_path_limited".to_owned()));
        assert!(!shadowsocks
            .limitations
            .contains(&"udp_relay_chain_is_not_supported".to_owned()));
    }

    if let Some(vmess) = capabilities
        .protocols
        .iter()
        .find(|protocol| protocol.protocol == "vmess")
    {
        assert_eq!(vmess.status, "supported");
        assert_eq!(vmess.inbound.tcp.level, "supported");
        assert_eq!(vmess.inbound.udp.level, "supported");
        assert_eq!(vmess.outbound.tcp.level, "supported");
        assert_eq!(vmess.outbound.udp.level, "supported");
        assert_eq!(vmess.mux.level, "supported");
        assert!(vmess.limitations.is_empty());
        assert!(!vmess
            .limitations
            .contains(&"vmess_none_zero_cipher_is_not_supported".to_owned()));
        assert!(!vmess
            .limitations
            .contains(&"vmess_mux_udp_is_not_supported".to_owned()));
        assert!(!vmess
            .limitations
            .contains(&"vmess_udp_is_not_implemented".to_owned()));
    }

    if let Some(trojan) = capabilities
        .protocols
        .iter()
        .find(|protocol| protocol.protocol == "trojan")
    {
        assert_eq!(trojan.status, "supported");
        assert_eq!(trojan.inbound.tcp.level, "supported");
        assert_eq!(trojan.inbound.udp.level, "supported");
        assert_eq!(trojan.outbound.tcp.level, "supported");
        assert_eq!(trojan.outbound.udp.level, "supported");
        assert_eq!(trojan.mux.level, "supported");
        assert!(trojan.limitations.is_empty());
    }

    if let Some(hysteria2) = capabilities
        .protocols
        .iter()
        .find(|protocol| protocol.protocol == "hysteria2")
    {
        assert!(!hysteria2
            .limitations
            .contains(&"udp_relay_chain_quic_path_not_supported".to_owned()));
        assert!(!hysteria2
            .limitations
            .contains(&"udp_relay_chain_is_not_supported".to_owned()));
    }

    let json = serde_json::to_value(&capabilities).expect("serialize capabilities");
    assert_snake_case_capability_strings(&json);
}

fn assert_snake_case_capability_strings(value: &serde_json::Value) {
    match value {
        serde_json::Value::String(s) => {
            assert!(
                !s.contains('-') && !s.contains(' '),
                "capability string should be snake_case-like: {s}"
            );
        }
        serde_json::Value::Array(values) => {
            for value in values {
                assert_snake_case_capability_strings(value);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                assert_snake_case_capability_strings(value);
            }
        }
        _ => {}
    }
}

#[test]
fn engine_query_service_reports_flow_lookup_and_filter_boundaries() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("parse config");

    let engine = Engine::new(config).expect("build engine");

    let missing = engine
        .query(QueryRequest::Flow(FlowGetQuery {
            flow_id: "missing".to_owned(),
        }))
        .expect_err("missing flow should fail");
    assert_eq!(missing.code, ApiErrorCode::NotFound);

    let filtered = engine
        .query(QueryRequest::ActiveFlows(FlowListQuery {
            limit: None,
            filter: FlowFilter {
                principal_key: Some("user:10001".to_owned()),
                ..FlowFilter::default()
            },
        }))
        .expect("principal filter should be supported");
    let QueryResponse::ActiveFlows(filtered) = filtered else {
        panic!("expected active flows query response");
    };
    assert!(filtered.is_empty());
}
