use std::fs;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use zero_config::RuntimeConfig;
use zero_engine::Engine;

#[test]
fn engine_builds_router_from_config() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [],
            "route": {
                "rules": [
                    {
                        "condition": {
                            "type": "domain",
                            "values": ["blocked.example"]
                        },
                        "action": { "type": "reject" }
                    }
                ],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    let engine = Engine::new(config).expect("engine should build");
    let action = engine.route_for(&zero_core::Address::Domain("blocked.example".to_owned()));

    assert!(matches!(action, zero_router::RouteAction::Reject));
}

#[test]
fn direct_mode_overrides_router_decision() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [],
            "mode": { "type": "direct" },
            "route": {
                "rules": [
                    {
                        "condition": {
                            "type": "domain",
                            "values": ["blocked.example"]
                        },
                        "action": { "type": "reject" }
                    }
                ],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    let engine = Engine::new(config).expect("engine should build");
    let action = engine.route_for(&zero_core::Address::Domain("blocked.example".to_owned()));

    assert!(matches!(action, zero_router::RouteAction::Direct));
}

#[test]
fn global_mode_routes_through_selector_group_tag() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [
                {
                    "tag": "direct",
                    "protocol": { "type": "direct" }
                }
            ],
            "outbound_groups": [
                {
                    "tag": "proxy",
                    "type": "selector",
                    "outbounds": ["direct"]
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
    .expect("config should parse");

    let engine = Engine::new(config).expect("engine should build");
    let action = engine.route_for(&zero_core::Address::Domain("example.com".to_owned()));

    assert!(matches!(
        action,
        zero_router::RouteAction::Route(ref tag) if tag == "proxy"
    ));
}

#[test]
fn rule_set_routes_domain_and_cidr_targets() {
    let project_dir = temp_test_dir("engine-rule-set-router");
    let domain_rules = project_dir.join("ads.txt");
    let cidr_rules = project_dir.join("lan.txt");
    fs::write(&domain_rules, "blocked.example\n.ads.local\n").expect("write domain rules");
    fs::write(&cidr_rules, "10.0.0.0/8\n").expect("write cidr rules");

    let config = RuntimeConfig::parse(&format!(
        r#"{{
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
            "rule_sets": {{
                    "ads": {{
                        "type": "file",
                        "path": "{}",
                        "format": "domain_list"
                    }},
                    "lan": {{
                        "type": "file",
                        "path": "{}",
                        "format": "cidr_list"
                    }}
            }},
            "route": {{
                "rules": [
                    {{
                        "condition": {{ "type": "rule_set", "tag": "ads" }},
                        "action": {{ "type": "route", "outbound": "block" }}
                    }},
                    {{
                        "condition": {{ "type": "rule_set", "tag": "lan" }},
                        "action": {{ "type": "route", "outbound": "direct" }}
                    }}
                ],
                "final": {{ "type": "direct" }}
            }}
        }}"#,
        escape_json_path(&domain_rules),
        escape_json_path(&cidr_rules),
    ))
    .expect("config should parse");

    let engine = Engine::new(config).expect("engine should build");

    let domain_action = engine.route_for(&zero_core::Address::Domain("api.ads.local".to_owned()));
    assert!(matches!(domain_action, zero_router::RouteAction::Route(ref tag) if tag == "block"));

    let ip_action = engine.route_for(&zero_core::Address::Ipv4([10, 1, 2, 3]));
    assert!(matches!(ip_action, zero_router::RouteAction::Route(ref tag) if tag == "direct"));

    cleanup_temp_dir(&project_dir);
}

#[test]
fn route_rules_can_match_inbound_tag_to_group() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "hk-in",
                    "listen": { "address": "127.0.0.1", "port": 7891 },
                    "protocol": { "type": "mixed" }
                },
                {
                    "tag": "jp-in",
                    "listen": { "address": "127.0.0.1", "port": 7892 },
                    "protocol": { "type": "mixed" }
                }
            ],
            "outbounds": [
                { "tag": "hk-a", "protocol": { "type": "direct" } },
                { "tag": "hk-b", "protocol": { "type": "direct" } },
                { "tag": "jp-a", "protocol": { "type": "direct" } },
                { "tag": "jp-b", "protocol": { "type": "direct" } }
            ],
            "outbound_groups": [
                {
                    "tag": "hk-lb",
                    "type": "load_balance",
                    "outbounds": ["hk-a", "hk-b"],
                    "strategy": "round_robin"
                },
                {
                    "tag": "jp-lb",
                    "type": "load_balance",
                    "outbounds": ["jp-a", "jp-b"],
                    "strategy": "round_robin"
                }
            ],
            "route": {
                "rules": [
                    {
                        "condition": { "type": "inbound", "values": ["hk-in"] },
                        "action": { "type": "route", "outbound": "hk-lb" }
                    },
                    {
                        "condition": { "type": "inbound", "values": ["jp-in"] },
                        "action": { "type": "route", "outbound": "jp-lb" }
                    }
                ],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    let engine = Engine::new(config).expect("engine should build");
    let address = zero_core::Address::Domain("example.com".to_owned());

    let hk_action = engine.route_decision_with_inbound(&address, None, Some("hk-in"));
    assert!(matches!(
        hk_action,
        zero_engine::RouteDecision::Route(ref tag) if tag == "hk-lb"
    ));

    let jp_action = engine.route_decision_with_inbound(&address, None, Some("jp-in"));
    assert!(matches!(
        jp_action,
        zero_engine::RouteDecision::Route(ref tag) if tag == "jp-lb"
    ));

    let default_action = engine.route_decision_with_inbound(&address, None, None);
    assert!(matches!(default_action, zero_engine::RouteDecision::Direct));
}

#[test]
fn zero_rule_ir_routes_mixed_domain_and_ip_matchers() {
    let project_dir = temp_test_dir("engine-zero-rule-ir-router");
    let matcher_path = project_dir.join("private.zero.json");
    let semantic = zero_rule::RuleSet::new(vec![
        zero_rule::Rule::DomainKeyword("telemetry".to_owned()),
        zero_rule::Rule::Ipv6Cidr("fd00::/8".parse().unwrap()),
    ]);
    fs::write(
        &matcher_path,
        zero_rule::protocol::encode_json(&semantic).expect("encode Zero Rule IR"),
    )
    .expect("write Zero Rule IR");

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "rule_sets": {{
                "private": {{
                    "type": "file",
                    "path": "{}",
                    "format": "zero_rule_ir"
                }}
            }},
            "route": {{
                "rules": [{{
                    "condition": {{ "type": "rule_set", "tag": "private" }},
                    "action": {{ "type": "reject" }}
                }}],
                "final": {{ "type": "direct" }}
            }}
        }}"#,
        escape_json_path(&matcher_path),
    ))
    .expect("config should parse");
    let engine = Engine::new(config).expect("engine should build");

    assert!(matches!(
        engine.route_for(&zero_core::Address::Domain(
            "api.Telemetry.example".to_owned()
        )),
        zero_router::RouteAction::Reject
    ));
    assert!(matches!(
        engine.route_for(&zero_core::Address::Ipv6(
            "fd12::1".parse::<std::net::Ipv6Addr>().unwrap().octets()
        )),
        zero_router::RouteAction::Reject
    ));
    assert!(matches!(
        engine.route_for(&zero_core::Address::Domain("example.com".to_owned())),
        zero_router::RouteAction::Direct
    ));

    cleanup_temp_dir(&project_dir);
}

#[test]
fn domain_route_trace_can_fall_through_to_resolved_ip() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "direct", "protocol": { "type": "direct" } },
                {
                    "tag": "proxy",
                    "protocol": {
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": 1080
                    }
                }
            ],
            "route": {
                "rules": [
                    {
                        "condition": {
                            "type": "ip",
                            "values": ["10.0.0.0/8"]
                        },
                        "action": { "type": "route", "outbound": "direct" }
                    }
                ],
                "final": { "type": "route", "outbound": "proxy" }
            }
        }"#,
    )
    .expect("config should parse");
    let engine = Engine::new(config).expect("engine should build");
    let target = zero_core::Address::Domain("domestic.example".to_owned());

    let unresolved = engine.route_trace_with_inbound(&target, None, None);
    assert!(matches!(
        unresolved.decision,
        zero_engine::RouteDecision::Route(ref tag) if tag == "proxy"
    ));
    assert!(unresolved.matched_rule.is_none());

    let resolved = engine.route_trace_with_inbound_and_resolved_ips(
        &target,
        None,
        None,
        &[IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))],
    );
    assert!(matches!(
        resolved.decision,
        zero_engine::RouteDecision::Route(ref tag) if tag == "direct"
    ));
    assert_eq!(resolved.matched_rule.expect("matched IP rule").index, 0);
    assert!(engine.route_requires_resolved_ip());
}

#[test]
fn reload_replaces_zrs_router_snapshot() {
    let project_dir = temp_test_dir("engine-zrs-reload");
    let first_path = project_dir.join("first.zrs");
    let second_path = project_dir.join("second.zrs");
    write_zrs(&first_path, "first.example");
    write_zrs(&second_path, "second.example");

    let first_config = zrs_config(&first_path);
    let engine = Engine::new(first_config).expect("engine should build first ZRS router");
    assert_route_rejected(&engine, "first.example");
    assert_route_direct(&engine, "second.example");

    engine
        .reload_config(zrs_config(&second_path))
        .expect("reload second ZRS router");
    assert_route_direct(&engine, "first.example");
    assert_route_rejected(&engine, "second.example");

    cleanup_temp_dir(&project_dir);
}

fn write_zrs(path: &Path, domain: &str) {
    let (compiled, _) = zero_rule::RuleSetCompiler
        .compile(zero_rule::RuleSet::new(vec![zero_rule::Rule::DomainExact(
            domain.to_owned(),
        )]))
        .expect("compile ZRS matcher");
    fs::write(path, zero_rule::zrs::encode(&compiled).expect("encode ZRS")).expect("write ZRS");
}

fn zrs_config(path: &Path) -> RuntimeConfig {
    RuntimeConfig::parse(&format!(
        r#"{{
            "rule_sets": {{
                "selected": {{
                    "type": "file",
                    "path": "{}",
                    "format": "zrs"
                }}
            }},
            "route": {{
                "rules": [{{
                    "condition": {{ "type": "rule_set", "tag": "selected" }},
                    "action": {{ "type": "reject" }}
                }}],
                "final": {{ "type": "direct" }}
            }}
        }}"#,
        escape_json_path(path),
    ))
    .expect("ZRS config should parse")
}

fn assert_route_rejected(engine: &Engine, domain: &str) {
    assert!(matches!(
        engine.route_for(&zero_core::Address::Domain(domain.to_owned())),
        zero_router::RouteAction::Reject
    ));
}

fn assert_route_direct(engine: &Engine, domain: &str) {
    assert!(matches!(
        engine.route_for(&zero_core::Address::Domain(domain.to_owned())),
        zero_router::RouteAction::Direct
    ));
}

fn temp_test_dir(prefix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("{prefix}-{nonce}"));
    fs::create_dir_all(&dir).expect("create temp test dir");
    dir
}

fn cleanup_temp_dir(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

fn escape_json_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "\\\\")
}
