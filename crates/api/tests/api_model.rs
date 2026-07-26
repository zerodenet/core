use std::collections::BTreeMap;

use serde_json::json;
use zero_api::{
    event_type, ApiErrorCode, ApiEvent, AuthContext, AuthInfo, CommandRequest, ConfigApplyCommand,
    ConfigValidateCommand, EndpointRef, FlowEventPayload, FlowOutcome, FlowTiming, Network,
    Permission, PolicySelectCommand, RouteDecision, TargetAddress, TrafficStats, EVENT_SCHEMA_ID,
};

#[test]
fn command_permissions_follow_cqrs_boundaries() {
    let config = CommandRequest::ConfigValidate(ConfigValidateCommand {
        config: json!({ "inbounds": [] }),
    });
    let select = CommandRequest::PolicySelect(PolicySelectCommand {
        policy_tag: "proxy".to_owned(),
        target_tag: "direct".to_owned(),
    });

    assert_eq!(config.required_permission(), Permission::Config);
    assert_eq!(select.required_permission(), Permission::Control);
}

#[test]
fn runtime_config_apply_is_an_explicit_non_persistent_command() {
    let command = CommandRequest::ConfigApplyRuntime(ConfigApplyCommand {
        config: json!({ "inbounds": [] }),
    });

    assert_eq!(command.required_permission(), Permission::Config);
    let value = serde_json::to_value(command).expect("serialize runtime config apply");
    assert_eq!(value["method"], "config.apply_runtime");
}

#[test]
fn command_request_serializes_with_stable_method_name() {
    let command = CommandRequest::PolicySelect(PolicySelectCommand {
        policy_tag: "proxy".to_owned(),
        target_tag: "direct".to_owned(),
    });

    let value = serde_json::to_value(command).expect("serialize command");

    assert_eq!(value["method"], "policies.select");
    assert_eq!(value["params"]["policy_tag"], "proxy");
    assert_eq!(value["params"]["target_tag"], "direct");
}

#[test]
fn api_error_codes_serialize_as_snake_case() {
    let value = serde_json::to_value(ApiErrorCode::PermissionDenied).expect("serialize");

    assert_eq!(value, "permission_denied");
    assert_eq!(
        ApiErrorCode::FeatureDisabled.as_code_str(),
        "feature_disabled"
    );
}

#[test]
fn admin_auth_context_implies_all_permissions() {
    let context = AuthContext {
        subject: Some("admin".to_owned()),
        permissions: vec![Permission::Admin],
    };

    assert!(context.allows(Permission::Read));
    assert!(context.allows(Permission::Control));
    assert!(context.allows(Permission::Config));
}

#[test]
fn flow_completed_event_serializes_as_normalized_envelope() {
    let mut auth = AuthInfo::new("vless");
    auth.principal_key = Some("user:10003".to_owned());
    auth.attributes
        .insert("uuid_hash".to_owned(), "sha256:31cd...e920".to_owned());

    let payload = FlowEventPayload {
        flow_id: "flow-010011".to_owned(),
        network: Network::Udp,
        inbound: EndpointRef {
            tag: "vless-in".to_owned(),
            protocol: "vless".to_owned(),
        },
        auth: Some(auth),
        target: TargetAddress {
            host: "8.8.8.8".to_owned(),
            port: 53,
        },
        route: RouteDecision {
            mode: "rule".to_owned(),
            target: Some("proxy".to_owned()),
        },
        policy: None,
        outbound: Some(EndpointRef {
            tag: "node-b".to_owned(),
            protocol: "socks5".to_owned(),
        }),
        traffic: TrafficStats {
            bytes_up: 3200,
            bytes_down: 8800,
            packets_up: Some(12),
            packets_down: Some(12),
            ..TrafficStats::default()
        },
        timing: FlowTiming {
            started_at_unix_ms: 1_760_000_020_000,
            ended_at_unix_ms: Some(1_760_000_025_120),
            duration_ms: Some(5120),
        },
        outcome: FlowOutcome::ChainedRelayed,
        close_reason: None,
        record: None,
    };

    let mut event = ApiEvent::new(
        "01JZVLESS0000000000000001",
        event_type::FLOW_COMPLETED,
        1_760_000_025_123,
        payload,
    );
    event.source_id = Some("edge-us-01".to_owned());
    event.sequence = Some(41002);
    event.principal_key = Some("user:10003".to_owned());
    event.labels = BTreeMap::from([("tenant".to_owned(), "main".to_owned())]);

    let value = serde_json::to_value(event).expect("serialize event");

    assert_eq!(value["schema_id"], EVENT_SCHEMA_ID);
    assert_eq!(value["event_type"], "flow.completed");
    assert_eq!(value["principal_key"], "user:10003");
    assert_eq!(value["payload"]["network"], "udp");
    assert_eq!(value["payload"]["traffic"]["bytes_down"], 8800);
    assert_eq!(value["payload"]["outcome"], "chained_relayed");
}

#[test]
fn event_type_catalog_lists_current_api_events() {
    assert_eq!(
        event_type::ALL,
        [
            event_type::FLOW_STARTED,
            event_type::FLOW_ROUTED,
            event_type::FLOW_UPDATED,
            event_type::FLOW_COMPLETED,
            event_type::FLOW_SNAPSHOT,
            event_type::POLICY_SELECTED,
            event_type::POLICY_PROBE_COMPLETED,
            event_type::POLICY_PASSIVE_RELAY_HEALTH_CHANGED,
            event_type::STATS_SAMPLED,
            event_type::CONFIG_CHANGED,
            event_type::ENGINE_STARTED,
            event_type::ENGINE_STOPPED,
            event_type::ENGINE_WARNING,
            event_type::IPC_CONNECTED,
            event_type::IPC_DISCONNECTED,
        ]
    );
    assert!(event_type::is_known("flow.completed"));
    assert!(!event_type::is_known("panel.user.changed"));
}

#[test]
fn passive_relay_health_event_payload_has_stable_state_names() {
    let payload = zero_api::PassiveRelayHealthChangedPayload {
        policy_tag: "auto".to_owned(),
        member_tag: "primary".to_owned(),
        target: "landing.test".to_owned(),
        port: 14788,
        state: zero_api::PassiveRelayHealthState::HalfOpen,
        quarantine_duration_ms: None,
    };
    let value = serde_json::to_value(payload).expect("serialize passive health payload");
    assert_eq!(value["state"], "half_open");
    let decoded: zero_api::PassiveRelayHealthChangedPayload =
        serde_json::from_value(value).expect("deserialize passive health payload");
    assert_eq!(decoded.state, zero_api::PassiveRelayHealthState::HalfOpen);
}
