use zero_config::{EventSinkConfig, RuntimeConfig};

fn config(api: serde_json::Value) -> String {
    serde_json::json!({
        "inbounds": [],
        "outbounds": [],
        "route": {"rules": [], "final": {"type": "direct"}},
        "api": api
    })
    .to_string()
}

#[test]
fn webhook_uses_a_complete_url_and_opaque_headers() {
    let runtime = RuntimeConfig::parse(&config(serde_json::json!({
        "event_sinks": [{
            "type": "webhook",
            "tag": "receiver",
            "url": "https://receiver.example/custom/intake?tenant=west",
            "events": ["flow.completed"],
            "source_id": "edge-west",
            "headers": {
                "authorization": "Custom opaque-value",
                "x-tenant": "west"
            }
        }]
    })))
    .expect("generic webhook config");

    let EventSinkConfig::Webhook { url, headers, .. } = &runtime.api.event_sinks[0] else {
        panic!("expected webhook");
    };
    assert_eq!(url, "https://receiver.example/custom/intake?tenant=west");
    assert_eq!(
        headers.get("authorization").map(String::as_str),
        Some("Custom opaque-value")
    );
}

#[test]
fn webhook_registrations_can_share_an_address_and_split_event_capabilities() {
    let shared_url = "https://receiver.example/events";
    let runtime = RuntimeConfig::parse(&config(serde_json::json!({
        "event_sinks": [
            {
                "type": "webhook",
                "tag": "traffic-delivery",
                "url": shared_url,
                "events": ["flow.completed"]
            },
            {
                "type": "webhook",
                "tag": "operations-delivery",
                "url": shared_url,
                "events": ["engine.warning"]
            }
        ]
    })))
    .expect("multiple webhook registrations");

    assert_eq!(runtime.api.event_sinks.len(), 2);
    let urls = runtime
        .api
        .event_sinks
        .iter()
        .map(|sink| match sink {
            EventSinkConfig::Webhook { url, .. } => url.as_str(),
            EventSinkConfig::JsonLines { .. } => panic!("expected webhook"),
        })
        .collect::<Vec<_>>();
    assert_eq!(urls, [shared_url, shared_url]);
    assert_eq!(
        runtime.api.event_sinks[0].events(),
        ["flow.completed".to_owned()]
    );
    assert_eq!(
        runtime.api.event_sinks[1].events(),
        ["engine.warning".to_owned()]
    );
}

#[test]
fn removed_push_workflow_is_not_accepted_as_configuration() {
    let raw = serde_json::json!({
        "inbounds": [],
        "outbounds": [],
        "route": {"rules": [], "final": {"type": "direct"}},
        "push": {
            "url": "https://central.example",
            "node_id": "node-17"
        }
    })
    .to_string();

    let error = RuntimeConfig::parse(&raw).expect_err("removed push must remain rejected");
    assert!(
        error.to_string().contains("unknown field `push`"),
        "{error}"
    );
}

#[test]
fn webhook_header_names_and_values_must_not_be_empty() {
    for headers in [
        serde_json::json!({"": "value"}),
        serde_json::json!({"x-empty": ""}),
    ] {
        let error = RuntimeConfig::parse(&config(serde_json::json!({
            "event_sinks": [{
                "type": "webhook",
                "tag": "receiver",
                "url": "https://central.example/events",
                "headers": headers
            }]
        })))
        .expect_err("empty header part must fail");
        assert!(error.to_string().contains("must not be empty"), "{error}");
    }
}
