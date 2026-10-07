use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use zero_config::RuntimeConfig;

fn endpoint(tag: &str) -> Value {
    json!({"tag":tag, "listen":{"address":"127.0.0.1", "port":51820},
        "directions":{"inbound":true,"outbound":true}, "protocol":{
            "type":"wireguard", "private_key":STANDARD.encode([1;32]), "addresses":["10.0.0.1/32"],
            "peers":[{"public_key":STANDARD.encode([2;32]), "endpoint":"127.0.0.1:51821", "allowed_ips":["10.0.0.0/24"]}]}})
}

fn input() -> Value {
    json!({"endpoints":[endpoint("a")], "route":{"rules":[], "final":{"type":"route", "outbound":"a"}}})
}

#[test]
fn canonical_endpoint_materializes_one_linked_resource_and_exports_once() {
    let mut config = RuntimeConfig::parse(&input().to_string()).unwrap();
    assert_eq!(config.inbounds.len(), 1);
    assert_eq!(config.outbounds.len(), 1);
    let zero_config::InboundProtocolConfig::Wireguard { addresses, .. } =
        &config.inbounds[0].protocol
    else {
        panic!("wrong inbound projection")
    };
    assert_eq!(addresses, &["10.0.0.1/32"]);
    config.materialize_endpoints().unwrap();
    assert_eq!(config.inbounds.len(), 1);
    assert_eq!(config.outbounds.len(), 1);
    let bindings = config.endpoint_bindings();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].endpoint_id, "endpoint:a");
    assert_eq!(bindings[0].inbound_tags, ["endpoint/a"]);
    assert_eq!(bindings[0].outbound_tags, ["a"]);
    let encoded = serde_json::to_value(&config).unwrap();
    assert_eq!(encoded["inbounds"], json!([]));
    assert_eq!(encoded["outbounds"], json!([]));
    assert_eq!(RuntimeConfig::parse(&encoded.to_string()).unwrap(), config);
}

#[test]
fn legacy_roles_have_one_identity_only_when_explicitly_linked() {
    let config = RuntimeConfig::parse(&input().to_string()).unwrap();
    let mut legacy = serde_json::to_value(&config).unwrap();
    legacy["endpoints"] = json!([]);
    legacy["inbounds"] = serde_json::to_value(&config.inbounds).unwrap();
    legacy["outbounds"] = serde_json::to_value(&config.outbounds).unwrap();
    let linked = RuntimeConfig::parse(&legacy.to_string())
        .unwrap()
        .endpoint_bindings();
    assert_eq!(linked.len(), 1);
    assert_eq!(linked[0].endpoint_id, "legacy:inbound:endpoint/a");
    legacy["outbounds"][0]["protocol"]["inbound_tag"] = Value::Null;
    assert_eq!(
        RuntimeConfig::parse(&legacy.to_string())
            .unwrap()
            .endpoint_bindings()
            .len(),
        2
    );
}

#[test]
fn endpoint_collisions_and_unsupported_directions_fail_before_runtime() {
    let mut value = input();
    value["outbounds"] = json!([{"tag":"a", "protocol":{"type":"direct"}}]);
    assert!(RuntimeConfig::parse(&value.to_string()).is_err());
    value = input();
    value["outbound_groups"] = json!([{"tag":"a","type":"selector","outbounds":["a"]}]);
    assert!(RuntimeConfig::parse(&value.to_string()).is_err());
    value = input();
    value["endpoints"][0]["listen"] = Value::Null;
    assert!(RuntimeConfig::parse(&value.to_string())
        .unwrap_err()
        .to_string()
        .contains("requires a listen binding"));
}

#[test]
fn disabled_endpoint_keeps_valid_references_and_explicit_direction_intent() {
    let mut value = input();
    value["endpoints"][0]["enabled"] = json!(false);
    let config = RuntimeConfig::parse(&value.to_string()).unwrap();
    let bindings = config.endpoint_bindings();
    assert!(!bindings[0].enabled);
    assert_eq!(bindings[0].outbound_tags, ["a"]);
    value["endpoints"][0]["enabled"] = json!(true);
    value["endpoints"][0]["directions"]["inbound"] = json!(false);
    let config = RuntimeConfig::parse(&value.to_string()).unwrap();
    let bindings = config.endpoint_bindings();
    assert!(bindings[0].enabled);
    assert!(!bindings[0].directions.inbound);
    assert!(bindings[0].directions.outbound);
}

#[test]
fn listening_endpoint_accepts_a_passive_peer_without_inventing_an_address() {
    let mut value = input();
    value["endpoints"][0]["protocol"]["peers"][0]
        .as_object_mut()
        .unwrap()
        .remove("endpoint");
    value["endpoints"][0]["directions"]["outbound"] = json!(false);
    let config = RuntimeConfig::parse(&value.to_string()).unwrap();
    let exported = serde_json::to_value(&config).unwrap();
    assert!(exported["endpoints"][0]["protocol"]["peers"][0]
        .get("endpoint")
        .is_none());
    assert_eq!(RuntimeConfig::parse(&exported.to_string()).unwrap(), config);
    value["endpoints"][0]["listen"] = Value::Null;
    value["endpoints"][0]["directions"] = json!({"inbound":false,"outbound":true});
    assert!(RuntimeConfig::parse(&value.to_string())
        .unwrap_err()
        .to_string()
        .contains("endpoint is invalid"));
}

#[test]
fn direct_packet_host_binding_is_explicit_and_cannot_reuse_the_ingress_tun() {
    let valid = serde_json::json!({"runtime":{"network":{"direct_packet_device":{"fd":9,"interface":"host-l3","router_addresses":["10.64.0.1"]}}},"route":{"rules":[],"final":{"type":"direct"}}});
    let parsed = RuntimeConfig::parse(&valid.to_string()).unwrap();
    assert_eq!(parsed.runtime.network.direct_packet_device.unwrap().fd, 9);
    let mut invalid = valid.clone();
    invalid["runtime"]["network"]["direct_packet_device"]["fd"] = 0.into();
    assert!(RuntimeConfig::parse(&invalid.to_string()).is_err());
    invalid = valid.clone();
    invalid["runtime"]["network"]["direct_packet_device"]["interface"] = "".into();
    assert!(RuntimeConfig::parse(&invalid.to_string()).is_err());
    invalid = valid;
    invalid["runtime"]["tun"] = serde_json::json!({"name":"host-l3","addr":"10.0.0.1"});
    assert!(RuntimeConfig::parse(&invalid.to_string()).is_err());
}
