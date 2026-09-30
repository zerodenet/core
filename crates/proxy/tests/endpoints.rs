#![cfg(feature = "wireguard")]

use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use serde_json::json;
use zero_api::{EndpointGetQuery, QueryRequest, QueryResponse, QueryService};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

fn handle(linked: bool) -> ProxyHandle {
    let config = json!({"endpoints":[{"tag":"a", "listen":linked.then(||json!({"address":"127.0.0.1","port":51820})),
        "protocol":{"type":"wireguard", "private_key":STANDARD.encode([21;32]),
            "addresses":["10.0.0.1/32"], "peers":[{
                "public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([22;32])).as_bytes()),
                "endpoint":"127.0.0.1:51821", "allowed_ips":["10.0.0.0/24"]}]}}],
        "route":{"rules":[], "final":{"type":"direct"}}});
    let proxy = Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap();
    ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy)
}

#[test]
fn shared_wireguard_endpoint_catalog_is_one_resource_and_never_leaks_keys() {
    let handle = handle(true);
    let QueryResponse::Endpoints(list) = handle
        .query(QueryRequest::Endpoints(Default::default()))
        .unwrap()
    else {
        panic!("wrong response");
    };
    assert_eq!(list.total, 1);
    let endpoint = &list.endpoints[0];
    assert_eq!(endpoint.inbound_tags, ["endpoint/a"]);
    assert_eq!(endpoint.outbound_tags, ["a"]);
    assert_eq!(endpoint.state, zero_api::EndpointRuntimeState::Stopped);
    assert!(!endpoint.effective.outbound);
    assert!(endpoint.supported.packet);
    assert!(endpoint.supported.directions.inbound);
    assert!(endpoint.generation.is_none());
    assert!(endpoint.supported.operations.contains(&"set_state".into()));
    let QueryResponse::EndpointDetails(details) = handle
        .query(QueryRequest::EndpointDetails(EndpointGetQuery {
            endpoint_id: endpoint.endpoint_id.clone(),
        }))
        .unwrap()
    else {
        panic!("wrong details");
    };
    assert_eq!(details.schema_id, "zero.endpoint.wireguard.v1");
    assert_eq!(details.core_instance_id, endpoint.core_instance_id);
    assert_eq!(details.config_revision, endpoint.config_revision);
    assert!(details.observed_at_unix_ms > 0);
    assert_eq!(details.details["peers"].as_array().unwrap().len(), 1);
    assert!(details.details["peers"][0]["peer_id"]
        .as_str()
        .unwrap()
        .starts_with("wireguard:"));
    assert!(details.details["peers"][0]["source_known"].is_null());
    assert!(details.details["peers"][0]["authenticated_endpoint"].is_null());
    let serialized = serde_json::to_string(&details).unwrap();
    assert!(!serialized.contains("private_key"));
    assert!(!serialized.contains(&STANDARD.encode([21; 32])));
    assert!(!serialized.contains("pre_shared_key"));
}

#[test]
fn outbound_only_catalog_declares_no_unbound_inbound_permission() {
    let handle = handle(false);
    let QueryResponse::Endpoint(endpoint) = handle
        .query(QueryRequest::Endpoint(EndpointGetQuery {
            endpoint_id: "endpoint:a".into(),
        }))
        .unwrap()
    else {
        panic!("wrong response");
    };
    assert!(!endpoint.supported.directions.inbound);
    assert!(endpoint.supported.directions.outbound);
    assert!(endpoint.inbound_tags.is_empty());
}
