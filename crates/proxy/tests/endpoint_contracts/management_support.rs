use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use serde_json::json;
use zero_api::{
    CommandRequest, EndpointGetQuery, EndpointPersistence, EndpointSetStateCommand,
    EndpointSnapshot, QueryRequest, QueryResponse, QueryService,
};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

pub fn config(a: u16, b: u16) -> RuntimeConfig {
    let endpoint = |tag, seed: u8, port, address| {
        json!({
        "tag":tag, "directions":{"inbound":true,"outbound":true},
        "listen":{"address":"127.0.0.1","port":port},
            "protocol":{"type":"wireguard","private_key":STANDARD.encode([seed;32]),
                "addresses":[address], "peers":[{
                    "public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([seed+1;32])).as_bytes()),
                    "endpoint":"127.0.0.1:9","allowed_ips":["10.0.0.0/24"]}]}
        })
    };
    RuntimeConfig::parse(
        &json!({
            "endpoints":[endpoint("a",61,a,"10.0.0.1/32"),endpoint("b",63,b,"10.0.0.2/32")],
            "route":{"rules":[],"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap()
}

pub fn handle(proxy: &Proxy) -> ProxyHandle {
    ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
}

pub fn endpoint(handle: &ProxyHandle, tag: &str) -> EndpointSnapshot {
    let QueryResponse::Endpoint(endpoint) = handle
        .query(QueryRequest::Endpoint(EndpointGetQuery {
            endpoint_id: format!("endpoint:{tag}"),
        }))
        .unwrap()
    else {
        panic!("wrong endpoint response")
    };
    endpoint
}

pub fn set_state(tag: &str, enabled: bool) -> CommandRequest {
    CommandRequest::EndpointSetState(EndpointSetStateCommand {
        endpoint_id: format!("endpoint:{tag}"),
        enabled,
        persistence: EndpointPersistence::RuntimeOnly,
        expected_intent_revision: None,
        expected_core_instance_id: None,
    })
}

pub async fn ready(handle: &ProxyHandle) {
    super::support::wait_for("both endpoints running", || {
        ["a", "b"]
            .into_iter()
            .all(|tag| endpoint(handle, tag).state == zero_api::EndpointRuntimeState::Running)
    })
    .await;
}
