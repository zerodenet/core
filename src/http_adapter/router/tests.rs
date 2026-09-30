use serde_json::{json, Value};
use zero_api::{AuthContext, Permission};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

use super::{route, HttpRequest, RouteResult};

fn handle() -> ProxyHandle {
    let config = json!({"inbounds":[{"tag":"wg/a","listen":{"address":"127.0.0.1","port":51820},
        "protocol":{"type":"wireguard","private_key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
            "peers":[{"public_key":"AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=","allowed_ips":["10.0.0.0/24"]}]}}],
        "route":{"rules":[],"final":{"type":"direct"}}});
    let proxy = Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap();
    ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy)
}

async fn get(handle: &ProxyHandle, path: &str, permissions: Vec<Permission>) -> (String, Value) {
    let result = route(
        &HttpRequest {
            method: "GET".into(),
            path: path.into(),
            headers: vec![],
            body: vec![],
        },
        handle,
        &AuthContext {
            subject: None,
            permissions,
        },
    )
    .await;
    let RouteResult::Respond(status, body) = result else {
        panic!("expected JSON response");
    };
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn endpoint_http_queries_preserve_encoded_resource_identity_and_detail_schema() {
    let handle = handle();
    let (_, list) = get(&handle, "/api/v1/endpoints?limit=1", vec![Permission::Read]).await;
    assert_eq!(list["result"]["total"], 1);
    let (_, endpoint) = get(
        &handle,
        "/api/v1/endpoints/legacy%3Ainbound%3Awg%2Fa",
        vec![Permission::Read],
    )
    .await;
    assert_eq!(endpoint["result"]["endpoint_id"], "legacy:inbound:wg/a");
    assert_eq!(endpoint["result"]["supported"]["packet"], true);
    let (_, details) = get(
        &handle,
        "/api/v1/endpoints/legacy%3Ainbound%3Awg%2Fa/details",
        vec![Permission::Read],
    )
    .await;
    assert_eq!(details["result"]["schema_id"], "zero.endpoint.wireguard.v1");
}

#[tokio::test]
async fn endpoint_http_queries_enforce_read_permission_and_return_structured_errors() {
    let handle = handle();
    let (status, denied) = get(&handle, "/api/v1/endpoints", vec![Permission::Control]).await;
    assert!(status.contains("403"));
    assert_eq!(denied["error"]["code"], "permission_denied");
    for (path, code) in [
        ("/api/v1/endpoints?offset=invalid", "invalid_argument"),
        ("/api/v1/endpoints/missing", "not_found"),
        ("/api/v1/endpoints/%FF", "invalid_argument"),
    ] {
        let (_, error) = get(&handle, path, vec![Permission::Read]).await;
        assert_eq!(error["ok"], false);
        assert_eq!(error["error"]["code"], code);
    }
}

#[tokio::test]
async fn endpoint_http_control_accepts_instance_condition_and_returns_conflict_without_startup() {
    let handle = handle();
    let (_, observed) = get(
        &handle,
        "/api/v1/endpoints/legacy%3Ainbound%3Awg%2Fa",
        vec![Permission::Read],
    )
    .await;
    let request = HttpRequest {
        method: "POST".into(),
        path: "/api/v1/commands".into(),
        headers: vec![],
        body: serde_json::to_vec(&json!({"method":"endpoints.set_state","params":{
            "endpoint_id":observed["result"]["endpoint_id"], "enabled":false,
            "expected_core_instance_id":"stale-instance",
            "expected_intent_revision":observed["result"]["intent_revision"]}}))
        .unwrap(),
    };
    for (permissions, expected_status, expected_code) in [
        (vec![Permission::Read], "403", "permission_denied"),
        (vec![Permission::Admin], "409", "conflict"),
    ] {
        let RouteResult::Respond(status, body) = route(
            &request,
            &handle,
            &AuthContext {
                subject: None,
                permissions,
            },
        )
        .await
        else {
            panic!("wrong response")
        };
        let error: Value = serde_json::from_slice(&body).unwrap();
        assert!(status.contains(expected_status));
        assert_eq!(error["error"]["code"], expected_code);
        if expected_code == "conflict" {
            assert_eq!(
                error["error"]["field_path"],
                "params.expected_core_instance_id"
            );
        }
    }
    let (_, after) = get(
        &handle,
        "/api/v1/endpoints/legacy%3Ainbound%3Awg%2Fa",
        vec![Permission::Read],
    )
    .await;
    assert_eq!(after["result"]["enabled"], observed["result"]["enabled"]);
    assert_eq!(
        after["result"]["intent_revision"],
        observed["result"]["intent_revision"]
    );
}
