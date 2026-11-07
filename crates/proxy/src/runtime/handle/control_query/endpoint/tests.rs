//! Same actual cold owner, legacy wire, page/counter/error semantics and credit.
use super::*;
use zero_api::{QueryRequest, QueryService, TrafficMetric, TrafficPlane, TrafficScope};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};
fn fixture() -> ProxyHandle {
    let proxy=crate::Proxy::new(zero_config::RuntimeConfig::parse(&serde_json::json!({
        "endpoints":[{"tag":"same-雪","directions":{"inbound":true,"outbound":true},"listen":{"host":"127.0.0.1","port":0},
            "protocol":{"type":"wireguard","private_key":"0101010101010101010101010101010101010101010101010101010101010101","addresses":["10.0.0.1/32"],
                "peers":[{"public_key":"de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f","endpoint":"127.0.0.1:51820","allowed_ips":["0.0.0.0/0"]}]}}],
        "inbounds":[{"tag":"legacy-雪","listen":{"host":"127.0.0.2","port":0},"protocol":{"type":"wireguard","private_key":"0101010101010101010101010101010101010101010101010101010101010101","addresses":["10.0.1.1/32"],"peers":[{"public_key":"de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f","allowed_ips":["10.0.1.0/24"]}]}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()).unwrap();
    ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy,
    )
}
fn times(expected: &mut serde_json::Value, actual: &serde_json::Value) {
    match (expected, actual) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            for (k, v) in a {
                if k == "observed_at_unix_ms" {
                    *v = b[k].clone();
                } else if let Some(actual) = b.get(k) {
                    times(v, actual);
                }
            }
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            for (a, b) in a.iter_mut().zip(b) {
                times(a, b);
            }
        }
        _ => {}
    }
}
#[test]
fn all_canonical_legacy_directions_details_and_page_wire_match_genuine_owned_queries() {
    let handle = fixture();
    let book = ControlBudget::new(ControlBudgetLimits::from_runtime(&Default::default()));
    for request in [
        QueryRequest::Endpoints(Default::default()),
        QueryRequest::Endpoints(zero_api::EndpointListQuery {
            offset: 1,
            limit: Some(1),
        }),
        QueryRequest::Endpoint(zero_api::EndpointGetQuery {
            endpoint_id: "endpoint:same-雪".into(),
        }),
        QueryRequest::EndpointDetails(zero_api::EndpointGetQuery {
            endpoint_id: "endpoint:same-雪".into(),
        }),
        QueryRequest::EndpointDetails(zero_api::EndpointGetQuery {
            endpoint_id: "legacy:inbound:legacy-雪".into(),
        }),
    ] {
        let mut expected = serde_json::to_value(handle.query(request.clone()).unwrap()).unwrap();
        let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
        {
            let guard = handle.control_endpoint_query(&request).unwrap().unwrap();
            let view = guard.freeze(&mut workspace).unwrap();
            assert!(book.usage().response_bytes > 0);
            let actual = serde_json::to_value(&view).unwrap();
            times(&mut expected, &actual);
            assert_eq!(actual, expected);
            assert_eq!(
                serde_json::to_value(view.body()).unwrap(),
                actual.as_object().unwrap().values().next().unwrap().clone()
            );
            if matches!(request, QueryRequest::EndpointDetails(_)) {
                let peer = &actual["endpoint_details"]["details"]["peers"][0];
                assert!(peer["peer_id"].as_str().unwrap().starts_with("wireguard:"));
                assert!(peer.get("private_key").is_none());
                assert!(peer.get("preshared_key").is_none());
                let facts = &view.observations[0].as_ref().unwrap().facts.wireguard;
                assert!(
                    facts
                        .peers
                        .iter()
                        .filter_map(|peer| peer.health.as_ref())
                        .all(|health| health.tag.is_empty()),
                    "labels stay in the original config"
                );
            }
        }
        drop(workspace);
        assert_eq!(book.usage().response_bytes, 0);
    }
}
#[test]
fn peer_and_endpoint_storage_refusal_holds_no_guard_or_native_authority_and_can_recover() {
    let handle = fixture();
    let mut limits = ControlBudgetLimits::from_runtime(&Default::default());
    limits.messages.response_bytes_total = 1;
    let book = ControlBudget::new(limits);
    let request = QueryRequest::EndpointDetails(zero_api::EndpointGetQuery {
        endpoint_id: "endpoint:same-雪".into(),
    });
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = handle.control_endpoint_query(&request).unwrap().unwrap();
        assert_eq!(
            guard.freeze(&mut workspace).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
    }
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
    assert_eq!(
        handle
            .proxy
            .host
            .original_budgets()
            .carrier
            .snapshot()
            .charged,
        0,
        "metadata queries never open an endpoint"
    );
    limits.messages.response_bytes_total = 1024 * 1024;
    book.prepare(limits).unwrap().commit().unwrap();
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = handle.control_endpoint_query(&request).unwrap().unwrap();
        let view = guard.freeze(&mut workspace).unwrap();
        assert_eq!(
            view.observations[0]
                .as_ref()
                .unwrap()
                .facts
                .wireguard
                .peers
                .len(),
            1
        );
    }
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
    let missing = QueryRequest::EndpointDetails(zero_api::EndpointGetQuery {
        endpoint_id: "missing-雪".into(),
    });
    let error = handle
        .control_endpoint_query(&missing)
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), zero_api::ApiErrorCode::NotFound);
    assert_eq!(
        error.message().to_string(),
        "endpoint `missing-雪` was not found"
    );
}
#[test]
fn original_endpoint_counter_captures_do_not_change_when_owning_meter_advances() {
    let handle = fixture();
    let engine = handle.engine_handle().inner();
    let id = "endpoint:same-雪";
    let meter = engine
        .traffic_meter(&TrafficScope::Endpoint {
            endpoint_id: id.into(),
        })
        .unwrap();
    meter.enable(TrafficPlane::Outer, &[TrafficMetric::TxBytes]);
    meter.sent(TrafficPlane::Outer, 19);
    let book = ControlBudget::new(ControlBudgetLimits::from_runtime(&Default::default()));
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let request = QueryRequest::Endpoint(zero_api::EndpointGetQuery {
        endpoint_id: id.into(),
    });
    {
        let guard = handle.control_endpoint_query(&request).unwrap().unwrap();
        let view = guard.freeze(&mut workspace).unwrap();
        let first = serde_json::to_value(&view).unwrap();
        assert_eq!(first["endpoint"]["counters"]["outer_tx_bytes"], 19);
        meter.sent(TrafficPlane::Outer, 23);
        assert_eq!(serde_json::to_value(&view).unwrap(), first);
    }
    drop(workspace);
    assert_eq!(
        engine
            .endpoint_snapshot(&zero_api::EndpointGetQuery {
                endpoint_id: id.into()
            })
            .unwrap()
            .counters
            .outer_tx_bytes,
        Some(42)
    );
}
