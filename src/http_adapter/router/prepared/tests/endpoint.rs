use super::*;
fn endpoint_handle() -> (ProxyHandle, Arc<ControlBudget>) {
    let config=zero_config::RuntimeConfig::parse(r#"{"inbounds":[{"tag":"wg/a","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"wireguard","private_key":"0101010101010101010101010101010101010101010101010101010101010101","addresses":["10.0.0.1/32"],"peers":[{"public_key":"de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f","allowed_ips":["10.0.0.0/24"]}]}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = zero_proxy::Proxy::new(config).unwrap();
    let book = proxy.application_control_budget();
    (
        ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy,
        ),
        book,
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
fn endpoint_http_get_encoded_identity_details_and_typed_wire_match_the_original_query() {
    let (handle, book) = endpoint_handle();
    for selected in [
        Selected::Endpoints("limit=1"),
        Selected::Endpoint("legacy%3Ainbound%3Awg%2Fa", false),
        Selected::Endpoint("legacy%3Ainbound%3Awg%2Fa", true),
    ] {
        let request = match selected {
            Selected::Endpoints(query) => handlers::endpoint_list_request(query).unwrap(),
            Selected::Endpoint(id, details) => handlers::endpoint_get_request(id, details).unwrap(),
            _ => unreachable!(),
        };
        let expected = handlers::serialize_query(handle.query(request.clone())).unwrap();
        let mut expected: serde_json::Value =
            serde_json::from_slice(&expected.bytes_for_test()).unwrap();
        let body = prepare(&handle, selected, &book).unwrap();
        let actual: serde_json::Value = serde_json::from_slice(&body.bytes_for_test()).unwrap();
        times(&mut expected, &actual);
        assert_eq!(actual, expected);
        assert_eq!(book.usage().response_bytes, body.len().unwrap());
        assert_eq!(book.usage().request_bytes, 0);
        drop(body);
        assert_eq!(book.usage().response_bytes, 0);
        let input = serde_json::to_vec(&request).unwrap();
        let body = prepare(&handle, Selected::Typed(&input), &book).unwrap();
        let actual: serde_json::Value = serde_json::from_slice(&body.bytes_for_test()).unwrap();
        times(&mut expected, &actual);
        assert_eq!(actual, expected);
        drop(body);
    }
    for (selected, message) in [
        (
            Selected::Endpoints("offset=bad"),
            "invalid endpoint pagination `offset`",
        ),
        (
            Selected::Endpoint("%FF", false),
            "endpoint ID is not valid UTF-8",
        ),
        (
            Selected::Endpoint("missing-%E9%9B%AA", false),
            "endpoint `missing-雪` was not found",
        ),
    ] {
        let body = prepare(&handle, selected, &book).unwrap();
        let actual: serde_json::Value = serde_json::from_slice(&body.bytes_for_test()).unwrap();
        assert_eq!(actual["ok"], false);
        assert_eq!(actual["error"]["message"], message);
        drop(body);
    }
    assert_eq!(book.usage().response_bytes, 0);
    assert_eq!(book.usage().request_bytes, 0);
}
