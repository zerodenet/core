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
fn ipc_endpoint_original_nested_wire_id_and_error_preserve_real_output_custody() {
    let (handle, book) = endpoint_handle();
    for request in [
        QueryRequest::Endpoints(Default::default()),
        QueryRequest::Endpoint(zero_api::EndpointGetQuery {
            endpoint_id: "legacy:inbound:wg/a".into(),
        }),
        QueryRequest::EndpointDetails(zero_api::EndpointGetQuery {
            endpoint_id: "legacy:inbound:wg/a".into(),
        }),
    ] {
        assert!(handles(&request));
        let id = Some(serde_json::json!({"same":[17,"雪"]}));
        let mut expected = serde_json::to_value(ApiResponse::ok_with_id(
            id.clone(),
            handle.query(request.clone()).unwrap(),
        ))
        .unwrap();
        let body = prepare(&handle, &request, id, &book).unwrap();
        let actual: serde_json::Value = serde_json::from_slice(body.bytes()).unwrap();
        times(&mut expected, &actual);
        assert_eq!(actual, expected);
        assert_eq!(book.usage().response_bytes, body.bytes().len());
        assert!(
            handle.query(request).is_ok(),
            "encoded output no longer holds original Engine/source guards"
        );
        drop(body);
        assert_eq!(book.usage().response_bytes, 0);
    }
    let request = QueryRequest::Endpoint(zero_api::EndpointGetQuery {
        endpoint_id: "missing-雪".into(),
    });
    let original = prepare(&handle, &request, None, &book).unwrap();
    let body: serde_json::Value = serde_json::from_slice(original.bytes()).unwrap();
    assert_eq!(body["error"]["code"], "not_found");
    assert_eq!(
        body["error"]["message"],
        "endpoint `missing-雪` was not found"
    );
    drop(original);
    let mut limits = book.limits();
    limits.messages.response_bytes_total = 1;
    book.prepare(limits).unwrap().commit().unwrap();
    let request = QueryRequest::EndpointDetails(zero_api::EndpointGetQuery {
        endpoint_id: "legacy:inbound:wg/a".into(),
    });
    let error = prepare(&handle, &request, None, &book).err().unwrap();
    assert!(input::is_invalid_input_frame(&error));
    assert_eq!(book.usage().response_bytes, 0);
    assert!(handle.query(request).is_ok());
}
