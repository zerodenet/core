use super::*;
#[test]
fn typed_traffic_http_uses_original_flat_wire_and_releases_period_before_holding_output() {
    let (handle, book) = fixture();
    let query = QueryRequest::TrafficStats(zero_api::TrafficListQuery {
        scopes: vec![
            zero_api::TrafficScope::Global,
            zero_api::TrafficScope::Global,
        ],
        ..Default::default()
    });
    let input = serde_json::to_vec(&query).unwrap();
    let original = prepare(&handle, Selected::Typed(&input), &book).unwrap();
    let actual = serde_json::from_slice::<serde_json::Value>(&original.bytes_for_test()).unwrap();
    assert!(
        actual["result"]["traffic_stats"].is_null(),
        "HTTP body remains flat"
    );
    assert_eq!(actual["result"]["scopes"].as_array().unwrap().len(), 1);
    assert_eq!(actual["result"]["scopes"][0]["scope"]["kind"], "global");
    assert_eq!(book.usage().response_bytes, original.len().unwrap());
    let expected = handlers::serialize_query(handle.query(query)).unwrap();
    let mut expected =
        serde_json::from_slice::<serde_json::Value>(&expected.bytes_for_test()).unwrap();
    fn times(a: &mut serde_json::Value, b: &serde_json::Value) {
        match (a, b) {
            (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
                for (k, v) in a {
                    if [
                        "sampled_at_unix_ms",
                        "capture_started_at_unix_ms",
                        "sampled_at_monotonic_ns",
                    ]
                    .contains(&k.as_str())
                    {
                        *v = b[k].clone()
                    } else if let Some(b) = b.get(k) {
                        times(v, b)
                    }
                }
            }
            (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
                for (a, b) in a.iter_mut().zip(b) {
                    times(a, b)
                }
            }
            _ => {}
        }
    }
    times(&mut expected, &actual);
    assert_eq!(expected, actual);
    drop(original);
    assert_eq!(book.usage().response_bytes, 0);
    assert_eq!(book.usage().request_bytes, 0);
}
