use super::*;
fn book() -> Arc<ControlBudget> {
    ControlBudget::new(
        zero_runtime::configuration::ControlBudgetLimits::from_runtime(&Default::default()),
    )
}
fn expected(raw: &str) -> Result<Value, serde_json::Error> {
    serde_json::from_str::<crate::ipc::protocol::IpcRequest>(raw)
        .map(|r| serde_json::to_value(r).unwrap())
}
#[test]
fn borrowed_envelope_keeps_the_actual_old_serde_wire_and_rejection_semantics() {
    for raw in [
        r#"{"type":"ping","id":null,"params":1,"params":2}"#,
        r#"{"id":{"b":[1,2],"a":"é"},"type":"ping"}"#,
        r#"{"request":{"health":{}},"type":"query"}"#,
        r#"{"type":"query","request":{"z_unknown":{"x":[1,2]},"a_unknown":null}}"#,
        r#"{"type":"command","method":"fakeip.clear","params":null}"#,
        r#"{"params":{},"method":"fakeip.clear","type":"command"}"#,
        r#"{"type":"subscribe","events":["flow.closed","stats.updated"]}"#,
        r#"{"events":null,"type":"subscribe"}"#,
        r#"{"type":"subscribe"}"#,
        r#"{"type":"ping","unused":1e999}"#,
        r#"{"type":"command","method":"fakeip.clear"}"#,
        r#"{"type":"ping","id":1,"id":2}"#,
        r#"{"type":"query","request":{},"request":{}}"#,
        r#"{"type":{"ping":null}}"#,
        r#"{"type":"ping","id":{"$serde_json::private::RawValue":"[1,{\"a\":2}]"}}"#,
        r#"{"type":"ping","id":{"$serde_json::private::RawValue":"["}}"#,
    ] {
        let old = expected(raw);
        let book = book();
        let decoded = decode_ipc(raw, &book);
        match (old, decoded) {
            (Ok(old), Ok(actual)) => {
                assert_eq!(serde_json::to_value(&*actual).unwrap(), old, "{raw}")
            }
            (Err(_), Err(DecodeError::Invalid { .. })) => {}
            (old, actual) => panic!("changed original IPC result for {raw}: {old:?} / {actual:?}"),
        }
        assert_eq!(book.usage().request_bytes, 0);
    }
}
#[test]
fn container_metadata_is_prepaid_before_owned_tree_and_held_until_its_owner_returns() {
    let raw = r#"{"type":"ping","id":{"tiny":[]}}"#;
    let book = book();
    let actual = decode_ipc(raw, &book).unwrap();
    let used = book.usage().request_bytes;
    assert!(used > raw.len() + std::mem::size_of::<Value>());
    let mut limits = book.limits();
    limits.messages.request_bytes_total = used - 1;
    assert!(book.prepare(limits).is_err());
    drop(actual);
    assert_eq!(book.usage().request_bytes, 0);
    limits.messages.request_bytes_total = 64;
    book.prepare(limits).unwrap().commit().unwrap();
    assert!(
        matches!(decode_ipc(raw,&book),Err(DecodeError::Capacity(ref e)) if e.kind()==io::ErrorKind::WouldBlock)
    );
    assert_eq!(book.usage().request_bytes, 0);
}
#[test]
fn query_preflight_preserves_real_value_magic_and_scope_serde_result() {
    for raw in [
        r#"{"traffic_stat":{"scope":{"kind":"global"}}}"#,
        r#"{"traffic_stats":{"scopes":[{"kind":"global"},{"kind":"global"}]}}"#,
        r#"{"unknown":{"$serde_json::private::RawValue":"[1,2]"}}"#,
        r#"{"$serde_json::private::RawValue":"{\"health\":{}}"}"#,
        r#"{"$serde_json::private::RawValue":"bad"}"#,
    ] {
        let book = book();
        let old = serde_json::from_str::<zero_api::QueryRequest>(raw);
        let new = decode_query(raw.as_bytes(), &book);
        match (old, new) {
            (Ok(a), Ok(b)) => assert_eq!(
                serde_json::to_value(a).unwrap(),
                serde_json::to_value(&*b).unwrap()
            ),
            (Err(a), Err(DecodeError::Invalid { source, .. })) => {
                assert_eq!(a.to_string(), source.to_string())
            }
            (old, new) => panic!("different original query result {raw}: {old:?} / {new:?}"),
        }
        assert_eq!(book.usage().request_bytes, 0);
    }
}

#[test]
fn actual_command_value_projection_is_paid_before_the_original_typed_conversion() {
    let book = book();
    for raw in [
        r#"{"method":"stats.reset","params":{"expected_core_instance_id":"actual","targets":[{"scope":{"kind":"global"},"expected_stats_epoch":"epoch"},{"scope":{"kind":"inbound","tag":"x"},"expected_stats_epoch":"epoch"}]}}"#,
        r#"{"params":{"include_cidrs":["10.0.0.0/8","fd00::/8"]},"method":"tun.start"}"#,
        r#"{"params":{"config":{"$serde_json::private::RawValue":"{\"runtime\":{}}"}},"method":"config.validate"}"#,
    ] {
        let _workspace = workspace(raw.as_bytes(), &book).unwrap();
        let bytes = body::bytes(raw.as_bytes(), &book, false).unwrap();
        let _held = book.reserve(ControlCharge::RequestBytes(bytes)).unwrap();
        let result = serde_json::from_str::<zero_api::CommandRequest>(raw);
        assert!(result.is_ok(), "original command oracle: {result:?}");
        assert!(book.usage().request_bytes > raw.len());
        drop(result);
    }
    assert_eq!(book.usage().request_bytes, 0);
}
