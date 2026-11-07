use super::*;
#[test]
fn tun_status_ipc_uses_same_proxy_owner_and_keeps_nested_wire_and_output_credit() {
    let handle = handle();
    let book = ControlBudget::new(ControlBudgetLimits::from_runtime(
        &zero_config::RuntimeOptionsConfig::default(),
    ));
    let request = QueryRequest::TunStatus(zero_api::TunStatusQuery);
    assert!(handles(&request));
    let id = Some(serde_json::json!({"same":[7,"雪"]}));
    let expected = serde_json::to_value(ApiResponse::ok_with_id(
        id.clone(),
        handle.query(request.clone()).unwrap(),
    ))
    .unwrap();
    let original = prepare(&handle, &request, id, &book).unwrap();
    let actual = serde_json::from_slice::<serde_json::Value>(original.bytes()).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(book.usage().response_bytes, original.bytes().len());
    // The same actual status owner lock has returned before native write custody.
    assert!(handle.query(request).is_ok());
    drop(original);
    assert_eq!(book.usage().response_bytes, 0);
}
#[test]
fn tun_status_capacity_rejection_is_local_and_releases_same_actual_proxy_guards() {
    let handle = handle();
    let mut limits =
        ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    limits.messages.response_bytes_total = 1;
    let book = ControlBudget::new(limits);
    let request = QueryRequest::TunStatus(zero_api::TunStatusQuery);
    let error = prepare(&handle, &request, None, &book).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(input::is_invalid_input_frame(&error));
    assert_eq!(
        error
            .get_ref()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(book.usage().response_bytes, 0);
    assert!(handle.query(request).is_ok());
}
