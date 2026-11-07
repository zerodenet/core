//! The actual compiled registry, original build facts and production wire DTO.
use super::*;
use zero_api::{QueryRequest, QueryService};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};
#[test]
fn actual_compiled_capabilities_need_prepaid_workspace_and_preserve_every_discovery_field() {
    let proxy = crate::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy,
    );
    let expected = serde_json::to_value(
        handle
            .query(QueryRequest::Capabilities(Default::default()))
            .unwrap(),
    )
    .unwrap();
    let mut limits = ControlBudgetLimits::from_runtime(&Default::default());
    limits.messages.response_bytes_total = 1;
    let book = ControlBudget::new(limits);
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    assert!(handle.control_capabilities_query(&mut workspace).is_err());
    assert_eq!(book.usage().response_bytes, 0);
    drop(workspace);
    limits.messages.response_bytes_total = 128 * 1024;
    book.prepare_limits(limits).unwrap().commit().unwrap();
    let held = book
        .reserve(ControlCharge::ResponseBytes(128 * 1024 - 1))
        .unwrap();
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    assert!(handle.control_capabilities_query(&mut workspace).is_err());
    assert_eq!(book.usage().response_bytes, 128 * 1024 - 1);
    drop((workspace, held));
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let view = handle.control_capabilities_query(&mut workspace).unwrap();
    assert_eq!(serde_json::to_value(&view).unwrap(), expected);
    assert_eq!(
        serde_json::to_value(view.body()).unwrap(),
        expected["capabilities"]
    );
    let actual_storage = view.protocols.capacity()
        * std::mem::size_of::<ProtocolCapabilityDescriptor>()
        + (view.features.capacity() + view.limitations.capacity())
            * std::mem::size_of::<&'static str>();
    assert_eq!(book.usage().response_bytes, actual_storage);
    assert!(view
        .protocols
        .iter()
        .any(|original| original.protocol == "block"));
    drop(view);
    assert_eq!(book.usage().response_bytes, actual_storage);
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
    book.close_admission();
    book.confirm_released().unwrap();
}
