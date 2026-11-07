//! Authoritative principal membership/revision comes from the same registry.
use super::*;
use zero_api::QueryService;
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};
#[test]
fn actual_principal_index_is_sorted_borrowed_and_releases_all_workspace_before_next_transition() {
    let engine = Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let mut ids = Vec::new();
    for principal in ["z-雪", "a-\"principal", "z-雪"] {
        let mut session = zero_core::Session::new(
            0,
            zero_core::Address::Domain("example.invalid".into()),
            443,
            zero_core::Network::Tcp,
            zero_core::ProtocolType::new("vless"),
        );
        let mut auth = zero_core::SessionAuth::new("vless");
        auth.principal_key = Some(principal.into());
        session.auth = Some(auth);
        engine.prepare_session(&mut session, "source").unwrap();
        ids.push(session.id);
    }
    let request = QueryRequest::PrincipalFlows(Default::default());
    let expected = serde_json::to_value(engine.query(request.clone()).unwrap()).unwrap();
    let limits = ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    let book = ControlBudget::new(limits);
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = engine.control_runtime_query(&request).unwrap();
        let view = guard.freeze(&mut workspace).unwrap().unwrap();
        let actual = serde_json::to_value(&view).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            actual["principal_flows"]["principals"][0]["principal_key"],
            "a-\"principal"
        );
        assert_eq!(
            actual["principal_flows"]["principals"][1]["active_flows"],
            2
        );
        assert!(
            book.usage().response_bytes > 0,
            "actual metadata vector was prepaid"
        );
    }
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
    let mut tiny_limits = limits;
    tiny_limits.messages.response_bytes_total = 1;
    let tiny = ControlBudget::new(tiny_limits);
    let mut tiny_workspace = tiny.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = engine.control_runtime_query(&request).unwrap();
        assert_eq!(
            guard.freeze(&mut tiny_workspace).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
    drop(tiny_workspace);
    assert_eq!(tiny.usage().response_bytes, 0);
    engine
        .finish_session(ids[0], crate::SessionOutcome::DirectRelayed)
        .unwrap();
    let expected = serde_json::to_value(engine.query(request.clone()).unwrap()).unwrap();
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let guard = engine.control_runtime_query(&request).unwrap();
    let view = guard.freeze(&mut workspace).unwrap().unwrap();
    assert_eq!(serde_json::to_value(&view).unwrap(), expected);
}
