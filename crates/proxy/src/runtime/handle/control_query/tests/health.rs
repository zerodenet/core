//! The original cold device rows, old wire shape and same real response book.
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use zero_api::{QueryRequest, QueryService};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};
#[test]
fn actual_health_peer_rows_are_paid_before_allocation_and_labels_stay_in_original_config() {
    let public = STANDARD.encode(PublicKey::from(&StaticSecret::from([2; 32])).as_bytes());
    let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[{"tag":"same-雪-original","protocol":{
            "type":"wireguard","private_key":STANDARD.encode([1;32]),
            "addresses":["10.0.0.1/32"],"mtu":1420,
            "peers":[{"public_key":public,"endpoint":"127.0.0.1:51820","allowed_ips":["0.0.0.0/0"]}]
        }}],"route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let proxy = crate::Proxy::new(config).unwrap();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy,
    );
    let request = QueryRequest::Health(Default::default());
    let expected = serde_json::to_value(handle.query(request).unwrap()).unwrap();
    let mut limits = ControlBudgetLimits::from_runtime(&Default::default());
    limits.messages.response_bytes_total = std::mem::size_of::<HealthRow>() - 1;
    let book = ControlBudget::new(limits);
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    assert!(handle.control_health_query(&mut workspace).is_err());
    assert_eq!(book.usage().response_bytes, 0);
    drop(workspace);
    limits.messages.response_bytes_total = 4096;
    book.prepare_limits(limits).unwrap().commit().unwrap();
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let view = handle.control_health_query(&mut workspace).unwrap();
    assert_eq!(view.rows.len(), 1);
    assert!(
        view.rows[0].facts.tag.is_empty(),
        "the frozen row never clones its config label"
    );
    assert_eq!(
        book.usage().response_bytes,
        std::mem::size_of::<HealthRow>()
    );
    assert_eq!(serde_json::to_value(&view).unwrap(), expected);
    assert_eq!(
        serde_json::to_value(view.body()).unwrap(),
        expected["health"]
    );
    drop(view);
    assert_eq!(
        book.usage().response_bytes,
        std::mem::size_of::<HealthRow>()
    );
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
    book.close_admission();
    book.confirm_released().unwrap();
}
