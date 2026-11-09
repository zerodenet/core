//! Genuine same-Host metadata custody, never fabricated IO/Scope proof.
use super::*;
#[test]
fn original_wait_failure_has_one_paid_cause_and_held_diagnostic_releases_after_its_weak_header() {
    let proxy = crate::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let original = proxy.protocols.runtime_host().original_budgets();
    let memory = original.packet.driver_metadata_view();
    let before = memory.snapshot().occupied;
    let (failure, retained) = prepare_failure(&memory).unwrap();
    assert!(retained.is_prepaid());
    let occupied = memory.snapshot().occupied;
    assert!(occupied.bytes() > before.bytes() && occupied.allocations() > before.allocations());
    let directory = tempfile::tempdir().unwrap();
    let actual = Arc::new(std::fs::File::open(directory.path().join("missing")).unwrap_err());
    let native_weak = Arc::downgrade(&actual);
    assert!(failure
        .0
        .set(
            ChannelError::new(Stage::Close, Reason::Revoked, Impact::Component)
                .with_source(actual.clone())
        )
        .is_ok());
    let weak = retained.downgrade();
    for _ in 0..20 {
        assert!(failure
            .0
            .set(ChannelError::new(
                Stage::Close,
                Reason::Revoked,
                Impact::Component
            ))
            .is_err());
        assert!(retained.ptr_eq(&retained.clone()));
        assert_eq!(
            memory.snapshot().occupied,
            occupied,
            "repeated original observations mint no metadata history"
        );
    }
    drop(actual);
    drop(failure);
    assert!(native_weak.upgrade().is_some());
    drop(retained);
    assert!(
        native_weak.upgrade().is_none(),
        "the pure diagnostic body owns no root/controller cycle"
    );
    assert!(weak.upgrade().is_none());
    assert!(
        memory.snapshot().occupied.bytes() > before.bytes(),
        "the real exported weak header still occupies its paid layout"
    );
    drop(weak);
    assert_eq!(
        memory.snapshot().occupied,
        before,
        "actual held metadata header lifetime, not just row len"
    );
}
