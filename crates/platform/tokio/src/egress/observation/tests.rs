//! Pure metadata guard; this test claims no native or task retirement evidence.
use super::*;
#[test]
fn same_egress_read_observation_pins_original_family_epoch_without_cloning_reason() {
    let control = EgressInterfaceControl::default();
    control.replace_for(false, Some(EgressInterface::new("physical0", 7).unwrap()));
    control.mark_unavailable_for(true, "original-雪-reason");
    control.record_ipv6_to_ipv4_fallback();
    let original = control.read_observation();
    assert_eq!(original.interface(false).unwrap().name(), "physical0");
    assert_eq!(
        original.unavailable_reason(true),
        Some("original-雪-reason")
    );
    assert_eq!(original.ipv6_to_ipv4_fallbacks(), 1);
    let generation = original.generation();
    assert!(
        matches!(
            control.0.try_write(),
            Err(std::sync::TryLockError::WouldBlock)
        ),
        "same actual original read guard remains held"
    );
    drop(original);
    control.mark_unavailable_for(false, "replacement-reason");
    let next = control.read_observation();
    assert!(next.interface(false).is_none());
    assert_eq!(next.unavailable_reason(false), Some("replacement-reason"));
    assert!(next.generation() > generation);
}
