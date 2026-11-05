use super::*;
fn instance(id: u64) -> ComponentInstanceId {
    ComponentInstanceId {
        component: zero_core::component::ComponentId(id),
        generation: zero_core::component::ResourceGeneration(1),
    }
}
#[test]
fn original_legacy_admission_debt_is_sticky_and_prevents_node_promotion() {
    let state = State::default();
    state.enter_legacy().unwrap();
    assert!(state.check_node_candidate(true, false).is_err());
    assert!(state.claim_node_streams(instance(1)).is_err());
    // Read-only queries and another permitted legacy open cannot clear debt.
    assert!(state.check_node_candidate(false, true).is_ok());
    state.enter_legacy().unwrap();
    assert!(state.claim_node_streams(instance(2)).is_err());
}
#[test]
fn unused_native_admissions_share_actual_registry_and_block_legacy_atomically() {
    let state = State::default();
    state.claim_node_streams(instance(1)).unwrap();
    state.claim_node_streams(instance(2)).unwrap();
    assert!(state.enter_legacy().is_err());
    assert!(state.check_node_candidate(false, true).is_err());
    // This is pure admission: no grant, I/O, driver or view was ever acquired.
    // Production release is exercised through actual Owner shutdown wire tests.
    state.release_node_streams(instance(1));
    assert!(state.enter_legacy().is_err());
    state.release_node_streams(instance(2));
    assert!(state.check_node_candidate(false, true).is_ok());
    state.enter_legacy().unwrap();
    assert!(state.claim_node_streams(instance(3)).is_err());
}
