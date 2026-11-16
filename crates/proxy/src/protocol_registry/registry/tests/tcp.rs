//! Cold registry projection cannot reconstruct retired standard executors.
use super::fixtures::{compiled_in_outbound_leaves, outbound_leaf_name};
#[test]
fn standard_tcp_protocols_require_their_actual_factory_and_provider() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = crate::register::protocol_registry();
    for (config, leaf, _) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        if protocol == "block" {
            assert!(!registry
                .claim_outbound_leaf(&config, leaf)
                .unwrap()
                .has_tcp_capability());
            continue;
        }
        assert!(
            registry.claim_outbound_leaf(&config, leaf).is_err(),
            "{protocol} restored a retired compatibility projection"
        );
    }
}
