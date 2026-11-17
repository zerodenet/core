//! Cold support lookup and kernel Block facts; execution comes from Host roles.
use super::fixtures::{compiled_in_outbound_leaves, outbound_leaf_name};
#[test]
fn compiled_outbound_variants_have_one_actual_protocol_owner_without_runtime_construction() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = crate::register::protocol_registry();
    for (config, leaf, expected) in compiled_in_outbound_leaves() {
        let name = outbound_leaf_name(&config, &leaf);
        let entries = registry
            .entries
            .iter()
            .filter(|entry| entry.support.name() == name)
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), expected, "{name}");
        if let zero_engine::ResolvedLeafOutbound::Proxy { identity } = leaf {
            if let Some(entry) = entries.first() {
                assert!(entry
                    .support
                    .supports_outbound(&config.outbounds[identity.config_index()].protocol));
            }
        }
    }
    assert!(tokio::runtime::Handle::try_current().is_err());
}
#[test]
fn block_uses_kernel_route_facts_without_an_adapter_or_native_resource() {
    use crate::inventory::{PreparedTcpCandidateExecution, PreparedTcpOutbound};
    let registry = crate::register::protocol_registry();
    assert!(registry
        .entries
        .iter()
        .all(|entry| entry.support.name() != "block"));
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let inventory = crate::inventory::ProtocolInventory::default();
    let resolved =
        zero_engine::ResolvedOutbound::Single(zero_engine::ResolvedLeafOutbound::Block {
            tag: Some("blocked"),
        });
    let prepared = inventory
        .prepare_tcp_outbound(
            crate::protocol_registry::OutboundAdapterContext::new(&config),
            &resolved,
            zero_engine::RouteMode::Auto,
            None,
        )
        .unwrap_or_else(|failure| panic!("{}", failure.error));
    let PreparedTcpOutbound::Single(candidate) = prepared else {
        panic!("Block is a single kernel route fact")
    };
    assert!(
        matches!(candidate.execution,PreparedTcpCandidateExecution::Block{tag} if tag=="blocked")
    );
    assert!(candidate.health_tag.is_none());
    assert!(candidate.endpoint.is_none());
}
