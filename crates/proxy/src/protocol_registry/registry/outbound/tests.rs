//! Direct and endpointless virtual leaves are not executable claims.
#[test]
fn direct_has_no_tcp_udp_or_packet_compatibility_entry() {
    let config=zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"named","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
    let inventory = crate::inventory::ProtocolInventory::default();
    for tag in [None, Some("named")] {
        assert!(inventory
            .claim_outbound_leaf(
                &config,
                zero_engine::ResolvedLeafOutbound::Direct {
                    tag,
                    dial_policy: Default::default(),
                    dial_generation: 0,
                }
            )
            .is_err());
    }
}
