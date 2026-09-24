use zero_config::RuntimeConfig;
use zero_engine::{ResolvedLeafOutbound, ResolvedOutbound};
use zero_stack::packet::{IPPROTO_ICMP, IPPROTO_TCP};

use super::{PacketRouteTarget, ProtocolInventory};

fn config() -> RuntimeConfig {
    RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{"tag": "direct", "protocol": {"type": "direct"}}],
            "route": {"rules": [], "final": {"type": "direct"}}
        }"#,
    )
    .unwrap()
}

#[test]
fn automatic_path_keeps_outbound_candidate_order() {
    let config = config();
    let inventory = ProtocolInventory::default();
    let blocked = inventory.prepare_packet_route_target(
        &config,
        ResolvedOutbound::Fallback {
            candidates: vec![
                ResolvedLeafOutbound::Block { tag: None },
                ResolvedLeafOutbound::Direct { tag: None },
            ],
        },
        Some(IPPROTO_ICMP),
    );
    let mut candidates = blocked.into_candidates().into_iter();
    assert!(matches!(candidates.next(), Some(PacketRouteTarget::Block)));
    assert!(matches!(
        candidates.next(),
        Some(PacketRouteTarget::DirectEcho)
    ));

    let direct = inventory.prepare_packet_route_target(
        &config,
        ResolvedOutbound::Single(ResolvedLeafOutbound::Direct { tag: None }),
        Some(IPPROTO_ICMP),
    );
    assert!(matches!(direct, PacketRouteTarget::DirectEcho));

    let flow = inventory.prepare_packet_route_target(
        &config,
        ResolvedOutbound::Single(ResolvedLeafOutbound::Direct { tag: None }),
        Some(IPPROTO_TCP),
    );
    assert!(matches!(flow, PacketRouteTarget::Flow));
}
