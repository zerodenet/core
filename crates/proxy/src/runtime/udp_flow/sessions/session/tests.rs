//! Actual Source backing survives pipe/frame/flow ownership independently.
use super::*;
use crate::runtime::pipe::UdpPipeInput;
use crate::runtime::udp_flow::sessions::model::{Identity, Lookup, UdpFlowKey};
use std::collections::HashMap;
use std::sync::Arc;
use zero_core::{InboundUdpDispatch, Network, ProtocolType, TargetHostSource};
use zero_traits::packet_metadata::PacketMetadataLayout;
use zero_tun::packet_memory::PacketReturnTotals;

fn packet(memory: &zero_tun::packet_memory::PacketDriverMetadataView) -> InboundUdpDispatch {
    let original = InboundUdpDispatch::new(
        ProtocolType::UNKNOWN,
        Address::Ipv4([127, 0, 0, 1]),
        443,
        vec![1],
        None,
    );
    let name = "actual.source.test";
    let credit = memory
        .reserve(
            original
                .sniffed_domain_metadata_layout(name.len(), false)
                .unwrap(),
        )
        .unwrap();
    original
        .with_sniffed_domain(name.into(), TargetHostSource::QuicSni, false)
        .with_domain_metadata(credit)
        .unwrap_or_else(|_| panic!("same real prepaid name"))
}
fn name_pointer(session: &Session) -> *const u8 {
    let Address::Domain(name) = &session.target else {
        panic!("actual name")
    };
    name.as_ptr()
}
#[test]
fn real_pipe_and_source_session_keep_each_copy_until_the_actual_last_snapshot() {
    let root = Arc::new(PacketReturnTotals::default());
    let memory = root.driver_metadata_view();
    let baseline = memory.snapshot().occupied;
    let packet = packet(&memory);
    let mut input = UdpPipeInput::from_inbound_dispatch_with_metadata(
        &packet,
        None,
        None,
        Some(memory.clone()),
    )
    .unwrap();
    let mut session = PreparedUdpSession::new(
        Session::new(7, input.target, 443, Network::Udp, ProtocolType::UNKNOWN),
        input.domain_memory.take(),
    )
    .unwrap();
    session.sniffed_sni(false).unwrap();
    let session = session.into_stored();
    let snapshot = session.clone();
    assert_eq!(
        name_pointer(&session),
        name_pointer(&snapshot),
        "snapshot shares actual immutable Source Session"
    );
    assert_eq!(session.sni.as_deref(), Some("actual.source.test"));
    drop(packet);
    drop(session);
    assert_ne!(memory.snapshot().occupied, baseline);
    assert_eq!(
        snapshot.target,
        Address::Domain("actual.source.test".into())
    );
    drop(snapshot);
    assert_eq!(memory.snapshot().occupied, baseline);
}
#[test]
fn paid_key_clones_and_existing_flow_lookup_need_no_new_domain_under_root_pressure() {
    let root = Arc::new(PacketReturnTotals::default());
    let memory = root.driver_metadata_view();
    let baseline = memory.snapshot().occupied;
    let address = Address::Domain("actual.source.test".into());
    let key = UdpFlowKey::new_with_metadata(&address, 443, Some(7), Some(&memory)).unwrap();
    let cloned = key.clone();
    let occupied = memory.snapshot().occupied;
    let mut map = HashMap::new();
    map.insert(key, 17u64);
    let limit = memory.snapshot().limit;
    let full = memory
        .reserve(PacketMetadataLayout::new(
            limit.allocations() - occupied.allocations(),
            limit.bytes() - occupied.bytes(),
        ))
        .unwrap();
    let before = memory.snapshot().occupied;
    let lookup = Lookup {
        target: &address,
        port: 443,
        client: Some(7),
        peer: None,
    };
    assert_eq!(map.get(&lookup as &dyn Identity), Some(&17));
    assert_eq!(memory.snapshot().occupied, before);
    drop(full);
    drop(map);
    assert_eq!(
        memory.snapshot().occupied,
        occupied,
        "actual cloned key still owns original name"
    );
    drop(cloned);
    assert_eq!(memory.snapshot().occupied, baseline);
}
#[test]
fn foreign_source_pipe_rejection_preserves_original_name_and_never_charges_the_other_root() {
    let actual = Arc::new(PacketReturnTotals::default());
    let foreign = Arc::new(PacketReturnTotals::default());
    let packet = packet(&actual.driver_metadata_view());
    let original = actual.driver_metadata().occupied;
    let foreign_baseline = foreign.driver_metadata().occupied;
    assert!(UdpPipeInput::from_inbound_dispatch_with_metadata(
        &packet,
        None,
        None,
        Some(foreign.driver_metadata_view())
    )
    .is_err());
    assert_eq!(actual.driver_metadata().occupied, original);
    assert_eq!(foreign.driver_metadata().occupied, foreign_baseline);
    assert_eq!(
        packet.target(),
        &Address::Domain("actual.source.test".into())
    );
    drop(packet);
}
