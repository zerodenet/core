use super::*;
use std::sync::Arc;
use zero_core::ProtocolType;
use zero_tun::packet_memory::PacketReturnTotals;
#[test]
fn actual_domain_key_pays_exact_owned_bytes_without_cloning_on_lookup_or_transfer() {
    let root = Arc::new(PacketReturnTotals::default());
    let before = root.driver_metadata().occupied;
    let dispatch = InboundUdpDispatch::new(
        ProtocolType::UNKNOWN,
        Address::Domain("Mixed.actual.example".into()),
        443,
        Vec::new(),
        Some(7),
    );
    let key = FlowKey::try_from_dispatch(&dispatch, Some(&root.driver_metadata_view())).unwrap();
    assert!(key.matches(&dispatch));
    assert_eq!(
        root.driver_metadata().occupied,
        before
            .checked_add(PacketMetadataLayout::new(1, 20))
            .unwrap()
    );
    let Address::Domain(name) = &key.target else {
        panic!("same exact domain key");
    };
    assert_eq!(name.capacity(), 20);
    assert_ne!(
        name.as_ptr(),
        match dispatch.target() {
            Address::Domain(name) => name.as_ptr(),
            _ => unreachable!(),
        }
    );
    let same = key;
    assert!(same.matches(&dispatch));
    drop(same);
    assert_eq!(root.driver_metadata().occupied, before);
}
