use super::*;
use crate::udp::{build_udp_fragments, parse_udp_datagram};
use zero_core::Address;

#[test]
fn udp_payload_budget_keeps_real_wire_fragments_charged_across_receive_suspension() {
    let wire = build_udp_fragments(1, &Address::Ipv4([203, 0, 113, 1]), 53, &[7; 48], 40).unwrap();
    assert!(wire.len() > 1);
    let payload_bytes = Arc::new(Semaphore::new(48));
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let mut assembly = Reassembly::default();
    let mut remaining = wire.into_iter();
    let first = remaining.next().unwrap();
    let first_bytes = parse_udp_datagram(&first).unwrap().payload().len();
    tx.try_send(Packet::reserve(parse_udp_datagram(&first).unwrap(), &payload_bytes).unwrap())
        .ok()
        .unwrap();
    assert!(assembly.push(rx.try_recv().unwrap()).is_none());
    assert_eq!(
        payload_bytes.available_permits(),
        48 - first_bytes,
        "dequeue and cancelled receive do not release incomplete real fragment payload"
    );
    let duplicate = Packet::reserve(parse_udp_datagram(&first).unwrap(), &payload_bytes).unwrap();
    assert!(assembly.push(duplicate).is_none());
    assert_eq!(
        payload_bytes.available_permits(),
        48 - first_bytes,
        "duplicate wire fragments retain no extra payload position"
    );
    let other = Hysteria2UdpPacket::new(2, 1, Address::Ipv4([203, 0, 113, 2]), 53, vec![0; 48]);
    assert!(
        Packet::reserve(other, &payload_bytes).is_none(),
        "a sibling logical association shares this same actual authentication's byte budget"
    );
    let mut completed = None;
    for wire in remaining {
        tx.try_send(Packet::reserve(parse_udp_datagram(&wire).unwrap(), &payload_bytes).unwrap())
            .ok()
            .unwrap();
        completed = assembly.push(rx.try_recv().unwrap());
    }
    assert_eq!(completed.unwrap().payload(), &[7; 48]);
    assert_eq!(
        payload_bytes.available_permits(),
        48,
        "only handoff of the completed payload releases all retained fragment bytes"
    );
    let whole = Hysteria2UdpPacket::new(2, 2, Address::Ipv4([203, 0, 113, 2]), 53, vec![0; 48]);
    tx.try_send(Packet::reserve(whole, &payload_bytes).unwrap())
        .ok()
        .unwrap();
    assert_eq!(payload_bytes.available_permits(), 0);
    drop(rx);
    assert_eq!(
        payload_bytes.available_permits(),
        48,
        "actual queue discard on receiver retirement returns its retained payload"
    );
}

#[test]
fn udp_payload_budget_rejects_new_assemblies_without_evicting_active_fragments() {
    let payload_bytes = Arc::new(Semaphore::new(4096));
    let mut assembly = Reassembly::default();
    let mut pending = Vec::new();
    for id in 0..65 {
        let wire =
            build_udp_fragments(id, &Address::Ipv4([203, 0, 113, 1]), 53, &[9; 48], 40).unwrap();
        let first = parse_udp_datagram(&wire[0]).unwrap();
        assert!(assembly
            .push(Packet::reserve(first, &payload_bytes).unwrap())
            .is_none());
        pending.push(wire);
    }
    assert_eq!(assembly.payloads.len(), 64);
    let mut completed = None;
    for wire in pending.remove(0).into_iter().skip(1) {
        completed = assembly
            .push(Packet::reserve(parse_udp_datagram(&wire).unwrap(), &payload_bytes).unwrap());
    }
    assert_eq!(
        completed.unwrap().payload(),
        &[9; 48],
        "capacity pressure preserved the oldest active packet"
    );
    drop(assembly);
    assert_eq!(payload_bytes.available_permits(), 4096);
}
