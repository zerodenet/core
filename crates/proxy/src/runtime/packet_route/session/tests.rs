use super::{PacketPlane, PacketSessionPins};

#[test]
fn new_tcp_connection_can_use_new_plane_while_existing_connection_stays_pinned() {
    let mut first = vec![0_u8; 40];
    first[0] = 0x45;
    first[2..4].copy_from_slice(&40_u16.to_be_bytes());
    first[9] = zero_stack::packet::IPPROTO_TCP;
    first[12..16].copy_from_slice(&[10, 0, 0, 2]);
    first[16..20].copy_from_slice(&[192, 168, 1, 119]);
    first[20..22].copy_from_slice(&40000_u16.to_be_bytes());
    first[22..24].copy_from_slice(&443_u16.to_be_bytes());
    first[32] = 0x50;
    let mut pins = PacketSessionPins::default();
    assert!(pins.record(&first, PacketPlane::Packet("wg".into())));
    assert!(!pins.permits(&first, &PacketPlane::Flow));
    let mut second = first.clone();
    second[20..22].copy_from_slice(&40001_u16.to_be_bytes());
    assert!(pins.permits(&second, &PacketPlane::Flow));
    assert!(pins.record(&second, PacketPlane::Flow));
    assert!(pins.permits(&first, &PacketPlane::Packet("wg".into())));
    assert!(pins.permits(&second, &PacketPlane::Flow));
}

#[test]
fn packet_plane_stays_pinned_when_route_selection_changes() {
    let mut packet = vec![0_u8; 28];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&28_u16.to_be_bytes());
    packet[9] = 1;
    packet[12..16].copy_from_slice(&[10, 0, 0, 2]);
    packet[16..20].copy_from_slice(&[10, 0, 0, 3]);
    let mut pins = PacketSessionPins::default();
    let wireguard = PacketPlane::Packet("wg".into());
    assert!(pins.permits(&packet, &wireguard));
    assert!(pins.record(&packet, wireguard.clone()));
    assert!(pins.permits(&packet, &wireguard));
    assert!(!pins.permits(&packet, &PacketPlane::Flow));
    assert!(!pins.permits(&packet, &PacketPlane::Packet("other".into())));
    assert!(pins.permits(&packet, &PacketPlane::Packet("wg".into())));
}
