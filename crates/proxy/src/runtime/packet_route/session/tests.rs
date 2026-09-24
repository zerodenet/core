use super::{PacketPlane, PacketSessionPins};

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
