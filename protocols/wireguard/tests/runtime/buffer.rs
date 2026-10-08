use super::*;

#[test]
fn retained_wire_and_plaintext_actions_survive_tunnel_drop_and_later_packets() {
    let mut a = tunnel(1, 2, "10.0.0.1/32");
    let mut b = tunnel(2, 1, "10.0.0.2/32");
    let a_ip = Ipv4Addr::new(10, 0, 0, 1);
    let b_ip = Ipv4Addr::new(10, 0, 0, 2);
    let request = ipv4_packet(a_ip, b_ip, b"first queued payload");
    let initiation = network_packets(a.send_ip_packet(&request).unwrap());
    let response = network_packets(
        b.receive_datagram(Some(outer(a_ip.into())), &initiation[0])
            .unwrap(),
    );
    let queued = network_packets(
        a.receive_datagram(Some(outer(b_ip.into())), &response[0])
            .unwrap(),
    );
    let retained = queued
        .iter()
        .flat_map(|wire| b.receive_datagram(Some(outer(a_ip.into())), wire).unwrap())
        .find_map(|action| match action {
            TunnelAction::ReceiveIp { packet, .. } => Some(packet),
            _ => None,
        })
        .unwrap();
    let pointer = retained.as_ptr();
    for size in [0, 1, 15, 16, 17, 1399, 1400] {
        let plain = ipv4_packet(a_ip, b_ip, &vec![0x5a; size]);
        let wire = a.send_ip_packet(&plain).unwrap();
        let TunnelAction::SendNetwork(wire) = wire.into_iter().next().unwrap() else {
            panic!("expected data")
        };
        assert_eq!(wire.len(), 32 + plain.len().div_ceil(16) * 16);
        let decoded = b.receive_datagram(Some(outer(a_ip.into())), &wire).unwrap();
        assert!(decoded.iter().any(
            |action| matches!(action, TunnelAction::ReceiveIp { packet, .. } if packet == &plain)
        ));
        assert_eq!(retained.as_ptr(), pointer);
        assert_eq!(retained, request);
    }
    let final_packet = ipv4_packet(a_ip, b_ip, b"retained encrypted packet");
    let wire = a.send_ip_packet(&final_packet).unwrap();
    drop(a);
    let TunnelAction::SendNetwork(wire) = wire.into_iter().next().unwrap() else {
        panic!("expected data")
    };
    let decoded = b.receive_datagram(Some(outer(a_ip.into())), &wire).unwrap();
    drop(b);
    assert_eq!(retained, request);
    assert!(decoded.iter().any(
        |action| matches!(action, TunnelAction::ReceiveIp { packet, .. } if packet == &final_packet)
    ));
}
