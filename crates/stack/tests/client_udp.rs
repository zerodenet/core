use std::net::IpAddr;

use tokio::sync::mpsc;
use zero_stack::{
    client_udp::ClientUdpEvent,
    packet,
    packet::{Endpoint, IcmpErrorKind},
    ClientUdpStack, ClientUdpStackError,
};

fn ip(value: &str) -> IpAddr {
    value.parse().unwrap()
}

#[tokio::test]
async fn client_udp_round_trips_ipv4_packets_in_memory() {
    let (a_outbound, mut a_packets) = mpsc::channel(16);
    let (b_outbound, mut b_packets) = mpsc::channel(16);
    let a = ClientUdpStack::new(vec![ip("10.0.0.1")], a_outbound, 1_420).unwrap();
    let b = ClientUdpStack::new(vec![ip("10.0.0.2")], b_outbound, 1_420).unwrap();
    let mut a_socket = a.bind(ip("10.0.0.1")).unwrap();
    let mut b_socket = b.bind(ip("10.0.0.2")).unwrap();

    a_socket
        .send_to(b"request", b_socket.local_endpoint())
        .await
        .unwrap();
    assert!(b.feed(&a_packets.recv().await.unwrap()));
    let request = b_socket.recv_from().await.unwrap();
    assert_eq!(request.payload, b"request");
    assert_eq!(request.source, a_socket.local_endpoint());

    b_socket.send_to(b"reply", request.source).await.unwrap();
    assert!(a.feed(&b_packets.recv().await.unwrap()));
    let reply = a_socket.recv_from().await.unwrap();
    assert_eq!(reply.payload, b"reply");
    assert_eq!(reply.source, b_socket.local_endpoint());
}

#[tokio::test]
async fn client_udp_reassembles_out_of_order_ipv6_fragments_before_delivery() {
    let (outbound, mut packets) = mpsc::channel(16);
    let (receiver_outbound, _receiver_packets) = mpsc::channel(16);
    let sender = ClientUdpStack::new(vec![ip("fd00::1")], outbound, 1_280).unwrap();
    let receiver = ClientUdpStack::new(vec![ip("fd00::2")], receiver_outbound, 1_280).unwrap();
    let source = sender.bind(ip("fd00::1")).unwrap();
    let mut destination = receiver.bind(ip("fd00::2")).unwrap();
    let payload = vec![0x42; 2_048];

    source
        .send_to(&payload, destination.local_endpoint())
        .await
        .unwrap();
    let mut fragments = Vec::new();
    while let Ok(packet) = packets.try_recv() {
        assert!(packet.len() <= 1_280);
        fragments.push(packet);
    }
    assert!(fragments.len() > 1);
    let mut delivered = false;
    for fragment in fragments.iter().rev() {
        delivered |= receiver.feed(fragment);
    }
    assert!(delivered);
    let received = destination.recv_from().await.unwrap();
    assert_eq!(received.payload, payload);
    assert_eq!(received.source, source.local_endpoint());
}

#[tokio::test]
async fn client_udp_bounds_each_socket_receive_queue() {
    let (outbound, _packets) = mpsc::channel(1);
    let stack = ClientUdpStack::new(vec![ip("10.0.0.2")], outbound, 1_420).unwrap();
    let mut socket = stack.bind(ip("10.0.0.2")).unwrap();
    let packet = packet::build_udp(
        ip("10.0.0.1"),
        socket.local_endpoint().ip,
        50_000,
        socket.local_endpoint().port,
        b"bounded",
    );
    for _ in 0..64 {
        assert!(stack.feed(&packet));
    }
    assert!(!stack.feed(&packet));
    assert_eq!(socket.recv_from().await.unwrap().payload, b"bounded");
    assert!(stack.feed(&packet));
}

#[tokio::test]
async fn client_udp_reports_matching_icmp_error_without_delivering_it_as_payload() {
    let (outbound, mut packets) = mpsc::channel(16);
    let stack = ClientUdpStack::new(vec![ip("10.0.0.2")], outbound, 1_420).unwrap();
    let mut socket = stack.bind(ip("10.0.0.2")).unwrap();
    let destination = Endpoint {
        ip: ip("203.0.113.7"),
        port: 53,
    };
    socket.send_to(b"query", destination).await.unwrap();
    let request = packets.recv().await.unwrap();

    let unrelated = packet::build_udp(
        socket.local_endpoint().ip,
        ip("203.0.113.8"),
        socket.local_endpoint().port,
        53,
        b"other",
    );
    let unrelated = packet::build_udp_unreachable_response(&unrelated, 1_420).unwrap();
    assert!(!stack.feed(&unrelated));

    let error = packet::build_udp_unreachable_response(&request, 1_420).unwrap();
    assert!(stack.feed(&error));
    let event = socket.recv_event().await.unwrap();
    assert!(matches!(
        event,
        ClientUdpEvent::IcmpError(error)
            if error.kind == IcmpErrorKind::DestinationUnreachable { code: 13 }
                && error.quoted_destination == destination
    ));
}

#[tokio::test]
async fn client_udp_reports_ipv6_packet_too_big() {
    let (outbound, mut packets) = mpsc::channel(16);
    let stack = ClientUdpStack::new(vec![ip("fd00::2")], outbound, 1_420).unwrap();
    let mut socket = stack.bind(ip("fd00::2")).unwrap();
    let destination = Endpoint {
        ip: ip("2001:db8::7"),
        port: 443,
    };
    socket.send_to(b"query", destination).await.unwrap();
    let _ = packets.recv().await.unwrap();
    let oversized = packet::build_udp(
        socket.local_endpoint().ip,
        destination.ip,
        socket.local_endpoint().port,
        destination.port,
        &vec![0x42; 1_500],
    );
    let error = packet::build_icmp_response(&oversized, 1_280).unwrap();
    assert!(stack.feed(&error));
    assert!(matches!(
        socket.recv_event().await,
        Some(ClientUdpEvent::IcmpError(error))
            if error.kind == IcmpErrorKind::PacketTooBig { mtu: Some(1_280) }
    ));
    socket
        .send_to(&vec![0x43; 1_300], destination)
        .await
        .unwrap();
    let first = packets.recv().await.unwrap();
    let second = packets.recv().await.unwrap();
    assert!(first.len() <= 1_280 && second.len() <= 1_280);
}

#[tokio::test]
async fn client_udp_lowers_only_matching_socket_path_mtu_after_packet_too_big() {
    let (outbound, mut packets) = mpsc::channel(16);
    let stack = ClientUdpStack::new(vec![ip("10.0.0.2")], outbound, 1_420).unwrap();
    let mut affected = stack.bind(ip("10.0.0.2")).unwrap();
    let unaffected = stack.bind(ip("10.0.0.2")).unwrap();
    let destination = Endpoint {
        ip: ip("203.0.113.7"),
        port: 443,
    };
    affected.send_to(b"initial", destination).await.unwrap();
    let _ = packets.recv().await.unwrap();
    unaffected.send_to(b"initial", destination).await.unwrap();
    let _ = packets.recv().await.unwrap();

    let oversized = packet::build_udp(
        affected.local_endpoint().ip,
        destination.ip,
        affected.local_endpoint().port,
        destination.port,
        &vec![0x42; 1_500],
    );
    let error = packet::build_icmp_response(&oversized, 900).unwrap();
    assert!(stack.feed(&error));
    assert!(matches!(
        affected.recv_event().await,
        Some(ClientUdpEvent::IcmpError(error))
            if error.kind == IcmpErrorKind::PacketTooBig { mtu: Some(900) }
    ));

    let payload = vec![0x43; 1_100];
    affected.send_to(&payload, destination).await.unwrap();
    let first = packets.recv().await.unwrap();
    let second = packets.recv().await.unwrap();
    assert!(first.len() <= 900 && second.len() <= 900);
    assert!(packet::parse_ip_fragment(&first).is_some());
    assert!(packet::parse_ip_fragment(&second).is_some());

    unaffected.send_to(&payload, destination).await.unwrap();
    let unchanged = packets.recv().await.unwrap();
    assert!(unchanged.len() > 900);
    assert!(packet::parse_ip_fragment(&unchanged).is_none());

    let other_destination = Endpoint {
        ip: ip("203.0.113.8"),
        port: 443,
    };
    affected.send_to(&payload, other_destination).await.unwrap();
    let reset = packets.recv().await.unwrap();
    assert!(reset.len() > 900);
    assert!(packet::parse_ip_fragment(&reset).is_none());
}

#[tokio::test]
async fn client_udp_rejects_unknown_local_address_family_mismatch_and_oversize() {
    let (outbound, _packets) = mpsc::channel(1);
    let stack = ClientUdpStack::new(vec![ip("10.0.0.1")], outbound, 1_420).unwrap();
    assert!(matches!(
        stack.bind(ip("10.0.0.2")),
        Err(ClientUdpStackError::UnknownLocalAddress)
    ));
    let socket = stack.bind(ip("10.0.0.1")).unwrap();
    assert_eq!(
        socket
            .send_to(
                b"data",
                Endpoint {
                    ip: ip("fd00::2"),
                    port: 53,
                },
            )
            .await,
        Err(ClientUdpStackError::AddressFamilyMismatch)
    );
    assert_eq!(
        socket
            .send_to(
                &vec![0_u8; 65_508],
                Endpoint {
                    ip: ip("10.0.0.2"),
                    port: 53,
                },
            )
            .await,
        Err(ClientUdpStackError::PayloadTooLarge)
    );
}

#[test]
fn client_udp_socket_limit_and_drop_release_capacity() {
    let (outbound, _packets) = mpsc::channel(1);
    let stack = ClientUdpStack::new(vec![ip("10.0.0.1")], outbound, 1_420).unwrap();
    let mut sockets = (0..1_024)
        .map(|_| stack.bind(ip("10.0.0.1")).unwrap())
        .collect::<Vec<_>>();
    assert!(matches!(
        stack.bind(ip("10.0.0.1")),
        Err(ClientUdpStackError::SocketLimit)
    ));
    sockets.pop();
    assert!(stack.bind(ip("10.0.0.1")).is_ok());
}

#[test]
fn client_udp_rejects_invalid_local_profiles() {
    let (outbound, _packets) = mpsc::channel(1);
    assert!(matches!(
        ClientUdpStack::new(vec![], outbound.clone(), 1_420),
        Err(ClientUdpStackError::MissingLocalAddress)
    ));
    assert!(matches!(
        ClientUdpStack::new(
            vec![ip("10.0.0.1"), ip("10.0.0.1")],
            outbound.clone(),
            1_420
        ),
        Err(ClientUdpStackError::DuplicateLocalAddress)
    ));
    assert!(matches!(
        ClientUdpStack::new(vec![ip("fd00::1")], outbound, 1_279),
        Err(ClientUdpStackError::InvalidMtu)
    ));
}
