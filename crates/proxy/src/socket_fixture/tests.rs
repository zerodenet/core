use super::*;
use std::{cell::Cell, net::Ipv4Addr, time::Duration};

#[test]
fn paired_allocator_retries_real_conflicts_in_both_protocol_directions() {
    let (tcp_candidate, occupied_udp) = tcp_udp();
    let (occupied_tcp, udp_candidate) = tcp_udp();
    let mut first_tcp = Some(tcp_candidate);
    let mut first_udp = Some(udp_candidate);
    let tcp_calls = Cell::new(0);
    let udp_calls = Cell::new(0);
    let (tcp, udp) = tcp_udp_with(
        || {
            tcp_calls.set(tcp_calls.get() + 1);
            first_tcp
                .take()
                .unwrap_or_else(|| std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap())
        },
        || {
            udp_calls.set(udp_calls.get() + 1);
            first_udp
                .take()
                .unwrap_or_else(|| std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap())
        },
    );
    assert!(
        tcp_calls.get() >= 2,
        "both initial fixed-address binds must fail"
    );
    assert!(udp_calls.get() >= 1, "the UDP allocator must actually run");
    let address = tcp.local_addr().unwrap();
    assert_eq!(address, udp.local_addr().unwrap());
    assert_ne!(address, occupied_udp.local_addr().unwrap());
    assert_ne!(address, occupied_tcp.local_addr().unwrap());

    let _client = std::net::TcpStream::connect_timeout(&address, Duration::from_secs(1)).unwrap();
    let _accepted = tcp.accept().unwrap();
    udp.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let sender = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    sender.send_to(b"pair", address).unwrap();
    let mut bytes = [0; 4];
    let (length, peer) = udp.recv_from(&mut bytes).unwrap();
    assert_eq!(length, bytes.len());
    assert_eq!(&bytes, b"pair");
    assert_eq!(peer, sender.local_addr().unwrap());
}
