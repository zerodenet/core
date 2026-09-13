use super::*;
#[test]
fn xdns_dns_wire_bounds_and_official_query_response_contract() {
    let domain = wire::domain("t.example.com").unwrap();
    let payload = vec![0x72; 100];
    let query = codec::query(&domain, &[1, 2, 3, 4, 5, 6, 7, 8], &payload).unwrap();
    let message = wire::parse(&query).unwrap();
    assert_eq!(message.flags, 0x100);
    assert_eq!(message.records[2][0].class, 4096);
    assert_eq!(wire::encode(&message).unwrap(), query);
    let (response, id, packets) = codec::response_for(message, &domain).unwrap();
    assert_eq!(id, Some([1, 2, 3, 4, 5, 6, 7, 8]));
    assert_eq!(packets, vec![payload]);
    let response = codec::response(response, &[vec![0xa5; 932]]).unwrap();
    assert!(response.len() <= codec::MAX_RESPONSE);
    assert_eq!(
        codec::response_packets(wire::parse(&response).unwrap(), &domain).unwrap(),
        vec![vec![0xa5; 932]]
    );
    for bytes in [
        &[0u8; 11][..],
        &[0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0xc0, 12, 0, 16, 0, 1][..],
    ] {
        assert!(wire::parse(bytes).is_err());
    }
    assert!(wire::domain("example..com").is_err());
    assert!(codec::query(&domain, &[0; 8], &[0; 224]).is_err());
}
#[test]
fn xdns_rejects_wrong_zone_edns_size_and_response_direction() {
    let domain = wire::domain("example.com").unwrap();
    let query = codec::query(&domain, &[0; 8], b"hello").unwrap();
    let mut message = wire::parse(&query).unwrap();
    let (response, id, _) =
        codec::response_for(message.clone(), &wire::domain("other.com").unwrap()).unwrap();
    assert_eq!(response.flags & 15, 3);
    assert!(id.is_none());
    message.records[2][0].class = 512;
    let (response, id, _) = codec::response_for(message.clone(), &domain).unwrap();
    assert_eq!(response.flags & 15, 1);
    assert!(id.is_none());
    message.flags |= 0x8000;
    assert!(codec::response_for(message, &domain).is_none());
}
#[tokio::test]
async fn xdns_carrier_preserves_logical_peer_and_polls_unsolicited_responses() {
    use crate::finalmask::{packet_socket, udp::Mask};
    let masks = [Mask::Xdns {
        domain: "example.com".into(),
    }];
    let server = packet_socket::wrap(
        std::net::UdpSocket::bind("127.0.0.1:0").unwrap(),
        &masks,
        true,
    )
    .unwrap();
    let client = packet_socket::wrap(
        std::net::UdpSocket::bind("127.0.0.1:0").unwrap(),
        &masks,
        false,
    )
    .unwrap();
    let physical = server.local_addr().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        client.send_to(b"first", physical).await.unwrap();
        let mut bytes = [0; 2048];
        let (n, logical) = server.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..n], b"first");
        assert_eq!(logical.port(), 0);
        assert!(logical.is_ipv6());
        server.send_to(&[0x55; 932], logical).await.unwrap();
        let (n, peer) = client.recv_from(&mut bytes).await.unwrap();
        assert_eq!(peer, physical);
        assert_eq!(&bytes[..n], &[0x55; 932]);
        tokio::time::sleep(Duration::from_millis(550)).await;
        server.send_to(b"unsolicited", logical).await.unwrap();
        let (n, peer) = client.recv_from(&mut bytes).await.unwrap();
        assert_eq!(peer, physical);
        assert_eq!(&bytes[..n], b"unsolicited");
    })
    .await
    .unwrap();
}
#[test]
fn pinned_official_go_xdns_samples_decode_without_native_reencoding() {
    let domain = wire::domain("t.example.com").unwrap();
    let query = wire::parse(include_bytes!("vectors/xdns-query.bin")).unwrap();
    let (_, id, packets) = codec::response_for(query, &domain).unwrap();
    assert!(id.is_some());
    assert_eq!(packets, vec![(0..100).map(|i| i as u8).collect::<Vec<_>>()]);
    let reply = wire::parse(include_bytes!("vectors/xdns-response.bin")).unwrap();
    assert_eq!(
        codec::response_packets(reply, &domain).unwrap(),
        vec![(0..900).map(|i| i as u8).collect::<Vec<_>>()]
    );
}
