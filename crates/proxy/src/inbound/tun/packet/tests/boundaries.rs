use super::*;

#[tokio::test]
async fn dns_capture_precedes_warmed_packet_rejection_for_tcp_and_udp() {
    let mut fixture = Fixture::new(reject_packet());
    for destination in ["203.0.113.1", "2001:db8::1"] {
        let destination = destination.parse().unwrap();
        for packet in [syn(destination, 53), datagram(destination, 53, b"dns")] {
            assert!(fixture.forward(&packet, 1500, false).await);
            assert!(!fixture.forward(&packet, 1500, true).await);
            assert!(fixture.forward(&packet, 1500, false).await);
        }
        // Capturing DNS must not admit another port to the flow stack.
        let other = datagram(destination, 443, b"not dns");
        assert!(fixture.forward(&other, 1500, true).await);
    }
    fixture.no_response();
}

#[tokio::test]
async fn mtu_rejection_precedes_warmed_flow_and_dns_capture() {
    let mut fixture = Fixture::new(direct());
    for destination in ["203.0.113.1", "2001:db8::1"] {
        let destination: IpAddr = destination.parse().unwrap();
        for port in [443, 53] {
            let small = datagram(destination, port, b"small");
            assert!(!fixture.forward(&small, 576, false).await);
            let large = datagram(destination, port, &[0x5a; 800]);
            assert!(fixture.forward(&large, 576, port == 53).await);
            let response = fixture.received.try_recv().expect("MTU response");
            assert!(response.len() <= 576);
            let error = packet::parse_icmp_error(&response).expect("valid ICMP error");
            assert_eq!(
                error.kind,
                packet::IcmpErrorKind::PacketTooBig { mtu: Some(576) }
            );
            assert_eq!(error.quoted_source.ip, client(destination));
            assert_eq!(error.quoted_destination.ip, destination);
            assert_eq!(error.quoted_destination.port, port);
            assert!(!fixture.forward(&small, 576, port == 53).await);
        }
    }
    fixture.no_response();
}

#[tokio::test]
async fn fake_ip_flows_keep_domain_restoration_ahead_of_packet_policy() {
    let mut value = reject_packet();
    value["runtime"] = json!({"dns":{
        "servers":{"system":{"type":"system"}},"default_server":"system",
        "answer":{"type":"fake_ip","cidr":"198.18.0.0/15","ipv6_cidr":"fdfe::/64"}
    }});
    let mut fixture = Fixture::new(value);
    let ordinary = datagram("203.0.113.1".parse().unwrap(), 443, b"ordinary");
    assert!(fixture.forward(&ordinary, 1500, false).await);
    for query_type in [1_u16, 28] {
        let mut query = vec![0x12, 0x34, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
        for label in ["allowed", "example"] {
            query.push(label.len() as u8);
            query.extend_from_slice(label.as_bytes());
        }
        query.push(0);
        query.extend_from_slice(&query_type.to_be_bytes());
        query.extend_from_slice(&1_u16.to_be_bytes());
        let response = fixture
            .proxy
            .resolver
            .answer_udp_query(&query)
            .await
            .unwrap();
        assert_eq!(response[3] & 15, 0);
        let destination = if query_type == 1 {
            IpAddr::V4(
                <[u8; 4]>::try_from(&response[response.len() - 4..])
                    .unwrap()
                    .into(),
            )
        } else {
            IpAddr::V6(
                <[u8; 16]>::try_from(&response[response.len() - 16..])
                    .unwrap()
                    .into(),
            )
        };
        let unmapped = if query_type == 1 {
            "198.18.255.254"
        } else {
            "fdfe::ffff"
        };
        for destination in [destination, unmapped.parse().unwrap()] {
            for packet in [
                syn(destination, 443),
                datagram(destination, 443, b"fake ip"),
            ] {
                for _ in 0..2 {
                    assert!(!fixture.forward(&packet, 1500, false).await);
                }
            }
        }
    }
    assert!(fixture.forward(&ordinary, 1500, false).await);
    fixture.no_response();
}
