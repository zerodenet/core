use super::*;

fn joined_reference(prefix: &[u8], data: &[u8]) -> u16 {
    let bytes: Vec<_> = prefix.iter().chain(data).copied().collect();
    let mut sum = 0_u32;
    for pair in bytes.chunks(2) {
        sum += u32::from(pair[0]) << 8 | u32::from(*pair.get(1).unwrap_or(&0));
    }
    while sum > 0xffff {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

#[test]
fn borrowed_transport_checksums_match_joined_reference_for_odd_even_and_maximum_packets() {
    let source4 = Ipv4Addr::new(192, 0, 2, 1);
    let destination4 = Ipv4Addr::new(198, 51, 100, 7);
    let source6 = "2001:db8::1".parse::<Ipv6Addr>().unwrap();
    let destination6 = "2001:db8::7".parse::<Ipv6Addr>().unwrap();
    for length in [0, 1, 2, 31, 512, 1_279, 65_515] {
        let data: Vec<_> = (0..length).map(|i| (i * 37 + 11) as u8).collect();
        for protocol in [IPPROTO_TCP, IPPROTO_UDP, super::super::IPPROTO_ICMPV6] {
            let mut prefix4 = Vec::from(source4.octets());
            prefix4.extend(destination4.octets());
            prefix4.extend([0, protocol]);
            prefix4.extend((length as u16).to_be_bytes());
            assert_eq!(
                transport_checksum_v4(&source4, &destination4, protocol, &data),
                joined_reference(&prefix4, &data)
            );
            let mut prefix6 = Vec::from(source6.octets());
            prefix6.extend(destination6.octets());
            prefix6.extend((length as u32).to_be_bytes());
            prefix6.extend([0, 0, 0, protocol]);
            assert_eq!(
                transport_checksum_v6(&source6, &destination6, protocol, &data),
                joined_reference(&prefix6, &data)
            );
        }
    }
}

#[test]
fn udp_computed_zero_is_still_transmitted_as_all_ones() {
    let source4 = Ipv4Addr::new(192, 0, 2, 1);
    let destination4 = Ipv4Addr::new(198, 51, 100, 7);
    let source6 = "2001:db8::1".parse::<Ipv6Addr>().unwrap();
    let destination6 = "2001:db8::7".parse::<Ipv6Addr>().unwrap();
    let mut udp = [0; 10];
    let compensation = transport_checksum_v4(&source4, &destination4, IPPROTO_UDP, &udp);
    udp[8..].copy_from_slice(&compensation.to_be_bytes());
    assert_eq!(
        transport_checksum_v4(&source4, &destination4, IPPROTO_UDP, &udp),
        0
    );
    assert_eq!(udp_checksum_v4(&source4, &destination4, &udp), 0xffff);
    udp[8..].fill(0);
    let compensation = transport_checksum_v6(&source6, &destination6, IPPROTO_UDP, &udp);
    udp[8..].copy_from_slice(&compensation.to_be_bytes());
    assert_eq!(
        transport_checksum_v6(&source6, &destination6, IPPROTO_UDP, &udp),
        0
    );
    assert_eq!(udp_checksum_v6(&source6, &destination6, &udp), 0xffff);
}

#[test]
fn checksum_matches_rfc1071_vector_and_odd_byte_padding() {
    assert_eq!(checksum(&[0, 1, 0xf2, 3, 0xf4, 0xf5, 0xf6, 0xf7]), 0x220d);
    assert_eq!(checksum(&[0x12]), 0xedff);
    assert_eq!(
        checksum(&[0xff; 65_535]),
        joined_reference(&[], &[0xff; 65_535])
    );
}
