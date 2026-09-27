use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use zero_platform_tokio::IcmpSocket;

#[tokio::test]
async fn loopback_icmp_echo_uses_the_host_socket_contract() {
    if std::env::var_os("ZERO_ICMP_LIVE_TEST").is_none() {
        return;
    }
    for (bind, target, request_type, reply_type) in [
        (
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            8,
            0,
        ),
        (
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            128,
            129,
        ),
    ] {
        let socket = IcmpSocket::bind(bind, None).expect("bind ICMP socket");
        let local = socket.connect(target).await.expect("connect ICMP socket");
        let mut request = vec![request_type, 0, 0, 0, 0x45, 0x67, 0, 1];
        request.extend_from_slice(b"zero-icmp-loopback");
        let checksum = match (local, target) {
            (IpAddr::V4(_), IpAddr::V4(_)) => checksum(&request),
            (IpAddr::V6(local), IpAddr::V6(target)) => icmpv6_checksum(local, target, &request),
            _ => panic!("ICMP address family mismatch"),
        };
        request[2..4].copy_from_slice(&checksum.to_be_bytes());
        socket.send(&request).await.expect("send ICMP echo");
        let reply_id = socket.reply_identifier(0x4567).expect("echo identifier");
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut buffer = [0_u8; 1500];
            loop {
                let (size, source) = socket.recv_from(&mut buffer).await.expect("receive ICMP");
                if source == target
                    && size >= request.len()
                    && buffer[0] == reply_type
                    && buffer[4..6] == reply_id.to_be_bytes()
                    && buffer[6..8] == request[6..8]
                    && buffer[8..request.len()] == request[8..]
                {
                    break;
                }
            }
        })
        .await
        .expect("ICMP echo reply timed out");
    }
}

fn icmpv6_checksum(source: Ipv6Addr, destination: Ipv6Addr, message: &[u8]) -> u16 {
    let mut pseudo = Vec::new();
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.extend_from_slice(&(message.len() as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, 58]);
    pseudo.extend_from_slice(message);
    checksum(&pseudo)
}

fn checksum(bytes: &[u8]) -> u16 {
    let (chunks, remainder) = bytes.as_chunks::<2>();
    let mut sum = chunks
        .iter()
        .map(|chunk| u32::from(u16::from_be_bytes([chunk[0], chunk[1]])))
        .sum::<u32>();
    if let Some(byte) = remainder.first() {
        sum += u32::from(*byte) << 8;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}
