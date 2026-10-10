use super::*;

fn request() -> Vec<u8> {
    let source = "10.0.0.1".parse().unwrap();
    let destination = "10.0.0.2".parse().unwrap();
    let mut message = vec![8, 0, 0, 0, 0, 7, 0, 1];
    message.extend_from_slice(b"ping");
    let checksum = packet::checksum(&message);
    message[2..4].copy_from_slice(&checksum.to_be_bytes());
    packet::build_icmp_echo_tunnel_probe(
        &packet::IcmpEchoRequest {
            source,
            destination,
            message: &message,
        },
        7,
        source,
        1500,
    )
    .unwrap()
}

#[test]
fn lookup_enforces_deadline_without_maintenance_and_admission_reclaims_capacity() {
    let now = Instant::now();
    let local = "10.0.0.3".parse().unwrap();
    let mut table = EchoTranslation::default();
    let first = table.request(&request(), local, 1, now).unwrap();
    let second = table
        .request(&request(), local, 2, now + Duration::from_secs(1))
        .unwrap();
    let reply = packet::build_local_icmp_echo_reply(&first, 1500).unwrap();
    assert!(table.response(&reply, now + REQUEST_TIMEOUT).is_none());
    assert_eq!(table.pending.len(), 1);
    let response = packet::build_local_icmp_echo_reply(&second, 1500).unwrap();
    assert_eq!(
        table.response(&response, now + REQUEST_TIMEOUT).unwrap().0,
        2
    );
    assert_eq!(table.bytes, 0);
    for _ in 0..MAX_REQUESTS {
        table.request(&request(), local, 3, now).unwrap();
    }
    assert_eq!(
        table.request(&request(), local, 4, now).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    table
        .request(&request(), local, 5, now + REQUEST_TIMEOUT)
        .unwrap();
    assert_eq!(table.pending.len(), 1);
    assert_eq!(table.bytes, request().len() * 2);
}

#[test]
fn maintenance_removes_closed_context_before_deadline_and_releases_burst_capacity() {
    let now = Instant::now();
    let local = "10.0.0.3".parse().unwrap();
    let mut table = EchoTranslation::default();
    for n in 0..MAX_REQUESTS {
        table.request(&request(), local, n, now).unwrap();
    }
    let peak = table.pending.capacity();
    table.expire(now, |n| *n != 0);
    assert_eq!(table.pending.len(), 1);
    assert!(table.pending.capacity() < peak / 4);
    assert_eq!(table.bytes, request().len() * 2);
    assert_eq!(table.next_expiration, Some(now + REQUEST_TIMEOUT));
    table.clear();
    assert_eq!(table.pending.capacity(), 0);
    assert_eq!(table.bytes, 0);
    assert_eq!(table.next_expiration, None);
}

#[test]
fn maintenance_keeps_the_earliest_live_deadline_after_early_response() {
    let now = Instant::now();
    let local = "10.0.0.3".parse().unwrap();
    let mut table = EchoTranslation::default();
    let first = table.request(&request(), local, 1, now).unwrap();
    table
        .request(&request(), local, 2, now + Duration::from_secs(5))
        .unwrap();
    table
        .response(
            &packet::build_local_icmp_echo_reply(&first, 1500).unwrap(),
            now,
        )
        .unwrap();
    table.expire_due(now + REQUEST_TIMEOUT);
    assert_eq!(
        table.next_expiration,
        Some(now + REQUEST_TIMEOUT + Duration::from_secs(5))
    );
    assert_eq!(table.pending.len(), 1);
}

#[test]
#[ignore = "local debug-profile comparison, not whole-kernel performance acceptance"]
fn compare_request_cost_with_forced_per_request_sweep() {
    let now = Instant::now();
    let local = "10.0.0.3".parse().unwrap();
    let packet = request();
    for force_sweep in [true, false] {
        let mut table = EchoTranslation::default();
        for _ in 0..512 {
            table.request(&packet, local, 0, now).unwrap();
        }
        let start = Instant::now();
        for _ in 0..10_000 {
            if force_sweep {
                table.expire(now, |_| false);
            }
            let translated = table.request(&packet, local, 1, now).unwrap();
            let reply = packet::build_local_icmp_echo_reply(&translated, 1500).unwrap();
            if force_sweep {
                table.expire(now, |_| false);
            }
            assert!(table.response(&reply, now).is_some());
        }
        eprintln!(
            "force_sweep={force_sweep} requests=10000 pending=512 elapsed_us={}",
            start.elapsed().as_micros()
        );
    }
}
