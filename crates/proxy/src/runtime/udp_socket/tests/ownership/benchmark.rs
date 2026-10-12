//! Synthetic table-admission comparison only; no business traffic is generated.
use super::*;
use std::{collections::HashMap, hint::black_box, time::Instant};

#[test]
#[ignore = "local admission microbenchmark; not throughput or power qualification"]
fn compare_direct_udp_admission_lookup_cost() {
    let proxy = proxy();
    let services = socket_set(&proxy, None).services;
    let policy = policy(&proxy, Some("first"));
    let generation = services.egress_generation();
    let mut local = Vec::new();
    let mut legacy = HashMap::new();
    const SOCKETS: usize = 64;
    const PEERS: usize = 128;
    const QUERIES: usize = 1_000;
    for socket in 0..SOCKETS {
        let mut replies = DirectUdpReplies::default();
        for peer in 0..PEERS {
            let ordinal = socket * PEERS + peer;
            let address = SocketAddr::from(([192, 0, 2, 1], (1_000 + ordinal) as u16));
            // Match the old flow value's session/guard word layout without a
            // second set of guards, so this remains table bookkeeping only.
            legacy.insert((socket, address), (ordinal as u64, 0_usize));
            replies.register(address, || {
                DirectUdpResponseFlow::new(
                    ordinal as u64,
                    services.clone(),
                    policy.clone(),
                    generation,
                )
            });
        }
        local.push(replies);
    }
    let candidate = "198.51.100.1:65530".parse().unwrap();
    let mut old_times = Vec::new();
    let mut new_times = Vec::new();
    for round in 0..5 {
        for old in if round % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        } {
            let start = Instant::now();
            let mut admitted = 0;
            for query in 0..QUERIES {
                let socket = black_box(query % SOCKETS);
                let candidate = black_box(candidate);
                let allowed = if old {
                    !legacy.contains_key(&(socket, candidate))
                        && legacy.keys().filter(|(id, _)| *id == socket).count()
                            < super::super::super::MAX_DIRECT_UDP_PEERS_PER_SOCKET
                } else {
                    local[socket].can_admit(&candidate)
                };
                admitted += usize::from(black_box(allowed));
            }
            assert_eq!(admitted, QUERIES);
            let elapsed = start.elapsed().as_nanos() / QUERIES as u128;
            if old {
                old_times.push(elapsed);
            } else {
                new_times.push(elapsed);
            }
        }
    }
    old_times.sort_unstable();
    new_times.sort_unstable();
    println!("Admission only / debug profile / {SOCKETS} sockets / {} owners / {QUERIES} queries x 5: global_count_ns={old_times:?}, socket_local_ns={new_times:?}", SOCKETS * PEERS);
    // Counts, latency, allocator footprint and whole-process cost are separate.
}
