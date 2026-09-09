use super::*;

fn parameters(name: &str) -> BbrParameters {
    match name {
        "conservative" => BbrParameters {
            startup_pacing_gain: 2.25,
            startup_window_gain: 1.75,
            window_gain: 1.75,
            startup_rounds: 2,
            drain_to_target: true,
            detect_overshooting: true,
            loss_multiplier: 1,
            avoid_overestimate: true,
            reduce_ack_height_on_bandwidth_growth: true,
            ..Default::default()
        },
        "aggressive" => BbrParameters {
            startup_pacing_gain: 3.0,
            startup_window_gain: 2.25,
            window_gain: 2.5,
            startup_rounds: 4,
            startup_ack_aggregation: true,
            expire_startup_ack_aggregation: true,
            ..Default::default()
        },
        _ => BbrParameters::default(),
    }
}

#[test]
fn packet_traces_match_official_bbr_profiles() {
    let expected = include_str!("reference/expected.csv");
    for profile in ["standard", "conservative", "aggressive"] {
        let base = Instant::now();
        let mut bbr = Bbr::new(parameters(profile), 38_400, base, 1200);
        let mut golden = expected
            .lines()
            .skip(1)
            .filter(|line| line.starts_with(profile));
        for (index, line) in include_str!("reference/events.csv")
            .lines()
            .skip(1)
            .enumerate()
        {
            let row = line.split(',').collect::<Vec<_>>();
            let n = row[1..]
                .iter()
                .map(|n| n.parse::<u64>().unwrap())
                .collect::<Vec<_>>();
            let now = base + Duration::from_micros(n[0]);
            let before = bbr.mode;
            let event = match row[0] {
                "S" => PacketEvent::Sent {
                    key: PacketKey(2, n[1]),
                    now,
                    bytes: n[2],
                    in_flight: n[3],
                    ack_eliciting: true,
                },
                "A" => PacketEvent::Acked {
                    key: PacketKey(2, n[1]),
                },
                "L" => PacketEvent::Lost {
                    key: PacketKey(2, n[1]),
                    bytes: n[2],
                },
                "F" => PacketEvent::FeedbackEnd {
                    now,
                    in_flight: n[2],
                    min_rtt: Duration::from_micros(n[3]),
                },
                "M" => {
                    bbr.on_mtu_update(n[1] as u16);
                    continue;
                }
                _ => panic!("unknown trace operation"),
            };
            bbr.on_packet_event(event);
            if row[0] == "F" {
                // Same deterministic PROBE_BW entry offset as generate_test.go.
                if bbr.mode == Mode::ProbeBw && before != Mode::ProbeBw {
                    bbr.cycle = 2;
                    bbr.pacing_gain = 1.0;
                    bbr.update_pacing(0);
                }
                let actual = format!(
                    "{profile},{index},{},{},{},{},{},{},{},{},{}",
                    bbr.mode as u8,
                    bbr.window(),
                    bbr.effective_pacing(),
                    bbr.bandwidth.best(),
                    bbr.round,
                    bbr.full_bandwidth,
                    bbr.recovery as u8,
                    bbr.min_rtt.as_micros(),
                    bbr.sampler.ack_height()
                );
                assert_eq!(
                    actual,
                    golden.next().unwrap(),
                    "{profile} feedback at event {index}"
                );
            }
        }
        assert!(golden.next().is_none());
    }
}
