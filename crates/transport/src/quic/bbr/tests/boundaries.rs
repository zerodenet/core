use super::*;

#[test]
fn probe_rtt_requires_drain_duration_and_a_new_round() {
    let now = Instant::now();
    let mut bbr = Bbr::new(BbrParameters::default(), 38_400, now, 1200);
    bbr.full_bandwidth = true;
    bbr.flight = 38_400;
    bbr.probe_rtt(now, false, true);
    assert_eq!(bbr.mode, Mode::ProbeRtt);
    assert_eq!(bbr.window(), 4800);
    assert!(bbr.probe_exit.is_none());
    bbr.flight = 4800;
    bbr.probe_rtt(now, false, false);
    bbr.probe_rtt(now + Duration::from_millis(201), false, false);
    assert_eq!(bbr.mode, Mode::ProbeRtt);
    bbr.probe_rtt(now + Duration::from_millis(202), true, false);
    assert_eq!(bbr.mode, Mode::ProbeBw);
}

#[test]
fn idle_restart_does_not_force_an_rtt_probe() {
    let now = Instant::now();
    let mut bbr = Bbr::new(BbrParameters::default(), 38_400, now, 1200);
    bbr.exiting_idle = true;
    bbr.probe_rtt(now, true, true);
    assert_eq!(bbr.mode, Mode::Startup);
}

#[test]
fn recovery_waits_for_full_bandwidth_and_packets_beyond_loss_boundary() {
    let now = Instant::now();
    let mut bbr = Bbr::new(BbrParameters::default(), 38_400, now, 1200);
    bbr.sampler.sequence = 40;
    bbr.update_recovery(20, true, true);
    assert_eq!(bbr.recovery, Recovery::None);
    bbr.full_bandwidth = true;
    bbr.update_recovery(21, true, false);
    assert_eq!(bbr.recovery, Recovery::Conservation);
    bbr.update_recovery(40, false, true);
    assert_eq!(bbr.recovery, Recovery::Growth);
    bbr.update_recovery(41, false, false);
    assert_eq!(bbr.recovery, Recovery::None);
}

#[test]
fn mtu_growth_and_blackhole_decrease_keep_windows_valid() {
    let now = Instant::now();
    let mut bbr = Bbr::new(BbrParameters::default(), 38_400, now, 1200);
    bbr.on_mtu_update(1400);
    assert_eq!(bbr.initial_window(), 44_800);
    assert_eq!(bbr.window(), 44_800);
    bbr.cwnd = bbr.max_window;
    bbr.on_mtu_update(1200);
    assert_eq!(bbr.window(), 24_000_000);
    bbr.cwnd = bbr.minimum_window();
    bbr.on_mtu_update(1400);
    assert_eq!(bbr.window(), 5600);
}

#[test]
fn bandwidth_maximum_expires_after_ten_rounds() {
    let mut filter = Filter::new(|v| v);
    filter.update(100, 1);
    for round in 2..=11 {
        filter.update(50, round);
        assert_eq!(filter.best(), 100);
    }
    filter.update(50, 12);
    assert_eq!(filter.best(), 50);
}

#[test]
fn packet_number_spaces_and_discards_do_not_cross_contaminate_samples() {
    let now = Instant::now();
    let mut sampler = Sampler::new(BbrParameters::default());
    for space in 0..3 {
        sampler.send(PacketKey(space, 0), now, 1200, space as u64 * 1200, true);
    }
    sampler.discard_space(0);
    sampler.discard(PacketKey(1, 0));
    let sample = sampler.feedback(
        now + Duration::from_millis(100),
        &[PacketKey(0, 0), PacketKey(1, 0), PacketKey(2, 0)],
        &[],
        0,
        1,
    );
    assert_eq!(sample.acked, 1200);
    assert_eq!(sample.lost, 0);
    assert_eq!(sampler.packet_bytes(PacketKey(2, 0)), None);
}

#[test]
fn conservative_overshoot_correction_requires_loss_and_retains_initial_rate_floor() {
    let now = Instant::now();
    let mut bbr = Bbr::new(
        BbrParameters {
            detect_overshooting: true,
            loss_multiplier: 1,
            ..Default::default()
        },
        38_400,
        now,
        1200,
    );
    bbr.transport_min_rtt = Duration::from_millis(100);
    bbr.bandwidth.update(800_000, 1);
    bbr.pacing = 8_000_000;
    bbr.has_unlimited_sample = true;
    bbr.update_pacing(0);
    assert_eq!(bbr.pacing, 8_000_000);
    bbr.update_pacing(1200);
    assert_eq!(bbr.pacing, 3_072_000);
    assert!(!bbr.p.detect_overshooting);
}
