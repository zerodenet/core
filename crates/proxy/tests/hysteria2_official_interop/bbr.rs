use super::bandwidth::upload;
#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn all_bbr_profiles_match_official_on_a_bounded_lossy_link() {
    for profile in ["standard", "conservative", "aggressive"] {
        for loss in [0, 20] {
            let official = upload(false, loss, false, Some(profile)).await;
            let zero = upload(true, loss, false, Some(profile)).await;
            eprintln!("BBR profile={profile} drop_every={loss} official={official:?} zero={zero:?} goodput_ratio={:.3}", zero.goodput() / official.goodput());
            assert!(
                official.official_profile,
                "reference did not select requested BBR profile"
            );
            assert!(
                (0.7..=1.3).contains(&(zero.goodput() / official.goodput())),
                "{profile} whole-transfer throughput differs"
            );
            assert!(
                (0.6..=1.4).contains(&(zero.tail_goodput / official.tail_goodput)),
                "{profile} steady transfer differs"
            );
            assert!(
                zero.first_chunk_seconds <= official.first_chunk_seconds * 2.5 + 0.1,
                "{profile} startup stalled"
            );
            assert!(zero.wire.peak_queue_us <= 200_000 && official.wire.peak_queue_us <= 200_000);
        }
    }
}
