use super::bandwidth::upload_with_windows;

#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn adaptive_receive_windows_remove_small_window_stalls_on_high_rtt_links() {
    let mut adaptive_rates = Vec::new();
    for zero_sender in [false, true] {
        let fixed = upload_with_windows(zero_sender, false).await;
        let adaptive = upload_with_windows(zero_sender, true).await;
        // The receiving peer owns the flow-control window being compared.
        eprintln!(
            "HY2 windows receiver={} fixed={fixed:?} adaptive={adaptive:?} improvement={:.3}",
            if zero_sender { "official" } else { "zero" },
            adaptive.goodput() / fixed.goodput()
        );
        assert!(
            adaptive.goodput() > fixed.goodput() * 1.5,
            "small receive window still stalls transfer"
        );
        adaptive_rates.push(adaptive.goodput());
    }
    let ratio = adaptive_rates[0] / adaptive_rates[1];
    assert!(
        (0.7..=1.3).contains(&ratio),
        "adaptive receiver throughput differs: {adaptive_rates:?}"
    );
}
