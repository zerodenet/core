use zero_transport::rate_limit::SharedRateLimiter;

#[test]
fn connection_budgets_are_independent_and_clones_share_capacity() {
    let connection = SharedRateLimiter::new(1);
    let second_stream = connection.clone();
    let other_connection = SharedRateLimiter::new(1);
    let burst = u64::from(SharedRateLimiter::MAX_BURST_BYTES);
    assert!(connection.check_n(burst).is_ok());
    assert!(second_stream.check_n(1).is_err());
    assert!(other_connection.check_n(burst).is_ok());
}

#[tokio::test]
async fn cancelled_transfer_does_not_reserve_future_capacity() {
    let connection = SharedRateLimiter::new(1);
    connection.throttle(16 * 1024).await;
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(5),
        connection.throttle(16 * 1024)
    )
    .await
    .is_err());
    // Cancellation did not charge the second 16 KiB to the shared budget.
    assert!(connection.check_n(1).unwrap_err() < std::time::Duration::from_secs(2));
}
