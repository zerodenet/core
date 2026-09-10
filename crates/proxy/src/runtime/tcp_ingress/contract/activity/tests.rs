use std::time::{Duration, Instant};

use super::TcpRelayActivity;

#[tokio::test]
async fn idle_wait_completes_after_inactivity() {
    let activity = TcpRelayActivity::new();
    let started = Instant::now();

    tokio::time::timeout(
        Duration::from_secs(1),
        activity.wait_for_idle(Duration::from_millis(40)),
    )
    .await
    .expect("idle wait should complete");

    assert!(started.elapsed() >= Duration::from_millis(40));
}

#[tokio::test]
async fn activity_refreshes_idle_deadline() {
    let activity = TcpRelayActivity::new();
    let waiting = activity.clone();
    let mut idle_wait = tokio::spawn(async move {
        waiting.wait_for_idle(Duration::from_millis(500)).await;
    });

    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        activity.touch();
    }

    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut idle_wait)
            .await
            .is_err(),
        "activity should extend the original idle deadline"
    );
    tokio::time::timeout(Duration::from_millis(300), idle_wait)
        .await
        .expect("idle wait should complete after refreshed deadline")
        .expect("idle wait task should not panic");
}
