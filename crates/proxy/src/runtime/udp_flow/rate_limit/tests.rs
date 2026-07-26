use std::time::Duration;

use zero_core::{Address, Network, ProtocolType, Session};

use super::UdpFlowRateLimiters;
use crate::runtime::principal_rate_limit::PrincipalRateLimitRegistry;

fn limited_session(up_bps: Option<u64>, down_bps: Option<u64>) -> Session {
    let mut session = Session::new(
        1,
        Address::Domain("example.test".to_owned()),
        443,
        Network::Udp,
        ProtocolType::UNKNOWN,
    );
    session.up_bps = up_bps;
    session.down_bps = down_bps;
    session
}

fn limiters(session: &Session) -> UdpFlowRateLimiters {
    UdpFlowRateLimiters::new(PrincipalRateLimitRegistry::default().acquire(session))
}

#[tokio::test]
async fn udp_upload_clones_share_one_rate_limit_timeline() {
    let limiters = limiters(&limited_session(Some(1), None));
    let cloned = limiters.clone();

    assert!(limiters.throttle_upload(16 * 1024).await);
    assert!(
        tokio::time::timeout(Duration::from_millis(5), cloned.throttle_upload(16 * 1024))
            .await
            .is_err(),
        "cloned UDP flow limiter did not retain upload debt"
    );
}

#[tokio::test]
async fn udp_upload_and_download_have_independent_timelines() {
    let limiters = limiters(&limited_session(Some(1), Some(1)));

    assert!(limiters.throttle_upload(16 * 1024).await);
    let admitted = tokio::time::timeout(
        Duration::from_millis(100),
        limiters.throttle_download(16 * 1024),
    )
    .await
    .expect("upload debt incorrectly throttled the download direction");
    assert!(admitted);
}

#[tokio::test]
async fn cancelling_udp_flow_wakes_a_rate_limited_packet() {
    let limiters = limiters(&limited_session(Some(1), None));
    assert!(limiters.throttle_upload(16 * 1024).await);

    let waiting = limiters.clone();
    let task = tokio::spawn(async move { waiting.throttle_upload(16 * 1024).await });
    tokio::task::yield_now().await;
    limiters.cancel();

    assert!(
        !tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .expect("cancelled UDP limiter remained asleep")
            .expect("UDP limiter task panicked"),
        "cancelled UDP limiter admitted the pending packet"
    );
}

#[tokio::test]
async fn udp_datagram_larger_than_the_burst_is_eventually_admitted() {
    let limiters = limiters(&limited_session(Some(1_000_000), None));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            limiters.throttle_upload(32 * 1024),
        )
        .await
        .expect("large UDP datagram remained permanently over burst"),
        "large UDP datagram was cancelled unexpectedly"
    );
}

#[tokio::test]
async fn absent_or_zero_udp_limits_do_not_delay_packets() {
    for session in [
        limited_session(None, None),
        limited_session(Some(0), Some(0)),
    ] {
        let limiters = limiters(&session);
        let admitted = tokio::time::timeout(Duration::from_millis(100), async {
            limiters.throttle_upload(64 * 1024).await && limiters.throttle_download(64 * 1024).await
        })
        .await
        .expect("unlimited UDP flow was delayed");
        assert!(admitted);
    }
}
