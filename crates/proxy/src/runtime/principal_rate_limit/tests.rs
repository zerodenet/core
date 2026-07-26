#[cfg(feature = "udp-runtime")]
use std::time::Duration;

use zero_core::{Address, Network, ProtocolType, Session, SessionAuth};

use super::PrincipalRateLimitRegistry;
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_flow::rate_limit::UdpFlowRateLimiters;

fn session(
    network: Network,
    principal_key: Option<&str>,
    policy_revision: Option<u64>,
    upload_bps: Option<u64>,
    download_bps: Option<u64>,
) -> Session {
    let mut session = Session::new(
        1,
        Address::Domain("example.test".to_owned()),
        443,
        network,
        ProtocolType::UNKNOWN,
    );
    session.up_bps = upload_bps;
    session.down_bps = download_bps;
    if let Some(principal_key) = principal_key {
        let mut auth = SessionAuth::new("test");
        auth.principal_key = Some(principal_key.to_owned());
        auth.policy_revision = policy_revision;
        auth.up_bps = upload_bps;
        auth.down_bps = download_bps;
        session.auth = Some(auth);
    }
    session
}

#[tokio::test]
#[cfg(feature = "udp-runtime")]
async fn same_principal_policy_shares_upload_debt_across_tcp_and_udp() {
    let registry = PrincipalRateLimitRegistry::default();
    let tcp = registry.acquire(&session(
        Network::Tcp,
        Some("tenant:user:7"),
        Some(9),
        Some(1),
        None,
    ));
    let udp = registry.acquire(&session(
        Network::Udp,
        Some("tenant:user:7"),
        Some(9),
        Some(1),
        None,
    ));
    let tcp_upload = tcp.upload().expect("TCP upload limiter");
    let udp = UdpFlowRateLimiters::new(udp);
    tcp_upload.throttle(16 * 1024).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(5), udp.throttle_upload(16 * 1024))
            .await
            .is_err(),
        "UDP traffic did not observe upload debt created by TCP"
    );
}

#[test]
fn upload_and_download_share_by_direction_but_not_with_each_other() {
    let registry = PrincipalRateLimitRegistry::default();
    let first = registry.acquire(&session(
        Network::Tcp,
        Some("tenant:user:7"),
        Some(9),
        Some(10),
        Some(20),
    ));
    let second = registry.acquire(&session(
        Network::Udp,
        Some("tenant:user:7"),
        Some(9),
        Some(10),
        Some(20),
    ));

    assert!(first
        .upload()
        .expect("first upload")
        .shares_timeline_with(&second.upload().expect("second upload")));
    assert!(first
        .download()
        .expect("first download")
        .shares_timeline_with(&second.download().expect("second download")));
    assert!(!first
        .upload()
        .expect("upload")
        .shares_timeline_with(&first.download().expect("download")));
}

#[test]
fn policy_revision_or_rate_change_starts_a_new_timeline() {
    let registry = PrincipalRateLimitRegistry::default();
    let baseline = registry.acquire(&session(
        Network::Tcp,
        Some("tenant:user:7"),
        Some(9),
        Some(10),
        None,
    ));
    let next_revision = registry.acquire(&session(
        Network::Tcp,
        Some("tenant:user:7"),
        Some(10),
        Some(10),
        None,
    ));
    let next_rate = registry.acquire(&session(
        Network::Tcp,
        Some("tenant:user:7"),
        Some(9),
        Some(20),
        None,
    ));
    let baseline = baseline.upload().expect("baseline upload");

    assert!(!baseline.shares_timeline_with(&next_revision.upload().expect("next revision upload")));
    assert!(!baseline.shares_timeline_with(&next_rate.upload().expect("next rate upload")));
}

#[test]
fn anonymous_sessions_keep_independent_timelines() {
    let registry = PrincipalRateLimitRegistry::default();
    let first = registry.acquire(&session(Network::Tcp, None, None, Some(10), None));
    let second = registry.acquire(&session(Network::Tcp, None, None, Some(10), None));

    assert!(!first
        .upload()
        .expect("first upload")
        .shares_timeline_with(&second.upload().expect("second upload")));
}

#[test]
fn zero_and_absent_limits_are_unlimited() {
    let registry = PrincipalRateLimitRegistry::default();
    for session in [
        session(Network::Tcp, Some("tenant:user:7"), Some(9), None, None),
        session(
            Network::Tcp,
            Some("tenant:user:7"),
            Some(9),
            Some(0),
            Some(0),
        ),
    ] {
        let limiters = registry.acquire(&session);
        assert!(limiters.upload().is_none());
        assert!(limiters.download().is_none());
    }
}

#[test]
fn final_policy_lease_removes_the_registry_entry() {
    let registry = PrincipalRateLimitRegistry::default();
    let first = registry.acquire(&session(
        Network::Tcp,
        Some("tenant:user:7"),
        Some(9),
        Some(10),
        None,
    ));
    let second = first.clone();
    assert_eq!(
        registry.inner.entries.lock().expect("registry lock").len(),
        1
    );

    drop(first);
    assert_eq!(
        registry.inner.entries.lock().expect("registry lock").len(),
        1
    );

    drop(second);
    assert!(
        registry
            .inner
            .entries
            .lock()
            .expect("registry lock")
            .is_empty(),
        "last policy lease left a stale registry key"
    );
}
