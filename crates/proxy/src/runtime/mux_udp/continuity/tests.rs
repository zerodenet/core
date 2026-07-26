use std::time::Duration;

use zero_config::RuntimeConfig;
use zero_core::{
    Address, InboundUdpDispatch, Network, ProtocolType, Session, SessionAuth, UdpContinuityKey,
};
use zero_engine::SessionOutcome;

use super::{
    MuxUdpContinuityAttach, MuxUdpContinuityRegistry, MuxUdpContinuityScope,
    MuxUdpDetachedCancellation,
};
use crate::runtime::udp_ingress::UdpIngressRuntime;

fn scope(principal: &str) -> MuxUdpContinuityScope {
    MuxUdpContinuityScope::new(
        "vless-in",
        "vless",
        Some(principal),
        UdpContinuityKey::from_bytes(&[1, 2, 3, 4]).expect("continuity key"),
    )
}

#[test]
fn registry_rejects_live_conflict_and_advances_generation_after_detach() {
    let registry = MuxUdpContinuityRegistry::<u64>::default();
    let scope = scope("user-1");
    let retention = Duration::from_secs(60);

    assert!(matches!(
        registry.attach(scope.clone(), retention),
        MuxUdpContinuityAttach::New { generation: 1 }
    ));
    assert!(matches!(
        registry.attach(scope.clone(), retention),
        MuxUdpContinuityAttach::Conflict { generation: 1 }
    ));
    assert!(registry.detach(&scope, 1, retention, None).is_ok());
    assert!(matches!(
        registry.attach(scope, retention),
        MuxUdpContinuityAttach::Reattached {
            generation: 2,
            dispatch: None
        }
    ));
}

#[test]
fn registry_scopes_the_same_wire_key_by_principal() {
    let registry = MuxUdpContinuityRegistry::<u64>::default();
    let retention = Duration::from_secs(60);

    assert!(matches!(
        registry.attach(scope("user-1"), retention),
        MuxUdpContinuityAttach::New { .. }
    ));
    assert!(matches!(
        registry.attach(scope("user-2"), retention),
        MuxUdpContinuityAttach::New { .. }
    ));
    assert_eq!(registry.snapshot().attached, 2);
    assert_eq!(registry.snapshot().retained, 0);
}

#[test]
fn expired_detached_entry_is_pruned_and_does_not_reattach() {
    let registry = MuxUdpContinuityRegistry::<u64>::default();
    let scope = scope("user-1");

    assert!(matches!(
        registry.attach(scope.clone(), Duration::ZERO),
        MuxUdpContinuityAttach::New { generation: 1 }
    ));
    assert!(registry.detach(&scope, 1, Duration::ZERO, None).is_ok());
    assert_eq!(registry.snapshot().retained, 1);
    assert_eq!(registry.prune_expired().removed, 1);
    assert!(matches!(
        registry.attach(scope, Duration::ZERO),
        MuxUdpContinuityAttach::New { generation: 1 }
    ));
}

#[test]
fn explicit_finish_removes_the_generation_without_retention() {
    let registry = MuxUdpContinuityRegistry::<u64>::default();
    let scope = scope("user-1");
    let retention = Duration::from_secs(60);

    assert!(matches!(
        registry.attach(scope.clone(), retention),
        MuxUdpContinuityAttach::New { generation: 1 }
    ));
    assert!(registry.finish(&scope, 1));
    assert_eq!(registry.snapshot().attached, 0);
    assert_eq!(registry.snapshot().retained, 0);
    assert!(matches!(
        registry.attach(scope, retention),
        MuxUdpContinuityAttach::New { generation: 1 }
    ));
}

#[test]
fn detached_runtime_state_moves_to_the_next_transport_generation() {
    let registry = MuxUdpContinuityRegistry::<u64>::default();
    let scope = scope("user-1");
    let retention = Duration::from_secs(60);

    assert!(matches!(
        registry.attach(scope.clone(), retention),
        MuxUdpContinuityAttach::New { generation: 1 }
    ));
    assert!(registry.detach(&scope, 1, retention, Some(0x5eed)).is_ok());

    assert!(matches!(
        registry.attach(scope, retention),
        MuxUdpContinuityAttach::Reattached {
            generation: 2,
            dispatch: Some(0x5eed)
        }
    ));
}

#[test]
fn expiry_returns_detached_runtime_state_for_settlement() {
    let registry = MuxUdpContinuityRegistry::<u64>::default();
    let scope = scope("user-1");

    assert!(matches!(
        registry.attach(scope.clone(), Duration::ZERO),
        MuxUdpContinuityAttach::New { generation: 1 }
    ));
    assert!(registry.detach(&scope, 1, Duration::ZERO, Some(73)).is_ok());

    let pruned = registry.prune_expired();
    assert_eq!(pruned.removed, 1);
    assert_eq!(pruned.dispatches, vec![73]);
}

#[test]
fn scheduled_expiry_removes_only_the_matching_detached_generation() {
    let registry = MuxUdpContinuityRegistry::<u64>::default();
    let scope = scope("user-1");

    assert!(matches!(
        registry.attach(scope.clone(), Duration::ZERO),
        MuxUdpContinuityAttach::New { generation: 1 }
    ));
    assert!(registry.detach(&scope, 1, Duration::ZERO, Some(91)).is_ok());
    assert_eq!(registry.expire(&scope, 1), Some(91));
    assert_eq!(registry.snapshot().retained, 0);
}

#[test]
fn stale_expiry_cannot_remove_a_reattached_generation() {
    let registry = MuxUdpContinuityRegistry::<u64>::default();
    let scope = scope("user-1");

    assert!(matches!(
        registry.attach(scope.clone(), Duration::ZERO),
        MuxUdpContinuityAttach::New { generation: 1 }
    ));
    assert!(registry
        .detach(&scope, 1, Duration::ZERO, Some(101))
        .is_ok());
    assert!(matches!(
        registry.attach(scope.clone(), Duration::from_secs(60)),
        MuxUdpContinuityAttach::Reattached {
            generation: 2,
            dispatch: Some(101)
        }
    ));

    assert_eq!(registry.expire(&scope, 1), None);
    assert_eq!(registry.snapshot().attached, 1);
}

#[tokio::test]
async fn detached_authenticated_dispatch_settles_on_principal_cancellation() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let engine = proxy.engine().clone();
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let mut dispatch = runtime
        .new_dispatch("vless-in")
        .await
        .expect("create UDP dispatch");
    let sink = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP sink");
    let sink_port = sink.local_addr().expect("sink address").port();
    let mut auth = SessionAuth::new("vless");
    auth.principal_key = Some("account:1".to_owned());
    runtime
        .dispatch_inbound_packet(
            &mut dispatch,
            &InboundUdpDispatch::new(
                ProtocolType::new("vless"),
                Address::Ipv4([127, 0, 0, 1]),
                sink_port,
                b"tracked".to_vec(),
                None,
            ),
            Some(&auth),
            None,
        )
        .await
        .expect("start authenticated UDP flow");
    let active = engine.active_sessions();
    assert_eq!(active.len(), 1);
    let session_id = active[0].id;

    let registry = MuxUdpContinuityRegistry::default();
    let scope = scope("account:1");
    let retention = Duration::from_secs(60);
    assert!(matches!(
        registry.attach(scope.clone(), retention),
        MuxUdpContinuityAttach::New { generation: 1 }
    ));
    assert!(registry
        .detach(&scope, 1, retention, Some(dispatch))
        .is_ok());

    assert_eq!(
        engine.close_principal_flows("account:1", "principal_disabled"),
        vec![session_id]
    );
    let dispatch = match registry.poll_detached_cancellation(&scope, 1) {
        MuxUdpDetachedCancellation::Cancelled(dispatch) => *dispatch,
        MuxUdpDetachedCancellation::Retained => panic!("detached dispatch ignored cancellation"),
        MuxUdpDetachedCancellation::Gone => panic!("detached dispatch disappeared"),
    };
    assert!(dispatch.finish_all().is_empty());
    assert!(engine.active_sessions().is_empty());
    assert_eq!(engine.completed_sessions().len(), 1);
}

#[tokio::test]
async fn detached_authenticated_dispatch_settles_when_shared_quota_is_exhausted() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let engine = proxy.engine().clone();
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let mut dispatch = runtime
        .new_dispatch("vless-in")
        .await
        .expect("create UDP dispatch");
    let sink = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP sink");
    let sink_port = sink.local_addr().expect("sink address").port();
    let mut auth = SessionAuth::new("vless");
    auth.principal_key = Some("account:1".to_owned());
    auth.policy_revision = Some(1);
    auth.quota_remaining_bytes = Some(12);
    runtime
        .dispatch_inbound_packet(
            &mut dispatch,
            &InboundUdpDispatch::new(
                ProtocolType::new("vless"),
                Address::Ipv4([127, 0, 0, 1]),
                sink_port,
                b"tracked".to_vec(),
                None,
            ),
            Some(&auth),
            None,
        )
        .await
        .expect("start quota-limited UDP flow");
    let active = engine.active_sessions();
    assert_eq!(active.len(), 1);
    let detached_session_id = active[0].id;
    assert_eq!(active[0].bytes_up, 7);

    let registry = MuxUdpContinuityRegistry::default();
    let scope = scope("account:1");
    let retention = Duration::from_secs(60);
    assert!(matches!(
        registry.attach(scope.clone(), retention),
        MuxUdpContinuityAttach::New { generation: 1 }
    ));
    assert!(registry
        .detach(&scope, 1, retention, Some(dispatch))
        .is_ok());

    let mut concurrent = Session::new(
        0,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    concurrent.apply_auth(auth);
    engine
        .prepare_session(&mut concurrent, "vless-in")
        .expect("admit concurrent session against shared quota");
    let concurrent_session_id = concurrent.id;
    let mut concurrent_handle = engine.track_session(concurrent_session_id);

    engine.record_session_inbound_rx(concurrent_session_id, 5);
    assert!(concurrent_handle.is_cancelled());
    assert_eq!(
        concurrent_handle.cancellation_reason().as_deref(),
        Some("quota_exhausted")
    );

    let dispatch = match registry.poll_detached_cancellation(&scope, 1) {
        MuxUdpDetachedCancellation::Cancelled(dispatch) => *dispatch,
        MuxUdpDetachedCancellation::Retained => {
            panic!("detached dispatch ignored shared quota exhaustion")
        }
        MuxUdpDetachedCancellation::Gone => panic!("detached dispatch disappeared"),
    };
    assert!(dispatch.finish_all().is_empty());
    let completed = engine.completed_sessions();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].id, detached_session_id);
    assert_eq!(completed[0].outcome, SessionOutcome::Cancelled);
    assert_eq!(
        completed[0].close_reason.as_deref(),
        Some("quota_exhausted")
    );
    assert_eq!(completed[0].bytes_up, 7);

    concurrent_handle
        .finish_with_reason(
            SessionOutcome::Cancelled,
            concurrent_handle.cancellation_reason(),
        )
        .expect("finish quota-consuming session");
    assert!(engine.active_sessions().is_empty());
    assert_eq!(engine.completed_sessions().len(), 2);
}
