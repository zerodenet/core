use super::*;

#[tokio::test(start_paused = true)]
async fn response_bindings_do_not_limit_the_targets_of_one_association() {
    let mut bindings = Bindings::with_limits(crate::validation::StateLimits {
        udp_capacity: Some(1),
        ..Default::default()
    });
    let client = "127.0.0.1:1234".parse().unwrap();
    for id in 0..8 {
        bindings.record(id, Some(1), client);
    }
    bindings.record(8, None, client);
    assert_eq!(bindings.0.len(), 9);
    tokio::time::advance(IDLE - Duration::from_secs(1)).await;
    bindings.touch(0);
    tokio::time::advance(Duration::from_secs(1)).await;
    bindings.prune();
    assert_eq!(bindings.0.len(), 1);
    assert_eq!(bindings.client(0), Some(client));
    assert_eq!(bindings.client(1), None);
    bindings.record(8, None, client);
    assert_eq!(bindings.0.len(), 2);
}

#[cfg(feature = "blake3")]
#[tokio::test(start_paused = true)]
async fn replay_capacity_never_evicts_a_live_session_and_retains_future_boundary() {
    let mut sessions = ReplaySessions::with_limits(crate::validation::StateLimits {
        udp_capacity: Some(MAX_REPLAY_SESSIONS),
        ..Default::default()
    });
    for id in 0..MAX_REPLAY_SESSIONS as u64 {
        assert!(sessions.accept(id, 0).unwrap());
    }
    assert!(sessions.accept(MAX_REPLAY_SESSIONS as u64, 0).is_err());
    tokio::time::advance(crate::udp::session::RETENTION - Duration::from_secs(1)).await;
    sessions.prune();
    assert!(!sessions.accept(0, 0).unwrap());
    assert!(sessions.accept(MAX_REPLAY_SESSIONS as u64, 0).is_err());
    tokio::time::advance(Duration::from_secs(1)).await;
    sessions.prune();
    assert!(sessions.entries.is_empty());
    assert!(sessions.accept(MAX_REPLAY_SESSIONS as u64, 0).unwrap());
}

#[tokio::test(start_paused = true)]
async fn responder_idle_timer_cleans_state_without_new_packets() {
    use super::super::{
        ShadowsocksInboundUdpCodec, ShadowsocksInboundUdpResponder,
        ShadowsocksInboundUdpResponderMode, ShadowsocksInboundUdpSession,
    };
    let codec = ShadowsocksInboundUdpCodec::new(crate::shared::CipherKind::Aes128Gcm, b"secret");
    let mut responder =
        ShadowsocksInboundUdpResponder::new(ShadowsocksInboundUdpSession::new(codec));
    responder.record_dispatch_success(1, None, "127.0.0.1:1234".parse().unwrap());
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    // Poll the actual idle receive loop; its persistent timer performs pruning.
    let _ = tokio::time::timeout(
        IDLE + MAINTENANCE,
        responder.read_inbound_dispatch_from_socket_tokio(&socket),
    )
    .await;
    match &responder.mode {
        ShadowsocksInboundUdpResponderMode::Single(session) => {
            assert!(!session.bindings.contains(1))
        }
        _ => unreachable!(),
    }
}

#[tokio::test(start_paused = true)]
async fn profile_expiry_removes_response_indexes_and_user_removal_cleans_sessions() {
    use super::super::{ShadowsocksInboundUdpResponder, ShadowsocksInboundUdpResponderMode};
    use zero_traits::DatagramCodec;
    let user = crate::transport::ShadowsocksInboundUserRef {
        password: "secret",
        principal_key: None,
        up_bps: None,
        down_bps: None,
        device_limit: None,
        quota_remaining_bytes: None,
        policy_revision: None,
    };
    let profile =
        crate::inbound::ShadowsocksInboundProfile::from_config_users("aes-128-gcm", [user])
            .unwrap();
    let mut responder = ShadowsocksInboundUdpResponder::from_profile(profile.clone());
    let packet =
        crate::udp::ShadowsocksDatagramCodec::new(crate::shared::CipherKind::Aes128Gcm, b"secret")
            .encode(
                &zero_core::Address::Domain("example.com".into()),
                53,
                b"query",
            )
            .unwrap();
    responder.decode_inbound_dispatch(&packet).unwrap();
    responder.record_dispatch_success(1, None, "127.0.0.1:1234".parse().unwrap());
    tokio::time::advance(IDLE).await;
    responder.maintain();
    if let ShadowsocksInboundUdpResponderMode::Profile { proxy_users, .. } = &responder.mode {
        assert!(proxy_users.is_empty());
    } else {
        unreachable!();
    }
    profile.replace_config_users([]).unwrap();
    tokio::time::advance(MAINTENANCE).await;
    responder.maintain();
    if let ShadowsocksInboundUdpResponderMode::Profile {
        sessions,
        proxy_users,
        ..
    } = &responder.mode
    {
        assert!(sessions.is_empty());
        assert!(proxy_users.is_empty());
    } else {
        unreachable!();
    }
}

#[cfg(feature = "blake3")]
#[test]
fn terminal_packet_does_not_consume_inbound_capacity() {
    let mut sessions = ReplaySessions::with_limits(crate::validation::StateLimits {
        udp_capacity: Some(1),
        ..Default::default()
    });
    assert!(!sessions.accept(1, u64::MAX).unwrap());
    assert!(sessions.accept(2, 0).unwrap());
}

#[cfg(feature = "blake3")]
mod identity;
