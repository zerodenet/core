use super::*;

#[tokio::test]
async fn fresh_acceptors_and_replaced_profiles_keep_authenticated_tcp2022_history() {
    timeout(Duration::from_secs(5), async {
        let case = cases()[1];
        let budget = crate::shared::ReplayBudget::new(Some(2)).unwrap();
        let store = crate::ShadowsocksInboundProfileStore::with_replay_budget(budget.clone());
        let users = [ShadowsocksInboundUserRef {
            password: case.user,
            principal_key: Some("facts:user"),
            up_bps: None,
            down_bps: None,
            device_limit: None,
            quota_remaining_bytes: None,
            policy_revision: Some(1),
        }];
        let prepare = |capacity| {
            store
                .prepare_with_identity(
                    "server",
                    case.method,
                    None,
                    &users,
                    ReplayPolicy::Reject,
                    StateLimits {
                        tcp_replay_capacity: Some(capacity),
                        ..Default::default()
                    },
                )
                .unwrap()
                .with_replay_budget_limit(2)
                .unwrap()
        };
        let profile = prepare(1).activate();
        let first = ShadowsocksInboundTcpAcceptor::new(profile.clone());
        let (bytes, _) = request(case, 41);
        let (server, _client) = connect_wire(&bytes).await;
        drop(first.accept_stream_facts(server).await.unwrap());
        let second = ShadowsocksInboundTcpAcceptor::new(profile);
        let (server, _client) = connect_wire(&bytes).await;
        assert!(second
            .accept_stream_facts(server)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("replay"));
        let replacement = ShadowsocksInboundTcpAcceptor::new(prepare(2).activate());
        let (server, _client) = connect_wire(&bytes).await;
        assert!(replacement
            .accept_stream_facts(server)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("replay"));
        let (fresh, _) = request(case, 42);
        let (server, _client) = connect_wire(&fresh).await;
        drop(replacement.accept_stream_facts(server).await.unwrap());
        assert_eq!(budget.retained(), 2);
        let outbound =
            crate::transport::ShadowsocksTransportState::with_replay_budget(budget.clone());
        let options = outbound.replay_component_options(ReplayPolicy::Reject, Some(3));
        assert!(
            options.pool().unwrap().check_and_insert(&[43; 16]).is_err(),
            "outbound shares the actual inbound total"
        );
        assert!(store
            .prepare_with_identity(
                "server",
                case.method,
                None,
                &users,
                ReplayPolicy::Reject,
                StateLimits::default()
            )
            .unwrap()
            .with_replay_budget_limit(1)
            .is_err());
        assert_eq!(
            budget.retained(),
            2,
            "failed shrink must not evict either accepted salt"
        );
    })
    .await
    .unwrap();
}
