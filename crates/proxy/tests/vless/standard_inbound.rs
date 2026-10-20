use super::*;
use tokio::time::{timeout, Duration};
#[path = "standard_inbound/fixture.rs"]
mod fixture;
use fixture::*;

#[tokio::test]
async fn standard_vless_tcp_uses_one_host_identity_full_auth_and_actual_tls_sni_routing() {
    timeout(Duration::from_secs(10), async {
        for tls in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let target = listener.local_addr().unwrap();
            let worker = tokio::spawn(async move {
                let (mut io, _) = listener.accept().await.unwrap();
                let mut payload = Vec::new();
                io.read_to_end(&mut payload).await.unwrap();
                assert_eq!(payload, b"original payload");
                io.write_all(b"after-half-close").await.unwrap();
                io.shutdown().await.unwrap();
            });
            let fixture = Fixture::new(tls, false);
            let before = fixture.probe.engine().mint_session_identity().unwrap();
            let (mut io, source) = fixture.connect().await;
            let mut request = vless_request_for_ipv4(USER_ID, [127, 0, 0, 1], target.port());
            request.extend_from_slice(b"original payload");
            io.write_all(&request).await.unwrap();
            io.shutdown().await.unwrap();
            let mut response = Vec::new();
            io.read_to_end(&mut response).await.unwrap();
            assert_eq!(
                response, b"\x00\x00after-half-close",
                "one original VLESS header, then reverse payload"
            );
            worker.await.unwrap();
            wait_for("standard VLESS settlement", || {
                !fixture.probe.completed_sessions().is_empty()
            })
            .await;
            let completed = fixture.probe.completed_sessions();
            assert_eq!(completed.len(), 1);
            let record = &completed[0];
            assert_eq!(record.id, before.id().0 + 1);
            assert_eq!(
                fixture
                    .probe
                    .engine()
                    .mint_session_identity()
                    .unwrap()
                    .id()
                    .0,
                record.id + 1
            );
            assert_eq!(record.source_port, Some(source.port()));
            assert_eq!(record.protocol.as_str(), "vless");
            assert_eq!(record.sni.as_deref(), tls.then_some("localhost"));
            assert_eq!((record.bytes_up, record.bytes_down), (16, 16));
            let auth = record.auth.as_ref().unwrap();
            assert_eq!(auth.scheme, "vless");
            assert_eq!(
                auth.principal_key.as_deref(),
                tls.then_some("configured:vless")
            );
            assert_eq!((auth.up_bps, auth.down_bps), (Some(65536), Some(32768)));
            assert_eq!(auth.device_limit, Some(2));
            assert_eq!(auth.quota_remaining_bytes, Some(4096));
            assert_eq!(auth.policy_revision, Some(7));
            let events = fixture
                .probe
                .latest(
                    usize::MAX,
                    EventFilter {
                        event_types: vec![
                            event_type::FLOW_STARTED.into(),
                            event_type::FLOW_COMPLETED.into(),
                        ],
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_eq!(events.len(), 2);
            assert!(events
                .iter()
                .all(|event| event.principal_key.as_deref() == auth.principal_key.as_deref()));
            if tls {
                let event = events
                    .iter()
                    .find(|event| event.event_type == event_type::FLOW_COMPLETED)
                    .unwrap();
                assert_eq!(event.payload["record"]["route"]["matched_rule"]["index"], 0);
                assert_eq!(
                    event.payload["record"]["route"]["matched_rule"]["condition"],
                    "sni: localhost"
                );
            }
            fixture.running.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn standard_vless_tcp_auth_command_rejection_does_not_mint_and_block_has_no_success_header() {
    timeout(Duration::from_secs(5), async {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fixture = Fixture::new(false, true);
        let before = fixture.probe.engine().mint_session_identity().unwrap();
        for invalid_command in [false, true] {
            let (mut io, _) = fixture.connect().await;
            let mut request = vless_request_for_ipv4(
                USER_ID,
                [127, 0, 0, 1],
                target.local_addr().unwrap().port(),
            );
            if invalid_command {
                request[18] = 0xff;
            } else {
                request[1..17].fill(0);
            }
            io.write_all(&request).await.unwrap();
            no_response(&mut io).await;
            assert!(fixture.probe.completed_sessions().is_empty());
        }
        let after = fixture.probe.engine().mint_session_identity().unwrap();
        assert_eq!(after.id().0, before.id().0 + 1);
        let (mut io, _) = fixture.connect().await;
        io.write_all(&vless_request_for_ipv4(
            USER_ID,
            [127, 0, 0, 1],
            target.local_addr().unwrap().port(),
        ))
        .await
        .unwrap();
        no_response(&mut io).await;
        wait_for("standard VLESS blocked settlement", || {
            !fixture.probe.completed_sessions().is_empty()
        })
        .await;
        let completed = fixture.probe.completed_sessions();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].id, after.id().0 + 1);
        assert_eq!((completed[0].bytes_up, completed[0].bytes_down), (0, 0));
        let events = fixture
            .probe
            .latest(
                usize::MAX,
                EventFilter {
                    event_types: vec![event_type::FLOW_COMPLETED.into()],
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload["record"]["result"]["outcome"], "blocked");
        assert!(timeout(Duration::from_millis(20), target.accept())
            .await
            .is_err());
        fixture.running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
