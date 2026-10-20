#![cfg(feature = "shadowsocks")]
use crate::support::{free_port, spawn_engine, wait_for, wait_for_listener};
use shadowsocks::{ShadowsocksAeadStream, ShadowsocksTcpConnectConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Duration},
};
use zero_api::{event_type, EventFilter, EventSource};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::TokioSocket;
use zero_proxy::Proxy;

#[path = "shadowsocks_inbound/fixture.rs"]
mod fixture;
use fixture::*;

#[tokio::test]
async fn standard_ss_tcp_keeps_actual_cipher_state_full_auth_one_identity_and_reverse_half_close() {
    timeout(Duration::from_secs(15), async {
        for case in cases() {
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
            let fixture = Fixture::new(case, false);
            let before = fixture.probe.engine().mint_session_identity().unwrap();
            let (mut client, source, _) = fixture.open(target).await;
            client.write_all(b"original payload").await.unwrap();
            client.shutdown().await.unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert_eq!(
                response, b"after-half-close",
                "{} original response salt/header",
                case.method
            );
            worker.await.unwrap();
            wait_for("standard SS settlement", || {
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
            assert_eq!(record.protocol.as_str(), "shadowsocks");
            assert_eq!((record.bytes_up, record.bytes_down), (16, 16));
            let auth = record.auth.as_ref().unwrap();
            assert_eq!(auth.scheme, "shadowsocks");
            assert_eq!(auth.principal_key.as_deref(), Some("configured:ss"));
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
                .all(|event| event.principal_key.as_deref() == Some("configured:ss")));
            fixture.running.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn standard_ss_tcp_auth_and_same_listener_replay_reject_before_host_identity() {
    timeout(Duration::from_secs(15), async {
        for case in cases().into_iter().filter(|case| case.authenticated()) {
            let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let fixture = Fixture::new(case, false);
            let before = fixture.probe.engine().mint_session_identity().unwrap();
            let destination = target.local_addr().unwrap();
            let (_, mut wire) = fixture.request(destination).await;
            *wire.last_mut().unwrap() ^= 1;
            fixture.reject_wire(&wire).await;
            assert!(fixture.probe.completed_sessions().is_empty());
            assert_eq!(
                fixture
                    .probe
                    .engine()
                    .mint_session_identity()
                    .unwrap()
                    .id()
                    .0,
                before.id().0 + 1
            );
            let worker = tokio::spawn(async move {
                let (mut io, _) = target.accept().await.unwrap();
                let mut payload = Vec::new();
                io.read_to_end(&mut payload).await.unwrap();
                assert_eq!(payload, b"payload");
                io.write_all(b"reply").await.unwrap();
                io.shutdown().await.unwrap();
            });
            let (mut client, _, wire) = fixture.open(destination).await;
            client.write_all(b"payload").await.unwrap();
            client.shutdown().await.unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert_eq!(response, b"reply");
            worker.await.unwrap();
            wait_for("first SS flow completes", || {
                fixture.probe.completed_sessions().len() == 1
            })
            .await;
            let before_replay = fixture.probe.engine().mint_session_identity().unwrap();
            fixture.reject_wire(&wire).await;
            assert_eq!(fixture.probe.completed_sessions().len(), 1);
            assert_eq!(
                fixture
                    .probe
                    .engine()
                    .mint_session_identity()
                    .unwrap()
                    .id()
                    .0,
                before_replay.id().0 + 1
            );
            fixture.running.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn standard_ss_tcp_block_preserves_no_reply_wire_without_connecting_target() {
    timeout(Duration::from_secs(10), async {
        for case in cases().into_iter().filter(|case| case.authenticated()) {
            let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let fixture = Fixture::new(case, true);
            let before = fixture.probe.engine().mint_session_identity().unwrap();
            let (_, wire) = fixture.request(target.local_addr().unwrap()).await;
            fixture.reject_wire(&wire).await;
            wait_for("SS host blocked settlement", || {
                !fixture.probe.completed_sessions().is_empty()
            })
            .await;
            let completed = fixture.probe.completed_sessions();
            assert_eq!(completed.len(), 1);
            assert_eq!(completed[0].id, before.id().0 + 1);
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
            assert_eq!(events[0].payload["record"]["result"]["outcome"], "blocked");
            assert!(timeout(Duration::from_millis(20), target.accept())
                .await
                .is_err());
            fixture.running.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}
