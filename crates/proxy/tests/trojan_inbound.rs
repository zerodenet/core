#![cfg(feature = "trojan")]
use crate::support;
use std::sync::Arc;
use support::interop::TempMaterial;
use support::{free_port, spawn_engine, wait_for, wait_for_listener};
use tokio::time::{timeout, Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use zero_api::{event_type, EventFilter, EventSource};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

const PASSWORD: &str = "test-password";
const CMD_TCP: u8 = 1;
const ATYP_IPV4: u8 = 1;

#[path = "trojan_inbound/fixture.rs"]
mod fixture;
use fixture::*;

#[tokio::test]
async fn standard_trojan_tcp_preserves_host_identity_full_auth_payload_and_reverse_half_close() {
    timeout(Duration::from_secs(10), async {
        for principal in [None, Some("configured:trojan")] {
            let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = target.local_addr().unwrap();
            let worker = tokio::spawn(async move {
                let (mut io, _) = target.accept().await.unwrap();
                let mut payload = Vec::new();
                io.read_to_end(&mut payload).await.unwrap();
                assert_eq!(payload, b"original payload");
                io.write_all(b"after-half-close").await.unwrap();
                io.shutdown().await.unwrap();
            });
            let fixture = Fixture::new(principal, false);
            let before = fixture.probe.engine().mint_session_identity().unwrap();
            let (mut client, source) = fixture.connect().await;
            let mut bytes = request(PASSWORD, CMD_TCP, address);
            bytes.extend_from_slice(b"original payload");
            client.write_all(&bytes).await.unwrap();
            client.shutdown().await.unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert_eq!(
                response, b"after-half-close",
                "Trojan adds no success reply bytes"
            );
            worker.await.unwrap();
            wait_for("standard Trojan settlement", || {
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
            assert_eq!(record.protocol.as_str(), "trojan");
            assert_eq!((record.bytes_up, record.bytes_down), (16, 16));
            let auth = record.auth.as_ref().unwrap();
            assert_eq!(auth.scheme, "trojan");
            assert_eq!(
                auth.principal_key.as_deref(),
                Some(principal.unwrap_or(PASSWORD))
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
            fixture.running.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn standard_trojan_tcp_auth_command_rejection_never_mints_and_host_block_adds_no_wire_reply()
{
    timeout(Duration::from_secs(10), async {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let fixture = Fixture::new(Some("configured:trojan"), true);
        let before = fixture.probe.engine().mint_session_identity().unwrap();
        for (password, command) in [("wrong-password", CMD_TCP), (PASSWORD, 0xff)] {
            let (mut client, _) = fixture.connect().await;
            client
                .write_all(&request(password, command, address))
                .await
                .unwrap();
            assert_no_response(&mut client).await;
            assert!(fixture.probe.completed_sessions().is_empty());
        }
        let after_rejections = fixture.probe.engine().mint_session_identity().unwrap();
        assert_eq!(
            after_rejections.id().0,
            before.id().0 + 1,
            "only our probe minted an identity"
        );
        let (mut client, _) = fixture.connect().await;
        client
            .write_all(&request(PASSWORD, CMD_TCP, address))
            .await
            .unwrap();
        assert_no_response(&mut client).await;
        wait_for("standard Trojan blocked settlement", || {
            !fixture.probe.completed_sessions().is_empty()
        })
        .await;
        let completed = fixture.probe.completed_sessions();
        assert_eq!(completed.len(), 1);
        let record = &completed[0];
        assert_eq!(record.id, after_rejections.id().0 + 1);
        assert_eq!(record.protocol.as_str(), "trojan");
        assert_eq!((record.bytes_up, record.bytes_down), (0, 0));
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

#[tokio::test]
async fn standard_trojan_tcp_actual_upstream_refusal_never_adds_success_or_error_bytes() {
    timeout(Duration::from_secs(5), async {
        // Keep the bound, non-listening endpoint owned throughout this test.
        let unavailable = tokio::net::TcpSocket::new_v4().unwrap();
        unavailable.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let fixture = Fixture::new(Some("configured:trojan"), false);
        let before = fixture.probe.engine().mint_session_identity().unwrap();
        let (mut client, _) = fixture.connect().await;
        client
            .write_all(&request(
                PASSWORD,
                CMD_TCP,
                unavailable.local_addr().unwrap(),
            ))
            .await
            .unwrap();
        assert_no_response(&mut client).await;
        wait_for("standard Trojan refused settlement", || {
            !fixture.probe.completed_sessions().is_empty()
        })
        .await;
        let completed = fixture.probe.completed_sessions();
        assert_eq!(completed.len(), 1);
        let record = &completed[0];
        assert_eq!(record.id, before.id().0 + 1);
        assert_eq!((record.bytes_up, record.bytes_down), (0, 0));
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
        assert_ne!(
            events[0].payload["record"]["result"]["outcome"],
            "direct_relayed"
        );
        fixture.running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
