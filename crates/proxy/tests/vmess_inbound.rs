#![cfg(feature = "vmess")]
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

const USER_ID: &str = "eeb6c16e-9257-47cf-a7b9-d01e05259c14";

#[path = "vmess_inbound/fixture.rs"]
mod fixture;
use fixture::*;
#[path = "vmess_inbound/reload.rs"]
mod reload;

#[tokio::test]
async fn standard_vmess_tcp_preserves_host_identity_full_auth_payload_and_reverse_half_close() {
    timeout(Duration::from_secs(10), async {
        for principal in [None, Some("configured:vmess")] {
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
            let (mut client, source) = vmess_client(&fixture, address).await;
            client.write_all(b"original payload").await.unwrap();
            client.shutdown().await.unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert_eq!(
                response, b"after-half-close",
                "original VMess response framing decodes exactly once"
            );
            worker.await.unwrap();
            wait_for("standard VMess settlement", || {
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
            assert_eq!(record.protocol.as_str(), "vmess");
            assert_eq!((record.bytes_up, record.bytes_down), (16, 16));
            let auth = record.auth.as_ref().unwrap();
            assert_eq!(auth.scheme, "vmess-uuid");
            assert_eq!(
                auth.principal_key.as_deref(),
                Some(principal.unwrap_or("eeb6c16e925747cfa7b9d01e05259c14"))
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
async fn standard_vmess_auth_rejection_never_mints_and_block_or_refusal_adds_no_body() {
    timeout(Duration::from_secs(10), async {
        for blocked in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let unavailable = tokio::net::TcpSocket::new_v4().unwrap();
            unavailable.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let target = if blocked {
                listener.local_addr().unwrap()
            } else {
                unavailable.local_addr().unwrap()
            };
            let fixture = Fixture::new(Some("configured:vmess"), blocked);
            let before = fixture.probe.engine().mint_session_identity().unwrap();
            let (mut rejected, _) = fixture.connect().await;
            rejected.write_all(&[0; 16]).await.unwrap();
            rejected.shutdown().await.unwrap();
            let mut bytes = Vec::new();
            let _ = rejected.read_to_end(&mut bytes).await;
            assert!(bytes.is_empty());
            assert!(fixture.probe.completed_sessions().is_empty());
            let after = fixture.probe.engine().mint_session_identity().unwrap();
            assert_eq!(after.id().0, before.id().0 + 1);
            #[cfg(target_os = "macos")]
            drop(unavailable); // Release only when the real refusal dial is about to start.
            let (mut client, _) = vmess_client(&fixture, target).await;
            let mut response = Vec::new();
            let _ = client.read_to_end(&mut response).await;
            assert!(
                response.is_empty(),
                "VMess authentication reply must not become business bytes"
            );
            wait_for("standard VMess rejection settlement", || {
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
            let outcome = &events[0].payload["record"]["result"]["outcome"];
            if blocked {
                assert_eq!(outcome, "blocked");
            } else {
                assert_ne!(outcome, "direct_relayed");
            }
            assert!(timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err());
            fixture.running.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}
