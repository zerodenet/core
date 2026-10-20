//! Actual configured DoQ -> sealed Quinn users -> canonical native host grant.
//! The remote TLS peer is a real socket fixture, not a substitute protocol.
use super::canonical::CanonicalMetadata;
use super::*;
use std::{
    io,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;
use zero_dns::DnsSystem;
use zero_transport::quic::owned::{
    HostedMetadataSocket, OwnedEndpointHandle, OwnedEndpointLimits, OwnedEndpointOwner,
};
#[path = "quic/peer.rs"]
mod peer;
use peer::Peer;

#[path = "quic/provider.rs"]
mod provider;
use provider::Connector;

fn dns(endpoint: SocketAddr) -> DnsSystem {
    let config=serde_json::from_value(serde_json::json!({
        "servers":{"quic":{"type":"doq","host":"localhost","port":endpoint.port(),"bootstrap":[endpoint.ip().to_string()],"server_name":"localhost"}},
        "default_server":"quic",
        "policy":{"timeout_ms":5000}
    })).unwrap();
    DnsSystem::build(Some(&config)).unwrap()
}
async fn finish(
    fixture: &CanonicalMetadata,
    owner: &mut OwnedEndpointOwner,
    receiver: &mut zero_transport::quic::owned::DriverExitReceiver,
) {
    owner.begin_shutdown();
    let tasks = owner.driver_tasks();
    assert_eq!(tasks.len(), 2, "actual Endpoint plus one Connection driver");
    fixture
        .drive(async {
            for task in tasks {
                receiver.receive(task, deadline()).await.unwrap();
            }
            assert!(owner
                .shutdown_users(deadline())
                .await
                .unwrap()
                .users_and_drivers_complete());
        })
        .await;
}

#[tokio::test]
async fn actual_configured_doq_uses_canonical_metadata_scope_tls_framing_and_native_host_receipts()
{
    tokio::time::timeout(Duration::from_secs(12), async {
        let peer = Peer::start(false).await;
        let (fixture, grant, tasks) = CanonicalMetadata::start(loopback(), limits(), n(2)).await;
        let retained = grant.tx.clone(); // Native diagnostic only, never another producer.
        let socket = HostedMetadataSocket::new(grant, &tasks).unwrap();
        let (mut owner, mut receiver) = match OwnedEndpointOwner::new_hosted(
            socket,
            tasks,
            OwnedEndpointLimits {
                drivers: n(2),
                users: n(3),
            },
        ) {
            Ok(value) => value,
            Err(_) => panic!("actual host scope matches"),
        };
        let handle = owner
            .start(Default::default(), None, Some(peer.client.clone()))
            .unwrap();
        let connector = Arc::new(Connector {
            handle,
            peer: peer.address,
            opens: AtomicUsize::new(0),
            pending: Arc::default(),
            released: Arc::default(),
        });
        let resolver = dns(peer.address);
        resolver.set_outbound_connector(connector.clone());
        let answer = fixture
            .drive(resolver.resolve_real_type("canonical.doq.test", 1))
            .await
            .unwrap();
        assert_eq!(answer, vec![zero_traits::IpAddress::V4([198, 51, 100, 91])]);
        connector.released.notified().await;
        assert_eq!(connector.opens.load(Ordering::Acquire), 1);
        assert_eq!(owner.active_users(), 0);
        assert!(retained.local_addr().is_ok());
        finish(&fixture, &mut owner, &mut receiver).await;
        assert!(
            retained.local_addr().is_ok(),
            "actual user/driver exit did not close the host's native original"
        );
        let report = fixture.shutdown().await;
        assert!(
            !report.is_complete(),
            "missing canonical external-user proof remains Unaccounted"
        );
        assert_eq!(
            retained.local_addr().unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        let native = fixture.slot.resources.io_evidence();
        assert_eq!(native.metadata.len(), 1);
        assert!(!native.metadata[0].busy);
        assert_eq!(native.metadata[0].main.confirmed_closed, 1);
        assert_eq!(native.metadata[0].main.live, 0);
        assert!(native.metadata[0].main.unknown.is_empty());
        assert_eq!(native.metadata[0].probes.live, 0);
        assert!(native.metadata[0].probes.unknown.is_empty());
        assert!(
            !native.physical_confirmed(),
            "native receipt is no canonical external-user graph proof"
        );
        peer.finish().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelling_actual_pending_doq_read_releases_users_without_faking_native_close_or_driver_ack(
) {
    tokio::time::timeout(Duration::from_secs(12), async {
        let peer = Peer::start(true).await;
        let (fixture, grant, tasks) = CanonicalMetadata::start(loopback(), limits(), n(2)).await;
        let retained = grant.tx.clone();
        let socket = HostedMetadataSocket::new(grant, &tasks).unwrap();
        let (mut owner, mut receiver) = match OwnedEndpointOwner::new_hosted(
            socket,
            tasks,
            OwnedEndpointLimits {
                drivers: n(2),
                users: n(3),
            },
        ) {
            Ok(value) => value,
            Err(_) => panic!("actual host scope matches"),
        };
        let handle = owner
            .start(Default::default(), None, Some(peer.client.clone()))
            .unwrap();
        let connector = Arc::new(Connector {
            handle,
            peer: peer.address,
            opens: AtomicUsize::new(0),
            pending: Arc::default(),
            released: Arc::default(),
        });
        let resolver = dns(peer.address);
        resolver.set_outbound_connector(connector.clone());
        let mut query = Box::pin(resolver.resolve_real_type("pending.canonical.doq.test", 1));
        fixture
            .drive(async {
                tokio::select! {biased;
                    ()=connector.pending.notified()=>{},
                    result=&mut query=>panic!("actual response read must be pending: {result:?}"),
                }
            })
            .await;
        peer.received.notified().await;
        assert_eq!(owner.active_users(), 3);
        drop(query);
        fixture.drive(connector.released.notified()).await;
        assert_eq!(
            owner.active_users(),
            0,
            "original stream and Connection are released before the notice"
        );
        assert!(
            retained.local_addr().is_ok(),
            "query cancellation is no native receipt"
        );
        assert_eq!(
            owner.driver_status().unreceived.len(),
            2,
            "query Drop did not accept either real driver outcome"
        );
        finish(&fixture, &mut owner, &mut receiver).await;
        let report = fixture.shutdown().await;
        assert!(!report.is_complete());
        assert_eq!(
            retained.local_addr().unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        let native = fixture.slot.resources.io_evidence();
        assert_eq!(native.metadata.len(), 1);
        assert!(!native.metadata[0].busy);
        assert_eq!(native.metadata[0].main.confirmed_closed, 1);
        assert_eq!(native.metadata[0].main.live, 0);
        assert!(native.metadata[0].main.unknown.is_empty());
        assert_eq!(native.metadata[0].probes.live, 0);
        assert!(native.metadata[0].probes.unknown.is_empty());
        assert!(
            !native.physical_confirmed(),
            "native receipt is no canonical external-user graph proof"
        );
        peer.finish().await;
    })
    .await
    .unwrap();
}
