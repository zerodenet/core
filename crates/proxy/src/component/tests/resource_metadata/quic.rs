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
        let (fixture, mut grant, tasks) =
            CanonicalMetadata::start(loopback(), limits(), n(2)).await;
        let retained = grant.tx.clone(); // Native diagnostic only, never another producer.
        let claim = grant
            .users_claim
            .take()
            .expect("actual canonical unique claim");
        let socket = HostedMetadataSocket::new(grant, &tasks).unwrap();
        let (mut owner, mut receiver) = match OwnedEndpointOwner::new_hosted_claimed(
            socket,
            tasks.clone(),
            OwnedEndpointLimits {
                drivers: n(2),
                users: n(3),
            },
            claim,
        ) {
            Ok(value) => value,
            Err(_) => panic!("actual host scope matches"),
        };
        let completion = owner.users_completion().unwrap();
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
        owner.begin_shutdown();
        let joined = fixture
            .drive(owner.shutdown_users(deadline()))
            .await
            .unwrap();
        assert!(joined.drivers.unjoined.is_empty());
        assert_eq!(joined.drivers.unreceived.len(), 2);
        let driver_tasks = owner.driver_tasks();
        drop(owner);
        drop(retained);
        assert!(
            completion.try_closed().is_none(),
            "actual joins did not receive either driver outcome"
        );
        fixture
            .drive(async {
                for task in driver_tasks {
                    receiver.receive(task, deadline()).await.unwrap();
                }
            })
            .await;
        let receipt = completion.try_closed().unwrap();
        assert!(fixture
            .resources()
            .confirm_metadata_users(1, &receipt, &tasks)
            .is_err());
        let (foreign, foreign_grant, foreign_tasks) =
            CanonicalMetadata::start(loopback(), limits(), n(1)).await;
        assert!(fixture
            .resources()
            .confirm_metadata_users(0, &receipt, &foreign_tasks)
            .is_err());
        assert!(foreign
            .resources()
            .confirm_metadata_users(0, &receipt, &foreign_tasks)
            .is_err());
        drop(foreign_grant);
        assert!(!foreign.shutdown().await.is_complete());
        fixture.confirm_users(receipt.clone(), &tasks).unwrap();
        fixture.confirm_users(receipt, &tasks).unwrap(); // same proof is idempotent
        assert!(
            !fixture.resources().io_evidence().physical_confirmed(),
            "users proof did not close native originals"
        );
        let report = fixture.shutdown().await;
        assert!(report.is_complete());
        let native = fixture.resources().io_evidence();
        assert_eq!(native.metadata.len(), 1);
        assert!(!native.metadata[0].busy);
        assert_eq!(native.metadata[0].main.confirmed_closed, 1);
        assert_eq!(native.metadata[0].main.live, 0);
        assert!(native.metadata[0].main.unknown.is_empty());
        assert_eq!(native.metadata[0].probes.live, 0);
        assert!(native.metadata[0].probes.unknown.is_empty());
        assert_eq!(native.metadata[0].confirmed_users(), 1);
        assert_eq!(native.metadata[0].unconfirmed_users(), Some(0));
        assert!(native.physical_confirmed());
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
        let (fixture, mut grant, tasks) =
            CanonicalMetadata::start(loopback(), limits(), n(2)).await;
        let retained = grant.tx.clone();
        let claim = grant
            .users_claim
            .take()
            .expect("actual canonical unique claim");
        let socket = HostedMetadataSocket::new(grant, &tasks).unwrap();
        let (mut owner, mut receiver) = match OwnedEndpointOwner::new_hosted_claimed(
            socket,
            tasks.clone(),
            OwnedEndpointLimits {
                drivers: n(2),
                users: n(3),
            },
            claim,
        ) {
            Ok(value) => value,
            Err(_) => panic!("actual host scope matches"),
        };
        let completion = owner.users_completion().unwrap();
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
        assert!(
            completion.try_closed().is_none(),
            "actual pending stream retains three logical permits"
        );
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
        drop(owner);
        assert!(
            completion.try_closed().is_none(),
            "an extra original TX view still exists after all real joins/receptions"
        );
        let partial = fixture.shutdown().await;
        assert!(!partial.is_complete());
        assert_eq!(
            retained.local_addr().unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        assert!(
            completion.try_closed().is_none(),
            "actual FD close is no users/view receipt"
        );
        drop(retained);
        let receipt = completion.try_closed().unwrap();
        assert!(!tasks.scope_identity().registration_open());
        fixture.confirm_users(receipt, &tasks).unwrap(); // real cleanup confirmation while closing
        let report = fixture.shutdown().await;
        assert!(report.is_complete());
        let native = fixture.resources().io_evidence();
        assert_eq!(native.metadata.len(), 1);
        assert!(!native.metadata[0].busy);
        assert_eq!(native.metadata[0].main.confirmed_closed, 1);
        assert_eq!(native.metadata[0].main.live, 0);
        assert!(native.metadata[0].main.unknown.is_empty());
        assert_eq!(native.metadata[0].probes.live, 0);
        assert!(native.metadata[0].probes.unknown.is_empty());
        assert_eq!(native.metadata[0].confirmed_users(), 1);
        assert_eq!(native.metadata[0].unconfirmed_users(), Some(0));
        assert!(native.physical_confirmed());
        peer.finish().await;
    })
    .await
    .unwrap();
}
