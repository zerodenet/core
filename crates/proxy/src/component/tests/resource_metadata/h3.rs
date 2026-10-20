//! Actual prepared H3 over the same canonical metadata/Scope/user graph.
use super::{canonical::CanonicalMetadata, *};
use bytes::{Buf, Bytes};
use zero_transport::quic::owned::{
    h3::{Http3DriverExit, Http3DriverFailure, PendingHttp3},
    DriverExitReceiver, HostedMetadataSocket, OwnedDriverOrigin, OwnedEndpointLimits,
    OwnedEndpointOwner,
};
#[path = "h3/peer.rs"]
mod peer;
use peer::Peer;

type Connected = (
    CanonicalMetadata,
    OwnedEndpointOwner,
    DriverExitReceiver,
    PendingHttp3,
    zero_platform_tokio::contract::scope::TaskRegistrar,
);
async fn connect(peer: &Peer) -> Connected {
    let (fixture, mut grant, tasks) = CanonicalMetadata::start(loopback(), limits(), n(3)).await;
    let claim = grant.users_claim.take().unwrap();
    let socket = HostedMetadataSocket::new(grant, &tasks).unwrap();
    let (mut owner, receiver) = OwnedEndpointOwner::new_hosted_claimed(
        socket,
        tasks.clone(),
        // Explicit single-request fixture only: one actual connection, H3's
        // control streams, pending accept/open slots, and one request's halves.
        OwnedEndpointLimits {
            drivers: n(3),
            users: n(16),
        },
        claim,
    )
    .ok()
    .unwrap();
    let handle = owner
        .start(Default::default(), None, Some(peer.client.clone()))
        .unwrap();
    let connection = fixture
        .drive(handle.connect(peer.address, "localhost"))
        .await
        .unwrap();
    let pending = owner.prepare_http3(connection).unwrap();
    (fixture, owner, receiver, pending, tasks)
}
async fn receive_all(
    fixture: &CanonicalMetadata,
    owner: &OwnedEndpointOwner,
    receiver: &mut DriverExitReceiver,
) {
    fixture
        .drive(async {
            for task in owner.driver_tasks() {
                receiver.receive(task, deadline()).await.unwrap();
            }
        })
        .await;
}

#[tokio::test]
async fn actual_prepared_h3_wire_sender_clones_and_response_streams_hold_canonical_users_until_real_drop(
) {
    tokio::time::timeout(Duration::from_secs(12), async {
        let peer = Peer::start(false).await;
        let (fixture, owner, mut receiver, mut pending, tasks) = connect(&peer).await;
        let completion = owner.users_completion().unwrap();
        let h3_task = pending.task_id();
        let mut sender = fixture.drive(pending.ready()).await.unwrap();
        assert_eq!(owner.driver_tasks().len(), 3);
        let before = owner.active_users();
        let retained = sender.clone();
        assert_eq!(
            owner.active_users(),
            before,
            "infallible H3 clone borrows the same actual raw connection and its original guard"
        );
        let mut request = fixture
            .drive(
                sender.send_request(
                    http_uri::Request::builder()
                        .method("POST")
                        .uri("https://localhost/prepared")
                        .body(())
                        .unwrap(),
                ),
            )
            .await
            .unwrap();
        fixture
            .drive(async {
                request
                    .send_data(Bytes::from_static(b"actual H3 upload"))
                    .await
                    .unwrap();
                request.finish().await.unwrap();
                assert_eq!(request.recv_response().await.unwrap().status(), 200);
                let mut bytes = Vec::new();
                while let Some(mut chunk) = request.recv_data().await.unwrap() {
                    let len = chunk.remaining();
                    assert!(bytes.len() + len <= 1024, "finite real fixture response");
                    bytes.extend_from_slice(&chunk.copy_to_bytes(len));
                }
                assert_eq!(bytes, b"actual H3 response");
            })
            .await;
        assert!(
            owner.active_users() > 0,
            "actual request/control stream wrappers still own leases"
        );
        assert!(completion.try_closed().is_none());
        owner.begin_shutdown();
        receive_all(&fixture, &owner, &mut receiver).await;
        let delivered = receiver.receive(h3_task, deadline()).await.unwrap();
        assert_eq!(delivered.joined.class(), TaskClass::Auxiliary);
        let body = delivered.body.unwrap();
        assert_eq!(body.origin, OwnedDriverOrigin::Http3);
        assert!(matches!(
            body.http3.as_deref(),
            Some(Http3DriverExit::Cancelled)
        ));
        drop(owner);
        drop(sender);
        assert!(completion.try_closed().is_none(), "actual response stream wrapper still retains its original directions after FIN and driver join");
        drop(request);
        assert!(
            completion.try_closed().is_none(),
            "last actual sender clone still owns the original connection guard"
        );
        drop(retained);
        let receipt = completion.try_closed().unwrap();
        assert!(!fixture.resources().io_evidence().physical_confirmed());
        fixture.confirm_users(receipt, &tasks).unwrap();
        let report = fixture.shutdown().await;
        assert!(report.is_complete());
        let proof = fixture.resources().io_evidence();
        assert_eq!(proof.metadata[0].confirmed_users(), 1);
        assert_eq!(proof.metadata[0].main.confirmed_closed, 1);
        assert!(proof.physical_confirmed());
        peer.finish().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_prepared_h3_remote_failure_keeps_the_same_typed_terminal_arc_until_unique_reception(
) {
    tokio::time::timeout(Duration::from_secs(12), async {
        let peer = Peer::start(true).await;
        let (fixture, owner, mut receiver, mut pending, tasks) = connect(&peer).await;
        let completion = owner.users_completion().unwrap();
        let mut sender = fixture.drive(pending.ready()).await.unwrap();
        let mut request = fixture
            .drive(
                sender.send_request(
                    http_uri::Request::builder()
                        .method("POST")
                        .uri("https://localhost/prepared")
                        .body(())
                        .unwrap(),
                ),
            )
            .await
            .unwrap();
        fixture
            .drive(async {
                request
                    .send_data(Bytes::from_static(b"actual H3 upload"))
                    .await
                    .unwrap();
                request.finish().await.unwrap();
                assert!(request.recv_response().await.is_err());
            })
            .await;
        let delivery = fixture
            .drive(receiver.receive(pending.task_id(), deadline()))
            .await
            .unwrap();
        let body = delivery.body.unwrap();
        assert_eq!(body.origin, OwnedDriverOrigin::Http3);
        let terminal = body.http3.clone().unwrap();
        assert!(
            matches!(terminal.as_ref(), Http3DriverExit::Closed(source) if !source.is_h3_no_error())
        );
        let error = delivery.joined.result().unwrap_err();
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error.as_ref());
        let actual = loop {
            let current = source.expect("real H3 driver source must remain typed");
            if let Some(actual) = current.downcast_ref::<Http3DriverFailure>() {
                break actual;
            }
            source = current.source();
        };
        assert!(Arc::ptr_eq(&actual.0, &terminal));
        assert!(
            completion.try_closed().is_none(),
            "received failure alone did not drop actual request/sender users"
        );
        drop(request);
        drop(sender);
        owner.begin_shutdown();
        receive_all(&fixture, &owner, &mut receiver).await;
        drop(owner);
        fixture
            .confirm_users(completion.try_closed().unwrap(), &tasks)
            .unwrap();
        assert!(fixture.shutdown().await.is_complete());
        assert_eq!(
            fixture.resources().io_evidence().metadata[0]
                .main
                .confirmed_closed,
            1
        );
        peer.finish().await;
    })
    .await
    .unwrap();
}
