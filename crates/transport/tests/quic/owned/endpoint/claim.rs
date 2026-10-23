use super::*;
use std::task::{Context, Waker};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::component::GrantedMetadataListener;

#[tokio::test]
async fn actual_claimed_quinn_graph_requires_real_stream_release_receiver_acceptance_and_last_view_drop(
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(3));
        let registrar = scope.registrar();
        let mut native = super::super::native_owner();
        let mut grant = native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let retained = grant.tx.clone();
        let (issuer, claim) = grant
            .control
            .claim_views(registrar.scope_identity())
            .unwrap();
        let observation = grant.control.take_observation().unwrap();
        let metadata = HostedMetadataSocket::new(
            GrantedMetadataListener {
                users_claim: None,
                scope: registrar.scope_identity(),
                rx: grant.rx,
                tx: grant.tx,
                observation,
            },
            &registrar,
        )
        .unwrap();
        let (mut owner, mut receiver) = OwnedEndpointOwner::new_hosted_claimed(
            metadata,
            registrar.clone(),
            OwnedEndpointLimits {
                drivers: bound(3),
                users: bound(4),
            },
            claim,
        )
        .ok()
        .unwrap();
        let completion = owner.users_completion().unwrap();
        let (server_config, client_config) = configs();
        let handle = owner
            .start(Default::default(), Some(server_config), Some(client_config))
            .unwrap();
        let local = handle.local_addr().unwrap();
        let (client, server) = tokio::join!(handle.connect(local, "localhost"), handle.accept());
        let client = client.unwrap();
        let server = server.unwrap();
        let mut send = client.open_uni().await.unwrap();
        let (_, receive) = tokio::join!(
            async {
                send.write_all(b"same actual graph").await.unwrap();
                send.finish().unwrap();
            },
            server.accept_uni(),
        );
        let mut receive = receive.unwrap();
        let mut bytes = [0; 17];
        receive.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"same actual graph");
        drop(client);
        drop(server);
        assert_eq!(
            owner.active_users(),
            2,
            "the actual stream wrappers still own leases"
        );
        let mut stopping = Box::pin(owner.shutdown_users(Instant::now() + Duration::from_secs(3)));
        assert!(stopping
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        drop(stopping);
        assert!(completion.try_closed().is_none());
        drop(send);
        drop(receive);
        let (users, classes) = tokio::join!(
            owner.shutdown_users(Instant::now() + Duration::from_secs(3)),
            collect(&mut scope, &mut receiver, 3),
        );
        assert!(users.unwrap().users_and_drivers_complete());
        assert_eq!(classes.len(), 3);
        assert!(
            completion.try_closed().is_none(),
            "the original owner still holds native views"
        );
        drop(owner);
        assert!(
            completion.try_closed().is_none(),
            "an actual retained TX view still exists"
        );
        drop(retained);
        let receipt = completion.try_closed().unwrap();
        assert!(issuer.validates_users(&receipt, &registrar));
        assert_eq!(
            native.occupied_main(),
            1,
            "this receipt is no native-close ACK"
        );
        grant.control.close().unwrap();
        assert_eq!(native.take_report().main.confirmed_closed, 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn claimed_constructor_returns_exact_foreign_claim_without_starting_any_driver() {
    let scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let registrar = scope.registrar();
    let mut first_native = super::super::native_owner();
    let mut first = first_native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let (_, claim) = first
        .control
        .claim_views(registrar.scope_identity())
        .unwrap();
    let mut second_native = super::super::native_owner();
    let mut second = second_native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let observation = second.control.take_observation().unwrap();
    let metadata = HostedMetadataSocket::new(
        GrantedMetadataListener {
            users_claim: None,
            scope: registrar.scope_identity(),
            rx: second.rx,
            tx: second.tx,
            observation,
        },
        &registrar,
    )
    .unwrap();
    let rejected = OwnedEndpointOwner::new_hosted_claimed(
        metadata,
        registrar,
        OwnedEndpointLimits {
            drivers: bound(1),
            users: bound(1),
        },
        claim,
    )
    .err()
    .unwrap();
    assert!(rejected.claim.matches(&first.rx, &first.tx));
    assert!(!rejected.native.matches_claim(&rejected.claim));
    drop(rejected);
    drop(first.rx);
    drop(first.tx);
    first.control.close().unwrap();
    second.control.close().unwrap();
    assert_eq!(first_native.take_report().main.confirmed_closed, 1);
    assert_eq!(second_native.take_report().main.confirmed_closed, 1);
}
