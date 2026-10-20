use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::component::GrantedMetadataListener;

#[tokio::test]
async fn actual_hosted_quinn_users_and_driver_joins_leave_native_close_with_original_host() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(3));
        let mut native = super::super::native_owner();
        let mut grant = native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let retained = grant.tx.clone();
        let observation = grant.control.take_observation().unwrap();
        let metadata = HostedMetadataSocket::new(
            GrantedMetadataListener {
                scope: scope.registrar().scope_identity(),
                rx: grant.rx,
                tx: grant.tx,
                observation,
            },
            &scope.registrar(),
        )
        .unwrap();
        let (mut owner, mut receiver) = match OwnedEndpointOwner::new_hosted(
            metadata,
            scope.registrar(),
            OwnedEndpointLimits {
                drivers: bound(3),
                users: bound(4),
            },
        ) {
            Ok(value) => value,
            Err(_) => panic!("the actual granting scope matches"),
        };
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
                send.write_all(b"host original").await.unwrap();
                send.finish().unwrap();
            },
            server.accept_uni()
        );
        let mut receive = receive.unwrap();
        let mut payload = [0; 13];
        receive.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"host original");
        drop(send);
        drop(receive);
        drop(client);
        drop(server);
        let (logical, classes) = tokio::join!(
            owner.shutdown_users(Instant::now() + Duration::from_secs(3)),
            collect(&mut scope, &mut receiver, 3)
        );
        assert_eq!(classes.len(), 3);
        assert!(logical.unwrap().users_and_drivers_complete());
        assert_eq!(
            owner
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap_err()
                .reason,
            Reason::Unsupported
        );
        assert_eq!(native.occupied_main(), 1);
        assert!(
            retained.local_addr().is_ok(),
            "logical/task proof did not sign a native close receipt"
        );
        grant.control.close().unwrap();
        let report = native.take_report();
        assert_eq!(report.main.confirmed_closed, 1);
        assert!(report.is_complete());
        assert_eq!(
            retained.local_addr().unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_hosted_metadata_foreign_scope_rejection_returns_original_views_to_host() {
    let scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let foreign = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let mut native = super::super::native_owner();
    let mut grant = native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let observation = grant.control.take_observation().unwrap();
    let rejected = HostedMetadataSocket::new(
        GrantedMetadataListener {
            scope: scope.registrar().scope_identity(),
            rx: grant.rx,
            tx: grant.tx,
            observation,
        },
        &foreign.registrar(),
    )
    .err()
    .unwrap();
    assert_eq!(rejected.error.kind(), io::ErrorKind::PermissionDenied);
    assert!(rejected.grant.tx.local_addr().is_ok());
    assert!(rejected
        .grant
        .observation
        .matches(&rejected.grant.rx, &rejected.grant.tx));
    assert_eq!(native.occupied_main(), 1);
    grant.control.close().unwrap();
    assert_eq!(native.take_report().main.confirmed_closed, 1);
}
