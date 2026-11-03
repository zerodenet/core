//! Background custody uses the original driver, never a synthetic caller.
use super::*;
use crate::contract::component::{ControlledStreamDemandClass, ResourceDeclaration};

#[tokio::test]
async fn background_driver_receipt_requires_its_actual_scope_join_and_original_native_close() {
    let mut fixture = Fixture::cold(1).await;
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let actual = fixture.owner.registration();
    let declaration = ResourceDeclaration::ControlledOutboundStream {
        endpoint: fixture.destination.clone(),
        binding: fixture.transport.binding().clone(),
        demand: ControlledStreamDemandClass::BackgroundControl,
        max_sockets: NonZeroUsize::new(1).unwrap(),
    };
    actual
        .bind_declared(
            &declaration,
            &scope.registrar(),
            &Arc::new(CancellationToken::new()),
        )
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    let mut grant = fixture.owner.grants().reserve().unwrap();
    let view = grant.claim_driver_view().unwrap();
    assert!(grant.claim_user_view().is_err());
    let carrier = fixture
        .transport
        .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
        .unwrap()
        .open(deadline())
        .await
        .unwrap();
    let (mut peer, _) = fixture.listener.accept().await.unwrap();
    let (io, adoption) = carrier.into_parts();
    let (_, witness) = adoption
        .spawn_driver(
            &scope.registrar(),
            TaskClass::Critical,
            io.into_driver(),
            |mut io| async move {
                let _view = view;
                let mut byte = [0; 1];
                io.read_exact(&mut byte).await.unwrap();
                io.write_all(&byte).await.unwrap();
                Ok(())
            },
        )
        .unwrap();
    peer.write_all(b"x").await.unwrap();
    peer.read_exact(&mut [0; 1]).await.unwrap();
    assert!(!witness.is_joined());
    assert!(!adoption
        .retire(Instant::now() + Duration::from_millis(20))
        .await
        .unwrap()
        .is_complete());
    assert!(actual.closed().is_none() && actual.users_closed().is_none());
    scope.next_exit().await.unwrap();
    assert!(witness.is_joined());
    let report = fixture.owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    let native = adoption.closed().unwrap();
    assert!(native.had_native_acquisition());
    assert_eq!(
        native.driver_exit().unwrap().id(),
        witness.try_joined().unwrap().id()
    );
    let users = adoption.users_closed().unwrap();
    assert!(users.had_native_acquisition() && users.matches(&adoption));
    assert!(
        users.caller_exit().is_none(),
        "one real driver, no extra caller task"
    );
    assert!(actual.closed().unwrap().matches(&actual));
    assert!(actual.users_closed().unwrap().matches(&actual));
    assert!(scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn driver_custody_is_unavailable_without_a_registered_background_declaration() {
    let mut fixture = Fixture::cold(1).await;
    fixture.owner.start(deadline()).await.unwrap();
    let mut grant = fixture.owner.grants().reserve().unwrap();
    assert_eq!(
        grant.claim_driver_view().err().unwrap().reason,
        Reason::InvalidConfig
    );
    assert_eq!(
        grant.claim_transferred_view().err().unwrap().reason,
        Reason::InvalidConfig
    );
    drop(grant);
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    for demand in [
        ControlledStreamDemandClass::Service,
        ControlledStreamDemandClass::Business,
    ] {
        let mut fixture = Fixture::cold(1).await;
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        fixture
            .owner
            .registration()
            .bind_declared(
                &ResourceDeclaration::ControlledOutboundStream {
                    endpoint: fixture.destination.clone(),
                    binding: fixture.transport.binding().clone(),
                    demand,
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &scope.registrar(),
                &Arc::new(CancellationToken::new()),
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut grant = fixture.owner.grants().reserve().unwrap();
        assert!(grant.claim_driver_view().is_err());
        assert!(grant.claim_transferred_view().is_err());
        drop(grant);
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
    }
}
