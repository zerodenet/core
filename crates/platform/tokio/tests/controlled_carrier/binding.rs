use super::*;
use crate::contract::component::{ControlledStreamDemandClass, ResourceDeclaration};

fn declaration(fixture: &Fixture, demand: ControlledStreamDemandClass) -> ResourceDeclaration {
    ResourceDeclaration::ControlledOutboundStream {
        endpoint: fixture.destination.clone(),
        binding: fixture.transport.binding().clone(),
        demand,
        max_sockets: NonZeroUsize::new(1).unwrap(),
    }
}
#[tokio::test]
async fn cold_binding_compares_complete_description_actual_owner_and_original_scope() {
    let mut fixture = Fixture::cold(1).await;
    let host_cancel = Arc::new(CancellationToken::new());
    let mut other = Fixture::cold(1).await;
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut foreign = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let registration = fixture.owner.registration();
    let description = declaration(&fixture, ControlledStreamDemandClass::Service);
    let binding = registration
        .bind_declared(&description, &scope.registrar(), &host_cancel)
        .unwrap();
    assert!(binding.matches_registration(&registration));
    assert!(!binding.matches_registration(&other.owner.registration()));
    assert!(binding.matches_declaration(&description));
    assert!(binding.scope_identity().matches(&scope.registrar()));
    assert!(!binding.scope_identity().matches(&foreign.registrar()));
    assert!(registration
        .bind_declared(&description, &scope.registrar(), &host_cancel)
        .unwrap()
        .matches_registration(&registration));
    assert!(registration
        .bind_declared(&description, &foreign.registrar(), &host_cancel)
        .is_err());
    let mut changed = description.clone();
    let ResourceDeclaration::ControlledOutboundStream {
        binding: ref mut changed_binding,
        ..
    } = changed
    else {
        unreachable!()
    };
    changed_binding.dial.source_ip = Some("127.0.0.2".parse().unwrap());
    assert!(!binding.matches_declaration(&changed));
    assert!(registration
        .bind_declared(&changed, &scope.registrar(), &host_cancel)
        .is_err());
    fixture.owner.start(deadline()).await.unwrap();
    assert!(registration
        .bind_declared(&description, &scope.registrar(), &host_cancel)
        .is_err());
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(other
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
    assert!(foreign.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn bound_native_carrier_rejects_foreign_scope_before_building_its_real_driver() {
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::cold(1).await;
        let host_cancel = Arc::new(CancellationToken::new());
        let mut actual = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let mut foreign = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let registration = fixture.owner.registration();
        registration
            .bind_declared(
                &declaration(&fixture, ControlledStreamDemandClass::Service),
                &actual.registrar(),
                &host_cancel,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let (carrier, mut peer) = fixture.connected().await;
        let (io, adoption) = carrier.into_parts();
        let built = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = built.clone();
        let failure = adoption
            .spawn_driver(
                &foreign.registrar(),
                TaskClass::Auxiliary,
                io.into_driver(),
                move |_io| {
                    observed.store(true, std::sync::atomic::Ordering::SeqCst);
                    async { Ok(()) }
                },
            )
            .err()
            .unwrap();
        assert_eq!(failure.reason, Reason::StalePlan);
        assert!(!built.load(std::sync::atomic::Ordering::SeqCst));
        assert!(adoption.slot.state().driver.is_none());
        assert!(adoption.closed().is_none());
        assert_eq!(
            peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(adoption.retire(deadline()).await.unwrap().is_complete());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(actual.shutdown(deadline()).await.is_complete());
        assert!(foreign.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn declared_business_registration_cannot_bypass_the_existing_service_only_provider() {
    let mut fixture = Fixture::cold(1).await;
    let host_cancel = Arc::new(CancellationToken::new());
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    fixture
        .owner
        .registration()
        .bind_declared(
            &declaration(&fixture, ControlledStreamDemandClass::Business),
            &scope.registrar(),
            &host_cancel,
        )
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    let no_business = fixture
        .transport
        .prepare_controlled_stream(
            fixture.destination.clone(),
            context(),
            fixture.grants.reserve().unwrap(),
        )
        .err()
        .unwrap();
    assert_eq!(no_business.reason, Reason::StalePlan);
    assert_eq!(no_business.delivery, DeliveryState::NotSubmitted);
    let mut demand = context();
    demand.session = Some(zero_core::component::SessionId(77));
    let failure = fixture
        .transport
        .prepare_controlled_stream(
            fixture.destination.clone(),
            demand,
            fixture.grants.reserve().unwrap(),
        )
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::StalePlan);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    assert_eq!(fixture.owner.retained(), 0);
    assert!(
        timeout(Duration::from_millis(20), fixture.listener.accept())
            .await
            .is_err()
    );
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}

mod native;

mod revocation;

mod business;
