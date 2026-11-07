//! One actual N1 original owner across Service and true admitted Business.
use super::*;
use crate::contract::network::UpstreamOpener;
use crate::controlled_carrier::CarrierBusinessDemand;
use zero_core::component::SessionId;
struct Demand(CancellationToken);
impl CarrierBusinessDemand for Demand {
    fn check(&self) -> Result<(), ChannelError> {
        if self.0.is_cancelled() {
            Err(error(
                instance(),
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ))
        } else {
            Ok(())
        }
    }
    fn session(&self) -> SessionId {
        SessionId(942)
    }
    fn cancellation(&self) -> CancellationToken {
        self.0.clone()
    }
    fn deadline(&self) -> Option<Instant> {
        None
    }
}
#[tokio::test]
async fn taskless_node_carrier_shares_one_original_limit_without_lending_driver_authority() {
    timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::cold(1).await;
        let mut owner_scope = TaskScope::suspended(tokio::runtime::Handle::current(), None);
        owner_scope.enable_registration().unwrap();
        let mut caller = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let host = Arc::new(CancellationToken::new());
        let registration = fixture.owner.registration();
        let descriptor = ResourceDeclaration::ControlledNodeCarrier {
            endpoint: fixture.destination.clone(),
            dial: fixture.transport.binding().dial.clone(),
            max_sockets: NonZeroUsize::new(1).unwrap(),
        };
        let bound = registration
            .bind_declared(&descriptor, &owner_scope.registrar(), &host)
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        assert!(grant.claim_driver_view().is_err());
        let view = grant.claim_transferred_view().unwrap();
        let service = grant.adoption();
        let carrier = fixture
            .transport
            .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
            .unwrap()
            .open(deadline())
            .await
            .unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let (mut io, _) = carrier.into_parts();
        io.write_all(b"service").await.unwrap();
        let mut bytes = [0; 7];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"service");
        assert!(service.retained_driver().is_none());
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        drop(io);
        drop(view);
        assert!(service.retire(deadline()).await.unwrap().is_complete());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(service.closed().unwrap().matches(&service));
        assert!(service.users_closed().unwrap().matches(&service));

        let (finish, finished) = tokio::sync::oneshot::channel();
        let (_, witness) = caller
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                finished.await.unwrap();
                Ok(())
            })
            .unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let business = grant.adoption();
        let view = grant.claim_user_view().unwrap();
        let demand = Arc::new(Demand(CancellationToken::new()));
        let ticket = bound
            .node_business_issuer(
                &host,
                &grant,
                &fixture.destination,
                fixture.transport.binding(),
            )
            .unwrap()
            .authorize_with_caller(demand.clone(), witness.clone())
            .unwrap();
        let native = NativeCarrierOpener::business(ticket, fixture.egress.clone(), None).unwrap();
        let carrier = native
            .prepare_controlled_stream(
                fixture.transport.binding(),
                fixture.destination.clone(),
                CarrierContext {
                    session: Some(SessionId(942)),
                    deadline: None,
                    cancel: demand.0.clone(),
                },
                grant,
            )
            .unwrap()
            .open_with_deadline(None)
            .await
            .unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let (mut io, _) = carrier.into_parts();
        io.write_all(b"business").await.unwrap();
        let mut bytes = [0; 8];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"business");
        assert!(business.retained_driver().is_none());
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        drop(io);
        drop(view);
        assert!(!business
            .retire(Instant::now() + Duration::from_millis(20))
            .await
            .unwrap()
            .is_complete());
        assert!(business.closed().unwrap().matches(&business));
        assert!(business.closed().unwrap().had_native_acquisition());
        assert!(business.users_closed().is_none());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        finish.send(()).unwrap();
        caller.next_exit().await.unwrap();
        let joined = witness.wait_registration_released().await;
        assert!(witness.matches_joined(&joined) && joined.registration_released());
        assert!(business.users_closed().unwrap().matches_caller(&witness));
        assert!(business.retire(deadline()).await.unwrap().is_complete());
        drop(fixture.grants.reserve().unwrap());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(registration.users_closed().unwrap().matches(&registration));
        assert!(owner_scope.shutdown(deadline()).await.is_complete());
        assert!(caller.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn taskless_node_carrier_cannot_forge_a_business_ticket_from_a_service_view_or_foreign_owner()
{
    let mut fixture = Fixture::cold(1).await;
    let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), None);
    scope.enable_registration().unwrap();
    let host = Arc::new(CancellationToken::new());
    let descriptor = ResourceDeclaration::ControlledNodeCarrier {
        endpoint: fixture.destination.clone(),
        dial: fixture.transport.binding().dial.clone(),
        max_sockets: NonZeroUsize::new(1).unwrap(),
    };
    let bound = fixture
        .owner
        .registration()
        .bind_declared(&descriptor, &scope.registrar(), &host)
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    let mut grant = fixture.grants.reserve().unwrap();
    let view = grant.claim_transferred_view().unwrap();
    let mut call = context();
    call.session = Some(SessionId(942));
    let failure = fixture
        .transport
        .prepare_controlled_stream(fixture.destination.clone(), call, grant)
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::StalePlan);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    drop(view);
    assert!(
        timeout(Duration::from_millis(20), fixture.listener.accept())
            .await
            .is_err()
    );
    let mut foreign = Fixture::new(1).await;
    let other = foreign.grants.reserve().unwrap();
    let failure = bound
        .node_business_issuer(
            &host,
            &other,
            &fixture.destination,
            fixture.transport.binding(),
        )
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::StalePlan);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    drop(other);
    assert!(foreign
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}
