//! Same original fixed Service owner/caller, with no independent TCP driver.
use super::*;
use crate::contract::component::{ControlledStreamDemandClass, ResourceDeclaration};

pub(super) fn description(
    fixture: &Fixture,
    demand: ControlledStreamDemandClass,
) -> ResourceDeclaration {
    ResourceDeclaration::ControlledOutboundStream {
        endpoint: fixture.destination.clone(),
        binding: fixture.transport.binding().clone(),
        demand,
        max_sockets: NonZeroUsize::new(1).unwrap(),
    }
}
fn caller(
    scope: &TaskScope,
) -> (
    tokio::sync::oneshot::Sender<()>,
    crate::contract::scope::TaskJoinWitness,
) {
    let (done, waiting) = tokio::sync::oneshot::channel();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            waiting.await.unwrap();
            Ok(())
        })
        .unwrap();
    (done, witness)
}

#[tokio::test]
async fn service_issuer_requires_original_host_fence_actual_scope_and_service_declaration() {
    for demand in [
        ControlledStreamDemandClass::Service,
        ControlledStreamDemandClass::Business,
        ControlledStreamDemandClass::BackgroundControl,
    ] {
        let mut fixture = Fixture::cold(1).await;
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let mut foreign = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let (done, witness) = caller(&scope);
        let (other_done, other) = caller(&foreign);
        let original = Arc::new(CancellationToken::new());
        let bound = fixture
            .owner
            .registration()
            .bind_declared(
                &description(&fixture, demand),
                &scope.registrar(),
                &original,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let user = grant.claim_user_view().unwrap();
        assert!(bound
            .service_issuer(
                &Arc::new(original.as_ref().clone()),
                &grant,
                witness.clone()
            )
            .is_err());
        assert!(bound.service_issuer(&original, &grant, other).is_err());
        let issued = bound.service_issuer(&original, &grant, witness.clone());
        assert_eq!(
            issued.is_ok(),
            demand == ControlledStreamDemandClass::Service
        );
        assert!(grant.slot.state().control.is_none());
        assert!(grant.slot.state().unknown.is_none());
        done.send(()).unwrap();
        let joined = scope.next_exit().await.unwrap();
        assert_eq!(witness.try_joined().unwrap().id(), joined.id);
        assert!(bound.service_issuer(&original, &grant, witness).is_err());
        other_done.send(()).unwrap();
        foreign.next_exit().await.unwrap();
        drop((issued, user, grant));
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
        assert!(foreign.shutdown(deadline()).await.is_complete());
    }
}

#[tokio::test]
async fn service_original_demand_is_bound_before_native_and_cannot_be_resigned_for_same_query() {
    let mut fixture = Fixture::cold(1).await;
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let (done, witness) = caller(&scope);
    let original = Arc::new(CancellationToken::new());
    let bound = fixture
        .owner
        .registration()
        .bind_declared(
            &description(&fixture, ControlledStreamDemandClass::Service),
            &scope.registrar(),
            &original,
        )
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    let mut grant = fixture.grants.reserve().unwrap();
    let cancel = CancellationToken::new();
    let end = Some(deadline());
    assert_eq!(
        bound
            .service_issuer(&original, &grant, witness.clone())
            .unwrap()
            .authorize(cancel.clone(), end)
            .err()
            .unwrap()
            .reason,
        Reason::InvalidConfig
    );
    let user = grant.claim_user_view().unwrap();
    let ticket = bound
        .service_issuer(&original, &grant, witness.clone())
        .unwrap()
        .authorize(cancel.clone(), end)
        .unwrap();
    let different_cancel = CancellationToken::new();
    assert_eq!(
        bound
            .service_issuer(&original, &grant, witness.clone())
            .unwrap()
            .authorize(different_cancel.clone(), end)
            .err()
            .unwrap()
            .reason,
        Reason::StalePlan
    );
    assert_eq!(
        bound
            .service_issuer(&original, &grant, witness)
            .unwrap()
            .authorize(cancel.clone(), Some(deadline()))
            .err()
            .unwrap()
            .reason,
        Reason::StalePlan
    );
    ticket.bind_caller(&grant.slot).unwrap(); // Same original ticket clone is idempotent.
    assert!(ticket.clone().check_liveness().is_ok());
    cancel.cancel();
    assert!(grant
        .slot
        .state()
        .business_cancel
        .as_ref()
        .unwrap()
        .is_cancelled());
    assert!(!different_cancel.is_cancelled());
    assert!(NativeCarrierOpener::service(ticket, fixture.egress.clone(), None).is_err());
    assert!(grant.slot.state().control.is_none());
    done.send(()).unwrap();
    scope.next_exit().await.unwrap();
    drop((user, grant));
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn fixed_service_ticket_rejects_business_metadata_and_changed_deadline_before_native() {
    use crate::contract::network::UpstreamOpener;
    for business in [false, true] {
        let mut fixture = Fixture::cold(1).await;
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let (done, witness) = caller(&scope);
        let original = Arc::new(CancellationToken::new());
        let bound = fixture
            .owner
            .registration()
            .bind_declared(
                &description(&fixture, ControlledStreamDemandClass::Service),
                &scope.registrar(),
                &original,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let adoption = grant.adoption();
        let user = grant.claim_user_view().unwrap();
        let mut request = context();
        let ticket = bound
            .service_issuer(&original, &grant, witness)
            .unwrap()
            .authorize(request.cancel.clone(), request.deadline)
            .unwrap();
        if business {
            request.session = Some(zero_core::component::SessionId(77));
        } else {
            request.deadline = None;
        }
        let native = NativeCarrierOpener::service(ticket, fixture.egress.clone(), None).unwrap();
        let denied = native
            .prepare_controlled_stream(
                fixture.transport.binding(),
                fixture.destination.clone(),
                request,
                grant,
            )
            .err()
            .unwrap();
        assert_eq!(denied.reason, Reason::StalePlan);
        assert_eq!(denied.delivery, DeliveryState::NotSubmitted);
        assert!(adoption.slot.state().control.is_none());
        assert!(adoption.slot.state().unknown.is_none());
        std::future::poll_fn(|cx| {
            assert!(fixture.listener.poll_accept(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        done.send(()).unwrap();
        scope.next_exit().await.unwrap();
        drop(user);
        assert!(!adoption.closed().unwrap().had_native_acquisition());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
    }
}
