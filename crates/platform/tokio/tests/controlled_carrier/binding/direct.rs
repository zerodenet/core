//! Self-bound library owner evidence; real production authority remains the
//! private Engine/Slot/resource issuer. No task or fixed target is fabricated.
use super::*;
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
        SessionId(812)
    }
    fn cancellation(&self) -> CancellationToken {
        self.0.clone()
    }
    fn deadline(&self) -> Option<Instant> {
        None
    }
}
#[tokio::test]
async fn direct_resource_binds_no_fake_endpoint_and_each_ticket_keeps_its_original_candidate() {
    timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::cold(1).await;
        let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
        // The carrier itself has no background driver and no task grant.
        let mut owner_scope = TaskScope::suspended(tokio::runtime::Handle::current(), None);
        owner_scope.enable_registration().unwrap();
        let mut caller = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let host = Arc::new(CancellationToken::new());
        let registration = fixture.owner.registration();
        let description = ResourceDeclaration::ControlledDirectStream {
            dial: fixture.transport.binding().dial.clone(),
            max_sockets: NonZeroUsize::new(1).unwrap(),
        };
        let bound = registration
            .bind_declared(&description, &owner_scope.registrar(), &host)
            .unwrap();
        assert!(bound.fixed_endpoint_binding().is_none());
        assert!(bound.scope_identity().resource_open());
        assert!(!bound.scope_identity().registration_open());
        assert!(bound.matches_declaration(&description));
        fixture.owner.start(deadline()).await.unwrap();
        for listener in [&fixture.listener, &second] {
            let peer_address = listener.local_addr().unwrap();
            let endpoint = destination(peer_address);
            let upstream = fixture.transport.binding().clone();
            let mut grant = fixture.grants.reserve().unwrap();
            let user = grant.claim_user_view().unwrap();
            let adoption = grant.adoption();
            assert!(bound.business_issuer(&host, &grant).is_err());
            assert!(bound
                .direct_business_issuer(
                    &Arc::new(host.as_ref().clone()),
                    &grant,
                    &endpoint,
                    &upstream
                )
                .is_err());
            let mut wrong_policy = upstream.clone();
            wrong_policy.dial.source_ip = None;
            assert!(bound
                .direct_business_issuer(&host, &grant, &endpoint, &wrong_policy)
                .is_err());
            assert!(bound
                .direct_business_issuer(
                    &host,
                    &grant,
                    &Destination::new(Address::Domain("example.test".into()), peer_address.port()),
                    &upstream
                )
                .is_err());
            let (finish, finished) = tokio::sync::oneshot::channel();
            let (_, witness) = caller
                .registrar()
                .spawn_tracked(TaskClass::Auxiliary, async move {
                    finished.await.unwrap();
                    Ok(())
                })
                .unwrap();
            let demand = Arc::new(Demand(CancellationToken::new()));
            let ticket = bound
                .direct_business_issuer(&host, &grant, &endpoint, &upstream)
                .unwrap()
                .authorize_with_caller(demand.clone(), witness.clone())
                .unwrap();
            // Same immutable demand Arc is idempotent; the same actual caller
            // and numeric SessionId cannot authorize a different demand on it.
            let repeated = bound
                .direct_business_issuer(&host, &grant, &endpoint, &upstream)
                .unwrap()
                .authorize_with_caller(demand.clone(), witness.clone())
                .unwrap();
            drop(repeated);
            let foreign = bound
                .direct_business_issuer(&host, &grant, &endpoint, &upstream)
                .unwrap()
                .authorize_with_caller(Arc::new(Demand(CancellationToken::new())), witness.clone())
                .err()
                .unwrap();
            assert_eq!(foreign.reason, Reason::StalePlan);
            assert_eq!(foreign.delivery, DeliveryState::NotSubmitted);
            let mut changed = endpoint.clone();
            changed.port = if changed.port == 65535 {
                1
            } else {
                changed.port + 1
            };
            assert_eq!(
                ticket
                    .check(
                        &grant,
                        &upstream,
                        &changed,
                        &CarrierContext {
                            session: Some(SessionId(812)),
                            deadline: None,
                            cancel: demand.0.clone(),
                        }
                    )
                    .unwrap_err()
                    .reason,
                Reason::StalePlan
            );
            let native =
                NativeCarrierOpener::business(ticket, fixture.egress.clone(), None).unwrap();
            use crate::contract::network::UpstreamOpener;
            let carrier = native
                .prepare_controlled_stream(
                    &upstream,
                    endpoint,
                    CarrierContext {
                        session: Some(SessionId(812)),
                        deadline: None,
                        cancel: demand.0.clone(),
                    },
                    grant,
                )
                .unwrap()
                .open_with_deadline(None)
                .await
                .unwrap();
            assert_eq!(carrier.endpoints().peer(), peer_address);
            assert!(carrier.endpoints().matches(&adoption));
            let (mut peer, actual_local) = listener.accept().await.unwrap();
            assert_eq!(carrier.endpoints().local(), Some(actual_local));
            let (mut io, _) = carrier.into_parts();
            io.write_all(b"request").await.unwrap();
            io.shutdown().await.unwrap();
            let mut request = Vec::new();
            peer.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, b"request");
            peer.write_all(b"reply").await.unwrap();
            peer.shutdown().await.unwrap();
            let mut response = Vec::new();
            io.read_to_end(&mut response).await.unwrap();
            assert_eq!(response, b"reply");
            drop(io);
            drop(user);
            assert_eq!(
                fixture.grants.reserve().err().unwrap().reason,
                Reason::Capacity
            );
            assert!(!adoption
                .retire(Instant::now() + Duration::from_millis(10))
                .await
                .unwrap()
                .is_complete());
            assert!(adoption.closed().unwrap().had_native_acquisition());
            assert!(adoption.users_closed().is_none());
            finish.send(()).unwrap();
            caller.next_exit().await.unwrap();
            assert!(adoption.users_closed().unwrap().matches_caller(&witness));
            assert!(adoption.retire(deadline()).await.unwrap().is_complete());
        }
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(registration.closed().is_some());
        assert!(registration.users_closed().unwrap().matches(&registration));
        assert!(owner_scope.shutdown(deadline()).await.is_complete());
        assert!(caller.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn original_demand_weak_identity_cannot_be_replaced_after_its_last_ticket_is_dropped() {
    timeout(Duration::from_secs(4), async {
        let mut fixture = Fixture::cold(1).await;
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let mut caller = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let host = Arc::new(CancellationToken::new());
        let endpoint = fixture.destination.clone();
        let upstream = fixture.transport.binding().clone();
        let bound = fixture
            .owner
            .registration()
            .bind_declared(
                &ResourceDeclaration::ControlledDirectStream {
                    dial: upstream.dial.clone(),
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &scope.registrar(),
                &host,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let user = grant.claim_user_view().unwrap();
        let adoption = grant.adoption();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let (_, witness) = caller
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                finished.await.unwrap();
                Ok(())
            })
            .unwrap();
        let original_cancel = CancellationToken::new();
        let demand = Arc::new(Demand(original_cancel.clone()));
        let weak = Arc::downgrade(&demand);
        let ticket = bound
            .direct_business_issuer(&host, &grant, &endpoint, &upstream)
            .unwrap()
            .authorize_with_caller(demand.clone(), witness.clone())
            .unwrap();
        let (entered, suspended) = tokio::sync::oneshot::channel();
        let (id, auxiliary) = adoption
            .spawn_auxiliary_weak(&scope.registrar().downgrade(), async move {
                entered.send(()).unwrap();
                std::future::pending().await
            })
            .unwrap();
        suspended.await.unwrap();
        drop(ticket);
        drop(demand);
        assert!(
            weak.upgrade().is_none(),
            "original slot does not retain demand/Engine"
        );
        assert!(adoption
            .slot
            .state()
            .business_identity
            .as_ref()
            .unwrap()
            .upgrade()
            .is_none());
        let failure = bound
            .direct_business_issuer(&host, &grant, &endpoint, &upstream)
            .unwrap()
            .authorize_with_caller(Arc::new(Demand(CancellationToken::new())), witness.clone())
            .err()
            .unwrap();
        assert_eq!(failure.reason, Reason::StalePlan);
        original_cancel.cancel();
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        assert!(exit.result.is_ok());
        assert!(auxiliary.try_joined().unwrap().result().is_ok());
        assert!(adoption.closed().is_none());
        drop(user);
        drop(grant);
        finish.send(()).unwrap();
        caller.next_exit().await.unwrap();
        assert!(adoption.retire(deadline()).await.unwrap().is_complete());
        assert!(!adoption.closed().unwrap().had_native_acquisition());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
        assert!(caller.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
