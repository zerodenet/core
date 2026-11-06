//! Native TCP Business and negotiated Service UDP share the same actual N.
use super::*;
use crate::contract::{component::ResourceDeclaration, network::UpstreamOpener};
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
        SessionId(944)
    }
    fn cancellation(&self) -> CancellationToken {
        self.0.clone()
    }
    fn deadline(&self) -> Option<Instant> {
        None
    }
}
#[tokio::test]
async fn association_tcp_business_and_related_service_datagram_share_original_limit_and_actual_close_barriers(
) {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::cold(2).await;
        let endpoint = fixture.destination.clone();
        let binding = fixture.transport.binding().clone();
        let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), None);
        scope.enable_registration().unwrap();
        let mut caller = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let host = Arc::new(CancellationToken::new());
        let registration = fixture.owner.registration();
        let bound = registration
            .bind_declared(
                &ResourceDeclaration::ControlledAssociationPool {
                    endpoint: endpoint.clone(),
                    dial: binding.dial.clone(),
                    max_sockets: NonZeroUsize::new(2).unwrap(),
                },
                &scope.registrar(),
                &host,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        assert_eq!(
            bound.demand(),
            crate::contract::component::ControlledStreamDemandClass::BackgroundControl,
            "original Service/BND class must not be relabelled Business"
        );
        assert!(bound.supports_business());
        let (begin, started) =
            tokio::sync::oneshot::channel::<crate::contract::scope::TaskJoinWitness>();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let mut business = fixture.grants.reserve().unwrap();
        let business_view = business.claim_user_view().unwrap();
        let business_original = business.adoption();
        let egress = fixture.egress.clone();
        let task_binding = binding.clone();
        let task_endpoint = endpoint.clone();
        let (_, witness) = caller
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let witness = started.await.unwrap();
                let demand = Arc::new(Demand(CancellationToken::new()));
                let ticket = bound
                    .node_business_issuer(&host, &business, &task_endpoint, &task_binding)
                    .unwrap()
                    .authorize_with_caller(demand.clone(), witness)
                    .unwrap();
                let native = NativeCarrierOpener::business(ticket, egress, None).unwrap();
                let carrier = native
                    .prepare_controlled_stream(
                        &task_binding,
                        task_endpoint,
                        CarrierContext {
                            session: Some(SessionId(944)),
                            deadline: None,
                            cancel: demand.0.clone(),
                        },
                        business,
                    )
                    .unwrap()
                    .open_with_deadline(None)
                    .await
                    .unwrap();
                let (mut io, _) = carrier.into_parts();
                io.write_all(b"business").await.unwrap();
                let mut bytes = [0; 4];
                io.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"live");
                finished.await.unwrap();
                drop(io);
                drop(business_view);
                Ok(())
            })
            .unwrap();
        begin.send(witness.clone()).unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let mut bytes = [0; 8];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"business");
        peer.write_all(b"live").await.unwrap();
        let mut parent = fixture.grants.reserve().unwrap();
        let view = parent.claim_transferred_view().unwrap();
        let original = parent.adoption();
        let control = fixture
            .transport
            .prepare_controlled_stream(endpoint.clone(), context(), parent)
            .unwrap()
            .open(deadline())
            .await
            .unwrap();
        let (mut service_peer, _) = fixture.listener.accept().await.unwrap();
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity,
            "a UDP child cannot bypass one Business plus its actual TCP parent"
        );
        assert!(business_original.closed().is_none());
        drop(control);
        drop(view);
        assert!(original.retire(deadline()).await.unwrap().is_complete());
        assert_eq!(service_peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(original.closed().unwrap().had_native_acquisition());
        finish.send(()).unwrap();
        caller.next_exit().await.unwrap();
        assert!(business_original
            .retire(deadline())
            .await
            .unwrap()
            .is_complete());
        let joined = witness.wait_registration_released().await;
        assert!(witness.matches_joined(&joined) && joined.registration_released());
        assert!(business_original
            .users_closed()
            .unwrap()
            .matches_caller(&witness));
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        let raw = NativeCarrierOpener::new(
            instance(),
            binding.clone(),
            fixture.listener.local_addr().unwrap(),
            fixture.egress.clone(),
            None,
        )
        .unwrap();
        let mut parent = fixture.grants.reserve().unwrap();
        let parent_view = parent.claim_transferred_view().unwrap();
        let parent_original = parent.adoption();
        let control = raw
            .prepare_controlled_stream(&binding, endpoint, context(), parent)
            .unwrap()
            .open(deadline())
            .await
            .unwrap();
        let (_service_peer, _) = fixture.listener.accept().await.unwrap();
        let udp_peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let candidates = raw
            .resolve_related_datagram(
                &binding,
                parent_original.clone(),
                destination(udp_peer.local_addr().unwrap()),
                context(),
            )
            .await
            .unwrap();
        let mut child = fixture.grants.reserve().unwrap();
        let child_view = child.claim_transferred_view().unwrap();
        let child_original = child.adoption();
        let packet = candidates.prepare_datagram(0, child).unwrap();
        packet.send(b"same-owner").await.unwrap();
        let mut bytes = [0; 32];
        let (size, _) = udp_peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], b"same-owner");
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        drop(control);
        drop(parent_view);
        assert!(!parent_original
            .retire(Instant::now() + Duration::from_millis(10))
            .await
            .unwrap()
            .is_complete());
        assert!(parent_original.closed().is_none());
        assert!(child_original.closed().is_none());
        drop(packet);
        drop(child_view);
        assert!(parent_original
            .retire(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(child_original.closed().unwrap().matches(&child_original));
        assert!(child_original.closed().unwrap().had_native_acquisition());
        assert!(parent_original.closed().unwrap().matches(&parent_original));
        assert!(parent_original
            .users_closed()
            .unwrap()
            .matches(&parent_original));
        assert!(child_original
            .users_closed()
            .unwrap()
            .matches(&child_original));
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(registration.users_closed().unwrap().matches(&registration));
        assert!(scope.shutdown(deadline()).await.is_complete());
        assert!(caller.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
