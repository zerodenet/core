//! A failed mode conversion returns the same real fixed controller and views.
use super::*;
use crate::contract::{component::ResourceDeclaration, network::UpstreamOpener};
use crate::controlled_carrier::loan::DatagramLoan;
use zero_core::{component::AssociationId, request::DatagramTargets};

#[tokio::test]
async fn multiple_constructor_rejects_actual_fixed_limits_and_returns_the_same_native_loan() {
    timeout(Duration::from_secs(3), async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let endpoint = destination(remote);
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let one = NonZeroUsize::new(1).unwrap();
        let host = Arc::new(CancellationToken::new());
        let totals = CarrierOriginalTotals::new(one);
        let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
        let mut donor = CarrierOwner::new(instance(), one, &host);
        donor
            .registration()
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledDatagramCarrier {
                    endpoint: endpoint.clone(),
                    binding: binding.clone(),
                    max_sockets: one,
                },
                &scope.registrar(),
                &host,
                &totals,
            )
            .unwrap();
        donor.start(deadline()).await.unwrap();
        let mut grant = donor.grants().reserve().unwrap();
        let view = grant.claim_transferred_view().unwrap();
        let actual = grant.adoption();
        let opener =
            NativeCarrierOpener::new(instance(), binding.clone(), remote, egress, None).unwrap();
        let raw = opener
            .prepare_controlled_datagram(&binding, endpoint.clone(), context(), grant)
            .unwrap();
        let local = raw.local_addr().unwrap();
        let loan = raw.into_datagram_loan(view, AssociationId(73)).unwrap();
        let origin = loan.owner.original().clone();
        let (session, same_origin) = loan.into_session();
        assert!(same_origin.same_original(&origin));
        assert!(matches!(session.limits.targets, DatagramTargets::Fixed(_)));
        let (error, loan) =
            DatagramLoan::new_multiple(session, same_origin, AssociationId(73), endpoint)
                .err()
                .expect("Fixed limits cannot be promoted to Multiple");
        assert_eq!(error.reason, Reason::StalePlan);
        assert_eq!(error.delivery, DeliveryState::Unknown);
        assert!(!loan.io.has_multiple_targets());
        assert!(loan.owner.original().same_original(&origin));
        assert_eq!(totals.snapshot().charged, 1);
        loan.io.send(b"same native fixed target").await.unwrap();
        let mut buffer = [0; 64];
        let (size, source) = peer.recv_from(&mut buffer).await.unwrap();
        assert_eq!(source, local);
        assert_eq!(&buffer[..size], b"same native fixed target");
        peer.send_to(b"same response", source).await.unwrap();
        assert_eq!(loan.io.receive(&mut buffer).await.unwrap(), 13);
        let DatagramLoan { io, mut owner } = loan;
        assert!(!owner.close(Instant::now()).await.unwrap().is_complete());
        assert!(actual.closed().is_none());
        assert_eq!(totals.snapshot().charged, 1);
        drop(io);
        assert!(owner.close(deadline()).await.unwrap().is_complete());
        let closed = actual.closed().unwrap();
        assert!(closed.matches(&actual) && closed.had_native_acquisition());
        assert!(donor.shutdown(deadline()).await.unwrap().is_complete());
        assert_eq!(totals.snapshot().charged, 0);
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
