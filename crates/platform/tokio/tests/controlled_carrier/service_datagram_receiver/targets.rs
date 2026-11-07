//! Exact actual Host Node source, single selection and original UDP custody.
use super::*;
use crate::contract::{BoxFuture, ChannelError};
use crate::controlled_carrier::loan::HostNodeDnsSource;
use std::sync::atomic::Ordering;
mod fixture;
use fixture::Actual;
#[tokio::test]
async fn foreign_host_node_source_is_denied_before_resolve_and_literal_does_not_lookup() {
    timeout(Duration::from_secs(3), async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let actual = Actual::new(peer.local_addr().unwrap(), true).await;
        // Token clones have identical cancellation behavior but foreign private
        // Arc identity; an arbitrary resolver wrapper cannot forge this Host.
        let foreign = HostNodeDnsSource::for_host(
            actual.source.clone(),
            &Arc::new(actual.host.as_ref().clone()),
        );
        let error = actual
            .issuer()
            .unwrap()
            .resolve_on_node(&foreign, actual.binding.network_generation)
            .await
            .err()
            .unwrap();
        assert_eq!(error.reason, Reason::StalePlan);
        assert_eq!(error.delivery, DeliveryState::NotSubmitted);
        assert_eq!(actual.source.calls.load(Ordering::SeqCst), 0);
        let candidates = actual
            .issuer()
            .unwrap()
            .resolve_on_node(&actual.node(), actual.binding.network_generation)
            .await
            .unwrap();
        assert_eq!(actual.source.calls.load(Ordering::SeqCst), 1);
        assert_eq!(candidates.peer(0).unwrap(), peer.local_addr().unwrap());
        let target = candidates.select(0, actual.issuer().unwrap()).unwrap();
        let mut wrong = actual.binding.clone();
        wrong.prefix.clear();
        assert!(target
            .validate_borrow(&wrong, &actual.endpoint, wrong.network_generation)
            .is_err());
        assert!(
            actual.issuer().is_err(),
            "one actual original cannot issue a second selected borrow"
        );
        target
            .validate_borrow(
                &actual.binding,
                &actual.endpoint,
                actual.binding.network_generation,
            )
            .unwrap();
        drop(target);
        actual.finish().await;
        let literal = Actual::new(peer.local_addr().unwrap(), false).await;
        let answer = literal
            .issuer()
            .unwrap()
            .resolve_on_node(&literal.node(), literal.binding.network_generation)
            .await
            .unwrap();
        assert_eq!(answer.peer(0).unwrap(), peer.local_addr().unwrap());
        assert_eq!(literal.source.calls.load(Ordering::SeqCst), 0);
        literal.finish().await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn selected_domain_receiver_keeps_nominal_binding_and_real_numeric_udp_native_original() {
    timeout(Duration::from_secs(3), async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let mut actual = Actual::new(remote, true).await;
        let answer = actual
            .issuer()
            .unwrap()
            .resolve_on_node(&actual.node(), actual.binding.network_generation)
            .await
            .unwrap();
        let target = answer.select(0, actual.issuer().unwrap()).unwrap();
        let mut donor_scope = TaskScope::empty(tokio::runtime::Handle::current());
        let one = NonZeroUsize::new(1).unwrap();
        let mut donor = CarrierOwner::new(
            ComponentInstanceId {
                component: ComponentId(883),
                ..instance()
            },
            one,
            &CancellationToken::new(),
        );
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: actual.binding.network_generation,
        };
        donor
            .registration()
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledDatagramCarrier {
                    endpoint: destination(remote),
                    binding: binding.clone(),
                    max_sockets: one,
                },
                &donor_scope.registrar(),
                &actual.host,
                &actual.totals,
            )
            .unwrap();
        donor.start(deadline()).await.unwrap();
        let mut original = donor.grants().reserve().unwrap();
        let view = original.claim_transferred_view().unwrap();
        let adoption = original.adoption();
        let native = NativeCarrierOpener::new(
            donor.registration().instance(),
            binding.clone(),
            remote,
            Arc::new(crate::EgressInterfaceControl::default()),
            None,
        )
        .unwrap()
        .prepare_controlled_datagram(&binding, destination(remote), actual.call.clone(), original)
        .unwrap();
        let loan = native.into_datagram_loan(view, AssociationId(44)).unwrap();
        let retained = loan.receiver_observation();
        let io = target
            .with_loan(loan)
            .adopt(actual.grant.take().unwrap())
            .unwrap();
        assert!(retained
            .retained()
            .unwrap()
            .receiver_adoption()
            .unwrap()
            .same_slot(&actual.original));
        assert!(actual.original.closed().is_none() && adoption.closed().is_none());
        let mut transferred = io
            .into_datagram_loan(actual.view.take().unwrap(), AssociationId(45))
            .unwrap();
        for payload in [b"selected Node wire".as_slice(), b"".as_slice()] {
            transferred.io.send(payload).await.unwrap();
            let mut bytes = [0; 64];
            let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], payload);
            peer.send_to(payload, source).await.unwrap();
            assert_eq!(
                transferred.io.receive(&mut bytes).await.unwrap(),
                payload.len()
            );
        }
        assert_eq!(actual.source.calls.load(Ordering::SeqCst), 1);
        drop(transferred);
        actual.finish().await;
        assert!(adoption.closed().unwrap().matches(&adoption));
        assert!(adoption.closed().unwrap().had_native_acquisition());
        assert!(donor.shutdown(deadline()).await.unwrap().is_complete());
        donor_scope.begin_shutdown();
        assert!(donor_scope.is_drained());
    })
    .await
    .unwrap();
}
