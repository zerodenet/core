use super::*;
use crate::contract::datagram::{recv, send};
#[tokio::test]
async fn real_three_layer_datagram_custody_keeps_one_wire_id_and_distinct_logical_ids_without_copying_payloads(
) {
    timeout(Duration::from_secs(5), async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let endpoint = destination(remote);
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let dial = zero_traits::DialPolicy {
            source_ip: Some(remote.ip()),
            ..Default::default()
        };
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: dial.clone(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let one = NonZeroUsize::new(1).unwrap();
        let host = Arc::new(CancellationToken::new());
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(3).unwrap());
        let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
        let mut owners = Vec::new();
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
        let native = grant.adoption();
        let opener =
            NativeCarrierOpener::new(instance(), binding.clone(), remote, egress, None).unwrap();
        let raw = opener
            .prepare_controlled_datagram(&binding, endpoint.clone(), context(), grant)
            .unwrap();
        let local = raw.local_addr().unwrap();
        let mut loan = raw.into_datagram_loan(view, AssociationId(971)).unwrap();
        let native_origin = loan.owner.original().clone();
        let mut adoptions = vec![native];
        let mut origins = vec![native_origin.clone()];
        let mut binding = UpstreamBinding {
            prefix: prefix(),
            ..binding
        };
        for (component, logical) in [(772, 972), (773, 973)] {
            let id = ComponentInstanceId {
                component: ComponentId(component),
                generation: instance().generation,
            };
            let mut owner = CarrierOwner::new(id, one, &host);
            owner
                .registration()
                .bind_declared_with_totals(
                    &ResourceDeclaration::ControlledServiceDatagram {
                        endpoint: endpoint.clone(),
                        dial: dial.clone(),
                        max_sockets: one,
                    },
                    &scope.registrar(),
                    &host,
                    &totals,
                )
                .unwrap();
            owner.start(deadline()).await.unwrap();
            let mut grant = owner.grants().reserve().unwrap();
            let view = grant.claim_transferred_view().unwrap();
            let adoption = grant.adoption();
            let borrowed = grant
                .adopt_service_datagram_loan(&binding, endpoint.clone(), &context(), loan)
                .unwrap();
            loan = borrowed
                .into_datagram_loan(view, AssociationId(logical))
                .unwrap();
            assert_eq!(loan.io.association(), AssociationId(logical));
            assert_eq!(loan.io.wire_association(), AssociationId(971));
            let origin = loan.owner.original().clone();
            assert_eq!(
                origin.parent().unwrap().instance(),
                origins.last().unwrap().instance()
            );
            assert!(origin.driver().is_none());
            origins.push(origin);
            adoptions.push(adoption);
            owners.push(owner);
            for payload in [b"same nested bytes".as_slice(), b"".as_slice()] {
                loan.io.send(payload).await.unwrap();
                let mut bytes = [0; 64];
                let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
                assert_eq!(source, local);
                assert_eq!(&bytes[..size], payload);
                peer.send_to(payload, source).await.unwrap();
                assert_eq!(loan.io.receive(&mut bytes).await.unwrap(), payload.len());
                assert_eq!(&bytes[..payload.len()], payload);
            }
            binding.prefix.push(RoleInstanceId {
                component: id.component,
                role: RoleId::Outbound(OutboundId(1)),
                generation: id.generation,
            });
        }
        assert_eq!(
            totals.snapshot().charged,
            3,
            "three actual custody owners, one actual UDP original"
        );
        let wire = loan.io.wire_association();
        let (mut session, origin) = loan.into_session();
        assert_eq!(origin.instance(), origins[2].instance());
        // Raw Tx/Rx retain their original binding; the logical ID is not an
        // authority to bypass NativePacket's exact original-ID validation.
        let mut frame = zero_core::packet::Datagram {
            association: AssociationId(973),
            address: endpoint,
            payload: b"raw unchanged bytes".to_vec().into(),
        };
        let failure = send(
            session.tx.as_mut(),
            session.control.as_mut(),
            &frame,
            &session.limits,
        )
        .await
        .unwrap_err();
        assert_eq!(
            (failure.error.reason, failure.delivery),
            (Reason::Protocol, DeliveryState::NotSubmitted)
        );
        let mut bytes = [0; 64];
        {
            let mut pending = Box::pin(peer.recv_from(&mut bytes));
            poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        frame.association = wire;
        let receipt = send(
            session.tx.as_mut(),
            session.control.as_mut(),
            &frame,
            &session.limits,
        )
        .await
        .unwrap();
        assert_eq!(receipt.accepted_bytes, frame.payload.len());
        let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], &*frame.payload);
        peer.send_to(&bytes[..size], source).await.unwrap();
        let received = recv(session.rx.as_mut()).await.unwrap().unwrap();
        assert_eq!(received.association, wire);
        assert_eq!(&*received.payload, &*frame.payload);
        assert!(!session.control.try_close().unwrap().is_complete());
        assert!(native_origin.closed().is_none());
        drop(session.tx);
        drop(session.rx);
        assert!(session
            .control
            .wait_closed(deadline())
            .await
            .unwrap()
            .is_complete());
        for (index, adoption) in adoptions.iter().enumerate() {
            let closed = adoption.closed().unwrap();
            assert!(closed.matches(adoption));
            assert_eq!(closed.had_native_acquisition(), index == 0);
            assert!(adoption.users_closed().unwrap().matches(adoption));
        }
        for owner in owners.iter_mut().rev() {
            assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
        }
        assert!(donor.shutdown(deadline()).await.unwrap().is_complete());
        assert_eq!(totals.snapshot().charged, 0);
        let closed = scope.shutdown(deadline()).await;
        assert!(closed.is_complete() && closed.exited_tasks.is_empty());
    })
    .await
    .unwrap();
}
