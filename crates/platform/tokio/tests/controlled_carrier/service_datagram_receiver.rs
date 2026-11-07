//! Real OS UDP donor, per-connection Service custody, no invented driver.
use super::*;
use crate::contract::{
    component::ResourceDeclaration, network::UpstreamOpener, DeliveryState, Reason, Stage,
};
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
use zero_core::component::{AssociationId, OutboundId, RoleId, RoleInstanceId};
fn prefix() -> Vec<RoleInstanceId> {
    vec![RoleInstanceId {
        component: instance().component,
        role: RoleId::Outbound(OutboundId(1)),
        generation: instance().generation,
    }]
}
#[tokio::test]
async fn service_datagram_receiver_retains_exact_actual_udp_loan_on_success_and_every_rejected_adoption(
) {
    timeout(Duration::from_secs(5), async {
        for case in [
            "service",
            "cached",
            "cached_api",
            "root",
            "stream",
            "empty",
            "business",
            "dial",
            "declaration_endpoint",
            "loan_endpoint",
            "cancelled",
            "owner_lost",
        ] {
            let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let remote = peer.local_addr().unwrap();
            let endpoint = destination(remote);
            let egress = Arc::new(crate::EgressInterfaceControl::default());
            let policy = zero_traits::DialPolicy {
                source_ip: Some("127.0.0.1".parse().unwrap()),
                ..Default::default()
            };
            let binding = UpstreamBinding {
                prefix: vec![],
                dial: policy.clone(),
                network_generation: NetworkGeneration(egress.generation()),
            };
            let one = NonZeroUsize::new(1).unwrap();
            let host = Arc::new(CancellationToken::new());
            let totals = CarrierOriginalTotals::new(NonZeroUsize::new(2).unwrap());
            let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
            let mut donor = CarrierOwner::new(instance(), one, &host);
            let donor_owner = donor.registration();
            donor_owner
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
            let original = grant.adoption();
            let opener =
                NativeCarrierOpener::new(instance(), binding.clone(), remote, egress, None)
                    .unwrap();
            let native = opener
                .prepare_controlled_datagram(&binding, endpoint.clone(), context(), grant)
                .unwrap();
            let local = native.local_addr().unwrap();
            let mut loan = native.into_datagram_loan(view, AssociationId(971)).unwrap();
            let donor_origin = loan.owner.original().clone();
            assert!(donor_origin.driver().is_none());
            loan.io.send(b"real UDP before transfer").await.unwrap();
            let mut bytes = [0; 64];
            let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
            assert_eq!(source, local);
            assert_eq!(&bytes[..size], b"real UDP before transfer");
            let observation = loan.receiver_observation();
            assert!(observation.retained().is_none());
            let receiving_id = ComponentInstanceId {
                component: ComponentId(772),
                generation: instance().generation,
            };
            let mut receiver = Some(CarrierOwner::new(receiving_id, one, &host));
            let receiver_owner = receiver.as_ref().unwrap().registration();
            let mut wrong = endpoint.clone();
            wrong.port = if wrong.port == u16::MAX {
                wrong.port - 1
            } else {
                wrong.port + 1
            };
            let declared = if case == "declaration_endpoint" {
                wrong.clone()
            } else {
                endpoint.clone()
            };
            let declaration = match case {
                "cached" => ResourceDeclaration::ControlledCarrierPool {
                    endpoint: declared,
                    dial: policy.clone(),
                    datagram: true,
                    max_sockets: one,
                },
                "root" => ResourceDeclaration::ControlledDatagramCarrier {
                    endpoint: declared,
                    binding: binding.clone(),
                    max_sockets: one,
                },
                "stream" => ResourceDeclaration::ControlledServiceStream {
                    endpoint: declared,
                    dial: policy.clone(),
                    max_sockets: one,
                },
                _ => ResourceDeclaration::ControlledServiceDatagram {
                    endpoint: declared,
                    dial: policy.clone(),
                    max_sockets: one,
                },
            };
            receiver_owner
                .bind_declared_with_totals(&declaration, &scope.registrar(), &host, &totals)
                .unwrap();
            receiver.as_mut().unwrap().start(deadline()).await.unwrap();
            let mut borrowed = UpstreamBinding {
                prefix: prefix(),
                ..binding
            };
            if case == "empty" {
                borrowed.prefix.clear();
            }
            if case == "dial" {
                borrowed.dial.source_ip = Some("127.0.0.2".parse().unwrap());
            }
            let mut call = context();
            if case == "business" {
                call.session = Some(zero_core::component::SessionId(9));
            }
            if case == "cancelled" || case == "owner_lost" {
                call.cancel.cancel();
            }
            let endpoint = if case == "loan_endpoint" {
                wrong
            } else {
                endpoint
            };
            let mut grant = receiver.as_ref().unwrap().grants().reserve().unwrap();
            let receiving = grant.adoption();
            let view = grant.claim_transferred_view().unwrap();
            assert_eq!(totals.snapshot().charged, 2);
            let acquired = if case == "cached_api" {
                grant.adopt_datagram_loan(&borrowed, endpoint.clone(), &call, loan)
            } else {
                grant.adopt_service_datagram_loan(&borrowed, endpoint.clone(), &call, loan)
            };
            if case != "service" {
                assert_eq!(
                    acquired
                        .err()
                        .expect("only actual exact per-connection Service custody is allowed")
                        .reason,
                    Reason::StalePlan,
                    "{case}"
                );
                let proof = observation
                    .retained()
                    .expect("rejected loan was strongly installed before validation");
                assert!(proof.receiver_adoption().unwrap().same_slot(&receiving));
                assert!(proof.retains(&original));
                assert!(proof.receiver_owner().unwrap().same_owner(&receiver_owner));
                assert!(original.closed().is_none() && donor_origin.closed().is_none());
                assert_eq!(
                    totals.snapshot().charged,
                    2,
                    "a validation error cannot refund actual UDP custody"
                );
                assert_eq!(
                    donor.grants().reserve().err().unwrap().reason,
                    Reason::Capacity
                );
                if case == "owner_lost" {
                    drop(receiver.take());
                    assert!(
                        observation.retained().is_some(),
                        "same actual Root still owns the receiver and LoanOwner"
                    );
                }
                drop(view);
                assert!(receiving.retire(deadline()).await.unwrap().is_complete());
                assert!(original.closed().unwrap().matches(&original));
                assert!(original.closed().unwrap().had_native_acquisition());
                assert!(original.users_closed().unwrap().matches(&original));
                assert!(receiving.closed().unwrap().matches(&receiving));
                assert!(!receiving.closed().unwrap().had_native_acquisition());
                assert!(receiving.users_closed().unwrap().matches(&receiving));
                assert!(observation.retained().is_none());
            } else {
                let io = acquired.unwrap();
                let proof = observation.retained().unwrap();
                assert!(proof.retains(&original));
                assert!(proof.receiver_adoption().unwrap().same_slot(&receiving));
                assert!(receiving.retained_driver().is_none());
                let mut transferred = io.into_datagram_loan(view, AssociationId(972)).unwrap();
                let child = transferred.owner.original().clone();
                assert!(child.driver().is_none());
                let parent = child.parent().expect("actual retained UDP donor lineage");
                assert!(parent.owner().same_owner(donor_origin.owner()));
                assert_eq!(parent.instance(), donor_origin.instance());
                for payload in [b"same service UDP".as_slice(), b"".as_slice()] {
                    transferred.io.send(payload).await.unwrap();
                    let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
                    assert_eq!(source, local);
                    assert_eq!(&bytes[..size], payload);
                    peer.send_to(payload, source).await.unwrap();
                    assert_eq!(
                        transferred.io.receive(&mut bytes).await.unwrap(),
                        payload.len()
                    );
                    assert_eq!(&bytes[..payload.len()], payload);
                }
                {
                    let mut receive = Box::pin(transferred.io.receive(&mut bytes));
                    poll_fn(|cx| {
                        assert!(receive.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    transferred
                        .owner
                        .cancel(crate::contract::CloseReason::Shutdown);
                    assert_eq!(
                        receive.await.unwrap_err().kind(),
                        std::io::ErrorKind::BrokenPipe
                    );
                }
                assert!(!transferred.owner.try_close().unwrap().is_complete());
                assert!(original.closed().is_none() && receiving.closed().is_none());
                assert_eq!(totals.snapshot().charged, 2);
                drop(transferred.io);
                assert!(transferred
                    .owner
                    .close(deadline())
                    .await
                    .unwrap()
                    .is_complete());
                assert!(transferred
                    .owner
                    .closed()
                    .unwrap()
                    .matches(&transferred.owner));
                let logical = child.closed().unwrap();
                assert!(logical.matches(&child) && !logical.had_native_acquisition());
                let native = donor_origin.closed().unwrap();
                assert!(native.matches(&donor_origin) && native.had_native_acquisition());
                assert!(original.users_closed().unwrap().matches(&original));
                assert!(receiving.users_closed().unwrap().matches(&receiving));
                assert!(observation.retained().is_none());
            }
            if let Some(mut receiver) = receiver {
                assert!(receiver.shutdown(deadline()).await.unwrap().is_complete());
                assert!(receiver_owner
                    .users_closed()
                    .unwrap()
                    .matches(&receiver_owner));
            }
            assert!(donor.shutdown(deadline()).await.unwrap().is_complete());
            assert!(donor_owner.users_closed().unwrap().matches(&donor_owner));
            assert_eq!(totals.snapshot().charged, 0);
            let closed = scope.shutdown(deadline()).await;
            assert!(
                closed.is_complete() && closed.exited_tasks.is_empty(),
                "taskless UDP does not invent a task or ACK"
            );
        }
    })
    .await
    .unwrap();
}
mod native_denied;

mod multilayer;
