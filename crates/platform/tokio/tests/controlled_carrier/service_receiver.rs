//! Only actual LoanOwner installation names a receiver; a token grants nothing.
use super::*;
use crate::contract::network::UpstreamOpener;
use crate::contract::{component::ResourceDeclaration, Reason};
use zero_core::component::{OutboundId, RoleId, RoleInstanceId};
#[tokio::test]
async fn per_connection_service_prefix_borrow_requires_exact_service_declaration_and_real_loan_custody(
) {
    timeout(Duration::from_secs(5), async {
        for case in ["service", "cached", "empty", "business"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = destination(listener.local_addr().unwrap());
            let egress = Arc::new(crate::EgressInterfaceControl::default());
            let binding = UpstreamBinding {
                prefix: vec![],
                dial: Default::default(),
                network_generation: NetworkGeneration(egress.generation()),
            };
            let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), None);
            scope.enable_registration().unwrap();
            let one = NonZeroUsize::new(1).unwrap();
            let host = Arc::new(CancellationToken::new());
            let mut donor = CarrierOwner::new(instance(), one, &host);
            donor
                .registration()
                .bind_declared(
                    &ResourceDeclaration::ControlledNodeCarrier {
                        endpoint: endpoint.clone(),
                        dial: Default::default(),
                        max_sockets: one,
                    },
                    &scope.registrar(),
                    &host,
                )
                .unwrap();
            donor.start(deadline()).await.unwrap();
            let mut grant = donor.grants().reserve().unwrap();
            let view = grant.claim_transferred_view().unwrap();
            let original = grant.adoption();
            let opener = NativeCarrierOpener::new(
                instance(),
                binding.clone(),
                listener.local_addr().unwrap(),
                egress,
                None,
            )
            .unwrap();
            let io = opener
                .prepare_controlled_stream(&binding, endpoint.clone(), context(), grant)
                .unwrap()
                .open(deadline())
                .await
                .unwrap()
                .into_parts()
                .0;
            let (mut peer, _) = listener.accept().await.unwrap();
            let mut loan = io.into_stream_loan(view).unwrap();
            let observation = loan.receiver_observation();
            assert!(
                observation.retained().is_none(),
                "origin/returned loan alone does not name a receiver"
            );
            let receiving_id = ComponentInstanceId {
                component: ComponentId(772),
                generation: instance().generation,
            };
            let mut receiver = CarrierOwner::new(receiving_id, one, &host);
            let declaration = if case == "cached" {
                ResourceDeclaration::ControlledCarrierPool {
                    endpoint: endpoint.clone(),
                    dial: Default::default(),
                    datagram: false,
                    max_sockets: one,
                }
            } else {
                ResourceDeclaration::ControlledServiceStream {
                    endpoint: endpoint.clone(),
                    dial: Default::default(),
                    max_sockets: one,
                }
            };
            receiver
                .registration()
                .bind_declared(&declaration, &scope.registrar(), &host)
                .unwrap();
            receiver.start(deadline()).await.unwrap();
            let mut borrowed = UpstreamBinding {
                prefix: vec![RoleInstanceId {
                    component: instance().component,
                    role: RoleId::Outbound(OutboundId(1)),
                    generation: instance().generation,
                }],
                ..binding
            };
            let mut grant = receiver.grants().reserve().unwrap();
            let receiving = grant.adoption();
            let view = grant.claim_transferred_view().unwrap();
            if case == "empty" {
                borrowed.prefix.clear();
            }
            let mut call = context();
            if case == "business" {
                call.session = Some(zero_core::component::SessionId(9));
            }
            let acquired = grant.adopt_service_stream_loan(&borrowed, endpoint, &call, loan);
            let mut io = if case == "service" {
                acquired.unwrap()
            } else {
                assert_eq!(acquired.err().unwrap().reason, Reason::StalePlan);
                let rejected = observation.retained()
                    .expect("rejected actual loan was installed before validation");
                assert!(rejected.receiver_adoption().unwrap().same_slot(&receiving));
                assert!(rejected.retains(&original));
                drop(view);
                assert!(receiving.retire(deadline()).await.unwrap().is_complete());
                assert!(original.closed().unwrap().matches(&original));
                assert!(original.users_closed().unwrap().matches(&original));
                assert!(receiver.shutdown(deadline()).await.unwrap().is_complete());
                assert!(donor.shutdown(deadline()).await.unwrap().is_complete());
                assert!(scope.shutdown(deadline()).await.is_complete());
                continue;
            };
            let proof = observation
                .retained()
                .expect("actual strong LoanOwner installed");
            assert!(proof.receiver_adoption().unwrap().same_slot(&receiving));
            assert!(proof
                .receiver_owner()
                .unwrap()
                .same_owner(&receiver.registration()));
            assert!(proof.retains(&original));
            assert!(
                receiving.retained_driver().is_none(),
                "controller installation cannot invent cached driver custody"
            );
            io.write_all(b"same").await.unwrap();
            let mut bytes = [0; 4];
            peer.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"same");
            drop(io);
            drop(view);
            assert!(receiving.retire(deadline()).await.unwrap().is_complete());
            assert!(observation.retained().is_none());
            assert!(original.closed().unwrap().matches(&original));
            assert!(original.users_closed().unwrap().matches(&original));
            assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
            assert!(receiver.shutdown(deadline()).await.unwrap().is_complete());
            assert!(donor.shutdown(deadline()).await.unwrap().is_complete());
            assert!(scope.shutdown(deadline()).await.is_complete());
        }
    })
    .await
    .unwrap();
}
