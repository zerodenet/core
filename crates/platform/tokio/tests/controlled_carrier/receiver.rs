//! Only actual LoanOwner installation names a receiver; a token grants nothing.
use super::*;
use crate::contract::component::ResourceDeclaration;
use crate::contract::network::UpstreamOpener;
use crate::contract::Reason;
use zero_core::component::{OutboundId, RoleId, RoleInstanceId};
#[tokio::test]
async fn exact_returned_stream_loan_observes_only_its_strong_receiver_and_native_retirement() {
    timeout(Duration::from_secs(5), async {
        for with_driver in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = destination(listener.local_addr().unwrap());
            let egress = Arc::new(crate::EgressInterfaceControl::default());
            let binding = UpstreamBinding {
                prefix: vec![],
                dial: Default::default(),
                network_generation: NetworkGeneration(egress.generation()),
            };
            let mut scope = TaskScope::suspended_hosted(
                tokio::runtime::Handle::current(),
                Some(NonZeroUsize::MIN),
            );
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
            receiver
                .registration()
                .bind_declared(
                    &ResourceDeclaration::ControlledCarrierPool {
                        endpoint: endpoint.clone(),
                        dial: Default::default(),
                        datagram: false,
                        max_sockets: one,
                    },
                    &scope.registrar(),
                    &host,
                )
                .unwrap();
            receiver.start(deadline()).await.unwrap();
            let borrowed = UpstreamBinding {
                prefix: vec![RoleInstanceId {
                    component: instance().component,
                    role: RoleId::Outbound(OutboundId(1)),
                    generation: instance().generation,
                }],
                ..binding
            };
            let mut grant = receiver.grants().reserve().unwrap();
            let receiving = grant.adoption();
            let view = if with_driver {
                grant.claim_driver_view().unwrap()
            } else {
                grant.claim_transferred_view().unwrap()
            };
            let mut io = grant
                .adopt_stream_loan(&borrowed, endpoint, &context(), loan)
                .unwrap();
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
            assert!(receiver
                .registration()
                .retained_cached_input(&original)
                .is_none());
            assert!(!receiver
                .registration()
                .retains_cached_carrier_loan(&original));
            io.write_all(b"same").await.unwrap();
            let mut bytes = [0; 4];
            peer.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"same");
            if with_driver {
                let (at_io, pending) = tokio::sync::oneshot::channel();
                let (_, driver) = receiving
                    .spawn_driver(
                        &scope.registrar(),
                        TaskClass::Critical,
                        io.into_driver(),
                        move |mut io| async move {
                            let _view = view;
                            let mut byte = [0];
                            let reading = io.read_exact(&mut byte);
                            tokio::pin!(reading);
                            std::future::poll_fn(|cx| {
                                assert!(
                                    std::future::Future::poll(reading.as_mut(), cx).is_pending()
                                );
                                std::task::Poll::Ready(())
                            })
                            .await;
                            at_io.send(()).unwrap();
                            reading.await.map_err(|source| {
                                crate::contract::ChannelError::new(
                                    crate::contract::Stage::Close,
                                    Reason::Io,
                                    crate::contract::Impact::Component,
                                )
                                .with_source(source)
                            })?;
                            Ok(())
                        },
                    )
                    .unwrap();
                pending.await.unwrap();
                assert!(
                    original.retained_driver().is_none(),
                    "the donor has no independent driver"
                );
                let (retained, actual) = receiver
                    .registration()
                    .retained_cached_input(&original)
                    .unwrap();
                assert!(retained.retains(&original) && actual.driver().same_registration(&driver));
                assert!(receiver
                    .registration()
                    .retains_cached_carrier_loan(&original));
                assert!(!donor.registration().retains_cached_carrier_loan(&original));
                assert!(
                    donor
                        .registration()
                        .retained_cached_input(&original)
                        .is_none(),
                    "a foreign owner cannot claim this receiver"
                );
                assert!(original.closed().is_none() && receiving.closed().is_none());
                peer.write_all(&[1]).await.unwrap();
                let joined = scope.next_receipt().await.unwrap();
                assert!(driver.matches_joined(joined.joined()));
                joined.joined().result().unwrap();
                scope.acknowledge(&joined).unwrap();
                assert!(
                    receiver
                        .registration()
                        .retained_cached_input(&original)
                        .is_none(),
                    "a completed driver is not a live cached receiver"
                );
                assert!(!receiver
                    .registration()
                    .retains_cached_carrier_loan(&original));
            } else {
                drop(io);
                drop(view);
            }
            assert!(receiving.retire(deadline()).await.unwrap().is_complete());
            assert!(observation.retained().is_none());
            assert!(original.closed().unwrap().matches(&original));
            assert!(original.users_closed().unwrap().matches(&original));
            assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
            assert!(receiver.shutdown(deadline()).await.unwrap().is_complete());
            assert!(donor.shutdown(deadline()).await.unwrap().is_complete());
            scope.begin_shutdown();
            let end = deadline();
            while let Some(receipt) = tokio::time::timeout_at(end, scope.next_receipt())
                .await
                .unwrap()
            {
                receipt.result().unwrap();
                scope.acknowledge(&receipt).unwrap();
            }
            assert!(scope.is_joined() && scope.is_drained());
        }
    })
    .await
    .unwrap();
}
