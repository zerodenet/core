//! Real native UDP, actual pending auxiliary IO/failure, and delayed Host ACK.
use super::*;
use crate::contract::{component::ResourceDeclaration, network::UpstreamOpener};
use std::future::{poll_fn, Future};
use std::task::Poll;
use tokio::net::UdpSocket;
use zero_core::component::AssociationId;
#[tokio::test]
async fn same_native_loan_observes_late_actual_close_and_ack_after_original_helper_fault() {
    timeout(Duration::from_secs(6), async {
        let peer = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let remote = peer.local_addr().unwrap();
        let endpoint = destination(remote);
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: zero_traits::DialPolicy {
                source_ip: Some("127.0.0.1".parse().unwrap()),
                ..Default::default()
            },
            network_generation: NetworkGeneration(egress.generation()),
        };
        let opener =
            NativeCarrierOpener::new(instance(), binding.clone(), remote, egress, None).unwrap();
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let mut owner = CarrierOwner::new(
            instance(),
            NonZeroUsize::new(1).unwrap(),
            &CancellationToken::new(),
        );
        let registration = owner.registration();
        registration
            .bind_declared(
                &ResourceDeclaration::ControlledDatagramCarrier {
                    endpoint: endpoint.clone(),
                    binding: binding.clone(),
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &scope.registrar(),
                &Arc::new(CancellationToken::new()),
            )
            .unwrap();
        owner.start(deadline()).await.unwrap();
        let mut grant = owner.grants().reserve().unwrap();
        let view = grant.claim_transferred_view().unwrap();
        let adoption = grant.adoption();
        let native = opener
            .prepare_controlled_datagram(&binding, endpoint.clone(), context(), grant)
            .unwrap();
        let local = native.local_addr().unwrap();
        let mut loan = native.into_datagram_loan(view, AssociationId(98)).unwrap();
        let (pending, observed) = tokio::sync::oneshot::channel();
        let server = peer.clone();
        let (id, witness) = adoption
            .spawn_auxiliary(&scope.registrar(), async move {
                let mut bytes = [0; 32];
                let (size, from) = {
                    let receiving = server.recv_from(&mut bytes);
                    tokio::pin!(receiving);
                    let mut pending = Some(pending);
                    poll_fn(|cx| match receiving.as_mut().poll(cx) {
                        Poll::Pending => {
                            if let Some(actual) = pending.take() {
                                actual.send(()).unwrap();
                            }
                            Poll::Pending
                        }
                        Poll::Ready(result) => Poll::Ready(result),
                    })
                    .await
                    .unwrap()
                };
                assert_eq!(from, local);
                assert_eq!(&bytes[..size], b"malformed actual reply");
                // The real awaited receive has completed with a malformed frame;
                // the actual same-Scope helper reports that protocol failure.
                Err(error(
                    instance(),
                    Stage::Receive,
                    Reason::Protocol,
                    DeliveryState::Unknown,
                )
                .with_source(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "actual received malformed helper frame",
                )))
            })
            .unwrap();
        observed.await.unwrap();
        assert!(!witness.is_joined());
        loan.io.send(b"malformed actual reply").await.unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), id);
        let original_error = receipt.result().unwrap_err();
        assert!(!receipt.joined().registration_released());
        assert!(loan.owner.closed().is_none());
        // The first real close waits on this exact still-held view. The helper
        // fault cannot turn actual view custody into completion.
        let report = loan
            .owner
            .close(Instant::now() + Duration::from_millis(10))
            .await
            .unwrap();
        assert!(!report.is_complete());
        assert!(adoption.closed().is_none());
        drop(loan.io);
        // External same-original provider progress accepts the actual helper
        // error and closes native, without calling the loan control again.
        let native_report = adoption.retire_native(deadline()).await.unwrap();
        assert!(native_report.closed().unwrap().matches(&adoption));
        let native = adoption
            .closed()
            .expect("matching original native close precedes Host ACK");
        assert!(native.matches(&adoption) && native.had_native_acquisition());
        assert!(adoption.users_closed().unwrap().matches(&adoption));
        let failed = adoption.auxiliary_exit().unwrap();
        assert!(Arc::ptr_eq(&failed.result().unwrap_err(), &original_error));
        assert!(witness.try_joined().unwrap().failure_accepted());
        assert!(
            loan.owner.closed().is_none(),
            "logical observer cannot skip actual registration ACK"
        );
        assert_eq!(
            owner.grants().reserve().err().unwrap().reason,
            Reason::Capacity
        );
        let callbacks = scope.defer_callbacks();
        scope.acknowledge(&receipt).unwrap();
        assert!(!witness.try_joined().unwrap().registration_released());
        assert!(loan.owner.closed().is_none());
        drop(callbacks);
        let joined = witness.wait_registration_released().await;
        assert!(witness.matches_joined(&joined) && joined.registration_released());
        // External original-provider progress only; no second call to the
        // loan's failed/pending control is needed to observe this exact close.
        let completed = adoption.retire(deadline()).await.unwrap();
        assert!(completed.is_complete());
        assert!(completed.failed_tasks.iter().any(|failed| failed.id == id
            && failed
                .error
                .retained_source()
                .is_some_and(|source| Arc::ptr_eq(source, &original_error))));

        assert!(loan.owner.closed().unwrap().matches(&loan.owner));
        assert!(Arc::ptr_eq(&failed.result().unwrap_err(), &original_error));
        let mut next = owner.grants().reserve().unwrap();
        let view = next.claim_transferred_view().unwrap();
        let next_original = next.adoption();
        let native = opener
            .prepare_controlled_datagram(&binding, endpoint, context(), next)
            .unwrap();
        let local = native.local_addr().unwrap();
        let mut next = native.into_datagram_loan(view, AssociationId(99)).unwrap();
        next.io.send(b"recovered").await.unwrap();
        let mut bytes = [0; 32];
        let (size, from) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(from, local);
        assert_eq!(&bytes[..size], b"recovered");
        drop(next.io);
        assert!(next.owner.close(deadline()).await.unwrap().is_complete());
        assert!(next_original.closed().unwrap().matches(&next_original));
        assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
        assert!(registration.closed().unwrap().matches(&registration));
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
