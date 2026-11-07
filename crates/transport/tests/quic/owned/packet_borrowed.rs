use super::*;
use crate::quic::owned::packet::{BorrowedPacketSegment, PacketSocket};
use std::{io::IoSliceMut, time::Duration};
use tokio::time::{timeout, Instant};
use zero_core::{
    component::{
        AssociationId, ComponentId, ComponentInstanceId, NetworkGeneration, OutboundId,
        ResourceGeneration, RoleId, RoleInstanceId,
    },
    request::Destination,
    Address,
};
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration,
        lifecycle::ResourceLifecycle,
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        scope::TaskScope,
        CancellationToken, Reason,
    },
    controlled_carrier::{CarrierOriginalTotals, CarrierOwner, NativeCarrierOpener},
    EgressInterfaceControl,
};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
fn instance(id: u64) -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(id),
        generation: ResourceGeneration(1),
    }
}

#[tokio::test]
async fn borrowed_packet_uses_actual_udp_and_holds_queues_until_original_scope_ack() {
    actual_borrowed_wire(false, false).await;
}
#[tokio::test]
async fn cancelled_host_keeps_same_borrowed_driver_for_final_accepted_fifo_and_actual_ack() {
    actual_borrowed_wire(true, false).await;
}
#[tokio::test]
async fn failed_original_borrowed_driver_returns_real_queues_and_native_proof_with_same_fault_after_held_ack(
) {
    actual_borrowed_wire(false, true).await;
}
async fn actual_borrowed_wire(cancelled: bool, faulted: bool) {
    timeout(Duration::from_secs(5), async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let endpoint = Destination::new(Address::Ipv4([127, 0, 0, 1]), remote.port());
        let egress = Arc::new(EgressInterfaceControl::default());
        let direct = UpstreamBinding {
            prefix: vec![],
            dial: zero_traits::DialPolicy {
                source_ip: Some("127.0.0.1".parse().unwrap()),
                ..Default::default()
            },
            network_generation: NetworkGeneration(egress.generation()),
        };
        let context = CarrierContext {
            session: None,
            deadline: Some(end()),
            cancel: CancellationToken::new(),
        };
        let host = Arc::new(CancellationToken::new());
        let mut donor_scope = TaskScope::empty(tokio::runtime::Handle::current());
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(bound(1)));
        scope.enable_registration().unwrap();
        let totals = CarrierOriginalTotals::new(bound(2));
        let buffer_book = totals.buffer_budget();
        let (socket, queues) =
            PacketSocket::prepare_with_budget(remote, buffer_book.clone(), &[]).unwrap();
        let mut donor = CarrierOwner::new(instance(935), bound(1), &host);
        donor
            .registration()
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledDatagramCarrier {
                    endpoint: endpoint.clone(),
                    binding: direct.clone(),
                    max_sockets: bound(1),
                },
                &donor_scope.registrar(),
                &host,
                &totals,
            )
            .unwrap();
        donor.start(end()).await.unwrap();
        let mut grant = donor.grants().reserve().unwrap();
        let original = grant.adoption();
        let view = grant.claim_transferred_view().unwrap();
        let native = NativeCarrierOpener::new(instance(935), direct.clone(), remote, egress, None)
            .unwrap()
            .prepare_controlled_datagram(&direct, endpoint.clone(), context.clone(), grant)
            .unwrap();
        let local = native.local_addr().unwrap();
        let mut loan = native.into_datagram_loan(view, AssociationId(945)).unwrap();
        let observed = loan.receiver_observation();
        let origin = loan.owner.original().clone();
        // Native donor authority remains independent. The receiver's original
        // cleanup fence is retained until endpoint/drivers return, while its
        // actual host binding still closes admission immediately.
        let receiver_host = Arc::new(CancellationToken::new());
        let cleanup = Arc::new(CancellationToken::new());
        let mut receiver = CarrierOwner::new(instance(936), bound(1), &cleanup);
        receiver
            .registration()
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledServiceDatagram {
                    endpoint: endpoint.clone(),
                    dial: direct.dial.clone(),
                    max_sockets: bound(1),
                },
                &scope.registrar(),
                &receiver_host,
                &totals,
            )
            .unwrap();
        receiver.start(end()).await.unwrap();
        let mut grant = receiver.grants().reserve().unwrap();
        let adopted = grant.adoption();
        let view = grant.claim_driver_view().unwrap();
        let prefix = UpstreamBinding {
            prefix: vec![RoleInstanceId {
                component: instance(935).component,
                role: RoleId::Outbound(OutboundId(1)),
                generation: instance(935).generation,
            }],
            ..direct
        };
        let borrowed = grant
            .adopt_service_datagram_loan(&prefix, endpoint, &context, loan)
            .unwrap();
        assert!(observed
            .retained()
            .unwrap()
            .receiver_adoption()
            .unwrap()
            .same_slot(&adopted));
        let mut segment =
            match BorrowedPacketSegment::prepare(borrowed, view, queues, scope.registrar(), remote)
            {
                Ok(segment) => segment,
                Err(_) => panic!("same actual loan"),
            };
        segment.start().unwrap();
        assert!(
            adopted.retained_driver().is_none(),
            "a service is not a retained cached pool"
        );
        assert_eq!(socket.socket.max_transmit_segments(), 1);
        socket
            .socket
            .try_send(&quinn::udp::Transmit {
                destination: remote,
                ecn: None,
                contents: b"original borrowed wire",
                segment_size: None,
                src_ip: None,
            })
            .unwrap();
        let mut bytes = [0; 64];
        let (size, sender) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(sender, local);
        assert_eq!(&bytes[..size], b"original borrowed wire");
        peer.send_to(b"same packet return", sender).await.unwrap();
        let mut metadata = [quinn::udp::RecvMeta::default()];
        let mut buffers = [IoSliceMut::new(&mut bytes)];
        let count = poll_fn(|cx| socket.socket.poll_recv(cx, &mut buffers, &mut metadata))
            .await
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(metadata[0].addr, remote);
        assert_eq!(&bytes[..metadata[0].len], b"same packet return");
        if faulted {
            // Revoke this actual donor while its real UDP receive is live.
            // No fake protocol report or fabricated task outcome is injected.
            while !segment.actual_receive_pending() {
                tokio::task::yield_now().await;
            }
            assert!(!segment.driver().unwrap().is_joined());
            original.request_retirement();
        } else if cancelled {
            scope.begin_shutdown();
            receiver_host.cancel();
            assert!(
                receiver.grants().reserve().is_err(),
                "actual host cancellation still denies new acquisition"
            );
            for contents in [
                b"accepted final data".as_slice(),
                b"accepted endpoint close".as_slice(),
            ] {
                socket
                    .socket
                    .try_send(&quinn::udp::Transmit {
                        destination: remote,
                        ecn: None,
                        contents,
                        segment_size: None,
                        src_ip: None,
                    })
                    .unwrap();
            }
            segment.stop_final();
            for expected in [
                b"accepted final data".as_slice(),
                b"accepted endpoint close".as_slice(),
            ] {
                let (count, sender) = peer.recv_from(&mut bytes).await.unwrap();
                assert_eq!(sender, local);
                assert_eq!(&bytes[..count], expected);
            }
            assert!(
                socket
                    .socket
                    .try_send(&quinn::udp::Transmit {
                        destination: remote,
                        ecn: None,
                        contents: b"too late",
                        segment_size: None,
                        src_ip: None,
                    })
                    .is_err(),
                "the same final original queue is sealed"
            );
        } else {
            segment.stop();
        }
        let receipt = scope.next_receipt().await.unwrap();
        let actual_failure = receipt.result().err();
        assert_eq!(actual_failure.is_some(), faulted);
        if let Some(source) = &actual_failure {
            assert_eq!(source.reason, Reason::Io);
        }
        assert!(!segment
            .driver()
            .unwrap()
            .try_joined()
            .unwrap()
            .registration_released());
        let result = segment
            .retire(Instant::now() + Duration::from_millis(10))
            .await;
        assert_eq!(result.err().unwrap().reason, Reason::Timeout);
        assert!(
            origin.closed().is_none(),
            "driver join alone has not closed the actual donor"
        );
        assert!(
            observed.retained().is_some(),
            "original controller remains strongly held before ACK"
        );
        scope.acknowledge(&receipt).unwrap();
        let _same_queues = if faulted {
            let returned = segment.retire_outcome(end()).await.unwrap();
            let (queues, joined, report) = returned.into_parts();
            let joined = joined.expect("same actual failed packet driver");
            assert_eq!(joined.id(), receipt.joined().id());
            assert!(Arc::ptr_eq(
                &joined.result().unwrap_err(),
                actual_failure.as_ref().unwrap()
            ));
            assert!(joined.failure_accepted());
            assert!(joined.registration_released());
            assert!(report.is_complete());
            assert!(report
                .failed_tasks
                .iter()
                .any(|failure| failure.id == joined.id()));
            assert!(report.exited_tasks.contains(&joined.id()));
            let row = report
                .failed_tasks
                .iter()
                .find(|row| row.id == joined.id())
                .unwrap();
            assert_eq!(row.class, joined.class());
            assert_eq!(row.error.stage, actual_failure.as_ref().unwrap().stage);
            assert_eq!(row.error.reason, actual_failure.as_ref().unwrap().reason);
            let reported = std::error::Error::source(&row.error)
                .unwrap()
                .downcast_ref::<Arc<zero_platform_tokio::contract::ChannelError>>()
                .expect("same retained original driver cause");
            assert!(Arc::ptr_eq(reported, actual_failure.as_ref().unwrap()));
            queues
        } else {
            segment.retire(end()).await.unwrap()
        };
        assert!(adopted
            .closed()
            .unwrap()
            .borrowed_origin()
            .unwrap()
            .same_original(&origin));
        assert!(original.closed().unwrap().had_native_acquisition());
        assert!(origin.closed().unwrap().matches(&origin));
        assert!(receiver.shutdown(end()).await.unwrap().is_complete());
        assert!(donor.shutdown(end()).await.unwrap().is_complete());
        scope.begin_shutdown();
        assert!(scope.is_drained());
        assert!(donor_scope.shutdown(end()).await.is_complete());
        assert_eq!(totals.snapshot().charged, 0);
        assert!(
            buffer_book.used_bytes() > 0,
            "returned unique queues retain their actual scratch"
        );
        drop(_same_queues);
        drop(socket);
        drop(segment);
        assert_eq!(buffer_book.used_bytes(), 0);
    })
    .await
    .unwrap();
}
