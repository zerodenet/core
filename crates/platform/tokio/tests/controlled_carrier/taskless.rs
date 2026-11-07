use super::*;
use crate::contract::component::{ControlledStreamDemandClass, ResourceDeclaration};

#[tokio::test]
async fn taskless_stream_loan_requires_original_control_and_returned_native_custody() {
    let mut fixture = Fixture::cold(1).await;
    let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
    let actual = fixture.owner.registration();
    actual
        .bind_declared(
            &ResourceDeclaration::ControlledOutboundStream {
                endpoint: fixture.destination.clone(),
                binding: fixture.transport.binding().clone(),
                demand: ControlledStreamDemandClass::BackgroundControl,
                max_sockets: NonZeroUsize::new(1).unwrap(),
            },
            &scope.registrar(),
            &Arc::new(CancellationToken::new()),
        )
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    let mut grant = fixture.grants.reserve().unwrap();
    let view = grant.claim_transferred_view().unwrap();
    let carrier = fixture
        .transport
        .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
        .unwrap()
        .open(deadline())
        .await
        .unwrap();
    let (mut peer, _) = fixture.listener.accept().await.unwrap();
    let (io, adoption) = carrier.into_parts();
    let mut loan = io.into_stream_loan(view).unwrap();
    assert_eq!(
        loan.owner
            .take_logical_close_participant()
            .err()
            .unwrap()
            .reason,
        Reason::Revoked,
        "the exact NativeControl cannot be replaced with logical completion"
    );
    let original = loan.owner.original().clone();
    assert!(original.driver().is_none());
    loan.io.write_all(b"x").await.unwrap();
    peer.read_exact(&mut [0; 1]).await.unwrap();
    {
        use std::future::Future;
        let mut bytes = [0; 1];
        let mut reading = Box::pin(loan.io.read_exact(&mut bytes));
        std::future::poll_fn(|cx| {
            assert!(
                reading.as_mut().poll(cx).is_pending(),
                "the actual native read is suspended"
            );
            std::task::Poll::Ready(())
        })
        .await;
        loan.owner.cancel(crate::contract::CloseReason::Shutdown);
        assert_eq!(
            reading.await.unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
    }
    assert!(!loan
        .owner
        .close(Instant::now() + Duration::from_millis(10))
        .await
        .unwrap()
        .is_complete());
    assert!(original.closed().is_none() && adoption.closed().is_none());
    assert_eq!(
        fixture.grants.reserve().err().unwrap().reason,
        Reason::Capacity
    );
    assert!(!loan.owner.try_close().unwrap().is_complete());
    assert_eq!(fixture.owner.retained(), 1);
    drop(loan.io);
    assert!(
        original.closed().is_none(),
        "returning a view does not execute original native close"
    );
    let report = loan.owner.try_close().unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert!(loan.owner.try_close().unwrap().closed_io.is_empty());
    assert!(loan.owner.closed().unwrap().matches(&loan.owner));
    assert!(original.closed().unwrap().matches(&original));
    assert!(adoption.closed().unwrap().driver_exit().is_none());
    assert!(adoption.closed().unwrap().had_native_acquisition());
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    assert!(
        actual.closed().is_none(),
        "slot closure is not whole-owner closure"
    );
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(actual.closed().unwrap().matches(&actual));
    let report = scope.shutdown(deadline()).await;
    assert!(report.is_complete() && report.exited_tasks.is_empty());
}

#[tokio::test]
async fn taskless_packet_loan_closes_its_actual_native_original_without_an_invented_worker() {
    use crate::contract::network::UpstreamOpener;
    use zero_core::component::AssociationId;
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
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
    let mut owner = CarrierOwner::new(
        instance(),
        NonZeroUsize::new(1).unwrap(),
        &CancellationToken::new(),
    );
    let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
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
    let original_slot = grant.adoption();
    let native = opener
        .prepare_controlled_datagram(&binding, endpoint, context(), grant)
        .unwrap();
    let local = native.local_addr().unwrap();
    let mut loan = native.into_datagram_loan(view, AssociationId(98)).unwrap();
    assert_eq!(
        loan.owner
            .take_logical_close_participant()
            .err()
            .unwrap()
            .reason,
        Reason::Revoked,
        "the exact NativeControl cannot be replaced with logical completion"
    );
    let original = loan.owner.original().clone();
    assert!(original.driver().is_none());
    loan.io.send(b"udp").await.unwrap();
    let mut bytes = [0; 16];
    let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
    assert_eq!(source, local);
    let noise = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    noise.send_to(b"foreign peer", source).await.unwrap();
    peer.send_to(&bytes[..size], source).await.unwrap();
    assert_eq!(loan.io.receive(&mut bytes).await.unwrap(), 3);
    assert!(!loan
        .owner
        .close(Instant::now() + Duration::from_millis(10))
        .await
        .unwrap()
        .is_complete());
    drop(loan.io);
    assert!(original.closed().is_none());
    assert!(loan.owner.close(deadline()).await.unwrap().is_complete());
    assert!(original.closed().unwrap().matches(&original));
    assert!(original_slot.closed().unwrap().driver_exit().is_none());
    assert!(original_slot.closed().unwrap().had_native_acquisition());
    assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
    assert!(registration.users_closed().unwrap().matches(&registration));
    let report = scope.shutdown(deadline()).await;
    assert!(report.is_complete() && report.exited_tasks.is_empty());
}

mod observation;
