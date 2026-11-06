use super::*;
use crate::contract::{component::ResourceDeclaration, network::UpstreamOpener};
use std::future::Future;
use tokio::net::UdpSocket;

#[tokio::test]
async fn negotiated_packet_keeps_both_originals_and_uses_one_actual_total_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let packet_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let egress = Arc::new(crate::EgressInterfaceControl::default());
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(egress.generation()),
    };
    let endpoint = destination(address);
    let opener =
        NativeCarrierOpener::new(instance(), binding.clone(), address, egress, None).unwrap();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut owner = CarrierOwner::new(
        instance(),
        NonZeroUsize::new(2).unwrap(),
        &CancellationToken::new(),
    );
    let original = owner.registration();
    original
        .bind_declared(
            &ResourceDeclaration::ControlledAssociationPool {
                endpoint: endpoint.clone(),
                dial: binding.dial.clone(),
                max_sockets: NonZeroUsize::new(2).unwrap(),
            },
            &scope.registrar(),
            &Arc::new(CancellationToken::new()),
        )
        .unwrap();
    owner.start(deadline()).await.unwrap();
    let mut grant = owner.grants().reserve().unwrap();
    let control_view = grant.claim_transferred_view().unwrap();
    let parent = grant.adoption();
    let stop = grant.cancellation();
    // A reservation is not a control original and cannot authorize another peer.
    assert!(opener
        .resolve_related_datagram(
            &binding,
            parent.clone(),
            destination(packet_peer.local_addr().unwrap()),
            context()
        )
        .await
        .is_err());
    let control = opener
        .prepare_controlled_stream(&binding, endpoint, context(), grant)
        .unwrap()
        .open(deadline())
        .await
        .unwrap();
    let (_accepted, _) = listener.accept().await.unwrap();
    let candidates = opener
        .resolve_related_datagram(
            &binding,
            parent.clone(),
            destination(packet_peer.local_addr().unwrap()),
            context(),
        )
        .await
        .unwrap();
    let mut grant = owner.grants().reserve().unwrap();
    let packet_view = grant.claim_transferred_view().unwrap();
    let child = grant.adoption();
    let packet = candidates.prepare_datagram(0, grant).unwrap();
    assert!(
        owner.grants().reserve().is_err(),
        "TCP and UDP charge the same total original cap"
    );
    packet.send(b"original").await.unwrap();
    let mut bytes = [0; 16];
    let (size, _) = packet_peer.recv_from(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"original");
    // Poll the actual native receive to Pending, with no peer response queued.
    let mut receiving = Box::pin(packet.receive(&mut bytes));
    assert!(receiving
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
        .is_pending());
    stop.cancel();
    assert_eq!(
        receiving.await.unwrap_err().kind(),
        std::io::ErrorKind::Interrupted
    );
    drop(control);
    drop(control_view);
    let blocked = parent
        .retire(Instant::now() + Duration::from_millis(10))
        .await
        .unwrap();
    assert!(!blocked.is_complete());
    assert!(
        parent.closed().is_none() && child.closed().is_none(),
        "retained actual UDP view blocks original TCP close"
    );
    drop(packet);
    drop(packet_view);
    assert!(parent.retire(deadline()).await.unwrap().is_complete());
    assert!(child.closed().unwrap().matches(&child));
    assert!(child.closed().unwrap().had_native_acquisition());
    assert!(parent.closed().unwrap().matches(&parent));
    assert!(parent.closed().unwrap().had_native_acquisition());
    assert!(
        parent.closed().unwrap().driver_exit().is_none(),
        "taskless originals do not invent a driver witness"
    );
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(original.closed().unwrap().matches(&original));
    assert!(scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn related_peer_does_not_expand_a_stream_only_declaration() {
    let mut f = Fixture::cold(2).await;
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let binding = f.transport.binding().clone();
    f.owner
        .registration()
        .bind_declared(
            &ResourceDeclaration::ControlledCarrierPool {
                endpoint: f.destination.clone(),
                dial: binding.dial.clone(),
                datagram: false,
                max_sockets: NonZeroUsize::new(2).unwrap(),
            },
            &scope.registrar(),
            &Arc::new(CancellationToken::new()),
        )
        .unwrap();
    f.owner.start(deadline()).await.unwrap();
    let (control, _peer) = f.connected().await;
    let parent = control.adoption().clone();
    let grant = f.grants.reserve().unwrap();
    assert!(grant.link_related_datagram(&parent).is_err());
    drop(grant);
    drop(control);
    assert!(f.owner.shutdown(deadline()).await.unwrap().is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn poll_based_loan_reexposure_retains_both_real_views_until_matching_native_close() {
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = peer.local_addr().unwrap();
    let egress = Arc::new(crate::EgressInterfaceControl::default());
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(egress.generation()),
    };
    let opener =
        NativeCarrierOpener::new(instance(), binding.clone(), address, egress, None).unwrap();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut owner = CarrierOwner::new(
        instance(),
        NonZeroUsize::new(1).unwrap(),
        &CancellationToken::new(),
    );
    owner
        .registration()
        .bind_declared(
            &ResourceDeclaration::ControlledDatagramCarrier {
                endpoint: destination(address),
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
    let io = opener
        .prepare_controlled_datagram(&binding, destination(address), context(), grant)
        .unwrap();
    let loan = io
        .into_datagram_loan(view, zero_core::component::AssociationId(7))
        .unwrap();
    let (mut session, original) = loan.into_session();
    assert_eq!(original.endpoints().unwrap().peer(), address);
    assert!(session
        .rx
        .as_mut()
        .poll_recv(&mut std::task::Context::from_waker(std::task::Waker::noop()))
        .is_pending());
    assert!(!session
        .control
        .wait_closed(Instant::now() + Duration::from_millis(5))
        .await
        .unwrap()
        .is_complete());
    drop(session.tx);
    assert!(!session
        .control
        .wait_closed(Instant::now() + Duration::from_millis(5))
        .await
        .unwrap()
        .is_complete());
    assert!(adoption.closed().is_none());
    drop(session.rx);
    assert!(session
        .control
        .wait_closed(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(adoption.closed().unwrap().matches(&adoption));
    assert!(adoption.closed().unwrap().had_native_acquisition());
    assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}
