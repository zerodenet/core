//! Real native originals, unique view return and actual scoped wire consumption.
use super::*;
use std::{
    future::{Future, poll_fn},
    net::SocketAddr,
    task::Poll,
};
use zero_platform_tokio::EgressInterfaceControl;
use zero_traits::DialPolicy;
struct NativePolicy {
    policy: DialPolicy,
    egress: EgressInterfaceControl,
}
impl DatagramBindingAuthorization for NativePolicy {
    fn policy(&self) -> &DialPolicy {
        &self.policy
    }
    fn preferred_port(&self) -> Option<u16> {
        None
    }
    fn check_current(&self, expected: NetworkGeneration) -> Result<(), ChannelError> {
        if expected.0 == self.egress.generation() {
            Ok(())
        } else {
            Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ))
        }
    }
    fn select(
        &self,
        peer: SocketAddr,
        expected: NetworkGeneration,
    ) -> Result<zero_platform_tokio::EgressSelection, ChannelError> {
        self.check_current(expected)?;
        Ok(self.egress.select_for_peer(peer))
    }
}

fn declaration() -> ResourceDeclaration {
    ResourceDeclaration::BoundOutboundDatagram {
        max_sockets: NonZeroUsize::new(1).unwrap(),
        max_bindings: NonZeroUsize::new(1).unwrap(),
    }
}
fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentCatalog::default().allocate_id().unwrap(),
        generation: ResourceGeneration(1),
    }
}
fn source() -> Arc<NativePolicy> {
    Arc::new(NativePolicy {
        policy: DialPolicy::default(),
        egress: EgressInterfaceControl::default(),
    })
}
fn request(provider: &HostBoundDatagramResources, peer: SocketAddr) -> BoundDatagramRequest {
    let source = source();
    let generation = NetworkGeneration(source.egress.generation());
    BoundDatagramRequest {
        binding: provider.authorize(source, generation).unwrap(),
        peer,
        expected_network_generation: generation,
    }
}
#[tokio::test]
async fn taskless_original_rotation_exceeds_native_bound_without_waiting_for_shared_peer_exit() {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = peer.local_addr().unwrap();
    let stop = scope.registrar().cancellation();
    let (task, witness) = scope.registrar().spawn_tracked(TaskClass::Critical, async move {
        let mut bytes = [0; 64];
        loop { tokio::select! { _ = stop.cancelled() => break, received = peer.recv_from(&mut bytes) => { let (size, from) = received.unwrap(); peer.send_to(&bytes[..size], from).await.unwrap(); } } }
        Ok(())
    }).unwrap();
    let provider =
        HostBoundDatagramResources::new(instance(), task, &declaration(), CancellationToken::new())
            .unwrap();
    let request = request(&provider, address);
    provider.activate(task).unwrap();
    for _ in 0..6 {
        let grant = provider
            .open_bound_outbound_datagram(request.clone())
            .unwrap();
        grant
            .socket
            .send_to_addr(b"same live association", address)
            .await
            .unwrap();
        let mut bytes = [0; 64];
        let (size, from) = tokio::time::timeout(
            Duration::from_secs(2),
            grant.socket.recv_from_addr(&mut bytes),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(from, address);
        assert_eq!(&bytes[..size], b"same live association");
        let returned = grant.socket.return_observation();
        let receipt = grant.socket.close().unwrap();
        assert_eq!(receipt.reservation(), grant.socket.reservation());
        assert!(!returned.is_returned());
        assert!(!provider.take_closed_io().is_complete());
        assert_eq!(
            provider
                .open_bound_outbound_datagram(request.clone())
                .unwrap_err()
                .reason,
            Reason::Capacity
        );
        drop(grant);
        assert!(returned.is_returned());
        assert!(
            !witness.is_joined(),
            "taskless socket rotation must not wait for the shared peer caller to exit"
        );
    }
    let report = scope
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await;
    assert!(report.is_complete());
    assert!(witness.is_joined());
    assert!(provider.shutdown_io().unwrap().is_complete());
}

#[tokio::test]
async fn scoped_wire_native_close_and_view_return_do_not_substitute_actual_join() {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let provider = HostBoundDatagramResources::new_scoped(
        instance(),
        scope.registrar().scope_identity(),
        &declaration(),
        CancellationToken::new(),
    )
    .unwrap();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let request = request(&provider, peer.local_addr().unwrap());
    let (transfer, io) = tokio::sync::oneshot::channel::<
        zero_platform_tokio::contract::component::GrantedBoundDatagram,
    >();
    let (entered, pending) = tokio::sync::oneshot::channel();
    let (close, closing) = tokio::sync::oneshot::channel();
    let (result, native_closed) = tokio::sync::oneshot::channel();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let grant = io.await.unwrap();
            let mut bytes = [0; 64];
            let mut receive = Box::pin(grant.socket.recv_from_addr(&mut bytes));
            poll_fn(|cx| {
                assert!(receive.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            entered.send(()).unwrap();
            closing.await.unwrap();
            let receipt = grant.socket.close().unwrap();
            assert!(receive.await.is_err());
            drop(grant);
            result.send(receipt).unwrap();
            Ok(())
        })
        .unwrap();
    provider.activate_driver(&witness).unwrap();
    let grant = provider
        .open_bound_outbound_datagram(request.clone())
        .unwrap();
    let returned = grant.socket.return_observation();
    let origin = grant.socket.original().unwrap();
    transfer.send(grant).unwrap();
    pending.await.unwrap();
    assert!(origin.closed().is_none());
    close.send(()).unwrap();
    let receipt = native_closed.await.unwrap();
    assert_eq!(origin.closed().unwrap().receipt(), receipt);
    assert!(returned.is_returned());
    assert!(
        !witness.is_joined(),
        "the real exit has not been consumed by its Scope"
    );
    assert_eq!(
        provider
            .open_bound_outbound_datagram(request)
            .unwrap_err()
            .reason,
        Reason::Capacity
    );
    assert!(provider.shutdown_io().is_err());
    scope.next_exit().await.unwrap().result.unwrap();
    assert!(witness.is_joined());
    assert!(provider.shutdown_io().unwrap().is_complete());
}
