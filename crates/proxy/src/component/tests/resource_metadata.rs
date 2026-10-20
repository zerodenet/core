use super::*;
use std::{future::poll_fn, io::IoSliceMut, num::NonZeroUsize, time::Duration};
use zero_core::component::{ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::{
    contract::{
        component::ResourceAccess,
        scope::{TaskClass, TaskScope},
    },
    controlled_datagram::metadata::{MetadataReceive, MetadataTransmit},
};
fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn limits() -> MetadataLimits {
    MetadataLimits {
        main_sockets: n(1),
        probe_sockets: n(1),
        probe_attempts_per_initialization: n(4),
        concurrent_send_calls: n(1),
        write_pollers: n(1),
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(1967),
        generation: ResourceGeneration(2),
    }
}
fn declaration(source: SocketAddr, attempts: usize) -> ResourceDeclaration {
    ResourceDeclaration::MetadataDatagram {
        source,
        limits: limits(),
        max_bind_attempts: n(attempts),
    }
}
fn resources(scope: &TaskScope, source: SocketAddr, attempts: usize) -> Arc<HostResources> {
    HostResources::new_for_instance(
        instance(),
        &[declaration(source, attempts)],
        CancellationToken::new(),
        scope.registrar().scope_identity(),
    )
    .unwrap()
}
fn request(source: SocketAddr) -> MetadataDatagramRequest {
    MetadataDatagramRequest {
        ordinal: 0,
        source,
        limits: limits(),
    }
}
fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

#[tokio::test]
async fn exact_ordinal_full_limits_start_phase_and_original_scope_are_required_without_native_attempts(
) {
    let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(n(1)));
    let source = loopback();
    assert!(HostResources::new(
        instance().component,
        &[declaration(source, 1)],
        CancellationToken::new()
    )
    .is_err());
    let host = resources(&scope, source, 1);
    let access = host.activate(deadline()).unwrap();
    assert!(host
        .listen_metadata_datagram(request(source))
        .await
        .is_err());
    scope.enable_registration().unwrap();
    drop(access);
    assert!(host
        .listen_metadata_datagram(request(source))
        .await
        .is_err());
    let _access = host.activate(deadline()).unwrap();
    let mut wrong = request(source);
    wrong.ordinal = 1;
    assert!(host.listen_metadata_datagram(wrong).await.is_err());
    wrong = request(source);
    wrong.limits.write_pollers = n(2);
    assert!(host.listen_metadata_datagram(wrong).await.is_err());
    wrong = request(source);
    wrong.source.set_ip("127.0.0.2".parse().unwrap());
    assert!(host.listen_metadata_datagram(wrong).await.is_err());
    assert!(!host.io_evidence().metadata[0].attempted);
    let grant = host
        .listen_metadata_datagram(request(source))
        .await
        .unwrap();
    let local = grant.tx.local_addr().unwrap();
    assert!(grant.scope.matches(&scope.registrar()));
    let foreign = TaskScope::new(tokio::runtime::Handle::current(), n(1));
    assert!(!grant.scope.matches(&foreign.registrar()));
    assert!(!grant
        .scope
        .same_scope(&foreign.registrar().scope_identity()));
    assert!(grant.observation.matches(&grant.rx, &grant.tx));
    let report = host.shutdown_io(deadline()).await.unwrap();
    assert!(!report.is_complete());
    let original = std::net::UdpSocket::bind(local).unwrap();
    drop(original);
    assert!(scope.shutdown(deadline()).await.is_complete());
    assert!(!host.io_evidence().physical_confirmed());
}

#[tokio::test]
async fn actual_metadata_receive_and_original_close_remain_distinct_from_real_scope_join_and_user_graph(
) {
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), n(1));
    let host = resources(&scope, loopback(), 1);
    let _access = host.activate(deadline()).unwrap();
    let mut grant = host
        .listen_metadata_datagram(request(loopback()))
        .await
        .unwrap();
    let local = grant.tx.local_addr().unwrap();
    let peer = tokio::net::UdpSocket::bind(loopback()).await.unwrap();
    peer.send_to(b"", local).await.unwrap();
    let mut bytes = [0u8; 16];
    let mut buffers = [IoSliceMut::new(&mut bytes)];
    let mut metadata = [MetadataReceive::default()];
    assert_eq!(
        poll_fn(|cx| grant.rx.poll_recv(cx, &mut buffers, &mut metadata))
            .await
            .unwrap(),
        1
    );
    assert_eq!(metadata[0].len, 0);
    assert_eq!(metadata[0].source, peer.local_addr().unwrap());
    let (entered, pending) = tokio::sync::oneshot::channel();
    let (id, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let mut bytes = [0u8; 16];
            let mut buffers = [IoSliceMut::new(&mut bytes)];
            let mut metadata = [MetadataReceive::default()];
            let mut entered = Some(entered);
            let error = poll_fn(|cx| {
                let result = grant.rx.poll_recv(cx, &mut buffers, &mut metadata);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        let _ = entered.send(());
                    }
                }
                result
            })
            .await
            .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::NotConnected);
            Ok(())
        })
        .unwrap();
    pending.await.unwrap();
    assert!(!witness.is_joined());
    assert!(std::net::UdpSocket::bind(local).is_err());
    let report = host.shutdown_io(deadline()).await.unwrap();
    assert!(!report.is_complete());
    assert_eq!(host.io_evidence().metadata[0].main.confirmed_closed, 1);
    assert!(
        !witness.is_joined(),
        "native close does not consume the real TaskExit"
    );
    let retained = std::net::UdpSocket::bind(local).unwrap();
    assert_eq!(scope.next_exit().await.unwrap().id, id);
    assert_eq!(witness.wait().await.id(), id);
    assert!(
        !host.io_evidence().physical_confirmed(),
        "unintegrated user obligation cannot become a whole graph ACK"
    );
    assert!(scope.shutdown(deadline()).await.is_complete());
    drop(retained);
}

#[tokio::test]
async fn actual_bind_failure_keeps_same_os_source_while_later_native_close_and_retry_evidence_are_retained(
) {
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), n(1));
    let occupied = std::net::UdpSocket::bind(loopback()).unwrap();
    let local = occupied.local_addr().unwrap();
    let host = resources(&scope, local, 2);
    let _access = host.activate(deadline()).unwrap();
    let failed = host
        .listen_metadata_datagram(request(local))
        .await
        .err()
        .unwrap();
    let first = host.io_evidence().metadata[0].first_fault.clone().unwrap();
    fn acquisition<'a>(
        error: &'a (dyn std::error::Error + 'static),
    ) -> Option<&'a zero_platform_tokio::controlled_datagram::DatagramAcquisitionError> {
        if let Some(found) = error.downcast_ref() {
            return Some(found);
        }
        error.source().and_then(acquisition)
    }
    let source = acquisition(&failed).unwrap().source.clone();
    assert_eq!(source.kind(), std::io::ErrorKind::AddrInUse);
    assert!(Arc::ptr_eq(
        &source,
        &acquisition(first.as_ref()).unwrap().source
    ));
    drop(occupied);
    let grant = host.listen_metadata_datagram(request(local)).await.unwrap();
    assert!(host.listen_metadata_datagram(request(local)).await.is_err());
    assert!(std::net::UdpSocket::bind(local).is_err());
    assert!(host.shutdown_io(deadline()).await.is_err());
    let held = std::net::UdpSocket::bind(local).unwrap();
    let proof = host.io_evidence();
    assert_eq!(
        proof.metadata[0].main.confirmed_closed, 2,
        "failed original and successful original both explicitly closed"
    );
    assert!(Arc::ptr_eq(
        &first,
        proof.metadata[0].first_fault.as_ref().unwrap()
    ));
    assert!(host.shutdown_io(deadline()).await.is_err());
    let retry = host.io_evidence();
    assert_eq!(retry.metadata[0].main.confirmed_closed, 2);
    assert!(Arc::ptr_eq(
        &first,
        retry.metadata[0].first_fault.as_ref().unwrap()
    ));
    assert_eq!(
        grant
            .tx
            .try_send(&MetadataTransmit {
                destination: local,
                contents: b"x",
                source_ip: None,
                ecn: None,
                segment_size: None
            })
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotConnected
    );
    assert!(scope.shutdown(deadline()).await.is_complete());
    drop(held);
}

#[path = "resource_metadata/wake.rs"]
mod wake;

#[path = "resource_metadata/canonical.rs"]
mod canonical;
