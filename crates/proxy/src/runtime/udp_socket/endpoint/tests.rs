use super::*;
use crate::component::{
    catalog::ComponentCatalog,
    datagram_retirement::{DatagramReservation, RetiredDatagrams},
};
use std::{net::SocketAddr, time::Duration};
use tokio::time::Instant;
use zero_core::{
    component::{InboundId, ResourceGeneration, RoleId},
    Network, ProtocolType, Session,
};
use zero_platform_tokio::contract::{
    lifecycle::{erase_control, DynChannelControl, ResourceLifecycle},
    scope::TaskScope,
};

fn limits() -> DirectEndpointLimits {
    DirectEndpointLimits {
        logical: NonZeroUsize::new(4).unwrap(),
        native_sockets: NonZeroUsize::new(4).unwrap(),
        pending_native_opens: NonZeroUsize::new(1).unwrap(),
        pending_bytes: NonZeroUsize::new(1_048_576).unwrap(),
        candidates_per_job: NonZeroUsize::new(8).unwrap(),
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
struct Fixture {
    _ingress: tokio::net::UdpSocket,
    proxy: crate::runtime::Proxy,
    retirement: Arc<RetiredDatagrams>,
    dispatcher: DatagramReservation,
    catalog: ComponentCatalog,
    identity: DirectEndpointIdentity,
}
impl Fixture {
    async fn new() -> Self {
        let ingress = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let proxy = super::super::tests::proxy();
        let catalog = ComponentCatalog::default();
        // A real independent allocator for this fixture host, including its
        // actual UDP ingress. This does not activate the production catalog.
        let inbound = catalog.allocate_id().unwrap();
        let resource = catalog.allocate_id().unwrap();
        let retirement = Arc::new(RetiredDatagrams::new(NonZeroUsize::new(16).unwrap()));
        let dispatcher = retirement.reserve().unwrap();
        let identity = DirectEndpointIdentity {
            resource: ComponentInstanceId {
                component: resource,
                generation: ResourceGeneration(1),
            },
            ingress: RoleInstanceId {
                component: inbound,
                role: RoleId::Inbound(InboundId(inbound.0)),
                generation: ResourceGeneration(1),
            },
            dispatcher: dispatcher.association(),
        };
        Self {
            _ingress: ingress,
            proxy,
            retirement,
            dispatcher,
            catalog,
            identity,
        }
    }
    fn owner(
        &self,
        scope: TaskScope,
        queue: UdpReceiveQueueConfig,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> DirectEndpointOwner {
        self.owner_with_limits(scope, queue, observer, limits())
    }
    fn owner_with_limits(
        &self,
        scope: TaskScope,
        queue: UdpReceiveQueueConfig,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
        grants: DirectEndpointLimits,
    ) -> DirectEndpointOwner {
        DirectEndpointOwner::new(
            self.identity,
            crate::protocol_registry::UdpRuntimeServices::new(self.proxy.tcp_runtime_services())
                .network(),
            None,
            queue,
            grants,
            scope,
            observer,
        )
        .unwrap()
    }
    fn open(
        &self,
        endpoint: &DirectEndpoint,
        target: SocketAddr,
        cancel: CancellationToken,
    ) -> (
        DirectEndpointTx,
        DirectEndpointRx,
        Box<dyn DynChannelControl>,
    ) {
        self.open_isolated(endpoint, target, cancel, None)
    }
    fn open_isolated(
        &self,
        endpoint: &DirectEndpoint,
        target: SocketAddr,
        cancel: CancellationToken,
        isolation: Option<u64>,
    ) -> (
        DirectEndpointTx,
        DirectEndpointRx,
        Box<dyn DynChannelControl>,
    ) {
        self.try_open(endpoint, target, cancel, isolation).unwrap()
    }
    fn try_open(
        &self,
        endpoint: &DirectEndpoint,
        target: SocketAddr,
        cancel: CancellationToken,
        isolation: Option<u64>,
    ) -> Result<
        (
            DirectEndpointTx,
            DirectEndpointRx,
            Box<dyn DynChannelControl>,
        ),
        ChannelError,
    > {
        let mut business = Session::new(
            0,
            Address::Ipv4([127, 0, 0, 1]),
            target.port(),
            Network::Udp,
            ProtocolType::UNKNOWN,
        );
        self.proxy
            .engine()
            .prepare_session(&mut business, "fixture-udp")
            .unwrap();
        let reservation = self.retirement.reserve().unwrap();
        let session = endpoint.open_isolated(
            SessionId(business.id),
            reservation.association(),
            super::super::tests::policy(&self.proxy, Some("first")),
            cancel,
            isolation,
        )?;
        Ok((
            session.tx,
            session.rx,
            reservation.adopt(erase_control(session.control)),
        ))
    }
}
async fn send(tx: &mut DirectEndpointTx, peer: &tokio::net::UdpSocket, bytes: &[u8]) -> SocketAddr {
    let target = peer.local_addr().unwrap();
    let sent = tx
        .send_new_packet(&Address::Ipv4([127, 0, 0, 1]), &[target], bytes)
        .await
        .unwrap();
    assert_eq!(sent.sent, bytes.len());
    let mut packet = vec![0; 65_535];
    let (size, local) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut packet))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&packet[..size], bytes);
    local
}

#[tokio::test]
async fn actual_shared_driver_keeps_source_port_peer_tombstones_and_read_guards() {
    let fixture = Fixture::new().await;
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut owner = fixture.owner(scope, UdpReceiveQueueConfig::default(), None);
    let endpoint = owner.endpoint();
    assert_eq!(endpoint.identity(), fixture.identity);
    owner.start(deadline()).await.unwrap();
    let first = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (mut tx1, mut rx1, mut ctl1) = fixture.open(
        &endpoint,
        first.local_addr().unwrap(),
        CancellationToken::new(),
    );
    let (mut tx2, mut rx2, mut ctl2) = fixture.open(
        &endpoint,
        second.local_addr().unwrap(),
        CancellationToken::new(),
    );
    let shared = send(&mut tx1, &first, b"first").await;
    assert_eq!(send(&mut tx2, &second, b"second").await, shared);
    first.send_to(b"already read", shared).await.unwrap();
    let (bytes, old) = rx1.recv().await.unwrap().unwrap();
    assert_eq!(bytes, b"already read");
    assert!(old.is_current());
    ctl1.cancel(zero_platform_tokio::contract::CloseReason::Completed);
    assert!(
        !old.is_current(),
        "cancel revokes an already-read token synchronously"
    );
    let logical = ctl1.wait_closed(deadline()).await.unwrap();
    assert!(logical.is_complete());
    assert!(
        logical.closed_io.is_empty(),
        "logical reply retirement is not shared-fd closure"
    );
    assert!(!old.is_current());
    assert!(tokio::net::UdpSocket::bind(shared).await.is_err());
    // A same-peer new business flow must not inherit the old tuple on the still
    // live socket. The unrelated peer keeps its original source endpoint.
    let (mut tx3, mut rx3, mut ctl3) = fixture.open(
        &endpoint,
        first.local_addr().unwrap(),
        CancellationToken::new(),
    );
    let collision = send(&mut tx3, &first, b"new first").await;
    assert_ne!(collision, shared);
    assert_eq!(send(&mut tx2, &second, b"still second").await, shared);
    second.send_to(b"current", shared).await.unwrap();
    let (bytes, current) = rx2.recv().await.unwrap().unwrap();
    assert_eq!(bytes, b"current");
    assert!(current.is_current());
    assert!(ctl2.wait_closed(deadline()).await.unwrap().is_complete());
    assert!(!current.is_current());
    tokio::net::UdpSocket::bind(shared).await.unwrap();
    // Zero length remains a real packet in both directions.
    assert_eq!(send(&mut tx3, &first, b"").await, collision);
    first.send_to(b"", collision).await.unwrap();
    assert!(rx3.recv().await.unwrap().unwrap().0.is_empty());
    assert!(tx3
        .send_new_packet(
            &Address::Ipv4([127, 0, 0, 1]),
            &[first.local_addr().unwrap()],
            &vec![0; 65_508]
        )
        .await
        .is_err());
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), 1);
    assert!(report.failed_tasks.is_empty());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 2 original Direct UDP sockets"]
    );
    tokio::net::UdpSocket::bind(collision).await.unwrap();
    assert!(ctl3.wait_closed(deadline()).await.unwrap().is_complete());
    assert!(endpoint
        .open(
            SessionId(old.session_id.unwrap()),
            fixture.dispatcher.association(),
            super::super::tests::policy(&fixture.proxy, Some("first")),
            CancellationToken::new()
        )
        .is_err());
    // Retain the independent allocator for this fixture's whole resource life.
    drop(fixture.catalog);
}

mod budget;
mod dispatch;
mod isolation;
mod lifecycle;
mod observer;
mod queue;

#[cfg(feature = "managed-datagram-runtime")]
mod peer;

#[cfg(feature = "managed-datagram-runtime")]
mod bound;

mod failure;

mod queue_usage;
