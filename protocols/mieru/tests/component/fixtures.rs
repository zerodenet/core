use crate::client::PoolKey;
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;
use zero_core::InboundRouteMultiplexer;
use zero_platform_tokio::{
    contract::{
        component::BuildContext, lifecycle::BuildError, CancellationToken, ChannelError, Impact,
        Reason, Stage,
    },
    TcpRelayStream,
};
#[path = "pool/io.rs"]
mod io;
pub(super) use io::{FailureControl, OriginalFailure};
#[path = "controlled.rs"]
mod controlled;
pub(super) use controlled::controlled_context;
use std::sync::Mutex;
use zero_core::{
    component::{ComponentId, ComponentRoleIds, NetworkGeneration, ResourceGeneration},
    request::Destination,
    Address,
};
use zero_platform_tokio::contract::{
    component::ResourceAccess,
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    packet::BoxPacketSession,
    scope::TaskScope,
    stream::BoxStream,
    BoxFuture,
};
struct NoResources;
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, BuildError>> {
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Internal,
            Impact::Component,
        ))
    })
}
impl ResourceAccess for NoResources {
    fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        denied()
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::TcpListener, BuildError>> {
        denied()
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::UdpSocket, BuildError>> {
        denied()
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        denied()
    }
}
pub(super) fn context(limit: usize) -> (TaskScope, BuildContext) {
    context_with(limit, Arc::new(Loopback::default()))
}
pub(super) fn context_with(limit: usize, opener: Arc<Loopback>) -> (TaskScope, BuildContext) {
    let scope = match NonZeroUsize::new(limit) {
        Some(limit) => TaskScope::new(tokio::runtime::Handle::current(), limit),
        None => TaskScope::empty(tokio::runtime::Handle::current()),
    };
    let context = BuildContext {
        component: ComponentId(61),
        generation: ResourceGeneration(1),
        dependencies: Arc::from([]),
        roles: ComponentRoleIds::default(),
        resources: Arc::new(NoResources),
        tasks: scope.registrar(),
        cancel: CancellationToken::new(),
        background_carrier: Some(UpstreamTransport::new(binding(), opener)),
    };
    (scope, context)
}
fn binding() -> UpstreamBinding {
    UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(7),
    }
}
pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
pub(super) fn key(tag: &str) -> PoolKey {
    PoolKey::new(tag, "127.0.0.1", 1, "u", "p", false)
}
pub(super) fn key_at(tag: &str, address: std::net::SocketAddr) -> PoolKey {
    PoolKey::new(tag, "127.0.0.1", address.port(), "u", "p", false)
}
#[derive(Default)]
pub(super) struct Loopback {
    pub(super) pending: Option<Arc<tokio::sync::Notify>>,
    pub(super) failure: Option<(Arc<FailureControl>, Arc<()>)>,
    pub(super) opens: AtomicUsize,
    pub(super) destinations: Mutex<Vec<(Destination, Option<Instant>)>>,
    pub(super) traffic: Arc<PhysicalTraffic>,
}
#[derive(Debug, Default)]
pub(super) struct PhysicalTraffic {
    pub(super) sent: AtomicU64,
    pub(super) received: AtomicU64,
}
impl zero_traits::IoObserver for PhysicalTraffic {
    fn stream_boundary(&self) {}
    fn sent(&self, bytes: usize) {
        self.sent.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    fn received(&self, bytes: usize) {
        self.received.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
impl UpstreamOpener for Loopback {
    fn open_stream(
        &self,
        selected: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        assert_eq!(selected, &binding());
        assert_eq!(context.session, None);
        assert!(!context.cancel.is_cancelled());
        assert_eq!(destination.address, Address::Ipv4([127, 0, 0, 1]));
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.destinations
            .lock()
            .unwrap()
            .push((destination.clone(), context.deadline));
        Box::pin(async move {
            let socket =
                tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, destination.port))
                    .await
                    .map_err(|source| {
                        ChannelError::new(Stage::Dial, Reason::Io, Impact::Component)
                            .with_source(source)
                    })?;
            let socket: BoxStream = match &self.failure {
                Some((control, marker)) => {
                    Box::pin(io::FailRead::new(socket, control.clone(), marker.clone()))
                }
                None => Box::pin(io::PendingRead::new(socket, self.pending.clone())),
            };
            Ok(Box::pin(zero_transport::observed::ObservedStream::new(
                socket,
                self.traffic.clone(),
            )) as BoxStream)
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        denied()
    }
}
pub(super) fn pending_socket(
    socket: tokio::net::TcpStream,
    observed: Arc<tokio::sync::Notify>,
) -> TcpRelayStream {
    TcpRelayStream::new(io::PendingRead::new(socket, Some(observed)))
}
pub(super) async fn echo() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    echo_connections(1).await
}
pub(super) async fn echo_connections(
    count: usize,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        for _ in 0..count {
            let (socket, _) = listener.accept().await.unwrap();
            let connection =
                crate::inbound::MieruInboundProfile::from_config(vec![("u".into(), "p".into())])
                    .accept_multiplexer(TcpRelayStream::new(socket))
                    .await
                    .unwrap();
            let mut streams = tokio::task::JoinSet::new();
            while let Ok(Some(mut stream)) = connection.accept_next().await {
                streams.spawn(async move {
                    let (mut read, mut write) = tokio::io::split(&mut stream);
                    let _ = tokio::io::copy(&mut read, &mut write).await;
                });
            }
            streams.shutdown().await;
        }
    });
    (address, task)
}
