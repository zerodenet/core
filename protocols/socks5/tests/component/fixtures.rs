use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
use zero_core::{
    component::*,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ResourceAccess},
    context::{ConnectFact, ConnectObserver, OpenContext, RequestIdentity},
    datagram::{BoxDatagramSession, DatagramRequest},
    lifecycle::BuildError,
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    packet::BoxPacketSession,
    scope::TaskScope,
    stream::BoxStream,
    BoxFuture, CancellationToken, ChannelError,
};
pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
pub fn destination() -> Destination {
    Destination::new(Address::Ipv4([203, 0, 113, 9]), 443)
}
pub fn endpoint() -> Destination {
    Destination::new(Address::Domain("proxy.example".into()), 1080)
}
pub struct Opener {
    pub io: Mutex<Option<BoxStream>>,
    pub calls: Mutex<Vec<(UpstreamBinding, Destination, Option<SessionId>)>>,
}
impl Opener {
    pub fn new(io: BoxStream) -> Arc<Self> {
        Arc::new(Self {
            io: Mutex::new(Some(io)),
            calls: Mutex::new(vec![]),
        })
    }
}
impl UpstreamOpener for Opener {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        self.calls
            .lock()
            .unwrap()
            .push((binding.clone(), destination, context.session));
        let io = self
            .io
            .lock()
            .unwrap()
            .take()
            .expect("one selected carrier");
        Box::pin(async { Ok(io) })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        panic!("no datagram role")
    }
}
#[derive(Default)]
pub struct Facts(pub Mutex<Vec<(Option<SessionId>, ConnectFact)>>);
impl ConnectObserver for Facts {
    fn record(&self, session: Option<SessionId>, fact: ConnectFact) {
        self.0.lock().unwrap().push((session, fact));
    }
}
pub fn context(opener: Arc<Opener>, facts: Arc<Facts>) -> OpenContext {
    OpenContext {
        identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::DiagnosticProbe,
        })),
        deadline: Some(deadline()),
        cancel: CancellationToken::new(),
        observer: Some(facts),
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![RoleInstanceId {
                    component: ComponentId(20),
                    role: RoleId::Outbound(OutboundId(21)),
                    generation: ResourceGeneration(2),
                }],
                dial: Default::default(),
                network_generation: NetworkGeneration(4),
            },
            opener,
        ),
    }
}
pub async fn instance(auth: bool) -> (ComponentInstance, TaskScope) {
    instance_at(endpoint(), auth).await
}
pub async fn instance_at(endpoint: Destination, auth: bool) -> (ComponentInstance, TaskScope) {
    let options = socks5::component::Options::new(
        endpoint,
        auth.then_some("user"),
        auth.then_some("secret"),
        Default::default(),
    )
    .unwrap();
    let scope = TaskScope::empty(tokio::runtime::Handle::current());
    let mut instance = socks5::component::Factory
        .build(
            Arc::new(options),
            BuildContext {
                component: ComponentId(7),
                generation: ResourceGeneration(1),
                dependencies: Arc::from([]),
                roles: ComponentRoleIds {
                    outbound: Some(OutboundId(8)),
                    ..Default::default()
                },
                resources: Arc::new(NoResources),
                tasks: scope.registrar(),
                cancel: CancellationToken::new(),
                background_carrier: None,
            },
        )
        .unwrap();
    assert!(instance.roles.datagram.is_some());
    assert!(instance.roles.packet.is_none());
    instance.lifecycle.start(deadline()).await.unwrap();
    (instance, scope)
}
struct NoResources;
impl ResourceAccess for NoResources {
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        panic!("file acquisition")
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::TcpListener, BuildError>> {
        panic!("listener acquisition")
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::UdpSocket, BuildError>> {
        panic!("socket acquisition")
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        panic!("device acquisition")
    }
}
