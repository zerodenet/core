//! The real library pool, original TCP grant and driver Scope; no proxy test SPI.
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};
use vmess::component::mux::{Factory, PoolFactory, PreparedMux};
use zero_core::{
    component::*,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
};
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ResourceAccess, ResourceDeclaration,
        },
        context::{ConnectFact, ConnectObserver, OpenContext, RequestIdentity},
        datagram::{BoxDatagramSession, DatagramRequest},
        lifecycle::BuildError,
        network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        packet::BoxPacketSession,
        scope::{TaskScope, TaskScopeIdentity},
        stream::BoxStream,
        BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    controlled_carrier::{
        CarrierOrigin, CarrierOwnerBinding, CarrierOwnerRegistration, NativeCarrierOpener,
    },
    EgressInterfaceControl,
};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile, OwnedWebSocketProfile};
pub const ID: &str = "11111111-2222-3333-4444-555555555555";
pub fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(4)
}
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, BuildError>> {
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Unsupported,
            Impact::Component,
        ))
    })
}
struct Grants {
    declaration: ResourceDeclaration,
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    cancel: Arc<CancellationToken>,
}
impl ResourceAccess for Grants {
    fn register_controlled_carrier(
        &self,
        ordinal: usize,
        owner: CarrierOwnerRegistration,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<CarrierOwnerBinding, BuildError> {
        if ordinal != 0 || owner.instance() != self.instance || !self.scope.matches(tasks) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        owner.bind_declared(&self.declaration, tasks, &self.cancel)
    }
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
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, BuildError>> {
        denied()
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, BuildError>,
    > {
        denied()
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        denied()
    }
}
/// The pool reselects this same literal library provider. Native acquisition
/// still passes its original declared grant to NativeCarrierOpener; selection
/// itself opens no I/O and cannot switch endpoints or generations.
struct FixedRoot(UpstreamTransport);
impl UpstreamOpener for FixedRoot {
    fn select_background_carrier(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        Box::pin(async move {
            if binding
                .as_ref()
                .is_some_and(|binding| binding != self.0.binding())
                || call.session.is_some()
                || call.cancel.is_cancelled()
                || call
                    .deadline
                    .is_some_and(|end| end <= tokio::time::Instant::now())
            {
                return Err(ChannelError::new(
                    Stage::Prepare,
                    Reason::StalePlan,
                    Impact::Component,
                ));
            }
            Ok(self.0.clone())
        })
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        denied()
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
/// The role may observe only the original pool installed above. It cannot dial
/// another root or invent original-close evidence from an address/instance id.
struct Selected {
    owner: CarrierOwnerRegistration,
    scope: TaskScopeIdentity,
    peer: std::net::SocketAddr,
}
impl UpstreamOpener for Selected {
    fn observe_carrier_origin(&self, origin: &CarrierOrigin) -> bool {
        origin.owner().same_owner(&self.owner)
            && origin
                .endpoints()
                .is_some_and(|facts| facts.peer() == self.peer)
            && origin
                .driver()
                .is_some_and(|driver| driver.scope_identity().same_scope(&self.scope))
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        denied()
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
#[derive(Default)]
pub struct Counts(pub Mutex<Vec<(Option<SessionId>, u64)>>);
impl ConnectObserver for Counts {
    fn record(&self, _: Option<SessionId>, _: ConnectFact) {}
    fn datagram_sent(&self, session: Option<SessionId>, bytes: u64) {
        self.0.lock().unwrap().push((session, bytes));
    }
}
pub struct Assembly {
    pub pool: ComponentInstance,
    pub role: ComponentInstance,
    pub scope: TaskScope,
    pub role_scope: TaskScope,
    pub registration: CarrierOwnerRegistration,
    pub counts: Arc<Counts>,
    upstream: UpstreamTransport,
}
impl Assembly {
    pub async fn start(peer: std::net::SocketAddr) -> Self {
        let actual = ComponentInstanceId {
            component: ComponentId(950),
            generation: ResourceGeneration(1),
        };
        let cap = NonZeroUsize::new(1).unwrap();
        let scope = TaskScope::new(tokio::runtime::Handle::current(), cap);
        let role_scope = TaskScope::empty(tokio::runtime::Handle::current());
        let egress = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let runtime = vmess::transport::VmessTransportRuntime::default();
        let prepared = PreparedMux::from_options_refs(
            vmess::transport::VmessOutboundBuildOptionsRef::<
                OwnedClientTlsProfile,
                OwnedWebSocketProfile,
                OwnedGrpcProfile,
            > {
                tag: "same-pool",
                server: "127.0.0.1",
                port: peer.port(),
                protocol: vmess::transport::VmessOutboundOptionsRef {
                    id: ID,
                    cipher: "aes-128-gcm",
                    mux_concurrency: Some(4),
                    mux_idle_timeout_secs: None,
                    mux_response_backlog_frames: None,
                    mux_response_backlog_bytes: None,
                },
                tls: None,
                ws: None,
                grpc: None,
            },
            &runtime,
            None,
            binding.clone(),
            cap,
            Duration::from_secs(3),
        )
        .unwrap();
        let pool_options = prepared.pool_options().clone();
        let cancel = CancellationToken::new();
        let resources = Arc::new(Grants {
            declaration: PoolFactory.describe(&pool_options).resources[0].clone(),
            instance: actual,
            scope: scope.registrar().scope_identity(),
            cancel: Arc::new(cancel.clone()),
        });
        let background = UpstreamTransport::new(
            binding.clone(),
            Arc::new(
                NativeCarrierOpener::new(actual, binding.clone(), peer, egress, None).unwrap(),
            ),
        );
        let background = UpstreamTransport::new(binding.clone(), Arc::new(FixedRoot(background)));
        let (mut pool, registration) = PoolFactory
            .build_with_registration(
                pool_options,
                BuildContext {
                    component: actual.component,
                    generation: actual.generation,
                    dependencies: Arc::from([]),
                    roles: Default::default(),
                    resources: resources.clone(),
                    tasks: scope.registrar(),
                    cancel,
                    background_carrier: Some(background),
                },
            )
            .unwrap();
        pool.lifecycle.start(end()).await.unwrap();
        let mut role = Factory
            .build(
                Arc::new(prepared.bind(actual.component)),
                BuildContext {
                    component: ComponentId(951),
                    generation: ResourceGeneration(1),
                    dependencies: Arc::from([actual]),
                    roles: Default::default(),
                    resources,
                    tasks: role_scope.registrar().without_tasks(),
                    cancel: CancellationToken::new(),
                    background_carrier: None,
                },
            )
            .unwrap();
        role.lifecycle.start(end()).await.unwrap();
        let upstream = UpstreamTransport::new(
            binding,
            Arc::new(Selected {
                owner: registration.clone(),
                scope: scope.registrar().scope_identity(),
                peer,
            }),
        );
        Self {
            pool,
            role,
            scope,
            role_scope,
            registration,
            counts: Arc::new(Counts::default()),
            upstream,
        }
    }
    pub fn context(&self) -> OpenContext {
        OpenContext {
            identity: RequestIdentity::Service(
                Arc::new(ServiceRequestFacts {
                    origin: ServiceOrigin::Runtime,
                    purpose: ServicePurpose::DiagnosticProbe,
                })
                .into(),
            ),
            deadline: Some(end()),
            cancel: CancellationToken::new(),
            observer: Some(self.counts.clone()),
            upstream: self.upstream.clone(),
        }
    }
}
