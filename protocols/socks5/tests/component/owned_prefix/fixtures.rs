use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};
use zero_core::{
    component::*,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ResourceAccess, ResourceDeclaration,
        },
        context::{OpenContext, RequestIdentity},
        datagram::{BoxDatagramSession, DatagramRequest},
        lifecycle::BuildError,
        network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        packet::BoxPacketSession,
        scope::{TaskScope, TaskScopeIdentity},
        stream::{BoxStream, ConnectRequest, DynStreamOutbound},
        BoxFuture, CancellationToken, ChannelError,
    },
    controlled_carrier::{
        loan::StreamLoan, CarrierOrigin, CarrierOwnerBinding, CarrierOwnerRegistration,
        NativeCarrierOpener,
    },
    EgressInterfaceControl,
};
pub fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}
pub fn endpoint(address: std::net::SocketAddr) -> Destination {
    Destination::new(Address::Ipv4([127, 0, 0, 1]), address.port())
}
pub fn target() -> Destination {
    Destination::new(Address::Ipv4([203, 0, 113, 9]), 443)
}
pub fn binding(prefix: Vec<RoleInstanceId>) -> UpstreamBinding {
    UpstreamBinding {
        prefix,
        dial: Default::default(),
        network_generation: NetworkGeneration(EgressInterfaceControl::default().generation()),
    }
}
pub fn context(upstream: UpstreamTransport) -> OpenContext {
    OpenContext {
        identity: RequestIdentity::Service(
            Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::BackgroundControl,
            })
            .into(),
        ),
        deadline: Some(end()),
        cancel: CancellationToken::new(),
        upstream,
        observer: None,
    }
}
pub struct Resources {
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    declaration: ResourceDeclaration,
    cancel: Arc<CancellationToken>,
    pub owner: Mutex<Option<CarrierOwnerRegistration>>,
}
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, BuildError>> {
    Box::pin(async { panic!("undeclared native resource") })
}
impl ResourceAccess for Resources {
    fn register_controlled_carrier(
        &self,
        ordinal: usize,
        owner: CarrierOwnerRegistration,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<CarrierOwnerBinding, BuildError> {
        assert_eq!(ordinal, 0);
        assert_eq!(owner.instance(), self.instance);
        assert!(self.scope.matches(tasks));
        let bound = owner.bind_declared(&self.declaration, tasks, &self.cancel)?;
        assert!(self.owner.lock().unwrap().replace(owner).is_none());
        Ok(bound)
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
pub struct Installed {
    pub instance: ComponentInstance,
    pub scope: TaskScope,
    pub resources: Arc<Resources>,
    pub role: RoleInstanceId,
}
impl Installed {
    pub async fn new(id: u64, endpoint: Destination) -> Self {
        let options = socks5::component::Options::new(endpoint, None, None, Default::default())
            .unwrap()
            .with_carrier_limit(NonZeroUsize::new(1).unwrap());
        let spec = socks5::component::Factory.describe(&options);
        let scope = TaskScope::empty(tokio::runtime::Handle::current());
        let cancel = Arc::new(CancellationToken::new());
        let instance = ComponentInstanceId {
            component: ComponentId(id),
            generation: ResourceGeneration(1),
        };
        let role = RoleInstanceId {
            component: instance.component,
            generation: instance.generation,
            role: RoleId::Outbound(OutboundId(id)),
        };
        let resources = Arc::new(Resources {
            instance,
            scope: scope.registrar().scope_identity(),
            declaration: spec.resources[0].clone(),
            cancel: cancel.clone(),
            owner: Mutex::new(None),
        });
        let mut built = socks5::component::Factory
            .build(
                Arc::new(options),
                BuildContext {
                    component: instance.component,
                    generation: instance.generation,
                    dependencies: Arc::from([]),
                    roles: ComponentRoleIds {
                        outbound: Some(OutboundId(id)),
                        ..Default::default()
                    },
                    resources: resources.clone(),
                    tasks: scope.registrar(),
                    cancel: (*cancel).clone(),
                    background_carrier: None,
                },
            )
            .unwrap();
        built.lifecycle.start(end()).await.unwrap();
        Self {
            instance: built,
            scope,
            resources,
            role,
        }
    }
    pub fn owner(&self) -> CarrierOwnerRegistration {
        self.resources
            .owner
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone()
    }
    pub fn root(&self, address: std::net::SocketAddr) -> UpstreamTransport {
        UpstreamTransport::new(
            binding(vec![]),
            Arc::new(
                NativeCarrierOpener::new(
                    ComponentInstanceId {
                        component: self.role.component,
                        generation: self.role.generation,
                    },
                    binding(vec![]),
                    address,
                    Arc::new(EgressInterfaceControl::default()),
                    None,
                )
                .unwrap(),
            ),
        )
    }
}
pub struct Prefix {
    pub role: Arc<dyn DynStreamOutbound>,
    pub root: UpstreamTransport,
    pub selected: UpstreamBinding,
    pub endpoint: Destination,
    pub original: Mutex<Option<CarrierOrigin>>,
    pub pending: Arc<std::sync::atomic::AtomicBool>,
}
impl UpstreamOpener for Prefix {
    fn borrow_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<StreamLoan, ChannelError>> {
        assert_eq!(binding, &self.selected);
        assert_eq!(endpoint, self.endpoint);
        assert!(call.session.is_none());
        Box::pin(async move {
            let mut ctx = context(self.root.clone());
            ctx.deadline = call.deadline;
            ctx.cancel = call.cancel;
            let mut loan = self
                .role
                .connect_carrier(
                    ConnectRequest {
                        destination: endpoint,
                    },
                    ctx,
                )
                .await?
                .io;
            *self.original.lock().unwrap() = Some(loan.owner.original().clone());
            loan.io.wrap(|io| {
                Box::pin(Probe {
                    io,
                    pending: self.pending.clone(),
                })
            });
            Ok(loan)
        })
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { panic!("a native Service prefix must not fall back to a bare stream") })
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
struct Probe {
    io: BoxStream,
    pending: Arc<std::sync::atomic::AtomicBool>,
}
impl tokio::io::AsyncRead for Probe {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let result = self.io.as_mut().poll_read(cx, buf);
        if result.is_pending() {
            self.pending
                .store(true, std::sync::atomic::Ordering::Release);
        }
        result
    }
}
impl tokio::io::AsyncWrite for Probe {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.io.as_mut().poll_write(cx, bytes)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.io.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.io.as_mut().poll_shutdown(cx)
    }
}
