use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpListener, runtime::Handle, time::Instant};
use zero_core::{
    component::{
        ComponentId, ComponentRoleIds, OutboundId, ResourceGeneration, RoleId, RoleInstanceId,
    },
    request::Destination,
};
use zero_platform_tokio::contract::{
    component::*,
    context::OpenContext,
    datagram::{BoxDatagramSession, DatagramRequest},
    lifecycle::*,
    network::*,
    packet::BoxPacketSession,
    scope::TaskScope,
    stream::*,
    *,
};

struct Grants {
    binds: Arc<AtomicUsize>,
    native: tokio::sync::Mutex<zero_platform_tokio::ControlledTcpListenerOwner>,
}
impl ResourceAccess for Grants {
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        panic!("no file declared")
    }
    fn listen_tcp(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, BuildError>> {
        Box::pin(async move {
            let ip = match address.address {
                zero_core::Address::Domain(_) => {
                    return Err(ChannelError::new(
                        Stage::Prepare,
                        Reason::Unsupported,
                        Impact::Component,
                    ))
                }
                zero_core::Address::Ipv4(ip) => std::net::IpAddr::V4(ip.into()),
                zero_core::Address::Ipv6(ip) => std::net::IpAddr::V6(ip.into()),
            };
            self.binds.fetch_add(1, Ordering::SeqCst);
            self.native
                .lock()
                .await
                .bind_listener(SocketAddr::new(ip, address.port))
        })
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, BuildError>,
    > {
        panic!("no datagram listener declared")
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        panic!("no device declared")
    }
}
struct Options {
    address: SocketAddr,
    grants: Arc<Grants>,
}
fn destination(address: SocketAddr) -> Destination {
    let host = match address.ip() {
        std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
        std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
    };
    Destination::new(host, address.port())
}
struct Factory;
struct Owner {
    address: SocketAddr,
    resources: Arc<dyn ResourceAccess>,
    listener: Option<zero_platform_tokio::ControlledTcpListener>,
    native: Arc<Grants>,
    ready: Arc<AtomicBool>,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        self.listener = Some(self.resources.listen_tcp(destination(self.address)).await?);
        self.ready.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.ready.store(false, Ordering::SeqCst);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.ready.store(false, Ordering::SeqCst);
        let listener = self.listener.take();
        drop(listener);
        self.native.native.lock().await.wait_closed(deadline).await
    }
}
struct Stream(Arc<AtomicBool>);
impl StreamOutbound for Stream {
    type Stream = tokio::io::DuplexStream;
    async fn connect(
        &self,
        _: ConnectRequest,
        _: OpenContext,
    ) -> Result<Connected<Self::Stream>, ChannelError> {
        if !self.0.load(Ordering::SeqCst) {
            return Err(ChannelError::new(
                Stage::Dial,
                Reason::Revoked,
                Impact::Session,
            ));
        }
        let (io, _) = tokio::io::duplex(32);
        Ok(Connected {
            io,
            peer_confirmation: PeerConfirmation::Confirmed,
        })
    }
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            task_limit: None,
            roles: vec![RoleKind::Stream],
            dependencies: vec![],
            static_identity: options.address.to_string().into_bytes().into(),
            resources: vec![ResourceDeclaration::TcpListener {
                destination: destination(options.address),
                max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
            }],
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let ready = Arc::new(AtomicBool::new(false));
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                address: options.address,
                resources: context.resources,
                listener: None,
                native: options.grants.clone(),
                ready: ready.clone(),
            }),
            roles: ComponentRoles {
                stream: Some(erase_stream(Stream(ready))),
                ..Default::default()
            },
        })
    }
}

#[tokio::test]
async fn stream_only_factory_builds_without_binding_and_keeps_owner_after_start_failure() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let binds = Arc::new(AtomicUsize::new(0));
    let mut scope = TaskScope::empty(Handle::current());
    let factory = Factory;
    let grants = Arc::new(Grants {
        binds: binds.clone(),
        native: tokio::sync::Mutex::new(
            zero_platform_tokio::ControlledTcpListenerOwner::new(
                &ResourceDeclaration::TcpListener {
                    destination: destination(address),
                    max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
                },
                std::num::NonZeroUsize::new(1).unwrap(),
                Default::default(),
            )
            .unwrap(),
        ),
    });
    let options = Arc::new(Options {
        address,
        grants: grants.clone(),
    });
    let spec = factory.describe(&options);
    assert_eq!(spec.roles.len(), 1);
    let mut instance = factory
        .build(
            options,
            BuildContext {
                component: ComponentId(1),
                generation: ResourceGeneration(1),
                dependencies: Arc::from([]),
                roles: ComponentRoleIds {
                    outbound: Some(OutboundId(1)),
                    ..Default::default()
                },
                resources: grants,
                tasks: scope.registrar(),
                cancel: scope.registrar().cancellation(),
                background_carrier: None,
            },
        )
        .unwrap();
    assert_eq!(binds.load(Ordering::SeqCst), 0);
    assert!(instance.roles.stream.is_some());
    assert!(instance.roles.datagram.is_none());
    assert!(instance.roles.packet.is_none());
    assert!(instance.roles.inbound.is_none());
    assert_eq!(
        instance
            .lifecycle
            .start(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err()
            .reason,
        Reason::Io
    );
    assert_eq!(binds.load(Ordering::SeqCst), 1);
    assert!(instance
        .lifecycle
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .is_complete());
    assert!(scope
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .is_complete());
    drop(occupied);
    let _released = TcpListener::bind(address).await.unwrap();
}

#[path = "contract_factory/upstream.rs"]
mod upstream;
