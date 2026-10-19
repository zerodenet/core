use std::{
    net::SocketAddr,
    num::NonZeroUsize,
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
}
impl ResourceAccess for Grants {
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        panic!("no file declared")
    }
    fn listen_tcp(&self, address: Destination) -> BoxFuture<'_, Result<TcpListener, BuildError>> {
        self.binds.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let host = match address.address {
                zero_core::Address::Domain(host) => host,
                zero_core::Address::Ipv4(ip) => std::net::Ipv4Addr::from(ip).to_string(),
                zero_core::Address::Ipv6(ip) => std::net::Ipv6Addr::from(ip).to_string(),
            };
            TcpListener::bind((host.as_str(), address.port))
                .await
                .map_err(|e| {
                    ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component).with_source(e)
                })
        })
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::UdpSocket, BuildError>> {
        panic!("no datagram listener declared")
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        panic!("no device declared")
    }
}
struct Options {
    address: SocketAddr,
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
    listener: Option<TcpListener>,
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
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        self.ready.store(false, Ordering::SeqCst);
        let listener = self.listener.take();
        let held = listener.is_some();
        drop(listener);
        Ok(CloseReport {
            released_resources: if held {
                vec![self.address.to_string()]
            } else {
                vec![]
            },
            ..Default::default()
        })
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
            roles: vec![RoleKind::Stream],
            dependencies: vec![],
            static_identity: options.address.to_string().into_bytes().into(),
            resources: vec![ResourceDeclaration::TcpListener(destination(
                options.address,
            ))],
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
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let factory = Factory;
    let options = Arc::new(Options { address });
    let spec = factory.describe(&options);
    assert_eq!(spec.roles.len(), 1);
    let mut instance = factory
        .build(
            options,
            BuildContext {
                component: ComponentId(1),
                generation: ResourceGeneration(1),
                roles: ComponentRoleIds {
                    outbound: Some(OutboundId(1)),
                    ..Default::default()
                },
                resources: Arc::new(Grants {
                    binds: binds.clone(),
                }),
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

struct BoundOpener {
    seen: std::sync::Mutex<Vec<UpstreamBinding>>,
}
impl UpstreamOpener for BoundOpener {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        self.seen.lock().unwrap().push(binding.clone());
        Box::pin(async {
            Err(ChannelError::new(
                Stage::Dial,
                Reason::StalePlan,
                Impact::Session,
            ))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        panic!("stream request must not turn into datagram")
    }
}

#[tokio::test]
async fn bound_upstream_retains_prefix_policy_and_generation_without_fallback() {
    use zero_core::{component::NetworkGeneration, Address};
    let opener = Arc::new(BoundOpener {
        seen: Default::default(),
    });
    let binding = UpstreamBinding {
        prefix: vec![RoleInstanceId {
            component: ComponentId(42),
            role: RoleId::Outbound(OutboundId(42)),
            generation: ResourceGeneration(1),
        }],
        dial: zero_traits::DialPolicy {
            address_family: zero_traits::AddressFamily::OnlyIpv6,
            interface: Some("contract-interface".into()),
            source_ip: Some("::1".parse().unwrap()),
        },
        network_generation: NetworkGeneration(7),
    };
    let mut rebuilt = binding.clone();
    rebuilt.prefix[0].generation = ResourceGeneration(2);
    let identities = std::collections::HashSet::from([binding.clone(), rebuilt]);
    assert_eq!(
        identities.len(),
        2,
        "a rebuilt prefix must not reuse its old pool key"
    );
    let upstream = UpstreamTransport::new(binding.clone(), opener.clone());
    let context = CarrierContext {
        session: None,
        deadline: Instant::now(),
        cancel: Default::default(),
    };
    let error = upstream
        .open_stream(
            Destination::new(Address::Domain("carrier.test".into()), 443),
            context,
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.reason, Reason::StalePlan);
    assert_eq!(opener.seen.lock().unwrap().as_slice(), [binding]);
}
