use crate::component::{catalog::ComponentCatalog, instance::ComponentSlot, ComponentRegistry};
use mieru::{
    client::ClientPool,
    component::{PoolFactory, PoolOptions, PoolProfile, StreamFactory, TcpPreparation},
    transport::MieruOutboundOptionsRef,
};
use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Duration, Instant},
};
use zero_config::{OutboundConfig, OutboundProtocolConfig, RuntimeConfig};
use zero_core::{
    component::NetworkGeneration,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address, InboundRouteMultiplexer,
};
use zero_platform_tokio::{
    contract::{
        component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
        context::{OpenContext, RequestIdentity},
        lifecycle::BuildError,
        network::{UpstreamBinding, UpstreamTransport},
        stream::{BoxStream, ConnectRequest},
        CancellationToken,
    },
    controlled_carrier::NativeCarrierOpener,
    EgressInterfaceControl, TcpRelayStream,
};
use zero_traits::ProtocolMetadata;

mod server;
pub(super) use server::Server;

pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(4)
}
pub(super) struct Assembly {
    pub catalog: ComponentCatalog,
    pub pool: Arc<ClientPool>,
    pub builds: Arc<AtomicUsize>,
    pub closes: Arc<AtomicUsize>,
    peer: SocketAddr,
    egress: Arc<EgressInterfaceControl>,
}
struct BackgroundRoot {
    egress: Arc<EgressInterfaceControl>,
    builds: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
    resolver: Option<Arc<dyn zero_platform_tokio::contract::dns::DnsService>>,
}
impl ComponentFactory for BackgroundRoot {
    type Options = PoolOptions;
    fn describe(&self, options: &PoolOptions) -> ComponentSpec {
        PoolFactory.describe(options)
    }
    fn build(
        &self,
        options: Arc<PoolOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let spec = self.describe(&options);
        let upstreams=spec.resources.iter().map(|resource| {
            let (endpoint,binding) = match resource {
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledOutboundStream {endpoint,binding,..}
                | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDatagramCarrier {endpoint,binding,..} => (endpoint,binding),
                _ => panic!("declared real root"),
            };
            let opener: Arc<dyn zero_platform_tokio::contract::network::UpstreamOpener> = if let Some(resolver) = &self.resolver {
                Arc::new(zero_platform_tokio::controlled_carrier::ResolvingCarrierOpener::new(context.instance(), binding.clone(), endpoint.clone(), resolver.clone(), self.egress.clone(), None)?)
            } else {
                let ip=match endpoint.address {Address::Ipv4(ip)=>std::net::Ipv4Addr::from(ip).into(),Address::Ipv6(ip)=>std::net::Ipv6Addr::from(ip).into(),_=>panic!("literal root")};
                Arc::new(NativeCarrierOpener::new(context.instance(),binding.clone(),SocketAddr::new(ip,endpoint.port),self.egress.clone(),None)?)
            };
            Ok(UpstreamTransport::new(binding.clone(),opener))
        }).collect::<Result<Vec<_>,BuildError>>()?;
        let mut actual = PoolFactory.build_with_carriers(options, context, upstreams)?;
        actual.lifecycle =
            zero_platform_tokio::contract::lifecycle::erase_lifecycle(server::Observed {
                inner: actual.lifecycle,
                closes: self.closes.clone(),
            });
        self.builds.fetch_add(1, Ordering::SeqCst);
        Ok(actual)
    }
}
impl Assembly {
    pub fn new(peer: SocketAddr) -> Self {
        Self::with_dns(peer, Arc::new(EgressInterfaceControl::default()), None)
    }
    pub fn with_dns(
        peer: SocketAddr,
        egress: Arc<EgressInterfaceControl>,
        resolver: Option<Arc<dyn zero_platform_tokio::contract::dns::DnsService>>,
    ) -> Self {
        let pool = Arc::new(ClientPool::default());
        let builds = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let factory = Arc::new(BackgroundRoot {
            egress: egress.clone(),
            builds: builds.clone(),
            closes: closes.clone(),
            resolver,
        });
        let mut registry: ComponentRegistry<OutboundConfig> =
            super::super::super::default_factories();
        let actual_pool = pool.clone();
        let network = egress.clone();
        registry
            .register_with_resources(
                mieru::MieruProtocol.descriptor(),
                StreamFactory,
                move |input, resources| {
                    let OutboundProtocolConfig::Mieru {
                        server,
                        port,
                        username,
                        password,
                        options,
                        transport,
                        ..
                    } = &input.protocol
                    else {
                        panic!("fixture protocol")
                    };
                    let profiles = resources
                        .candidate()
                        .expect("actual catalog candidate")
                        .outbounds
                        .iter()
                        .filter_map(|outbound| {
                            let OutboundProtocolConfig::Mieru {
                                server,
                                port,
                                username,
                                password,
                                options,
                                transport,
                                ..
                            } = &outbound.protocol
                            else {
                                return None;
                            };
                            Some(
                                PoolProfile::new(
                                    TcpPreparation {
                                        tag: &outbound.tag,
                                        server,
                                        port: *port,
                                        protocol: MieruOutboundOptionsRef {
                                            username: username.as_deref().unwrap_or(password),
                                            password,
                                            options,
                                            udp: matches!(
                                                transport,
                                                zero_config::MieruTransport::Udp
                                            ),
                                        },
                                        dial: outbound.dial.to_policy(),
                                    },
                                    UpstreamBinding {
                                        prefix: vec![],
                                        dial: outbound.dial.to_policy(),
                                        network_generation: NetworkGeneration(network.generation()),
                                    },
                                )
                                .unwrap(),
                            )
                        });
                    let pool = actual_pool
                        .component_options(NonZeroUsize::new(2).unwrap())
                        .with_profiles(profiles)
                        .unwrap();
                    let prepared = pool
                        .prepare_tcp(TcpPreparation {
                            tag: &input.tag,
                            server,
                            port: *port,
                            protocol: MieruOutboundOptionsRef {
                                username: username.as_deref().unwrap_or(password),
                                password,
                                options,
                                udp: matches!(transport, zero_config::MieruTransport::Udp),
                            },
                            dial: input.dial.to_policy(),
                        })
                        .unwrap();
                    let owner = resources.share(
                        "mieru.test.actual_pool",
                        factory.clone(),
                        Arc::new(pool),
                    )?;
                    Ok(prepared.bind(owner))
                },
            )
            .unwrap();
        Self {
            catalog: ComponentCatalog::with_factories(registry),
            pool,
            builds,
            closes,
            peer,
            egress,
        }
    }
    pub fn config(&self) -> Arc<RuntimeConfig> {
        Arc::new(RuntimeConfig::parse(&format!(r#"{{"outbounds":[{{"tag":"a","protocol":{{"type":"mieru","server":"127.0.0.1","port":{},"username":"u","password":"p"}}}},{{"tag":"b","protocol":{{"type":"mieru","server":"127.0.0.1","port":{},"username":"u","password":"p"}}}}],"route":{{"final":{{"type":"direct"}}}}}}"#,self.peer.port(),self.peer.port())).unwrap())
    }
    pub async fn open(&self, slot: &Arc<ComponentSlot>) -> BoxStream {
        self.try_open(slot).await.unwrap()
    }
    pub fn execution_context(&self, slot: &Arc<ComponentSlot>) -> OpenContext {
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: slot.dial.clone(),
            network_generation: NetworkGeneration(self.egress.generation()),
        };
        // The Stream only compares this selected binding. Its actual physical
        // acquire uses the persistent pool owner's registered background opener.
        let upstream = UpstreamTransport::new(
            binding.clone(),
            Arc::new(
                NativeCarrierOpener::new(
                    slot.instance_id(),
                    binding,
                    self.peer,
                    self.egress.clone(),
                    None,
                )
                .unwrap(),
            ),
        );
        OpenContext {
            identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::DiagnosticProbe,
            })),
            deadline: Some(deadline()),
            cancel: CancellationToken::new(),
            observer: None,
            upstream,
        }
    }
    pub async fn try_open(
        &self,
        slot: &Arc<ComponentSlot>,
    ) -> Result<BoxStream, zero_platform_tokio::contract::ChannelError> {
        slot.stream(deadline())
            .await?
            .connect(
                ConnectRequest {
                    destination: Destination::new(Address::Ipv4([203, 0, 113, 1]), 443),
                },
                self.execution_context(slot),
            )
            .await
            .map(|connected| connected.io)
    }
}
