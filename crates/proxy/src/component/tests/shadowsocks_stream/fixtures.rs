use crate::component::{
    catalog::ComponentCatalog, instance::ComponentSlot, ComponentRecipe, ResourcePreparation,
};
use shadowsocks::{
    component::{ReplayFactory, StreamFactory, TcpPreparation},
    transport::{ShadowsocksOutboundOptionsRef, ShadowsocksTransportState},
    validation::{ReplayPolicy, StateLimits},
};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::time::{Duration, Instant};
use zero_core::{
    component::*,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::contract::{
    context::{ConnectFact, ConnectObserver, OpenContext, RequestIdentity},
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    stream::BoxStream,
    BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
};
pub(super) const PASSWORD: &str = "real-native-password";
pub(super) const KEY2022: &str = "AAAAAAAAAAAAAAAAAAAAAA==";
pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(4)
}
pub(super) fn destination() -> Destination {
    Destination::new(Address::Domain("native.example".into()), 443)
}
pub(super) fn prepare(
    state: &ShadowsocksTransportState,
    address: SocketAddr,
    cipher: &str,
    password: &str,
    capacity: Option<usize>,
) -> shadowsocks::component::PreparedTcp {
    state
        .prepare_tcp(TcpPreparation {
            server: "127.0.0.1",
            port: address.port(),
            protocol: ShadowsocksOutboundOptionsRef { cipher, password },
            replay_policy: ReplayPolicy::Reject,
            limits: StateLimits {
                tcp_replay_capacity: capacity,
                ..Default::default()
            },
            plugin: None,
            dial: Default::default(),
        })
        .unwrap()
}
pub(super) struct Assembly {
    pub(super) streams: Vec<Arc<ComponentSlot>>,
    pub(super) replay: Option<Arc<ComponentSlot>>,
}
pub(super) fn assembly(
    state: &ShadowsocksTransportState,
    address: SocketAddr,
    cipher: &str,
    password: &str,
    count: usize,
    capacity: Option<usize>,
) -> Assembly {
    let ids = ComponentCatalog::default();
    let mut allocate = || ids.allocate_id();
    let mut preparation = ResourcePreparation::new(&mut allocate);
    let mut recipes = Vec::new();
    for _ in 0..count {
        let prepared = prepare(state, address, cipher, password, capacity);
        let dependency = prepared.replay_options().map(|options| {
            preparation
                .share(
                    "shadowsocks.tcp2022.replay",
                    Arc::new(ReplayFactory),
                    Arc::new(options.clone()),
                )
                .unwrap()
        });
        let options = prepared.bind(dependency).unwrap();
        let id = ids.allocate_id().unwrap();
        recipes.push((
            id,
            ComponentRecipe::from_validated(Arc::new(StreamFactory), Arc::new(options)),
        ));
    }
    let order = preparation
        .validate(recipes.iter().map(|(id, recipe)| (*id, recipe.spec())))
        .unwrap();
    let resources = preparation.into_resources().unwrap();
    assert_eq!(resources.len(), usize::from(cipher.starts_with("2022-")));
    let replay = resources.first().map(|resource| {
        Arc::new(ComponentSlot::from_recipe(
            resource.component(),
            ResourceGeneration(1),
            resource.recipe().clone(),
            Default::default(),
            "library_shadowsocks_replay",
        ))
    });
    if let Some(replay) = &replay {
        assert_eq!(order[0], replay.id);
        assert!(replay.recipe.spec().roles.is_empty());
    }
    let streams = recipes
        .into_iter()
        .map(|(id, recipe)| {
            let dependencies: Arc<[ComponentInstanceId]> = replay
                .as_ref()
                .map(|slot| vec![slot.instance_id()])
                .unwrap_or_default()
                .into();
            Arc::new(ComponentSlot::from_bound_recipe(
                id,
                ResourceGeneration(1),
                recipe,
                Default::default(),
                "library_shadowsocks",
                dependencies,
            ))
        })
        .collect();
    Assembly { streams, replay }
}
impl Assembly {
    pub(super) async fn start(&self) {
        if let Some(replay) = &self.replay {
            replay.start(deadline()).await.unwrap();
        }
        for stream in &self.streams {
            stream.start(deadline()).await.unwrap();
        }
    }
    pub(super) async fn close(&self) {
        for stream in self.streams.iter().rev() {
            assert!(stream.shutdown(deadline()).await.unwrap().is_complete());
        }
        if let Some(replay) = &self.replay {
            let report = replay.shutdown(deadline()).await.unwrap();
            assert!(report.is_complete());
            assert_eq!(report.exited_tasks.len(), 1);
            assert!(report.failed_tasks.is_empty());
        }
    }
}
#[derive(Debug, Default)]
pub(super) struct Observations {
    pub(super) facts: Mutex<Vec<(Option<SessionId>, ConnectFact)>>,
    pub(super) protocol: Mutex<Option<(u64, u64)>>,
    pub(super) outer: AtomicU64,
    pub(super) dials: AtomicUsize,
}
impl ConnectObserver for Observations {
    fn record(&self, session: Option<SessionId>, fact: ConnectFact) {
        self.facts.lock().unwrap().push((session, fact));
    }
    fn protocol_traffic(&self, _: Option<SessionId>, read: u64, written: u64) {
        *self.protocol.lock().unwrap() = Some((read, written));
    }
}
impl zero_traits::IoObserver for Observations {
    fn stream_boundary(&self) {}
    fn received(&self, _: usize) {}
    fn sent(&self, bytes: usize) {
        self.outer.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
struct Opener {
    address: SocketAddr,
    observations: Arc<Observations>,
}
impl UpstreamOpener for Opener {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        assert!(binding.prefix.is_empty());
        assert_eq!(context.session, None);
        assert_eq!(destination.port, self.address.port());
        Box::pin(async move {
            self.observations.dials.fetch_add(1, Ordering::Relaxed);
            tokio::net::TcpStream::connect(self.address)
                .await
                .map(|io| {
                    Box::pin(zero_transport::observed::ObservedStream::new(
                        io,
                        self.observations.clone(),
                    )) as BoxStream
                })
                .map_err(|source| {
                    ChannelError::new(Stage::Dial, Reason::Io, Impact::Session).with_source(source)
                })
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        panic!("Shadowsocks Stream did not request UDP")
    }
}
pub(super) fn context(address: SocketAddr, observations: Arc<Observations>) -> OpenContext {
    let network = zero_platform_tokio::EgressInterfaceControl::default();
    OpenContext {
        identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::DiagnosticProbe,
        })),
        deadline: Some(deadline()),
        cancel: CancellationToken::new(),
        observer: Some(observations.clone()),
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![],
                dial: Default::default(),
                network_generation: NetworkGeneration(network.generation()),
            },
            Arc::new(Opener {
                address,
                observations,
            }),
        ),
    }
}
