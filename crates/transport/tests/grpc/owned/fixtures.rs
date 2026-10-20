use super::*;
pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(4)
}
pub(super) struct Denied;
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, BuildError>> {
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Unsupported,
            Impact::Component,
        ))
    })
}
impl ResourceAccess for Denied {
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
#[derive(Debug, Default)]
pub(super) struct Traffic {
    pub(super) tx: AtomicUsize,
    pub(super) rx: AtomicUsize,
    pub(super) boundaries: AtomicUsize,
}
impl zero_traits::IoObserver for Traffic {
    fn stream_boundary(&self) {
        self.boundaries.fetch_add(1, Ordering::SeqCst);
    }
    fn sent(&self, n: usize) {
        self.tx.fetch_add(n, Ordering::SeqCst);
    }
    fn received(&self, n: usize) {
        self.rx.fetch_add(n, Ordering::SeqCst);
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
pub(super) struct Fixture {
    pub(super) pool: GrpcPool,
    pub(super) options: Arc<NativePoolOptions>,
    pub(super) scope: TaskScope,
    pub(super) owner: ComponentInstance,
    pub(super) actual: ComponentInstanceId,
    pub(super) binding: UpstreamBinding,
    pub(super) traffic: Arc<Traffic>,
}
impl Fixture {
    pub(super) fn cold(
        peer: std::net::SocketAddr,
        carriers: usize,
        streams: usize,
        tls: Option<OwnedClientTlsProfile>,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        let pool = GrpcPool::default();
        Self::on_pool(pool, peer, carriers, streams, tls, observer)
    }
    pub(super) fn on_pool(
        pool: GrpcPool,
        peer: std::net::SocketAddr,
        carriers: usize,
        streams: usize,
        tls: Option<OwnedClientTlsProfile>,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        let cap = NonZeroUsize::new(carriers).unwrap();
        let scope = TaskScope::new(tokio::runtime::Handle::current(), cap);
        let egress = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let actual = ComponentInstanceId {
            component: ComponentId(810),
            generation: ResourceGeneration(4),
        };
        let traffic = Arc::new(Traffic::default());
        let upstream = UpstreamTransport::new(
            binding.clone(),
            Arc::new(
                NativeCarrierOpener::new(
                    actual,
                    binding.clone(),
                    peer,
                    egress,
                    Some(observer.unwrap_or_else(|| traffic.clone())),
                )
                .unwrap(),
            ),
        );
        let options = Arc::new(
            NativePoolOptions::from_pool(
                &pool,
                NativePoolSettings {
                    server: peer.ip().to_string(),
                    port: peer.port(),
                    grpc: crate::profile::OwnedGrpcProfile {
                        service_names: vec!["owned".into()],
                        multi_mode: true,
                        ..Default::default()
                    },
                    tls,
                    dial: binding.dial.clone(),
                },
                cap,
                NonZeroUsize::new(streams).unwrap(),
            )
            .unwrap(),
        );
        let context = BuildContext {
            component: actual.component,
            generation: actual.generation,
            dependencies: Arc::from([]),
            roles: ComponentRoleIds::default(),
            resources: Arc::new(Denied),
            tasks: scope.registrar(),
            cancel: CancellationToken::new(),
            background_carrier: Some(upstream),
        };
        let owner = NativePoolFactory.build(options.clone(), context).unwrap();
        Self {
            pool,
            options,
            scope,
            owner,
            actual,
            binding,
            traffic,
        }
    }
    pub(super) async fn start(&mut self) {
        self.owner.lifecycle.start(deadline()).await.unwrap();
    }
    pub(super) fn demand(&self) -> Demand {
        Demand {
            deadline: deadline(),
            cancel: CancellationToken::new(),
            binding: self.binding.clone(),
        }
    }
    pub(super) fn managed(&self) -> Arc<Managed> {
        self.pool.0.managed(self.actual).unwrap()
    }
    pub(super) async fn close(&mut self) -> (CloseReport, CloseReport) {
        let end = deadline();
        let (protocol, tasks) =
            tokio::join!(self.owner.lifecycle.shutdown(end), self.scope.shutdown(end));
        (protocol.unwrap(), tasks)
    }
}
