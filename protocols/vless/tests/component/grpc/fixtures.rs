use super::*;
pub(super) const ID: &str = "11111111-2222-3333-4444-555555555555";
pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(4)
}
pub(super) fn input<'a>(
    port: u16,
    grpc: &'a OwnedGrpcProfile,
) -> crate::transport::VlessOutboundBuildOptionsRef<
    'a,
    OwnedClientTlsProfile,
    OwnedWebSocketProfile,
    OwnedGrpcProfile,
    OwnedH2Profile,
    OwnedHttpUpgradeProfile,
    OwnedSplitHttpProfile,
> {
    crate::transport::VlessOutboundBuildOptionsRef {
        tag: "native-grpc",
        server: "127.0.0.1",
        port,
        protocol: crate::transport::VlessOutboundOptionsRef {
            encryption: None,
            id: ID,
            flow: None,
            testpre: 0,
            testseed: &[],
            mux_concurrency: None,
            xudp_concurrency: None,
            mux_idle_timeout_secs: None,
            mux_response_backlog_frames: None,
            mux_response_backlog_bytes: None,
            reality: None,
            quic: None,
        },
        tls: None,
        ws: None,
        grpc: Some(grpc),
        h2: None,
        http_upgrade: None,
        split_http: None,
        final_mask: None,
        mkcp: None,
        hysteria: None,
        download: None,
    }
}
pub(super) fn prepare(
    runtime: &crate::transport::VlessTransportRuntime,
    port: u16,
    grpc: &OwnedGrpcProfile,
) -> PreparedGrpc {
    PreparedGrpc::from_options_refs(
        input(port, grpc),
        runtime,
        None,
        Default::default(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(3).unwrap(),
    )
    .unwrap()
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
    fn listen_tcp(&self, _: Destination) -> BoxFuture<'_, Result<TcpListener, BuildError>> {
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
#[derive(Default)]
pub(super) struct Observations {
    pub(super) facts: Mutex<Vec<(Option<SessionId>, ConnectFact)>>,
    pub(super) traffic: Mutex<Vec<(Option<SessionId>, u64, u64)>>,
}
impl ConnectObserver for Observations {
    fn record(&self, s: Option<SessionId>, fact: ConnectFact) {
        self.facts.lock().unwrap().push((s, fact));
    }
    fn protocol_traffic(&self, s: Option<SessionId>, rx: u64, tx: u64) {
        self.traffic.lock().unwrap().push((s, rx, tx));
    }
}
pub(super) struct Assembly {
    pub(super) pool: ComponentInstance,
    pub(super) role: ComponentInstance,
    pub(super) scope: TaskScope,
    pub(super) role_scope: TaskScope,
    pub(super) actual: ComponentInstanceId,
    pub(super) upstream: UpstreamTransport,
    pub(super) pool_options: zero_transport::grpc::NativePoolOptions,
    pub(super) observations: Arc<Observations>,
}
impl Assembly {
    pub(super) fn cold(prepared: PreparedGrpc, peer: std::net::SocketAddr) -> Self {
        let actual = ComponentInstanceId {
            component: ComponentId(701),
            generation: ResourceGeneration(1),
        };
        let scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let role_scope = TaskScope::empty(tokio::runtime::Handle::current());
        let egress = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let upstream = UpstreamTransport::new(
            binding.clone(),
            Arc::new(NativeCarrierOpener::new(actual, binding, peer, egress, None).unwrap()),
        );
        let context =
            |component, generation, dependencies, tasks, background_carrier| BuildContext {
                component,
                generation,
                dependencies,
                roles: ComponentRoleIds::default(),
                resources: Arc::new(Denied),
                tasks,
                cancel: CancellationToken::new(),
                background_carrier,
            };
        let pool_options = prepared.pool_options().clone();
        let pool = PoolFactory
            .build(
                Arc::new(pool_options.clone()),
                context(
                    actual.component,
                    actual.generation,
                    Arc::from([]),
                    scope.registrar(),
                    Some(upstream.clone()),
                ),
            )
            .unwrap();
        let role = StreamFactory
            .build(
                Arc::new(prepared.bind(actual.component)),
                context(
                    ComponentId(702),
                    ResourceGeneration(2),
                    Arc::from([actual]),
                    role_scope.registrar(),
                    None,
                ),
            )
            .unwrap();
        Self {
            pool,
            role,
            scope,
            role_scope,
            actual,
            upstream,
            pool_options,
            observations: Arc::new(Observations::default()),
        }
    }
    pub(super) async fn start(&mut self) {
        self.pool.lifecycle.start(deadline()).await.unwrap();
        self.role.lifecycle.start(deadline()).await.unwrap();
    }
    pub(super) fn context(&self) -> OpenContext {
        OpenContext {
            identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::DiagnosticProbe,
            })),
            deadline: Some(deadline()),
            cancel: CancellationToken::new(),
            upstream: self.upstream.clone(),
            observer: Some(self.observations.clone()),
        }
    }
    pub(super) async fn close(&mut self) -> (CloseReport, CloseReport) {
        let end = deadline();
        let role = self.role.lifecycle.shutdown(end).await.unwrap();
        assert!(role.is_complete());
        let (pool, tasks, role_tasks) = tokio::join!(
            self.pool.lifecycle.shutdown(end),
            self.scope.shutdown(end),
            self.role_scope.shutdown(end)
        );
        assert!(role_tasks.is_complete() && role_tasks.exited_tasks.is_empty());
        (pool.unwrap(), tasks)
    }
}
pub(super) fn destination() -> Destination {
    Destination::new(Address::Domain("real.example".into()), 443)
}
pub(super) fn golden(command: u8) -> Vec<u8> {
    let mut wire = vec![
        0, 0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x55, 0x55,
        0x55, 0x55, 0, command, 1, 187, 2, 12,
    ];
    wire.extend_from_slice(b"real.example");
    wire
}
