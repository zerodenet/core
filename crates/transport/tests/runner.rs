use std::{
    io,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;
use zero_core::{
    component::{
        ComponentId, ComponentInstanceId, ComponentRoleIds, NetworkGeneration, OutboundId,
        ResourceGeneration,
    },
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::contract::{
    admission::SessionOwner,
    component::{BuildContext, ComponentFactory, ResourceAccess, RoleKind},
    context::{OpenContext, RequestIdentity},
    datagram::{BoxDatagramSession, DatagramRequest},
    lifecycle::{BuildError, CloseReport, ResourceLifecycle, Unfinished},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    packet::BoxPacketSession,
    scope::TaskScope,
    stream::{BoxStream, ConnectRequest, Connected, PeerConfirmation},
    BoxFuture, CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};
use zero_traits::DialPolicy;
use zero_transport::{
    pooled_driver::PooledStream,
    runner::{
        GuardedCall, HandshakeError, LifecycleChain, PooledBoundLeaf, PooledRoleFactory,
        PooledRoleOptions, PooledRoleProtocol, StatelessOutboundFactory, StatelessOutboundOptions,
        StreamProtocolHandshake,
    },
    RuntimeError, TcpRelayStream,
};

struct NoResources;
impl ResourceAccess for NoResources {
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        panic!("undeclared file")
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, BuildError>> {
        panic!("undeclared TCP listener")
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, BuildError>,
    > {
        panic!("undeclared datagram listener")
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        panic!("undeclared device")
    }
}

struct DuplexOpener {
    opens: Arc<AtomicUsize>,
    stream: Mutex<Option<BoxStream>>,
}

impl UpstreamOpener for DuplexOpener {
    fn open_stream(
        &self,
        _binding: &UpstreamBinding,
        _destination: Destination,
        _context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        self.opens.fetch_add(1, Ordering::AcqRel);
        let stream = self.stream.lock().unwrap().take();
        Box::pin(async move {
            stream.ok_or_else(|| ChannelError::new(Stage::Dial, Reason::Io, Impact::Session))
        })
    }

    fn open_datagram(
        &self,
        _binding: &UpstreamBinding,
        _request: DatagramRequest,
        _context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        panic!("unused in stream test")
    }
}

fn service_open_context(
    opener: Arc<dyn UpstreamOpener>,
    dial: DialPolicy,
    deadline: Instant,
) -> OpenContext {
    OpenContext {
        identity: RequestIdentity::Service(
            Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::DiagnosticProbe,
            })
            .into(),
        ),
        deadline: Some(deadline),
        cancel: CancellationToken::new(),
        observer: None,
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![],
                dial,
                network_generation: NetworkGeneration(1),
            },
            opener,
        ),
    }
}

struct RecordingLifecycle {
    name: &'static str,
    log: Arc<Mutex<Vec<String>>>,
    shutdown_calls: Arc<AtomicUsize>,
    fail_once: AtomicBool,
    incomplete_once: AtomicBool,
}

impl RecordingLifecycle {
    fn new(name: &'static str, log: Arc<Mutex<Vec<String>>>) -> (Self, Arc<AtomicUsize>) {
        let shutdown_calls = Arc::new(AtomicUsize::new(0));
        (
            Self {
                name,
                log,
                shutdown_calls: shutdown_calls.clone(),
                fail_once: AtomicBool::new(false),
                incomplete_once: AtomicBool::new(false),
            },
            shutdown_calls,
        )
    }

    fn fail_first_shutdown(self) -> Self {
        self.fail_once.store(true, Ordering::Release);
        self
    }

    fn incomplete_first_shutdown(self) -> Self {
        self.incomplete_once.store(true, Ordering::Release);
        self
    }
}

impl ResourceLifecycle for RecordingLifecycle {
    async fn start(&mut self, _deadline: Instant) -> Result<(), BuildError> {
        self.log
            .lock()
            .unwrap()
            .push(format!("start:{}", self.name));
        Ok(())
    }

    fn revoke(&self, _reason: CloseReason) {
        self.log
            .lock()
            .unwrap()
            .push(format!("revoke:{}", self.name));
    }

    async fn shutdown(&mut self, _deadline: Instant) -> Result<CloseReport, BuildError> {
        self.shutdown_calls.fetch_add(1, Ordering::AcqRel);
        self.log
            .lock()
            .unwrap()
            .push(format!("shutdown:{}", self.name));
        if self.fail_once.swap(false, Ordering::AcqRel) {
            return Err(ChannelError::new(
                Stage::Close,
                Reason::Io,
                Impact::Component,
            ));
        }
        if self.incomplete_once.swap(false, Ordering::AcqRel) {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(format!("pending:{}", self.name))],
                ..Default::default()
            });
        }
        Ok(CloseReport {
            released_resources: vec![self.name.to_owned()],
            ..Default::default()
        })
    }
}

#[tokio::test]
async fn lifecycle_chain_orders_start_revoke_shutdown_and_retains_receipts_across_retries() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (outer, outer_calls) = RecordingLifecycle::new("outer", log.clone());
    let outer = outer.fail_first_shutdown();
    let (inner, inner_calls) = RecordingLifecycle::new("inner", log.clone());

    let mut chain = LifecycleChain::new()
        .push_typed(outer)
        .push_typed(inner)
        .into_boxed();
    let deadline = Instant::now() + Duration::from_secs(5);
    chain.start(deadline).await.unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        vec!["start:outer".to_owned(), "start:inner".to_owned()]
    );

    let err = chain.shutdown(deadline).await.unwrap_err();
    assert_eq!(err.stage, Stage::Close);
    assert_eq!(err.reason, Reason::Io);
    assert_eq!(inner_calls.load(Ordering::Acquire), 1);
    assert_eq!(outer_calls.load(Ordering::Acquire), 1);

    let report = chain.shutdown(deadline).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(inner_calls.load(Ordering::Acquire), 1);
    assert_eq!(outer_calls.load(Ordering::Acquire), 2);
    assert!(report.released_resources.contains(&"inner".to_owned()));
    assert!(report.released_resources.contains(&"outer".to_owned()));
}

#[tokio::test]
async fn lifecycle_chain_skips_completed_entry_when_sibling_is_incomplete() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (outer, outer_calls) = RecordingLifecycle::new("outer", log.clone());
    let outer = outer.incomplete_first_shutdown();
    let (inner, inner_calls) = RecordingLifecycle::new("inner", log.clone());

    let mut chain = LifecycleChain::new()
        .push_typed(outer)
        .push_typed(inner)
        .into_boxed();
    let deadline = Instant::now() + Duration::from_secs(5);
    chain.start(deadline).await.unwrap();

    let first = chain.shutdown(deadline).await.unwrap();
    assert!(!first.is_complete());
    assert_eq!(first.released_resources, vec!["inner".to_owned()]);
    assert_eq!(inner_calls.load(Ordering::Acquire), 1);
    assert_eq!(outer_calls.load(Ordering::Acquire), 1);

    let second = chain.shutdown(deadline).await.unwrap();
    assert!(second.is_complete());
    assert_eq!(second.released_resources, vec!["outer".to_owned()]);
    assert_eq!(inner_calls.load(Ordering::Acquire), 1);
    assert_eq!(outer_calls.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn guarded_call_enforces_admission_dial_and_cancellation() {
    let cancel = CancellationToken::new();
    let (mut owner, admission) = SessionOwner::new(ComponentId(7), &cancel);
    let opener = Arc::new(DuplexOpener {
        opens: Arc::new(AtomicUsize::new(0)),
        stream: Mutex::new(None),
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    let ctx = service_open_context(opener, DialPolicy::default(), deadline);

    let err = GuardedCall::enter(&admission, &ctx, Some(&DialPolicy::default()))
        .err()
        .expect("unstarted admission must reject");
    assert_eq!(err.reason, Reason::Revoked);
    assert_eq!(err.delivery, DeliveryState::NotSubmitted);

    owner.start(deadline).await.unwrap();

    let different_dial = DialPolicy {
        interface: Some("eth0".into()),
        ..Default::default()
    };
    let err = GuardedCall::enter(&admission, &ctx, Some(&different_dial))
        .err()
        .expect("dial mismatch must reject");
    assert_eq!(err.stage, Stage::Prepare);
    assert_eq!(err.reason, Reason::StalePlan);
    assert_eq!(err.delivery, DeliveryState::NotSubmitted);

    let call = GuardedCall::enter(&admission, &ctx, Some(&DialPolicy::default())).unwrap();
    ctx.cancel.cancel();
    let err = call
        .run(
            Stage::Handshake,
            DeliveryState::Unknown,
            std::future::pending::<()>(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.stage, Stage::Handshake);
    assert_eq!(err.reason, Reason::Cancelled);
    assert_eq!(err.delivery, DeliveryState::Unknown);

    owner.revoke(CloseReason::Shutdown);
    let err = call
        .run(
            Stage::Handshake,
            DeliveryState::Unknown,
            std::future::pending::<()>(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.reason, Reason::Revoked);
}

struct DummyOptions {
    endpoint: Destination,
    dial: DialPolicy,
    identity: Arc<[u8]>,
}

impl StatelessOutboundOptions for DummyOptions {
    fn endpoint(&self) -> &Destination {
        &self.endpoint
    }

    fn dial(&self) -> &DialPolicy {
        &self.dial
    }

    fn static_identity(&self) -> Arc<[u8]> {
        self.identity.clone()
    }
}

#[derive(Clone, Copy, Default)]
struct PrefixHandshake;

impl StreamProtocolHandshake for PrefixHandshake {
    type Options = DummyOptions;
    type Prepared = Vec<u8>;

    fn prepare(
        &self,
        _options: &DummyOptions,
        request: &ConnectRequest,
    ) -> Result<Vec<u8>, HandshakeError> {
        if request.destination.port == 0 {
            return Err(HandshakeError::invalid_config(io::Error::new(
                io::ErrorKind::InvalidInput,
                "port zero",
            )));
        }
        Ok(format!("HELLO:{}\n", request.destination.port).into_bytes())
    }

    async fn handshake(
        &self,
        _options: &DummyOptions,
        mut io: TcpRelayStream,
        bytes: Vec<u8>,
        call: &GuardedCall<'_>,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        call.run_io(Stage::Handshake, DeliveryState::Unknown, async {
            io.write_all(&bytes).await?;
            io.flush().await
        })
        .await?;
        Ok(Connected {
            io: Box::pin(io),
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}

#[tokio::test]
async fn stateless_outbound_factory_runs_prepare_open_handshake_and_shutdown() {
    let factory = StatelessOutboundFactory::<PrefixHandshake>::stream_only();
    let options = Arc::new(DummyOptions {
        endpoint: Destination::new(Address::Ipv4([127, 0, 0, 1]), 8080),
        dial: DialPolicy::default(),
        identity: Arc::from(&b"dummy-stateless"[..]),
    });

    let spec = factory.describe(&options);
    assert_eq!(spec.roles, vec![RoleKind::Stream]);
    assert!(spec.task_limit.is_none());

    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let context = BuildContext {
        component: ComponentId(9),
        generation: ResourceGeneration(1),
        dependencies: Arc::from([]),
        roles: ComponentRoleIds {
            outbound: Some(OutboundId(11)),
            ..Default::default()
        },
        resources: Arc::new(NoResources),
        tasks: scope.registrar(),
        cancel: CancellationToken::new(),
        background_carrier: None,
    };
    let mut instance = factory.build(options, context).unwrap();
    let stream_role = instance.roles.stream.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    instance.lifecycle.start(deadline).await.unwrap();

    let opens = Arc::new(AtomicUsize::new(0));
    let (client_io, mut server_io) = tokio::io::duplex(256);
    let opener: Arc<dyn UpstreamOpener> = Arc::new(DuplexOpener {
        opens: opens.clone(),
        stream: Mutex::new(Some(Box::pin(client_io) as BoxStream)),
    });

    let bad_ctx = service_open_context(opener.clone(), DialPolicy::default(), deadline);
    let err = stream_role
        .connect(
            ConnectRequest {
                destination: Destination::new(Address::Ipv4([1, 1, 1, 1]), 0),
            },
            bad_ctx,
        )
        .await
        .err()
        .expect("port 0 must fail in prepare");
    assert_eq!(err.stage, Stage::Prepare);
    assert_eq!(err.reason, Reason::InvalidConfig);
    assert_eq!(opens.load(Ordering::Acquire), 0);

    let good_ctx = service_open_context(opener.clone(), DialPolicy::default(), deadline);
    let mut connected = stream_role
        .connect(
            ConnectRequest {
                destination: Destination::new(Address::Ipv4([1, 1, 1, 1]), 443),
            },
            good_ctx,
        )
        .await
        .unwrap();
    assert_eq!(opens.load(Ordering::Acquire), 1);

    let mut header = [0u8; 10];
    server_io.read_exact(&mut header).await.unwrap();
    assert_eq!(&header, b"HELLO:443\n");
    server_io.write_all(b"OK").await.unwrap();
    let mut reply = [0u8; 2];
    connected.io.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"OK");

    let report = instance.lifecycle.shutdown(deadline).await.unwrap();
    assert!(report.is_complete());
    let after_ctx = service_open_context(opener, DialPolicy::default(), deadline);
    let err = stream_role
        .connect(
            ConnectRequest {
                destination: Destination::new(Address::Ipv4([1, 1, 1, 1]), 443),
            },
            after_ctx,
        )
        .await
        .err()
        .expect("shutdown must revoke admissions");
    assert_eq!(err.reason, Reason::Revoked);
}

struct DummyPooledOptions {
    pool_owner: ComponentId,
    dial: DialPolicy,
    identity: Arc<[u8]>,
    canary: Arc<()>,
}

impl PooledRoleOptions for DummyPooledOptions {
    fn pool_owner(&self) -> ComponentId {
        self.pool_owner
    }

    fn dial(&self) -> &DialPolicy {
        &self.dial
    }

    fn static_identity(&self) -> Arc<[u8]> {
        self.identity.clone()
    }
}

#[derive(Clone)]
struct DummyBoundLeaf {
    pool: ComponentInstanceId,
    _canary: Arc<()>,
}

impl PooledBoundLeaf for DummyBoundLeaf {
    fn pool_instance(&self) -> Option<ComponentInstanceId> {
        Some(self.pool)
    }

    async fn open_stream(
        &self,
        _destination: Destination,
        _context: &OpenContext,
    ) -> Result<PooledStream, RuntimeError> {
        Err(RuntimeError::Io(io::Error::other("mock pool unavailable")))
    }
}

#[derive(Clone, Copy, Default)]
struct DummyPooledProtocol;

impl PooledRoleProtocol for DummyPooledProtocol {
    type Options = DummyPooledOptions;
    type BoundLeaf = DummyBoundLeaf;

    fn bind_leaf(
        &self,
        options: &DummyPooledOptions,
        pool: ComponentInstanceId,
    ) -> Result<DummyBoundLeaf, RuntimeError> {
        Ok(DummyBoundLeaf {
            pool,
            _canary: options.canary.clone(),
        })
    }
}

#[tokio::test]
async fn pooled_role_factory_validates_dependencies_and_drops_weak_resources_on_owner_drop() {
    let factory = PooledRoleFactory::<DummyPooledProtocol>::stream_only();
    let canary = Arc::new(());
    let options = Arc::new(DummyPooledOptions {
        pool_owner: ComponentId(42),
        dial: DialPolicy::default(),
        identity: Arc::from(&b"dummy-pooled-role"[..]),
        canary: canary.clone(),
    });
    assert_eq!(
        factory.describe(&options).dependencies,
        vec![ComponentId(42)]
    );

    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );

    // Missing pool dependency must be rejected with StalePlan before start.
    let bad_build = BuildContext {
        component: ComponentId(10),
        generation: ResourceGeneration(1),
        dependencies: Arc::from([]),
        roles: ComponentRoleIds::default(),
        resources: Arc::new(NoResources),
        tasks: scope.registrar(),
        cancel: CancellationToken::new(),
        background_carrier: None,
    };
    let err = factory
        .build(options.clone(), bad_build)
        .err()
        .expect("missing dependency must fail build");
    assert_eq!(err.reason, Reason::StalePlan);

    let good_build = BuildContext {
        component: ComponentId(10),
        generation: ResourceGeneration(1),
        dependencies: Arc::from([ComponentInstanceId {
            component: ComponentId(42),
            generation: ResourceGeneration(3),
        }]),
        roles: ComponentRoleIds::default(),
        resources: Arc::new(NoResources),
        tasks: scope.registrar(),
        cancel: CancellationToken::new(),
        background_carrier: None,
    };
    let mut instance = factory.build(options, good_build).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    instance.lifecycle.start(deadline).await.unwrap();
    assert_eq!(
        Arc::strong_count(&canary),
        3,
        "test + RoleOwner options + bound leaf"
    );

    // Cached stream role handle retains only Weak references: revoking and dropping
    // lifecycle releases both options and bound leaf even while stream_role lives!
    let stream_role = instance.roles.stream.take().unwrap();
    instance.lifecycle.revoke(CloseReason::Shutdown);
    assert_eq!(
        Arc::strong_count(&canary),
        2,
        "revoke immediately drops the bound leaf"
    );
    drop(instance.lifecycle);
    assert_eq!(
        Arc::strong_count(&canary),
        1,
        "cached role handle must never pin retired options or protocol resources"
    );
    drop(stream_role);
}
